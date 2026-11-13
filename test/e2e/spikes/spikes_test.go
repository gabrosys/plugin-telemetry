//go:build e2e

package spikes_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/preflight"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
)

const (
	peerCluster      = "spikes-peer"
	passwordCluster  = "spikes-pw"
	queriesConfigMap = "spikes-queries"
	queriesSecret    = "spikes-queries-secret"
	queriesKey       = "queries"
	defaultQueries   = "cnpg-default-monitoring"
	monitorSecret    = "otel-monitor" //nolint:gosec // a Secret name, not a credential
	monitorRole      = "otel_monitor"

	// invalid_authorization_specification, sent for peer and pg_hba rejections.
	sqlStateInvalidAuthorization = "28000"

	pluginNamespace = "cnpg-system"
	pluginLabel     = "app.kubernetes.io/name"
	pluginName      = "plugin-telemetry"
)

const queries = `spikes:
  query: "SELECT 1 AS one"
  metrics:
    - one:
        usage: "GAUGE"
        description: "Always 1"
`

var (
	cnpgGroup = apiv1.SchemeGroupVersion.Group

	sidecarAccess = map[string]framework.SidecarAccess{
		peerCluster: {
			ConfigMaps: []string{defaultQueries, queriesConfigMap},
			Secrets:    []string{queriesSecret},
		},
		passwordCluster: {
			ConfigMaps: []string{defaultQueries, queriesConfigMap},
			Secrets:    []string{queriesSecret, monitorSecret},
		},
	}
)

// instance is what must not change on an instance Pod.
type instance struct {
	UID     types.UID
	PodSpec string
}

// review is one access review and its outcome.
type review struct {
	attrs   authorizationv1.ResourceAttributes
	allowed bool
}

func (r review) String() string {
	gr := r.attrs.Resource
	if r.attrs.Group != "" {
		gr += "." + r.attrs.Group
	}
	if r.attrs.Name != "" {
		gr += "/" + r.attrs.Name
	}

	return fmt.Sprintf("%-6s %-60s %t", r.attrs.Verb, gr, r.allowed)
}

// decided returns the reviews whose outcome equals allowed.
func decided(reviews []review, allowed bool) []string {
	var out []string
	for _, r := range reviews {
		if r.allowed == allowed {
			out = append(out, r.String())
		}
	}

	return out
}

func table(reviews []review) string {
	lines := make([]string, 0, len(reviews))
	for _, r := range reviews {
		lines = append(lines, r.String())
	}

	return strings.Join(lines, "\n")
}

func formatRules(rules []authorizationv1.ResourceRule) string {
	lines := make([]string, 0, len(rules))
	for _, r := range rules {
		lines = append(lines, fmt.Sprintf("verbs=%v apiGroups=%q resources=%v resourceNames=%v",
			r.Verbs, r.APIGroups, r.Resources, r.ResourceNames))
	}

	return strings.Join(lines, "\n")
}

func containerNames(containers []corev1.Container) []string {
	names := make([]string, 0, len(containers))
	for _, ctr := range containers {
		names = append(names, ctr.Name)
	}

	return names
}

func toJSON(v any) string {
	b, err := json.Marshal(v)
	Expect(err).NotTo(HaveOccurred())

	return string(b)
}

func withQueries(cluster *apiv1.Cluster) *apiv1.Cluster {
	cluster.Spec.Monitoring = &apiv1.MonitoringConfiguration{
		CustomQueriesConfigMap: []apiv1.ConfigMapKeySelector{{
			LocalObjectReference: apiv1.LocalObjectReference{Name: queriesConfigMap},
			Key:                  queriesKey,
		}},
		CustomQueriesSecret: []apiv1.SecretKeySelector{{
			LocalObjectReference: apiv1.LocalObjectReference{Name: queriesSecret},
			Key:                  queriesKey,
		}},
	}

	return cluster
}

func sidecarSecurityContext(peer bool) *corev1.SecurityContext {
	sc := &corev1.SecurityContext{
		RunAsNonRoot:             ptr.To(true),
		AllowPrivilegeEscalation: ptr.To(false),
		ReadOnlyRootFilesystem:   ptr.To(true),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
	if peer {
		sc.RunAsUser = ptr.To(metadata.PeerUID)
	}

	return sc
}

func sidecarMounts(peer bool) []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{
		{Name: "telemetry-token", MountPath: metadata.SidecarTokenDir, ReadOnly: true},
		{Name: "telemetry-sa-mask", MountPath: metadata.PodTokenDir, ReadOnly: true},
	}
	if peer {
		mounts = append(mounts, corev1.VolumeMount{
			Name: "scratch-data", MountPath: metadata.SocketDir, SubPath: "run", ReadOnly: true,
		})
	}

	return mounts
}

// readable lists the objects the sidecar of cluster may read.
func readable(namespace, cluster string) []authorizationv1.ResourceAttributes {
	access := sidecarAccess[cluster]
	attrs := []authorizationv1.ResourceAttributes{
		{Namespace: namespace, Group: cnpgGroup, Resource: "clusters", Name: cluster},
	}
	for _, name := range access.ConfigMaps {
		attrs = append(attrs, authorizationv1.ResourceAttributes{Namespace: namespace, Resource: "configmaps", Name: name})
	}
	for _, name := range access.Secrets {
		attrs = append(attrs, authorizationv1.ResourceAttributes{Namespace: namespace, Resource: "secrets", Name: name})
	}

	return attrs
}

// forbidden lists what the sidecar of cluster must not do.
func forbidden(namespace, cluster, other string) []authorizationv1.ResourceAttributes {
	attrs := []authorizationv1.ResourceAttributes{
		{Namespace: namespace, Verb: "get", Resource: "secrets", Name: cluster + apiv1.SuperUserSecretSuffix},
		{Namespace: namespace, Verb: "get", Resource: "secrets", Name: cluster + apiv1.ApplicationUserSecretSuffix},
		{Namespace: namespace, Verb: "list", Resource: "secrets"},
		{Namespace: namespace, Verb: "list", Resource: "configmaps"},
		{Namespace: namespace, Verb: "get", Group: cnpgGroup, Resource: "clusters", Name: other},
	}

	targets := append([]authorizationv1.ResourceAttributes{
		{Namespace: namespace, Group: cnpgGroup, Resource: "clusters"},
		{Namespace: namespace, Resource: "configmaps"},
		{Namespace: namespace, Resource: "secrets"},
		{Namespace: namespace, Resource: "pods"},
	}, readable(namespace, cluster)...)
	for _, verb := range []string{"create", "update", "patch", "delete"} {
		for _, target := range targets {
			target.Verb = verb
			attrs = append(attrs, target)
		}
	}

	return attrs
}

var _ = Describe("spikes", Ordered, ContinueOnFailure, func() {
	var (
		env      framework.Env
		c        client.Client
		cfg      *rest.Config
		ns       string
		image    string
		clusters map[string]*apiv1.Cluster
	)

	key := func(name string) client.ObjectKey { return client.ObjectKey{Namespace: ns, Name: name} }

	sidecarUser := func(cluster string) string {
		return framework.ServiceAccountUser(ns, metadata.ServiceAccountName(cluster))
	}

	primary := func(ctx context.Context, cluster string) types.NamespacedName {
		var cl apiv1.Cluster
		Expect(c.Get(ctx, key(cluster), &cl)).To(Succeed())
		Expect(cl.Status.CurrentPrimary).NotTo(BeEmpty())

		return types.NamespacedName{Namespace: ns, Name: cl.Status.CurrentPrimary}
	}

	instances := func(ctx context.Context) (map[string]instance, error) {
		out := map[string]instance{}
		for name := range clusters {
			pods, err := framework.InstancePods(ctx, c, key(name))
			if err != nil {
				return nil, err
			}
			for _, pod := range pods {
				out[pod.Name] = instance{UID: pod.UID, PodSpec: pod.Annotations[framework.PodSpecAnnotation]}
			}
		}

		return out, nil
	}

	pluginPods := func(ctx context.Context) []string {
		var pods corev1.PodList
		Expect(c.List(ctx, &pods, client.InNamespace(pluginNamespace),
			client.MatchingLabels{pluginLabel: pluginName})).To(Succeed())
		names := make([]string, 0, len(pods.Items))
		for _, pod := range pods.Items {
			if pod.DeletionTimestamp == nil {
				names = append(names, pod.Name)
			}
		}
		slices.Sort(names)

		return names
	}

	// bumpDescriptions changes spec.description, which bumps the generation and triggers a reconcile.
	bumpDescriptions := func(ctx context.Context) {
		for name := range clusters {
			var cluster apiv1.Cluster
			Expect(c.Get(ctx, key(name), &cluster)).To(Succeed())
			generation := cluster.Generation
			patch := client.MergeFrom(cluster.DeepCopy())
			cluster.Spec.Description = "spikes reconcile " + time.Now().UTC().Format(time.RFC3339Nano)
			Expect(c.Patch(ctx, &cluster, patch)).To(Succeed())
			Expect(cluster.Generation).To(BeNumerically(">", generation), name)
		}
	}

	reviewAll := func(ctx context.Context, user string, checks []authorizationv1.ResourceAttributes) []review {
		reviews := make([]review, 0, len(checks))
		for _, attrs := range checks {
			allowed, err := framework.CanI(ctx, cfg, user, attrs)
			Expect(err).NotTo(HaveOccurred())
			reviews = append(reviews, review{attrs: attrs, allowed: allowed})
		}

		return reviews
	}

	BeforeAll(func(ctx SpecContext) {
		var err error
		env, err = framework.LoadEnv(os.Getenv)
		Expect(err).NotTo(HaveOccurred())
		c, cfg, err = framework.NewClients(env.KubeContext)
		Expect(err).NotTo(HaveOccurred())

		Eventually(framework.PluginAvailable).WithContext(ctx).WithArguments(c).
			WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(BeTrue())
		image, err = framework.SidecarImage(ctx, c)
		Expect(err).NotTo(HaveOccurred())
		AddReportEntry("sidecar-image", image)

		namespace, err := framework.CreateNamespace(ctx, c, "spikes")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, namespace))).To(Succeed())
		})
		ns = namespace.Name

		meta := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Namespace: ns, Name: name} }
		Expect(c.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: meta(queriesConfigMap),
			Data:       map[string]string{queriesKey: queries},
		})).To(Succeed())
		Expect(c.Create(ctx, &corev1.Secret{
			ObjectMeta: meta(queriesSecret),
			StringData: map[string]string{queriesKey: queries},
		})).To(Succeed())
		Expect(c.Create(ctx, &corev1.Secret{
			ObjectMeta: meta(monitorSecret),
			Type:       corev1.SecretTypeBasicAuth,
			StringData: map[string]string{
				corev1.BasicAuthUsernameKey: monitorRole,
				corev1.BasicAuthPasswordKey: rand.Text(),
			},
		})).To(Succeed())

		for name, access := range sidecarAccess {
			Expect(framework.CreateSidecarIdentity(ctx, c, ns, name, access)).To(Succeed())
		}

		clusters = map[string]*apiv1.Cluster{
			peerCluster: withQueries(framework.PeerCluster(ns, peerCluster, env.PostgresImage)),
			passwordCluster: withQueries(framework.PasswordCluster(
				ns, passwordCluster, env.PostgresImage, monitorRole, monitorSecret)),
		}
		Eventually(func(ctx context.Context) error {
			for _, cluster := range clusters {
				if err := client.IgnoreAlreadyExists(c.Create(ctx, cluster.DeepCopy())); err != nil {
					return err
				}
			}

			return nil
		}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		Eventually(func(g Gomega, ctx context.Context) {
			for name := range clusters {
				ready, err := framework.ClusterReady(ctx, c, key(name))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(BeTrue(), "cluster %s is not ready", name)
			}
		}).WithContext(ctx).WithTimeout(10 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
	}, NodeTimeout(11*time.Minute))

	It("runs against the requested CNPG version", func(ctx SpecContext) {
		img, err := framework.OperatorImage(ctx, c)
		Expect(err).NotTo(HaveOccurred())
		AddReportEntry("cnpg-operator-image", img)
		Expect(framework.MatchesCNPGVersion(img, env.CNPGVersion)).To(BeTrue(), img)
	}, SpecTimeout(time.Minute))

	Context("AC1: sidecar injection", func() {
		DescribeTable("injects the sidecar into every instance",
			func(ctx SpecContext, name string, peer bool) {
				pods, err := framework.InstancePods(ctx, c, key(name))
				Expect(err).NotTo(HaveOccurred())
				Expect(pods).To(HaveLen(clusters[name].Spec.Instances))

				for _, pod := range pods {
					Expect(pod.Status.Phase).To(Equal(corev1.PodRunning), pod.Name)

					i := slices.IndexFunc(pod.Spec.InitContainers, func(ctr corev1.Container) bool {
						return ctr.Name == metadata.SidecarContainerName
					})
					Expect(i).NotTo(BeNumerically("<", 0), "pod %s has no %s init container", pod.Name, metadata.SidecarContainerName)
					sidecar := pod.Spec.InitContainers[i]

					j := slices.IndexFunc(pod.Status.InitContainerStatuses, func(s corev1.ContainerStatus) bool {
						return s.Name == metadata.SidecarContainerName
					})
					Expect(j).NotTo(BeNumerically("<", 0), "pod %s has no %s status", pod.Name, metadata.SidecarContainerName)
					status := pod.Status.InitContainerStatuses[j]

					AddReportEntry(pod.Name+" "+metadata.SidecarContainerName, toJSON(map[string]any{
						"initContainers":  containerNames(pod.Spec.InitContainers),
						"image":           sidecar.Image,
						"restartPolicy":   sidecar.RestartPolicy,
						"securityContext": sidecar.SecurityContext,
						"volumeMounts":    sidecar.VolumeMounts,
						"status":          status,
					}))

					Expect(sidecar.Image).To(Equal(image))
					Expect(sidecar.RestartPolicy).To(HaveValue(Equal(corev1.ContainerRestartPolicyAlways)))
					Expect(sidecar.SecurityContext).To(Equal(sidecarSecurityContext(peer)))
					Expect(sidecar.VolumeMounts).To(ConsistOf(sidecarMounts(peer)))
					Expect(sidecar.VolumeMounts).NotTo(ContainElement(HaveField("Name", HavePrefix("kube-api-access"))))
					Expect(status.Started).To(HaveValue(BeTrue()))
				}
			},
			Entry("peer mode", peerCluster, true, SpecTimeout(time.Minute)),
			Entry("password mode", passwordCluster, false, SpecTimeout(time.Minute)),
		)

		It("keeps every instance across reconciles and plugin restarts", func(ctx SpecContext) {
			want := 0
			for _, cluster := range clusters {
				want += cluster.Spec.Instances
			}
			before, err := instances(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(before).To(HaveLen(want))
			uids := map[string]types.UID{}
			for pod, in := range before {
				Expect(in.PodSpec).NotTo(BeEmpty(), "pod %s has no %s annotation", pod, framework.PodSpecAnnotation)
				uids[pod] = in.UID
			}
			AddReportEntry("instance pod UIDs before", toJSON(uids))

			bumpDescriptions(ctx)

			oldPlugins := pluginPods(ctx)
			Expect(framework.RestartPlugin(ctx, c)).To(Succeed())
			Eventually(framework.PluginRolledOut).WithContext(ctx).WithArguments(c).
				WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(BeTrue())
			newPlugins := pluginPods(ctx)
			AddReportEntry("plugin pods", toJSON(map[string][]string{"before": oldPlugins, "after": newPlugins}))
			Expect(newPlugins).NotTo(ContainElement(BeElementOf(oldPlugins)))

			// CNPG does not reconcile on its own within the window; this reconcile reaches the new replicas.
			bumpDescriptions(ctx)

			Consistently(func(g Gomega, ctx context.Context) {
				now, err := instances(ctx)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(now).To(Equal(before))
				for name := range clusters {
					var cluster apiv1.Cluster
					g.Expect(c.Get(ctx, key(name), &cluster)).To(Succeed())
					g.Expect(cluster.Status.Phase).To(Equal(apiv1.PhaseHealthy), name)
				}
			}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(10 * time.Second).Should(Succeed())

			after, err := instances(ctx)
			Expect(err).NotTo(HaveOccurred())
			uids = map[string]types.UID{}
			for pod, in := range after {
				uids[pod] = in.UID
			}
			AddReportEntry("instance pod UIDs after", toJSON(uids))
		}, SpecTimeout(7*time.Minute))
	})

	Context("AC2: peer mode", func() {
		It("connects as cnpg_metrics_exporter through the read-only socket mount", func(ctx SpecContext) {
			rep, err := framework.Preflight(ctx, cfg, primary(ctx, peerCluster))
			Expect(err).NotTo(HaveOccurred())
			AddReportEntry("preflight "+peerCluster, toJSON(rep))

			Expect(rep.UID).To(BeEquivalentTo(metadata.PeerUID))
			Expect(rep.Postgres).NotTo(BeNil())
			Expect(rep.Postgres.Host).To(Equal(metadata.SocketDir))
			Expect(rep.Postgres.Connected).To(BeTrue(), rep.Postgres.Error)
			Expect(rep.Postgres.CurrentUser).To(Equal(metadata.MetricsExporterRole))
			Expect(rep.Postgres.WALFiles).To(BeNumerically(">=", 1))
			Expect(rep.Postgres.SQLState).To(BeEmpty())
			Expect(rep.Postgres.Error).To(BeEmpty())
		}, SpecTimeout(time.Minute))

		It("is refused as postgres", func(ctx SpecContext) {
			rep, err := framework.Preflight(ctx, cfg, primary(ctx, peerCluster), "--pg-user", "postgres")
			Expect(err).NotTo(HaveOccurred())
			AddReportEntry("preflight "+peerCluster+" as postgres", toJSON(rep))

			Expect(rep.Postgres).NotTo(BeNil())
			Expect(rep.Postgres.Connected).To(BeFalse())
			Expect(rep.Postgres.SQLState).To(Equal(sqlStateInvalidAuthorization))
			Expect(rep.Postgres.Error).To(ContainSubstring(`Peer authentication failed for user "postgres"`))
		}, SpecTimeout(time.Minute))
	})

	Context("AC3: password mode", func() {
		It("connects as the dedicated role over 127.0.0.1", func(ctx SpecContext) {
			pod := primary(ctx, passwordCluster)
			var rep preflight.Report
			Eventually(func(g Gomega, ctx context.Context) {
				var err error
				rep, err = framework.Preflight(ctx, cfg, pod,
					"--pg-host", "127.0.0.1", "--pg-user", monitorRole, "--password-secret", monitorSecret)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(rep.Postgres).NotTo(BeNil())
				g.Expect(rep.Postgres.Connected).To(BeTrue(), rep.Postgres.Error)
				g.Expect(rep.Postgres.CurrentUser).To(Equal(monitorRole))
			}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
			AddReportEntry("preflight "+passwordCluster, toJSON(rep))
		}, SpecTimeout(3*time.Minute))

		It("refuses the same role from another pod", func(ctx SpecContext) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: passwordCluster + "-client"},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: ptr.To(false),
					Containers: []corev1.Container{{
						Name:            "preflight",
						Image:           image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Args: []string{
							"instance", "preflight", "--kube=false",
							"--pg-host", passwordCluster + apiv1.ServiceReadWriteSuffix, "--pg-user", monitorRole,
						},
						Env: []corev1.EnvVar{{
							Name: "PGPASSWORD",
							ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: monitorSecret},
								Key:                  corev1.BasicAuthPasswordKey,
							}},
						}},
						SecurityContext: &corev1.SecurityContext{
							RunAsNonRoot:             ptr.To(true),
							AllowPrivilegeEscalation: ptr.To(false),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
							SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
						},
					}},
				},
			}

			logs, err := framework.RunPod(ctx, c, cfg, pod)
			Expect(err).NotTo(HaveOccurred(), logs)
			AddReportEntry("preflight from "+pod.Name, strings.TrimSpace(logs))

			rep, err := framework.DecodeReport(logs)
			Expect(err).NotTo(HaveOccurred())
			Expect(rep.Postgres).NotTo(BeNil())
			Expect(rep.Postgres.Connected).To(BeFalse())
			Expect(rep.Postgres.SQLState).To(Equal(sqlStateInvalidAuthorization))
			Expect(rep.Postgres.Error).To(ContainSubstring("pg_hba.conf rejects connection"))
		}, SpecTimeout(3*time.Minute))
	})

	Context("AC4: sidecar identity", func() {
		DescribeTable("lets the sidecar identity read what it needs",
			func(ctx SpecContext, name string) {
				var checks []authorizationv1.ResourceAttributes
				for _, attrs := range readable(ns, name) {
					for _, verb := range []string{"get", "watch"} {
						attrs.Verb = verb
						checks = append(checks, attrs)
					}
				}

				reviews := reviewAll(ctx, sidecarUser(name), checks)
				AddReportEntry(name+" can-i (want true)", table(reviews))
				Expect(decided(reviews, false)).To(BeEmpty())
			},
			Entry(peerCluster, peerCluster, SpecTimeout(time.Minute)),
			Entry(passwordCluster, passwordCluster, SpecTimeout(time.Minute)),
		)

		DescribeTable("denies everything else",
			func(ctx SpecContext, name, other string) {
				reviews := reviewAll(ctx, sidecarUser(name), forbidden(ns, name, other))
				AddReportEntry(name+" can-i (want false)", table(reviews))
				Expect(decided(reviews, true)).To(BeEmpty())

				rules, err := framework.Rules(ctx, cfg, sidecarUser(name), ns)
				Expect(err).NotTo(HaveOccurred())
				AddReportEntry(name+" rules", formatRules(rules))
				Expect(framework.UnexpectedRules(rules)).To(BeEmpty(), formatRules(rules))
			},
			Entry(peerCluster, peerCluster, passwordCluster, SpecTimeout(2*time.Minute)),
			Entry(passwordCluster, passwordCluster, peerCluster, SpecTimeout(2*time.Minute)),
		)

		DescribeTable("keeps the pod token out of the sidecar",
			func(ctx SpecContext, name string) {
				rep, err := framework.Preflight(ctx, cfg, primary(ctx, name))
				Expect(err).NotTo(HaveOccurred())
				AddReportEntry("preflight "+name+" kube", toJSON(rep))

				Expect(rep.PodTokenPresent).To(BeFalse())
				Expect(rep.Kube).NotTo(BeNil())
				Expect(rep.Kube.Username).To(Equal(sidecarUser(name)))
				Expect(rep.Kube.ClusterReadable).To(BeTrue(), rep.Kube.Error)
			},
			Entry(peerCluster, peerCluster, SpecTimeout(time.Minute)),
			Entry(passwordCluster, passwordCluster, SpecTimeout(time.Minute)),
		)
	})
})
