//go:build e2e

package plugin_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/operator"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

const (
	exporterName   = "otel"
	headersSecret  = "otel-headers" //nolint:gosec // a Secret name, not a credential
	headersKey     = "headers"
	headerName     = "x-e2e-sentinel"
	monitorSecret  = "otel-monitor" //nolint:gosec // a Secret name, not a credential
	monitorRole    = "otel_monitor"
	baseQueries    = "base-queries"
	addedQueries   = "added-queries"
	querySetName   = "extra-queries"
	queriesKey     = "queries"
	partOfLabel    = "app.kubernetes.io/part-of"
	partOfValue    = "plugin-e2e"
	unlistedLabel  = "plugin-e2e/unlisted"
	touchAnnot     = "plugin-e2e/touch"
	extraAttribute = "plugin.e2e.extra"
	extraValue     = "extra-value"
	resourceLabel  = "cnpg.cluster.label."
	exportInterval = 10 * time.Second

	clusterTimeout = 10 * time.Minute
	specPoll       = 3 * time.Second

	// invalid_authorization_specification, sent for peer and pg_hba rejections.
	sqlStateAuthClass = "28"
)

var cnpgGroup = apiv1.SchemeGroupVersion.Group

type mode struct {
	name     string
	password bool
}

// The same specs run in both connection modes.
var _ = func() bool {
	for _, m := range []mode{{name: "peer"}, {name: "password", password: true}} {
		describeMode(m)
	}

	return true
}()

// instance is what must not change on an instance Pod: its identity, its Pod spec and the sidecar's restarts.
type instance struct {
	UID      types.UID
	PodSpec  string
	Restarts int32
	Deleting bool
}

func readTestdata(name string) []byte {
	GinkgoHelper()

	root, err := os.OpenRoot("testdata")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(root.Close)

	data, err := root.ReadFile(name)
	Expect(err).NotTo(HaveOccurred())

	return data
}

func configMap(ns, file string) *corev1.ConfigMap {
	GinkgoHelper()

	var cm corev1.ConfigMap
	Expect(yaml.UnmarshalStrict(readTestdata(file), &cm)).To(Succeed())
	cm.Namespace = ns

	return &cm
}

func querySet(ns, file string) *v1alpha1.MetricQuerySet {
	GinkgoHelper()

	var set v1alpha1.MetricQuerySet
	Expect(yaml.UnmarshalStrict(readTestdata(file), &set)).To(Succeed())
	set.Namespace = ns

	return &set
}

// monitoredConfigMaps returns the sorted names of the ConfigMaps in the Cluster's spec.monitoring.
func monitoredConfigMaps(cluster *apiv1.Cluster) []string {
	var names []string
	for _, ref := range cluster.Spec.Monitoring.CustomQueriesConfigMap {
		names = append(names, ref.Name)
	}
	slices.Sort(names)

	return names
}

// desiredExporter is the valid TelemetryExporter the Cluster ends up using.
func desiredExporter(ns string) *v1alpha1.TelemetryExporter {
	exporter := framework.ExporterFor(ns, exporterName)
	exporter.Spec.Exporter.Headers = &v1alpha1.KeySelector{Name: headersSecret, Key: headersKey}
	exporter.Spec.ResourceLabels = []string{partOfLabel}
	exporter.Spec.ResourceAttributes = map[string]string{extraAttribute: extraValue}

	return exporter
}

// readRule is the rule the Role holds for one kind: get, list and watch on the named objects.
func readRule(group, resource string, names ...string) rbacv1.PolicyRule {
	return rbacv1.PolicyRule{
		APIGroups:     []string{group},
		Resources:     []string{resource},
		ResourceNames: names,
		Verbs:         []string{"get", "list", "watch"},
	}
}

// jsonEscaped is s as it appears inside a JSON string in the sidecar's logs.
func jsonEscaped(s string) string {
	return strings.Trim(strconv.Quote(s), `"`)
}

func sidecarLogs(ctx context.Context, cfg *rest.Config, ns, pod string) (string, error) {
	return framework.ContainerLogs(ctx, cfg, types.NamespacedName{Namespace: ns, Name: pod},
		metadata.SidecarContainerName, &corev1.PodLogOptions{})
}

//nolint:maintidx // one Ordered container per mode; the specs share its state
func describeMode(m mode) {
	Describe(m.name, Ordered, func() {
		var (
			env     framework.Env
			c       client.Client
			cfg     *rest.Config
			reader  *framework.CollectorReader
			ns      string
			cluster *apiv1.Cluster

			baseline  map[string]instance
			sentinels []string

			cname = "plugin-" + m.name

			clusterCreated  time.Time
			exporterCreated time.Time
			statusOKAfter   time.Duration
		)

		key := func(name string) client.ObjectKey { return client.ObjectKey{Namespace: ns, Name: name} }
		sidecarUser := func() string { return framework.ServiceAccountUser(ns, metadata.ServiceAccountName(cname)) }

		getCluster := func(ctx context.Context) *apiv1.Cluster {
			GinkgoHelper()

			var cl apiv1.Cluster
			Expect(c.Get(ctx, key(cname), &cl)).To(Succeed())

			return &cl
		}

		pods := func(ctx context.Context) []corev1.Pod {
			GinkgoHelper()

			list, err := framework.InstancePods(ctx, c, key(cname))
			Expect(err).NotTo(HaveOccurred())
			slices.SortFunc(list, func(a, b corev1.Pod) int { return strings.Compare(a.Name, b.Name) })

			return list
		}

		states := func(ctx context.Context) map[string]instance {
			GinkgoHelper()

			out := map[string]instance{}
			for _, pod := range pods(ctx) {
				in := instance{
					UID: pod.UID, PodSpec: pod.Annotations[framework.PodSpecAnnotation],
					Deleting: pod.DeletionTimestamp != nil,
				}
				for _, s := range pod.Status.InitContainerStatuses {
					if s.Name == metadata.SidecarContainerName {
						in.Restarts = s.RestartCount
					}
				}
				out[pod.Name] = in
			}

			return out
		}

		noRollout := func(ctx context.Context) {
			GinkgoHelper()

			Expect(states(ctx)).To(Equal(baseline), "an instance Pod was replaced, or its sidecar restarted")
		}

		status := func(g Gomega, ctx context.Context) operator.ClusterStatus {
			st, ok, err := framework.PluginStatus(getCluster(ctx))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "the Cluster has no status from the plugin")

			return st
		}

		lines := func(g Gomega, ctx context.Context, since time.Time) []otlp.Line {
			out, err := reader.Since(ctx, since)
			g.Expect(err).NotTo(HaveOccurred())

			return out
		}

		// exports are the exports of this Cluster's pods in lines.
		exports := func(ls []otlp.Line) []otlp.Export {
			var out []otlp.Export
			for _, e := range otlp.Exports(ls) {
				if e.Resource["service.namespace"] == ns && e.Resource["service.name"] == cname {
					out = append(out, e)
				}
			}

			return out
		}

		// has reports whether any series of e belongs to the metric called name.
		has := func(e otlp.Export, name string) bool {
			return slices.ContainsFunc(e.Series, func(s string) bool { return strings.HasPrefix(s, name+"{") })
		}

		// podsExporting returns the pods whose export in ls holds the metric called name.
		podsExporting := func(ls []otlp.Line, name string) []string {
			var out []string
			for _, e := range exports(ls) {
				if has(e, name) {
					out = append(out, e.Resource["k8s.pod.name"])
				}
			}
			slices.Sort(out)

			return slices.Compact(out)
		}

		instanceNames := func(ctx context.Context) []string {
			GinkgoHelper()

			var out []string
			for _, pod := range pods(ctx) {
				out = append(out, pod.Name)
			}

			return out
		}

		// awaitMetric waits until both instances export the metric called name after since.
		awaitMetric := func(ctx SpecContext, name string, since time.Time, timeout time.Duration) {
			GinkgoHelper()

			Eventually(func(g Gomega, ctx SpecContext) {
				g.Expect(podsExporting(lines(g, ctx, since), name)).To(ConsistOf(instanceNames(ctx)), "metric %s", name)
			}).WithContext(ctx).WithTimeout(timeout).WithPolling(specPoll).Should(Succeed())
		}

		role := func(g Gomega, ctx context.Context) []rbacv1.PolicyRule {
			objects, err := framework.Identity(ctx, c, ns, cname)
			g.Expect(err).NotTo(HaveOccurred())

			return objects.Role.Rules
		}

		allowed := func(ctx context.Context, verb, group, resource, name string) bool {
			GinkgoHelper()

			ok, err := framework.CanI(ctx, cfg, sidecarUser(), authorizationv1.ResourceAttributes{
				Namespace: ns, Verb: verb, Group: group, Resource: resource, Name: name,
			})
			Expect(err).NotTo(HaveOccurred())

			return ok
		}

		// touch sets an annotation on the Cluster and returns the resourceVersion of that write.
		touch := func(ctx context.Context, value string) string {
			GinkgoHelper()

			cl := getCluster(ctx)
			patch := client.MergeFrom(cl.DeepCopy())
			if cl.Annotations == nil {
				cl.Annotations = map[string]string{}
			}
			cl.Annotations[touchAnnot] = value
			Expect(c.Patch(ctx, cl, patch)).To(Succeed())

			return cl.ResourceVersion
		}

		// identityGone fails until none of the four identity objects exists.
		identityGone := func(ctx context.Context) error {
			for _, probe := range []struct {
				obj  client.Object
				name string
			}{
				{&corev1.ServiceAccount{}, metadata.ServiceAccountName(cname)},
				{&rbacv1.Role{}, metadata.ServiceAccountName(cname)},
				{&rbacv1.RoleBinding{}, metadata.ServiceAccountName(cname)},
				{&corev1.Secret{}, metadata.TokenSecretName(cname)},
			} {
				err := c.Get(ctx, key(probe.name), probe.obj)
				if err == nil {
					return fmt.Errorf("%T %s is still there", probe.obj, probe.name) //nolint:err113 // a polled condition
				}
				if !apierrors.IsNotFound(err) {
					return fmt.Errorf("get %T %s: %w", probe.obj, probe.name, err)
				}
			}

			return nil
		}

		BeforeAll(func(ctx SpecContext) {
			var err error
			env, err = framework.LoadEnv(os.Getenv)
			Expect(err).NotTo(HaveOccurred())
			c, cfg, err = framework.NewClients(env.KubeContext)
			Expect(err).NotTo(HaveOccurred())
			reader = framework.NewCollectorReader(cfg)

			Eventually(framework.PluginAvailable).WithContext(ctx).WithArguments(c).
				WithTimeout(environmentTimeout).WithPolling(environmentPollInterval).Should(BeTrue())
			Expect(framework.CollectorReady(ctx, c)).To(Succeed())

			namespace, err := framework.CreateNamespace(ctx, c, "plugin-"+m.name)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(c.Delete(ctx, namespace))).To(Succeed())
			})
			ns = namespace.Name

			headerSentinel := "sentinel-" + rand.Text()
			sentinels = []string{headerSentinel}
			Expect(c.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: headersSecret},
				StringData: map[string]string{headersKey: headerName + "=" + headerSentinel},
			})).To(Succeed())
			Expect(c.Create(ctx, configMap(ns, "configmap-base.yaml"))).To(Succeed())

			if m.password {
				password := "sentinel-" + rand.Text()
				sentinels = append(sentinels, password)
				Expect(c.Create(ctx, &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: monitorSecret},
					Type:       corev1.SecretTypeBasicAuth,
					StringData: map[string]string{
						corev1.BasicAuthUsernameKey: monitorRole,
						corev1.BasicAuthPasswordKey: password,
					},
				})).To(Succeed())
				cluster = framework.PasswordCluster(ns, cname, env.PostgresImage, monitorRole, monitorSecret)
			} else {
				cluster = framework.PeerCluster(ns, cname, env.PostgresImage)
			}
			framework.WithExporter(cluster, exporterName)
			cluster.Spec.Instances = 2
			cluster.Labels = map[string]string{partOfLabel: partOfValue, unlistedLabel: "x"}
			cluster.Spec.Monitoring = &apiv1.MonitoringConfiguration{
				CustomQueriesConfigMap: []apiv1.ConfigMapKeySelector{{
					LocalObjectReference: apiv1.LocalObjectReference{Name: baseQueries},
					Key:                  queriesKey,
				}},
			}
			clusterCreated = time.Now()
			Expect(c.Create(ctx, cluster)).To(Succeed())
			// Runs before the namespace's cleanup: a namespace deleted around a live Cluster stays Terminating.
			DeferCleanup(func(ctx SpecContext) {
				Expect(client.IgnoreNotFound(c.Delete(ctx, cluster))).To(Succeed())
				Eventually(func(ctx SpecContext) bool {
					return apierrors.IsNotFound(c.Get(ctx, key(cname), &apiv1.Cluster{}))
				}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(specPoll).Should(BeTrue())
			}, NodeTimeout(4*time.Minute))

			Eventually(func(ctx context.Context) (bool, error) {
				return framework.ClusterReady(ctx, c, key(cname))
			}).WithContext(ctx).WithTimeout(clusterTimeout).WithPolling(5 * time.Second).Should(BeTrue())

			baseline = states(ctx)
			Expect(baseline).To(HaveLen(2))
			for name, in := range baseline {
				Expect(in.PodSpec).NotTo(BeEmpty(), "pod %s has no %s annotation", name, framework.PodSpecAnnotation)
			}
		}, NodeTimeout(clusterTimeout+2*time.Minute))

		// A failed spec reports what the sidecars logged and what CloudNativePG says of the Cluster.
		JustAfterEach(func(ctx SpecContext) {
			if !CurrentSpecReport().Failed() {
				return
			}
			for _, pod := range pods(ctx) {
				logs, err := sidecarLogs(ctx, cfg, ns, pod.Name)
				if err != nil {
					logs = err.Error()
				}
				AddReportEntry("sidecar logs of "+pod.Name, logs)
			}
		}, NodeTimeout(time.Minute))

		It("AC5: a Cluster whose TelemetryExporter doesn't exist yet gets its sidecar, reports exporterState missing, and exports nothing",
			func(ctx SpecContext) {
				for _, pod := range pods(ctx) {
					i := slices.IndexFunc(pod.Status.InitContainerStatuses, func(s corev1.ContainerStatus) bool {
						return s.Name == metadata.SidecarContainerName
					})
					Expect(i).To(BeNumerically(">=", 0), "pod %s has no %s status", pod.Name, metadata.SidecarContainerName)
					Expect(pod.Status.InitContainerStatuses[i].Started).To(HaveValue(BeTrue()), pod.Name)
				}

				Eventually(func(g Gomega, ctx SpecContext) {
					st := status(g, ctx)
					g.Expect(st.Exporter).To(Equal(exporterName))
					g.Expect(st.ExporterState).To(Equal("missing"))
					g.Expect(st.Identity).To(Equal("ready"))
				}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(specPoll).Should(Succeed())

				for _, pod := range pods(ctx) {
					Eventually(func(ctx SpecContext) (string, error) {
						return sidecarLogs(ctx, cfg, ns, pod.Name)
					}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(
						ContainSubstring(jsonEscaped(fmt.Sprintf("telemetryexporter %q does not exist", exporterName))), pod.Name)
				}

				since := clusterCreated
				Consistently(func(g Gomega, ctx SpecContext) {
					g.Expect(exports(lines(g, ctx, since))).To(BeEmpty(), "the sidecars export without a TelemetryExporter")
				}).WithContext(ctx).WithTimeout(3 * exportInterval).WithPolling(specPoll).Should(Succeed())

				noRollout(ctx)
			}, SpecTimeout(5*time.Minute))

		It("AC2: the ServiceAccount, Role, RoleBinding and token exist, owned by the Cluster, and the Role names exactly the referenced objects",
			func(ctx SpecContext) {
				owner := getCluster(ctx)
				objects, err := framework.Identity(ctx, c, ns, cname)
				Expect(err).NotTo(HaveOccurred())

				account := metadata.ServiceAccountName(cname)
				for _, obj := range []client.Object{objects.ServiceAccount, objects.Role, objects.RoleBinding, objects.Token} {
					Expect(obj.GetOwnerReferences()).To(ConsistOf(SatisfyAll(
						HaveField("Kind", apiv1.ClusterKind),
						HaveField("Name", cname),
						HaveField("UID", owner.UID),
						HaveField("Controller", HaveValue(BeTrue())),
					)), "%T %s", obj, obj.GetName())
				}
				Expect(objects.ServiceAccount.Name).To(Equal(account))
				Expect(objects.RoleBinding.RoleRef.Name).To(Equal(account))
				Expect(objects.RoleBinding.Subjects).To(ConsistOf(SatisfyAll(
					HaveField("Kind", rbacv1.ServiceAccountKind),
					HaveField("Name", account),
					HaveField("Namespace", ns),
				)))
				Expect(objects.Token.Type).To(Equal(corev1.SecretTypeServiceAccountToken))
				Expect(objects.Token.Annotations).To(HaveKeyWithValue(corev1.ServiceAccountNameKey, account))
				Expect(objects.Token.Data).To(HaveKey(corev1.ServiceAccountTokenKey))

				// The exporter doesn't exist, so the Role names the Cluster, the exporter and what the Cluster's own
				// spec.monitoring lists, which CloudNativePG completes with its default queries.
				queries := monitoredConfigMaps(owner)
				Expect(queries).To(ContainElement(baseQueries))
				want := []rbacv1.PolicyRule{
					readRule(cnpgGroup, "clusters", cname),
					readRule(metadata.PluginName, "telemetryexporters", exporterName),
					readRule("", "configmaps", queries...),
				}
				if m.password {
					want = append(want, readRule("", "secrets", monitorSecret))
				}
				Expect(objects.Role.Rules).To(ConsistOf(want))

				for _, verb := range []string{"get", "list", "watch"} {
					Expect(allowed(ctx, verb, "", "configmaps", baseQueries)).To(BeTrue(), verb)
					Expect(allowed(ctx, verb, "", "configmaps", "unreferenced")).To(BeFalse(), verb)
				}
			}, SpecTimeout(2*time.Minute))

		It("AC5: an exporter that is invalid from the start leaves the sidecar idle", func(ctx SpecContext) {
			// Without metrics.interval the interval is 30 s, so a 20 s timeout fails Build; the CRD only checks both set.
			exporter := desiredExporter(ns)
			exporter.Spec.Metrics.Interval = nil
			exporter.Spec.Exporter.Timeout = &metav1.Duration{Duration: 20 * time.Second}

			created := time.Now()
			Expect(c.Create(ctx, exporter)).To(Succeed())

			// CloudNativePG stops polling the status while the exporter is missing; each touch is a reconcile that
			// asks again, in case the plugin's cache didn't hold the exporter yet.
			Eventually(func(g Gomega, ctx SpecContext) {
				touch(ctx, fmt.Sprint("created-invalid-", time.Now().UnixNano()))
				st := status(g, ctx)
				g.Expect(st.ExporterState).To(Equal("invalid"))
				g.Expect(st.Message).To(ContainSubstring("exporter.timeout"))
			}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(specPoll).Should(Succeed())

			for _, pod := range pods(ctx) {
				Eventually(func(ctx SpecContext) (string, error) {
					return sidecarLogs(ctx, cfg, ns, pod.Name)
				}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(
					ContainSubstring("invalid configuration, waiting for a valid one"), pod.Name)
			}

			// The collector receives other exporters in the window, so an empty result is not a dead collector.
			Eventually(func(g Gomega, ctx SpecContext) {
				var plugin []map[string]string
				for _, r := range otlp.Resources(lines(g, ctx, created)) {
					if r["service.name"] == pluginServiceName {
						plugin = append(plugin, r)
					}
				}
				g.Expect(plugin).NotTo(BeEmpty(), "the collector received nothing from the plugin since %s", created)
			}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(specPoll).Should(Succeed())
			Consistently(func(g Gomega, ctx SpecContext) {
				g.Expect(exports(lines(g, ctx, created))).To(BeEmpty(), "the sidecars export with an invalid TelemetryExporter")
			}).WithContext(ctx).WithTimeout(4 * exportInterval).WithPolling(specPoll).Should(Succeed())

			noRollout(ctx)
		}, SpecTimeout(8*time.Minute))

		It("AC3: after the TelemetryExporter is valid, metrics from both instances reach the collector with the design 6.6 resource attributes and no rollout",
			func(ctx SpecContext) {
				// The exporter exists since the previous spec, invalid; this makes it valid.
				var current v1alpha1.TelemetryExporter
				Expect(c.Get(ctx, key(exporterName), &current)).To(Succeed())
				patch := client.MergeFrom(current.DeepCopy())
				current.Spec = desiredExporter(ns).Spec

				exporterCreated = time.Now()
				Expect(c.Patch(ctx, &current, patch)).To(Succeed())

				Eventually(func(g Gomega, ctx SpecContext) {
					g.Expect(status(g, ctx).ExporterState).To(Equal("ok"))
				}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(time.Second).Should(Succeed())
				statusOKAfter = time.Since(exporterCreated)

				cl := getCluster(ctx)
				Eventually(func(g Gomega, ctx SpecContext) {
					ls := lines(g, ctx, exporterCreated)
					g.Expect(podsExporting(ls, "cnpg_e2e_base_one")).To(ConsistOf(instanceNames(ctx)))

					// The primary-only query runs on the primary and nowhere else, as seen in one export.
					g.Expect(podsExporting(ls, "cnpg_e2e_primary_one")).To(ConsistOf(cl.Status.CurrentPrimary))
				}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(specPoll).Should(Succeed())

				ls, err := reader.Since(ctx, exporterCreated)
				Expect(err).NotTo(HaveOccurred())
				byPod := map[string]otlp.Export{}
				for _, e := range exports(ls) {
					if has(e, "cnpg_e2e_base_one") {
						byPod[e.Resource["k8s.pod.name"]] = e
					}
				}
				stdout, stderr, err := framework.Exec(ctx, cfg, types.NamespacedName{Namespace: ns, Name: pods(ctx)[0].Name},
					metadata.SidecarContainerName, []string{"/plugin-telemetry", "version"})
				Expect(err).NotTo(HaveOccurred(), stderr)
				versionFields := strings.Fields(stdout)
				Expect(versionFields).To(HaveLen(4), stdout)
				sidecarVersion := versionFields[1]
				Expect(sidecarVersion).NotTo(BeEmpty())

				for _, pod := range pods(ctx) {
					e, ok := byPod[pod.Name]
					Expect(ok).To(BeTrue(), pod.Name)
					Expect(e.Resource).To(SatisfyAll(
						HaveKeyWithValue("service.name", cname),
						HaveKeyWithValue("service.namespace", ns),
						HaveKeyWithValue("service.instance.id", ns+"."+pod.Name+".otel-metrics"),
						HaveKeyWithValue("k8s.namespace.name", ns),
						HaveKeyWithValue("k8s.pod.name", pod.Name),
						HaveKeyWithValue("k8s.pod.uid", string(pod.UID)),
						HaveKeyWithValue("k8s.node.name", pod.Spec.NodeName),
						HaveKeyWithValue("k8s.container.name", metadata.SidecarContainerName),
						HaveKeyWithValue(resourceLabel+partOfLabel, partOfValue),
						HaveKeyWithValue(extraAttribute, extraValue),
					), pod.Name)
					Expect(e.Scopes).To(ContainElement(otlp.Scope{Name: export.ScopeName, Version: sidecarVersion}), pod.Name)
					for k := range e.Resource {
						if strings.HasPrefix(k, resourceLabel) {
							Expect(k).To(Equal(resourceLabel+partOfLabel), "only the allowlisted Cluster labels become attributes")
						}
					}
				}

				noRollout(ctx)
			}, SpecTimeout(8*time.Minute))

		It("AC5: the status turns ok within 30 s of the TelemetryExporter's creation", func(ctx SpecContext) {
			Expect(exporterCreated).NotTo(BeZero(), "the exporter was not created")
			Expect(statusOKAfter).To(BeNumerically("<=", 30*time.Second))

			Eventually(func(g Gomega, ctx SpecContext) {
				st := status(g, ctx)
				g.Expect(st.ExporterState).To(Equal("ok"))
				g.Expect(st.Identity).To(Equal("ready"))
				g.Expect(st.Problems).To(BeEmpty())
				g.Expect(st.Message).To(BeEmpty())
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(Succeed())
		}, SpecTimeout(2*time.Minute))

		It("AC2: adding a ConfigMap to spec.monitoring grants it and its metrics arrive", func(ctx SpecContext) {
			cl := getCluster(ctx)
			patch := client.MergeFrom(cl.DeepCopy())
			cl.Spec.Monitoring.CustomQueriesConfigMap = append(cl.Spec.Monitoring.CustomQueriesConfigMap, apiv1.ConfigMapKeySelector{
				LocalObjectReference: apiv1.LocalObjectReference{Name: addedQueries},
				Key:                  queriesKey,
			})
			Expect(c.Patch(ctx, cl, patch)).To(Succeed())

			Eventually(func(g Gomega, ctx SpecContext) {
				names := monitoredConfigMaps(getCluster(ctx))
				g.Expect(names).To(ContainElements(addedQueries, baseQueries))
				g.Expect(role(g, ctx)).To(ContainElement(readRule("", "configmaps", names...)))
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(time.Second).Should(Succeed())
			for _, verb := range []string{"get", "list", "watch"} {
				Expect(allowed(ctx, verb, "", "configmaps", addedQueries)).To(BeTrue(), verb)
			}

			// The ConfigMap is granted before it exists; creating it later is picked up.
			created := time.Now()
			Expect(c.Create(ctx, configMap(ns, "configmap-added.yaml"))).To(Succeed())
			awaitMetric(ctx, "cnpg_e2e_added_three", created, 2*time.Minute)

			noRollout(ctx)
		}, SpecTimeout(5*time.Minute))

		It("AC2: adding a query set to the TelemetryExporter grants it without a Cluster edit, and its metrics arrive", func(ctx SpecContext) {
			Expect(c.Create(ctx, querySet(ns, "metricqueryset.yaml"))).To(Succeed())
			generation := getCluster(ctx).Generation

			var exporter v1alpha1.TelemetryExporter
			Expect(c.Get(ctx, key(exporterName), &exporter)).To(Succeed())
			patch := client.MergeFrom(exporter.DeepCopy())
			exporter.Spec.Metrics.QuerySets = []string{querySetName}
			created := time.Now()
			Expect(c.Patch(ctx, &exporter, patch)).To(Succeed())

			Eventually(func(g Gomega, ctx SpecContext) {
				g.Expect(role(g, ctx)).To(ContainElement(readRule(metadata.PluginName, "metricquerysets", querySetName)))
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(time.Second).Should(Succeed())
			Expect(getCluster(ctx).Generation).To(Equal(generation), "the Cluster was edited")

			awaitMetric(ctx, "cnpg_e2e_set_five", created, 2*time.Minute)

			// The sidecar reads all five kinds through its Role: get, list and the streaming watch.
			for _, verb := range []string{"get", "list", "watch"} {
				for _, target := range []struct{ group, resource, name string }{
					{cnpgGroup, "clusters", cname},
					{metadata.PluginName, "telemetryexporters", exporterName},
					{metadata.PluginName, "metricquerysets", querySetName},
					{"", "configmaps", baseQueries},
					{"", "secrets", headersSecret},
				} {
					Expect(allowed(ctx, verb, target.group, target.resource, target.name)).To(BeTrue(),
						"%s %s/%s", verb, target.resource, target.name)
				}
			}

			noRollout(ctx)
		}, SpecTimeout(5*time.Minute))

		It("AC4: editing the ConfigMap exports the new metric within 60 s, with no restart and no rollout", func(ctx SpecContext) {
			var cm corev1.ConfigMap
			Expect(c.Get(ctx, key(baseQueries), &cm)).To(Succeed())
			patch := client.MergeFrom(cm.DeepCopy())
			cm.Data = configMap(ns, "configmap-base-edited.yaml").Data

			edited := time.Now()
			Expect(c.Patch(ctx, &cm, patch)).To(Succeed())
			awaitMetric(ctx, "cnpg_e2e_edited_two", edited, time.Minute)

			noRollout(ctx)
		}, SpecTimeout(3*time.Minute))

		It("item 2: ten annotation edits and the Cluster's own status updates keep cnpg_otel_config_info's generation and every series",
			func(ctx SpecContext) {
				// latest returns, per pod, the last export since the time, with the config_info point it holds.
				latest := func(g Gomega, ctx SpecContext, since time.Time) map[string]otlp.Export {
					out := map[string]otlp.Export{}
					for _, e := range exports(lines(g, ctx, since)) {
						out[e.Resource["k8s.pod.name"]] = e
					}
					g.Expect(out).To(HaveLen(2))

					return out
				}
				info := func(e otlp.Export) []string {
					var out []string
					for _, s := range e.Series {
						if strings.HasPrefix(s, "cnpg_otel_config_info{") {
							out = append(out, s)
						}
					}

					return out
				}

				var before map[string]otlp.Export
				Eventually(func(g Gomega, ctx SpecContext) {
					before = latest(g, ctx, time.Now().Add(-2*exportInterval))
					for pod, e := range before {
						g.Expect(info(e)).To(HaveLen(1), pod)
					}
				}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(specPoll).Should(Succeed())

				edits := time.Now()
				var lastWrite string
				for i := range 10 {
					lastWrite = touch(ctx, fmt.Sprint(i))
					time.Sleep(500 * time.Millisecond)
				}

				// The status is a subresource with its own field manager: CloudNativePG wrote it after the edits began.
				Eventually(func(g Gomega, ctx SpecContext) {
					cl := getCluster(ctx)
					g.Expect(cl.ManagedFields).To(ContainElement(SatisfyAll(
						HaveField("Subresource", "status"),
						HaveField("Time", HaveValue(HaveField("Time", BeTemporally(">=", edits.Truncate(time.Second))))),
					)), "no status write since the edits (last edit wrote %s)", lastWrite)
				}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(specPoll).Should(Succeed())

				// Wait for two exports of each pod after the edits, so a reload would have shown.
				var after map[string]otlp.Export
				Eventually(func(g Gomega, ctx SpecContext) {
					after = latest(g, ctx, edits.Add(2*exportInterval))
				}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(specPoll).Should(Succeed())

				for pod, e := range before {
					Expect(info(after[pod])).To(Equal(info(e)), "the configuration generation of %s changed", pod)
					Expect(after[pod].Series).To(ContainElements(e.Series), "series of %s disappeared", pod)
				}

				noRollout(ctx)
			}, SpecTimeout(6*time.Minute))

		It("AC2: removing a ConfigMap and a query set revokes them, stops their metrics and ignores later edits", func(ctx SpecContext) {
			var exporter v1alpha1.TelemetryExporter
			Expect(c.Get(ctx, key(exporterName), &exporter)).To(Succeed())
			exporterPatch := client.MergeFrom(exporter.DeepCopy())
			exporter.Spec.Metrics.QuerySets = nil
			Expect(c.Patch(ctx, &exporter, exporterPatch)).To(Succeed())

			cl := getCluster(ctx)
			clusterPatch := client.MergeFrom(cl.DeepCopy())
			cl.Spec.Monitoring.CustomQueriesConfigMap = slices.DeleteFunc(cl.Spec.Monitoring.CustomQueriesConfigMap,
				func(s apiv1.ConfigMapKeySelector) bool { return s.Name == addedQueries })
			Expect(c.Patch(ctx, cl, clusterPatch)).To(Succeed())

			Eventually(func(g Gomega, ctx SpecContext) {
				rules := role(g, ctx)
				names := monitoredConfigMaps(getCluster(ctx))
				g.Expect(names).NotTo(ContainElement(addedQueries))
				g.Expect(rules).To(ContainElement(readRule("", "configmaps", names...)))
				g.Expect(rules).NotTo(ContainElement(HaveField("Resources", ContainElement("metricquerysets"))))
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(time.Second).Should(Succeed())
			Expect(allowed(ctx, "get", "", "configmaps", addedQueries)).To(BeFalse())
			Expect(allowed(ctx, "watch", metadata.PluginName, "metricquerysets", querySetName)).To(BeFalse())

			removed := []string{"cnpg_e2e_added_three", "cnpg_e2e_set_five"}
			Eventually(func(g Gomega, ctx SpecContext) {
				window := exports(lines(g, ctx, time.Now().Add(-exportInterval-3*time.Second)))
				g.Expect(window).NotTo(BeEmpty())
				for _, e := range window {
					for _, name := range removed {
						g.Expect(has(e, name)).To(BeFalse(), "%s still exported by %s", name, e.Resource["k8s.pod.name"])
					}
				}
			}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(specPoll).Should(Succeed())

			var cm corev1.ConfigMap
			Expect(c.Get(ctx, key(addedQueries), &cm)).To(Succeed())
			cmPatch := client.MergeFrom(cm.DeepCopy())
			cm.Data = configMap(ns, "configmap-added-edited.yaml").Data
			edited := time.Now()
			Expect(c.Patch(ctx, &cm, cmPatch)).To(Succeed())

			Consistently(func(g Gomega, ctx SpecContext) {
				for _, e := range exports(lines(g, ctx, edited)) {
					g.Expect(has(e, "cnpg_e2e_added_later_four")).To(BeFalse())
					g.Expect(has(e, "cnpg_e2e_added_three")).To(BeFalse())
				}
			}).WithContext(ctx).WithTimeout(4 * exportInterval).WithPolling(specPoll).Should(Succeed())

			Expect(podsExporting(lines(Default, ctx, edited), "cnpg_e2e_base_one")).To(ConsistOf(instanceNames(ctx)))
			noRollout(ctx)
		}, SpecTimeout(6*time.Minute))

		It("security: the sidecar can't log in as postgres and its token can't read the Cluster's Secrets", func(ctx SpecContext) {
			for _, pod := range pods(ctx) {
				name := types.NamespacedName{Namespace: ns, Name: pod.Name}
				if m.password {
					// The role's own password is sent, so it is the user that is refused.
					rep, err := framework.Preflight(ctx, cfg, name,
						"--pg-host", "127.0.0.1", "--pg-user", "postgres", "--password-secret", monitorSecret)
					Expect(err).NotTo(HaveOccurred())
					Expect(rep.Postgres).NotTo(BeNil())
					Expect(rep.Postgres.Connected).To(BeFalse(), pod.Name)
					Expect(rep.Postgres.SQLState).To(HavePrefix(sqlStateAuthClass), rep.Postgres.Error)

					continue
				}
				rep, err := framework.Preflight(ctx, cfg, name, "--pg-user", "postgres")
				Expect(err).NotTo(HaveOccurred())
				Expect(rep.Postgres).NotTo(BeNil())
				Expect(rep.Postgres.Connected).To(BeFalse(), pod.Name)
				Expect(rep.Postgres.SQLState).To(Equal("28000"))
				Expect(rep.Postgres.Error).To(ContainSubstring(`Peer authentication failed for user "postgres"`))
			}

			for _, secret := range []string{
				cname + apiv1.SuperUserSecretSuffix,
				cname + apiv1.ApplicationUserSecretSuffix,
				cname + apiv1.DefaultServerCaSecretSuffix,
				cname + apiv1.ReplicationSecretSuffix,
			} {
				Expect(allowed(ctx, "get", "", "secrets", secret)).To(BeFalse(), secret)
			}
			Expect(allowed(ctx, "list", "", "secrets", "")).To(BeFalse())
		}, SpecTimeout(3*time.Minute))

		It("security: no seeded Secret value appears in the sidecar's logs or in print-config", func(ctx SpecContext) {
			for _, pod := range pods(ctx) {
				logs, err := sidecarLogs(ctx, cfg, ns, pod.Name)
				Expect(err).NotTo(HaveOccurred())
				Expect(logs).NotTo(BeEmpty())

				stdout, stderr, err := framework.Exec(ctx, cfg, types.NamespacedName{Namespace: ns, Name: pod.Name},
					metadata.SidecarContainerName, []string{"/plugin-telemetry", "instance", "print-config"})
				Expect(err).NotTo(HaveOccurred(), stderr)
				Expect(stdout).To(ContainSubstring("headersFile: secret/"+headersSecret+"/"+headersKey), "print-config shows no configuration")
				Expect(stdout).To(ContainSubstring(headerName + ": <redacted>"))

				for _, sentinel := range sentinels {
					Expect(logs).NotTo(ContainSubstring(sentinel), "logs of %s", pod.Name)
					Expect(stdout).NotTo(ContainSubstring(sentinel), "print-config of %s", pod.Name)
					Expect(stderr).NotTo(ContainSubstring(sentinel), "print-config stderr of %s", pod.Name)
				}
			}
		}, SpecTimeout(3*time.Minute))

		It("AC5: an invalid TelemetryExporter is reported, and the sidecars keep the configuration in force", func(ctx SpecContext) {
			// Without metrics.interval the interval is 30 s, so a 20 s timeout fails Build; the CRD only checks both set.
			var exporter v1alpha1.TelemetryExporter
			Expect(c.Get(ctx, key(exporterName), &exporter)).To(Succeed())
			valid := exporter.DeepCopy()
			patch := client.MergeFrom(exporter.DeepCopy())
			exporter.Spec.Metrics.Interval = nil
			exporter.Spec.Exporter.Timeout = &metav1.Duration{Duration: 20 * time.Second}
			invalidAt := time.Now()
			Expect(c.Patch(ctx, &exporter, patch)).To(Succeed())

			touch(ctx, "invalid") // a reconcile makes CloudNativePG ask for the status
			Eventually(func(g Gomega, ctx SpecContext) {
				st := status(g, ctx)
				g.Expect(st.ExporterState).To(Equal("invalid"))
				g.Expect(st.Message).To(ContainSubstring("exporter.timeout"))
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(Succeed())

			for _, pod := range pods(ctx) {
				Eventually(func(ctx SpecContext) (string, error) {
					return sidecarLogs(ctx, cfg, ns, pod.Name)
				}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(
					ContainSubstring("invalid configuration, keeping the one in force"), pod.Name)
			}

			// The configuration was valid before, so it stays in force: the sidecars keep exporting every 10 s.
			Eventually(func(g Gomega, ctx SpecContext) {
				g.Expect(podsExporting(lines(g, ctx, invalidAt.Add(2*exportInterval)), "cnpg_e2e_base_one")).
					To(ConsistOf(instanceNames(ctx)))
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(Succeed())
			noRollout(ctx)

			restored := time.Now()
			var current v1alpha1.TelemetryExporter
			Expect(c.Get(ctx, key(exporterName), &current)).To(Succeed())
			restore := client.MergeFrom(current.DeepCopy())
			current.Spec = valid.Spec
			Expect(c.Patch(ctx, &current, restore)).To(Succeed())

			touch(ctx, "restored")
			Eventually(func(g Gomega, ctx SpecContext) {
				st := status(g, ctx)
				g.Expect(st.ExporterState).To(Equal("ok"))
				g.Expect(st.Message).To(BeEmpty())
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(Succeed())
			awaitMetric(ctx, "cnpg_e2e_base_one", restored, time.Minute)
			noRollout(ctx)
		}, SpecTimeout(6*time.Minute))

		itParity(func() parityTarget {
			return parityTarget{env: env, c: c, cfg: cfg, reader: reader, ns: ns, cluster: cname, mode: m.name}
		})

		It("AC5: deleting the TelemetryExporter stops exports, and the status says missing after the next reconcile", func(ctx SpecContext) {
			deleted := time.Now()
			Expect(c.Delete(ctx, framework.ExporterFor(ns, exporterName))).To(Succeed())

			// An export in flight or a watch event in transit may still land; then nothing does.
			Eventually(func(g Gomega, ctx SpecContext) {
				g.Expect(exports(lines(g, ctx, time.Now().Add(-2*exportInterval)))).To(BeEmpty())
			}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(specPoll).Should(Succeed())
			quiet := time.Now()
			Consistently(func(g Gomega, ctx SpecContext) {
				g.Expect(exports(lines(g, ctx, quiet))).To(BeEmpty(), "the sidecars export after %s without a TelemetryExporter", deleted)
			}).WithContext(ctx).WithTimeout(4 * exportInterval).WithPolling(specPoll).Should(Succeed())

			// CloudNativePG calls SetStatusInCluster on a reconcile, which an annotation edit triggers.
			touch(ctx, "after-delete")
			Eventually(func(g Gomega, ctx SpecContext) {
				st := status(g, ctx)
				g.Expect(st.Exporter).To(Equal(exporterName))
				g.Expect(st.ExporterState).To(Equal("missing"))
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(Succeed())

			// A Pod that a rollout deletes keeps its UID and annotations until it is gone, so look for a while.
			Consistently(func(ctx SpecContext) map[string]instance {
				return states(ctx)
			}).WithContext(ctx).WithTimeout(30*time.Second).WithPolling(specPoll).Should(Equal(baseline),
				"an instance Pod is being replaced, or its sidecar restarted")
		}, SpecTimeout(8*time.Minute))

		It("AC2: deleting the Cluster deletes the four objects", func(ctx SpecContext) {
			Expect(identityGone(ctx)).NotTo(Succeed(), "the identity is gone before the Cluster is deleted")

			Expect(c.Delete(ctx, getCluster(ctx))).To(Succeed())

			Eventually(identityGone).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(specPoll).Should(Succeed())
		}, SpecTimeout(5*time.Minute))
	})
}
