//go:build e2e

package plugin_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"maps"
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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/operator"
	"github.com/gabrosys/plugin-telemetry/internal/preflight"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
	"github.com/gabrosys/plugin-telemetry/test/fixture"
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
	extraAttribute = "plugin.e2e.extra"
	extraValue     = "extra-value"
	resourceLabel  = "cnpg.cluster.label."
	exportInterval = 10 * time.Second

	clusterTimeout = 10 * time.Minute
	badGlob        = "pg_stat_["
	specPoll       = 3 * time.Second

	// invalid_authorization_specification, sent for peer and pg_hba rejections.
	sqlStateAuthClass = "28"
	// sqlStateInvalidAuthorization is the full state of a refused peer or pg_hba login.
	sqlStateInvalidAuthorization = "28000"
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

// touchCluster retries framework.Touch on API errors and returns the write's resourceVersion.
func touchCluster(ctx context.Context, c client.Client, key client.ObjectKey, value string) string {
	GinkgoHelper()

	var rv string
	Eventually(func(g Gomega) {
		var err error
		rv, err = framework.Touch(ctx, c, key, value)
		g.Expect(err).NotTo(HaveOccurred())
	}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

	return rv
}

func configMap(ns, file string) *corev1.ConfigMap {
	GinkgoHelper()

	var cm corev1.ConfigMap
	Expect(yaml.UnmarshalStrict(fixture.Read(GinkgoTB(), "testdata", file), &cm)).To(Succeed())
	cm.Namespace = ns

	return &cm
}

func querySet(ns, file string) *v1alpha1.MetricQuerySet {
	GinkgoHelper()

	var set v1alpha1.MetricQuerySet
	Expect(yaml.UnmarshalStrict(fixture.Read(GinkgoTB(), "testdata", file), &set)).To(Succeed())
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

// reconcileCounts is the identity-reconcile counter per plugin pod and attribute set.
type reconcileCounts map[string]map[string]float64

// equal reports whether both hold the same pods, attribute sets and values; a replaced pod differs.
func (r reconcileCounts) equal(o reconcileCounts) bool {
	return maps.EqualFunc(r, o, maps.Equal[map[string]float64])
}

func (r reconcileCounts) String() string {
	var out []string
	for _, pod := range slices.Sorted(maps.Keys(r)) {
		for _, attrs := range slices.Sorted(maps.Keys(r[pod])) {
			out = append(out, fmt.Sprintf("%s %s = %g", pod, attrs, r[pod][attrs]))
		}
	}
	if len(out) == 0 {
		return "none"
	}

	return strings.Join(out, "\n")
}

// clusterOwned maps "Kind/name" to the resourceVersion of the ServiceAccounts, Roles, RoleBindings and
// Secrets of ns whose controller owner is a CloudNativePG Cluster, the objects the identity controller's Owns() matches.
func clusterOwned(ctx context.Context, c client.Client, ns string) (map[string]string, error) {
	out := map[string]string{}
	for kind, list := range map[string]client.ObjectList{
		"ServiceAccount": &corev1.ServiceAccountList{}, "Role": &rbacv1.RoleList{},
		"RoleBinding": &rbacv1.RoleBindingList{}, "Secret": &corev1.SecretList{},
	} {
		if err := c.List(ctx, list, client.InNamespace(ns)); err != nil {
			return nil, fmt.Errorf("list %s in %s: %w", kind, ns, err)
		}
		err := meta.EachListItem(list, func(obj runtime.Object) error {
			o, ok := obj.(client.Object)
			if !ok {
				return fmt.Errorf("%T is not a client.Object", obj)
			}
			owner := metav1.GetControllerOf(o)
			if owner == nil || owner.Kind != apiv1.ClusterKind {
				return nil
			}
			gv, err := schema.ParseGroupVersion(owner.APIVersion)
			if err != nil {
				return fmt.Errorf("owner apiVersion of %s/%s: %w", kind, o.GetName(), err)
			}
			if gv.Group == cnpgGroup {
				out[kind+"/"+o.GetName()] = o.GetResourceVersion()
			}

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk %s in %s: %w", kind, ns, err)
		}
	}

	return out, nil
}

// changedOwned lists the objects of after whose resourceVersion differs from before, and those that vanished.
func changedOwned(before, after map[string]string) string {
	var out []string
	for _, k := range slices.Sorted(maps.Keys(after)) {
		switch rv, ok := before[k]; {
		case !ok:
			out = append(out, fmt.Sprintf("%s appeared at %s", k, after[k]))
		case rv != after[k]:
			out = append(out, fmt.Sprintf("%s %s -> %s", k, rv, after[k]))
		}
	}
	for _, k := range slices.Sorted(maps.Keys(before)) {
		if _, ok := after[k]; !ok {
			out = append(out, k+" vanished")
		}
	}
	if len(out) == 0 {
		return "no change"
	}

	return strings.Join(out, "\n")
}

// clustersEverywhere lists "namespace/name generation" of every Cluster, marked when it is being deleted.
func clustersEverywhere(ctx context.Context, c client.Client) (string, error) {
	var list apiv1.ClusterList
	if err := c.List(ctx, &list); err != nil {
		return "", fmt.Errorf("list Clusters: %w", err)
	}

	out := make([]string, 0, len(list.Items))
	for i := range list.Items {
		cl := &list.Items[i]
		entry := fmt.Sprintf("%s/%s generation %d", cl.Namespace, cl.Name, cl.Generation)
		if cl.DeletionTimestamp != nil {
			entry += " (deleting)"
		}
		out = append(out, entry)
	}
	slices.Sort(out)

	return strings.Join(out, "\n"), nil
}

//nolint:maintidx // one Ordered container per mode; the specs share its state
func describeMode(m mode) {
	Describe(m.name, Ordered, Serial, func() {
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
			exporterValidAt time.Time
			statusOKAfter   time.Duration
		)

		key := func(name string) client.ObjectKey { return client.ObjectKey{Namespace: ns, Name: name} }
		sidecarUser := func() string { return framework.ServiceAccountUser(ns, metadata.ServiceAccountName(cname)) }

		getCluster := func(g Gomega, ctx context.Context) *apiv1.Cluster {
			GinkgoHelper()

			var cl apiv1.Cluster
			g.Expect(c.Get(ctx, key(cname), &cl)).To(Succeed())

			return &cl
		}

		pods := func(g Gomega, ctx context.Context) []corev1.Pod {
			GinkgoHelper()

			list, err := framework.InstancePods(ctx, c, key(cname))
			g.Expect(err).NotTo(HaveOccurred())
			slices.SortFunc(list, func(a, b corev1.Pod) int { return strings.Compare(a.Name, b.Name) })

			return list
		}

		states := func(g Gomega, ctx context.Context) map[string]instance {
			GinkgoHelper()

			out := map[string]instance{}
			for _, pod := range pods(g, ctx) {
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

			Eventually(func(g Gomega) {
				g.Expect(states(g, ctx)).To(Equal(baseline), "an instance Pod was replaced, or its sidecar restarted")
			}).WithTimeout(30 * time.Second).WithPolling(specPoll).Should(Succeed())
		}

		touch := func(ctx context.Context, value string) string {
			GinkgoHelper()

			return touchCluster(ctx, c, key(cname), value)
		}

		primary := func(ctx context.Context) types.NamespacedName {
			GinkgoHelper()

			current := getCluster(Default, ctx).Status.CurrentPrimary
			Expect(current).NotTo(BeEmpty())

			return key(current)
		}

		reviewAll := func(ctx context.Context, checks []authorizationv1.ResourceAttributes) []review {
			GinkgoHelper()

			reviews := make([]review, 0, len(checks))
			for _, attrs := range checks {
				ok, err := framework.CanI(ctx, cfg, sidecarUser(), attrs)
				Expect(err).NotTo(HaveOccurred())
				reviews = append(reviews, review{attrs: attrs, allowed: ok})
			}

			return reviews
		}

		status := func(g Gomega, ctx context.Context) operator.ClusterStatus {
			st, ok, err := framework.PluginStatus(getCluster(g, ctx))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "the Cluster has no status from the plugin")

			return st
		}

		lines := func(g Gomega, ctx context.Context, since time.Time) []otlp.Line {
			out, err := reader.Since(ctx, since)
			g.Expect(err).NotTo(HaveOccurred())

			return out
		}

		// pluginExported asserts the collector got an export from the plugin since the time.
		pluginExported := func(g Gomega, ctx context.Context, since time.Time) {
			var plugin []map[string]string
			for _, r := range otlp.Resources(lines(g, ctx, since)) {
				if r["service.name"] == pluginServiceName {
					plugin = append(plugin, r)
				}
			}
			g.Expect(plugin).NotTo(BeEmpty(), "the collector received nothing from the plugin since %s", since)
		}

		// identityReconciles returns the latest cnpg_otel_plugin_identity_reconciles_total value per plugin pod
		// and attribute set. It fails unless every plugin pod has exported a point at or after since.
		identityReconciles := func(g Gomega, ctx SpecContext, since time.Time) reconcileCounts {
			pluginPods, err := framework.PluginPods(ctx, c)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pluginPods).NotTo(BeEmpty(), "no plugin pod")

			recent := lines(g, ctx, since)
			counts := reconcileCounts{}
			for _, pod := range pluginPods {
				counts[pod] = map[string]float64{}
				for _, p := range otlp.Points(otlp.ForResource(recent, "k8s.pod.name", pod), selfmetrics.IdentityReconciles) {
					if !p.Time.Before(since) {
						counts[pod][fmt.Sprint(p.Attrs)] = p.Value
					}
				}
				g.Expect(counts[pod]).NotTo(BeEmpty(), "%s exported no %s point since %s", pod, selfmetrics.IdentityReconciles, since)
			}

			return counts
		}

		awaitPluginExport := func(ctx SpecContext, since time.Time) {
			GinkgoHelper()

			Eventually(func(g Gomega, ctx SpecContext) {
				pluginExported(g, ctx, since)
			}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(specPoll).Should(Succeed())
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

		instanceNames := func(g Gomega, ctx context.Context) []string {
			GinkgoHelper()

			var out []string
			for _, pod := range pods(g, ctx) {
				out = append(out, pod.Name)
			}

			return out
		}

		// awaitMetric waits until both instances export the metric called name after since.
		awaitMetric := func(ctx SpecContext, name string, since time.Time, timeout time.Duration) {
			GinkgoHelper()

			Eventually(func(g Gomega, ctx SpecContext) {
				g.Expect(podsExporting(lines(g, ctx, since), name)).To(ConsistOf(instanceNames(g, ctx)), "metric %s", name)
			}).WithContext(ctx).WithTimeout(timeout).WithPolling(specPoll).Should(Succeed())
		}

		role := func(g Gomega, ctx context.Context) []rbacv1.PolicyRule {
			objects, err := framework.Identity(ctx, c, ns, cname)
			g.Expect(err).NotTo(HaveOccurred())

			return objects.Role.Rules
		}

		allowed := func(g Gomega, ctx context.Context, verb, group, resource, name string) bool {
			GinkgoHelper()

			ok, err := framework.CanI(ctx, cfg, sidecarUser(), authorizationv1.ResourceAttributes{
				Namespace: ns, Verb: verb, Group: group, Resource: resource, Name: name,
			})
			g.Expect(err).NotTo(HaveOccurred())

			return ok
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
			ns = namespace.Name
			DeferCleanup(func(ctx SpecContext) {
				Expect(framework.DeleteNamespace(ctx, c, ns, specPoll)).To(Succeed())
			}, NodeTimeout(4*time.Minute))

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

			Eventually(func(ctx context.Context) (bool, error) {
				return framework.ClusterReady(ctx, c, key(cname))
			}).WithContext(ctx).WithTimeout(clusterTimeout).WithPolling(5 * time.Second).Should(BeTrue())

			baseline = states(Default, ctx)
			Expect(baseline).To(HaveLen(2))
			for name, in := range baseline {
				Expect(in.PodSpec).NotTo(BeEmpty(), "pod %s has no %s annotation", name, framework.PodSpecAnnotation)
			}
		}, NodeTimeout(clusterTimeout+2*time.Minute))

		// A failed spec reports what the sidecars logged.
		JustAfterEach(func(ctx SpecContext) {
			if !CurrentSpecReport().Failed() {
				return
			}
			for _, pod := range pods(Default, ctx) {
				logs, err := sidecarLogs(ctx, cfg, ns, pod.Name)
				if err != nil {
					logs = err.Error()
				}
				AddReportEntry("sidecar logs of "+pod.Name, logs)
			}
		}, NodeTimeout(time.Minute))

		It("a Cluster whose TelemetryExporter doesn't exist yet gets its sidecar, reports exporterState missing, and exports nothing",
			func(ctx SpecContext) {
				for _, pod := range pods(Default, ctx) {
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

				for _, pod := range pods(Default, ctx) {
					Eventually(func(ctx SpecContext) (string, error) {
						return sidecarLogs(ctx, cfg, ns, pod.Name)
					}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(
						ContainSubstring(jsonEscaped(fmt.Sprintf("telemetryexporter %q does not exist", exporterName))), pod.Name)
				}

				since := clusterCreated
				Consistently(func(g Gomega, ctx SpecContext) {
					g.Expect(exports(lines(g, ctx, since))).To(BeEmpty(), "the sidecars export without a TelemetryExporter")
				}).WithContext(ctx).WithTimeout(3 * exportInterval).WithPolling(specPoll).Should(Succeed())
				awaitPluginExport(ctx, since)

				noRollout(ctx)
			}, SpecTimeout(8*time.Minute))

		It("the ServiceAccount, Role, RoleBinding and token exist, owned by the Cluster, and the Role names exactly the referenced objects",
			func(ctx SpecContext) {
				owner := getCluster(Default, ctx)
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
					Expect(allowed(Default, ctx, verb, "", "configmaps", baseQueries)).To(BeTrue(), verb)
					Expect(allowed(Default, ctx, verb, "", "configmaps", "unreferenced")).To(BeFalse(), verb)
				}
			}, SpecTimeout(2*time.Minute))

		It("the sidecar is injected into every instance with the expected container", func(ctx SpecContext) {
			image, err := framework.SidecarImage(ctx, c)
			Expect(err).NotTo(HaveOccurred())

			list := pods(Default, ctx)
			Expect(list).To(HaveLen(getCluster(Default, ctx).Spec.Instances))
			for _, pod := range list {
				Expect(pod.Status.Phase).To(Equal(corev1.PodRunning), pod.Name)

				i := slices.IndexFunc(pod.Spec.InitContainers, func(ctr corev1.Container) bool {
					return ctr.Name == metadata.SidecarContainerName
				})
				Expect(i).To(BeNumerically(">=", 0), "pod %s has no %s init container", pod.Name, metadata.SidecarContainerName)
				sidecar := pod.Spec.InitContainers[i]

				j := slices.IndexFunc(pod.Status.InitContainerStatuses, func(s corev1.ContainerStatus) bool {
					return s.Name == metadata.SidecarContainerName
				})
				Expect(j).To(BeNumerically(">=", 0), "pod %s has no %s status", pod.Name, metadata.SidecarContainerName)

				Expect(sidecar.Image).To(Equal(image), pod.Name)
				Expect(sidecar.RestartPolicy).To(HaveValue(Equal(corev1.ContainerRestartPolicyAlways)), pod.Name)
				Expect(sidecar.SecurityContext).To(Equal(sidecarSecurityContext(!m.password)), pod.Name)
				Expect(sidecar.VolumeMounts).To(ConsistOf(sidecarMounts(!m.password)), pod.Name)
				Expect(sidecar.VolumeMounts).NotTo(ContainElement(HaveField("Name", HavePrefix("kube-api-access"))), pod.Name)
				Expect(pod.Status.InitContainerStatuses[j].Started).To(HaveValue(BeTrue()), pod.Name)
			}
		}, SpecTimeout(2*time.Minute))

		It("the sidecar identity reads what it needs, nothing else, and has no Pod token", func(ctx SpecContext) {
			owner := getCluster(Default, ctx)
			readable := []authorizationv1.ResourceAttributes{
				{Namespace: ns, Group: cnpgGroup, Resource: "clusters", Name: cname},
				{Namespace: ns, Group: metadata.PluginName, Resource: "telemetryexporters", Name: exporterName},
			}
			for _, name := range monitoredConfigMaps(owner) {
				readable = append(readable, authorizationv1.ResourceAttributes{Namespace: ns, Resource: "configmaps", Name: name})
			}
			if m.password {
				readable = append(readable, authorizationv1.ResourceAttributes{Namespace: ns, Resource: "secrets", Name: monitorSecret})
			}

			var checks []authorizationv1.ResourceAttributes
			for _, attrs := range readable {
				for _, verb := range []string{"get", "watch"} {
					attrs.Verb = verb
					checks = append(checks, attrs)
				}
			}
			reviews := reviewAll(ctx, checks)
			AddReportEntry("can-i (want true)", table(reviews))
			Expect(decided(reviews, false)).To(BeEmpty())

			other := cname + "-other"
			reviews = reviewAll(ctx, forbidden(ns, cname, other, readable))
			AddReportEntry("can-i (want false)", table(reviews))
			Expect(decided(reviews, true)).To(BeEmpty())

			rules, nonResource, err := framework.Rules(ctx, cfg, sidecarUser(), ns)
			Expect(err).NotTo(HaveOccurred())
			AddReportEntry("rules", formatRules(rules))
			Expect(framework.UnexpectedRules(rules)).To(BeEmpty(), formatRules(rules))
			Expect(framework.UnexpectedNonResourceRules(nonResource)).To(BeEmpty(), "%v", nonResource)

			rep, err := framework.Preflight(ctx, cfg, primary(ctx))
			Expect(err).NotTo(HaveOccurred())
			Expect(rep.PodTokenPresent).To(BeFalse())
			Expect(rep.Kube).NotTo(BeNil())
			Expect(rep.Kube.Username).To(Equal(sidecarUser()))
			Expect(rep.Kube.ClusterReadable).To(BeTrue(), rep.Kube.Error)
		}, SpecTimeout(3*time.Minute))

		if !m.password {
			It("the sidecar connects as cnpg_metrics_exporter through the read-only socket mount", func(ctx SpecContext) {
				rep, err := framework.Preflight(ctx, cfg, primary(ctx))
				Expect(err).NotTo(HaveOccurred())

				Expect(rep.UID).To(BeEquivalentTo(metadata.PeerUID))
				Expect(rep.Postgres).NotTo(BeNil())
				Expect(rep.Postgres.Host).To(Equal(metadata.SocketDir))
				Expect(rep.Postgres.Connected).To(BeTrue(), rep.Postgres.Error)
				Expect(rep.Postgres.CurrentUser).To(Equal(metadata.MetricsExporterRole))
				Expect(rep.Postgres.WALFiles).To(BeNumerically(">=", 1))
				Expect(rep.Postgres.SQLState).To(BeEmpty())
				Expect(rep.Postgres.Error).To(BeEmpty())
			}, SpecTimeout(time.Minute))
		}

		if m.password {
			It("the sidecar connects as the dedicated role over 127.0.0.1", func(ctx SpecContext) {
				pod := primary(ctx)
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
				Expect(rep.Postgres.Host).To(Equal("127.0.0.1"))
			}, SpecTimeout(3*time.Minute))

			It("the dedicated role is refused from another Pod", func(ctx SpecContext) {
				image, err := framework.SidecarImage(ctx, c)
				Expect(err).NotTo(HaveOccurred())
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: cname + "-client"},
					Spec: corev1.PodSpec{
						RestartPolicy:                corev1.RestartPolicyNever,
						AutomountServiceAccountToken: ptr.To(false),
						Containers: []corev1.Container{{
							Name:            "preflight",
							Image:           image,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Args: []string{
								"instance", "preflight", "--kube=false",
								"--pg-host", cname + apiv1.ServiceReadWriteSuffix, "--pg-user", monitorRole,
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

				rep, err := framework.DecodeReportFromLogs(logs)
				Expect(err).NotTo(HaveOccurred())
				Expect(rep.Postgres).NotTo(BeNil())
				Expect(rep.Postgres.Connected).To(BeFalse())
				Expect(rep.Postgres.SQLState).To(Equal(sqlStateInvalidAuthorization))
				Expect(rep.Postgres.Error).To(ContainSubstring("pg_hba.conf rejects connection"))
			}, SpecTimeout(3*time.Minute))
		}

		It("an exporter that is invalid from the start leaves the sidecar idle", func(ctx SpecContext) {
			// The CRD doesn't check glob syntax, so a bad exclude pattern fails Build.
			exporter := desiredExporter(ns)
			exporter.Spec.Metrics.Exclude.Queries = []string{badGlob}

			created := time.Now()
			Expect(c.Create(ctx, exporter)).To(Succeed())

			// CloudNativePG stops polling the status while the exporter is missing; each touch is a reconcile that
			// asks again, in case the plugin's cache didn't hold the exporter yet.
			Eventually(func(g Gomega, ctx SpecContext) {
				_, err := framework.Touch(ctx, c, key(cname), fmt.Sprint("created-invalid-", time.Now().UnixNano()))
				g.Expect(err).NotTo(HaveOccurred())
				st := status(g, ctx)
				g.Expect(st.ExporterState).To(Equal("invalid"))
				g.Expect(st.Message).To(ContainSubstring("exclude.queries[0]"))
			}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(specPoll).Should(Succeed())

			for _, pod := range pods(Default, ctx) {
				Eventually(func(ctx SpecContext) (string, error) {
					return sidecarLogs(ctx, cfg, ns, pod.Name)
				}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(
					ContainSubstring("invalid configuration, waiting for a valid one"), pod.Name)
			}

			awaitPluginExport(ctx, created)
			Consistently(func(g Gomega, ctx SpecContext) {
				g.Expect(exports(lines(g, ctx, created))).To(BeEmpty(), "the sidecars export with an invalid TelemetryExporter")
			}).WithContext(ctx).WithTimeout(4 * exportInterval).WithPolling(specPoll).Should(Succeed())

			noRollout(ctx)
		}, SpecTimeout(8*time.Minute))

		It("after the TelemetryExporter is valid, metrics from both instances reach the collector with the service.*, k8s.* and allowlisted-label resource attributes and no rollout",
			func(ctx SpecContext) {
				// The exporter exists since the previous spec, invalid; this makes it valid.
				var current v1alpha1.TelemetryExporter
				Expect(c.Get(ctx, key(exporterName), &current)).To(Succeed())
				patch := client.MergeFrom(current.DeepCopy())
				current.Spec = desiredExporter(ns).Spec

				exporterValidAt = time.Now()
				Expect(c.Patch(ctx, &current, patch)).To(Succeed())

				Eventually(func(g Gomega, ctx SpecContext) {
					g.Expect(status(g, ctx).ExporterState).To(Equal("ok"))
				}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(time.Second).Should(Succeed())
				statusOKAfter = time.Since(exporterValidAt)

				cl := getCluster(Default, ctx)
				Eventually(func(g Gomega, ctx SpecContext) {
					ls := lines(g, ctx, exporterValidAt)
					g.Expect(podsExporting(ls, "cnpg_e2e_base_one")).To(ConsistOf(instanceNames(g, ctx)))

					// The primary-only query runs on the primary and nowhere else, as seen in one export.
					g.Expect(podsExporting(ls, "cnpg_e2e_primary_one")).To(ConsistOf(cl.Status.CurrentPrimary))
				}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(specPoll).Should(Succeed())

				ls, err := reader.Since(ctx, exporterValidAt)
				Expect(err).NotTo(HaveOccurred())
				byPod := map[string]otlp.Export{}
				for _, e := range exports(ls) {
					if has(e, "cnpg_e2e_base_one") {
						byPod[e.Resource["k8s.pod.name"]] = e
					}
				}
				stdout, stderr, err := framework.Exec(ctx, cfg, types.NamespacedName{Namespace: ns, Name: pods(Default, ctx)[0].Name},
					metadata.SidecarContainerName, []string{"/plugin-telemetry", "version"})
				Expect(err).NotTo(HaveOccurred(), stderr)
				versionFields := strings.Fields(stdout)
				Expect(versionFields).To(HaveLen(4), stdout)
				sidecarVersion := versionFields[1]
				Expect(sidecarVersion).NotTo(BeEmpty())

				for _, pod := range pods(Default, ctx) {
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

		It("the status turns ok within 30 s of the TelemetryExporter becoming valid", func(ctx SpecContext) {
			Expect(exporterValidAt).NotTo(BeZero(), "the exporter was not made valid")
			Expect(statusOKAfter).To(BeNumerically("<=", 30*time.Second))

			Eventually(func(g Gomega, ctx SpecContext) {
				st := status(g, ctx)
				g.Expect(st.ExporterState).To(Equal("ok"))
				g.Expect(st.Identity).To(Equal("ready"))
				g.Expect(st.Problems).To(BeEmpty())
				g.Expect(st.Message).To(BeEmpty())
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(Succeed())
		}, SpecTimeout(2*time.Minute))

		It("adding a ConfigMap to spec.monitoring grants it and its metrics arrive", func(ctx SpecContext) {
			cl := getCluster(Default, ctx)
			patch := client.MergeFrom(cl.DeepCopy())
			cl.Spec.Monitoring.CustomQueriesConfigMap = append(cl.Spec.Monitoring.CustomQueriesConfigMap, apiv1.ConfigMapKeySelector{
				LocalObjectReference: apiv1.LocalObjectReference{Name: addedQueries},
				Key:                  queriesKey,
			})
			Expect(c.Patch(ctx, cl, patch)).To(Succeed())

			Eventually(func(g Gomega, ctx SpecContext) {
				names := monitoredConfigMaps(getCluster(g, ctx))
				g.Expect(names).To(ContainElements(addedQueries, baseQueries))
				g.Expect(role(g, ctx)).To(ContainElement(readRule("", "configmaps", names...)))
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(time.Second).Should(Succeed())
			for _, verb := range []string{"get", "list", "watch"} {
				Expect(allowed(Default, ctx, verb, "", "configmaps", addedQueries)).To(BeTrue(), verb)
			}

			// The ConfigMap is granted before it exists; creating it later is picked up.
			created := time.Now()
			Expect(c.Create(ctx, configMap(ns, "configmap-added.yaml"))).To(Succeed())
			awaitMetric(ctx, "cnpg_e2e_added_three", created, 2*time.Minute)

			noRollout(ctx)
		}, SpecTimeout(5*time.Minute))

		It("adding a query set to the TelemetryExporter grants it without a Cluster edit, and its metrics arrive", func(ctx SpecContext) {
			Expect(c.Create(ctx, querySet(ns, "metricqueryset.yaml"))).To(Succeed())
			generation := getCluster(Default, ctx).Generation

			var exporter v1alpha1.TelemetryExporter
			Expect(c.Get(ctx, key(exporterName), &exporter)).To(Succeed())
			patch := client.MergeFrom(exporter.DeepCopy())
			exporter.Spec.Metrics.QuerySets = []string{querySetName}
			created := time.Now()
			Expect(c.Patch(ctx, &exporter, patch)).To(Succeed())

			Eventually(func(g Gomega, ctx SpecContext) {
				g.Expect(role(g, ctx)).To(ContainElement(readRule(metadata.PluginName, "metricquerysets", querySetName)))
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(time.Second).Should(Succeed())
			Expect(getCluster(Default, ctx).Generation).To(Equal(generation), "the Cluster was edited")

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
					Expect(allowed(Default, ctx, verb, target.group, target.resource, target.name)).To(BeTrue(),
						"%s %s/%s", verb, target.resource, target.name)
				}
			}

			noRollout(ctx)
		}, SpecTimeout(5*time.Minute))

		It("editing the ConfigMap exports the new metric within 60 s, with no restart and no rollout", func(ctx SpecContext) {
			var cm corev1.ConfigMap
			Expect(c.Get(ctx, key(baseQueries), &cm)).To(Succeed())
			patch := client.MergeFrom(cm.DeepCopy())
			cm.Data = configMap(ns, "configmap-base-edited.yaml").Data

			edited := time.Now()
			Expect(c.Patch(ctx, &cm, patch)).To(Succeed())
			awaitMetric(ctx, "cnpg_e2e_edited_two", edited, time.Minute)

			noRollout(ctx)
		}, SpecTimeout(3*time.Minute))

		It("ten annotation edits and the Cluster's own status updates keep cnpg_otel_config_info's generation and every series",
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

				var (
					reconcilesBefore reconcileCounts
					lastReading      reconcileCounts
					lastReadingAt    time.Time
					reconcilesAfter  reconcileCounts
					ownedBefore      map[string]string
					clustersBefore   string
				)
				defer func() {
					report, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
					defer cancel()

					baseline := reconcilesBefore.String()
					if reconcilesBefore == nil {
						baseline = "baseline (not reached), last reading:\n" + lastReading.String()
					}
					changes := "no baseline snapshot"
					ownedAfter, ownedErr := clusterOwned(report, c, ns)
					switch {
					case ownedErr != nil:
						changes = ownedErr.Error()
					case ownedBefore != nil:
						changes = changedOwned(ownedBefore, ownedAfter)
					}
					clustersAfter, clustersErr := clustersEverywhere(report, c)
					if clustersErr != nil {
						clustersAfter = clustersErr.Error()
					}
					AddReportEntry("identity reconciles at baseline", baseline)
					end := "not read"
					if reconcilesAfter != nil {
						end = reconcilesAfter.String()
					}
					if clustersBefore == "" {
						clustersBefore = "not reached"
					}
					AddReportEntry("identity reconciles at the end", end)
					AddReportEntry("Cluster-owned objects in "+ns+" that changed", changes)
					AddReportEntry("Clusters at baseline", clustersBefore)
					AddReportEntry("Clusters at the end", clustersAfter)
				}()

				var before map[string]otlp.Export
				Eventually(func(g Gomega, ctx SpecContext) {
					before = latest(g, ctx, time.Now().Add(-2*exportInterval))
					for pod, e := range before {
						g.Expect(info(e)).To(HaveLen(1), pod)
					}
				}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(specPoll).Should(Succeed())

				// The baseline is taken when every plugin pod has exported since mark and the counters did not move
				// over two consecutive readings at least 2*exportInterval apart. A reconcile between the baseline's
				// newest point and the first edit (at most about one export interval) still shows as a difference.
				mark := time.Now()
				Eventually(func(g Gomega, ctx SpecContext) {
					readAt := time.Now()
					since := mark
					if lastReading != nil {
						since = lastReadingAt
						g.Expect(readAt.Sub(lastReadingAt)).To(BeNumerically(">=", 2*exportInterval), "waiting for the second reading")
					}
					cur := identityReconciles(g, ctx, since)
					previous := lastReading
					lastReading, lastReadingAt = cur, readAt
					g.Expect(previous).NotTo(BeNil(), "the first reading is taken, a second one confirms it")
					g.Expect(cur).To(Equal(previous), "the identity controller reconciled between two readings")
					reconcilesBefore = cur
				}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(specPoll).Should(Succeed())

				var err error
				ownedBefore, err = clusterOwned(ctx, c, ns)
				Expect(err).NotTo(HaveOccurred())
				clustersBefore, err = clustersEverywhere(ctx, c)
				Expect(err).NotTo(HaveOccurred())

				edits := time.Now()
				var lastWrite string
				for i := range 10 {
					lastWrite = touch(ctx, fmt.Sprint(i))
					time.Sleep(500 * time.Millisecond)
				}

				// The status is a subresource with its own field manager: CloudNativePG wrote it after the edits began.
				Eventually(func(g Gomega, ctx SpecContext) {
					cl := getCluster(g, ctx)
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

				// Annotation edits don't change the generation, so the identity controller has nothing to reconcile.
				Eventually(func(g Gomega, ctx SpecContext) {
					reconcilesAfter = identityReconciles(g, ctx, edits.Add(2*exportInterval))
					g.Expect(reconcilesAfter.equal(reconcilesBefore)).To(BeTrue(), "identity reconciles moved:\nbefore %v\nafter %v", reconcilesBefore, reconcilesAfter)
				}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(specPoll).Should(Succeed())

				noRollout(ctx)
			}, SpecTimeout(6*time.Minute))

		It("identity controller: a deleted RoleBinding comes back with the same Cluster generation and no rollout",
			func(ctx SpecContext) {
				objects, err := framework.Identity(ctx, c, ns, cname)
				Expect(err).NotTo(HaveOccurred())
				oldUID := objects.RoleBinding.UID
				generation := getCluster(Default, ctx).Generation

				Expect(c.Delete(ctx, objects.RoleBinding)).To(Succeed())

				Eventually(func(g Gomega, ctx SpecContext) {
					recreated, err := framework.Identity(ctx, c, ns, cname)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(recreated.RoleBinding.UID).NotTo(Equal(oldUID))
				}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

				Expect(getCluster(Default, ctx).Generation).To(Equal(generation))
				noRollout(ctx)
			}, SpecTimeout(2*time.Minute))

		It("removing a ConfigMap and a query set revokes them, stops their metrics and ignores later edits", func(ctx SpecContext) {
			var exporter v1alpha1.TelemetryExporter
			Expect(c.Get(ctx, key(exporterName), &exporter)).To(Succeed())
			exporterPatch := client.MergeFrom(exporter.DeepCopy())
			exporter.Spec.Metrics.QuerySets = nil
			Expect(c.Patch(ctx, &exporter, exporterPatch)).To(Succeed())

			cl := getCluster(Default, ctx)
			clusterPatch := client.MergeFrom(cl.DeepCopy())
			cl.Spec.Monitoring.CustomQueriesConfigMap = slices.DeleteFunc(cl.Spec.Monitoring.CustomQueriesConfigMap,
				func(s apiv1.ConfigMapKeySelector) bool { return s.Name == addedQueries })
			Expect(c.Patch(ctx, cl, clusterPatch)).To(Succeed())

			Eventually(func(g Gomega, ctx SpecContext) {
				rules := role(g, ctx)
				names := monitoredConfigMaps(getCluster(g, ctx))
				g.Expect(names).NotTo(ContainElement(addedQueries))
				g.Expect(rules).To(ContainElement(readRule("", "configmaps", names...)))
				g.Expect(rules).NotTo(ContainElement(HaveField("Resources", ContainElement("metricquerysets"))))
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(time.Second).Should(Succeed())
			Expect(allowed(Default, ctx, "get", "", "configmaps", addedQueries)).To(BeFalse())
			Expect(allowed(Default, ctx, "watch", metadata.PluginName, "metricquerysets", querySetName)).To(BeFalse())

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

			Expect(podsExporting(lines(Default, ctx, edited), "cnpg_e2e_base_one")).To(ConsistOf(instanceNames(Default, ctx)))
			noRollout(ctx)
		}, SpecTimeout(6*time.Minute))

		It("security: the sidecar can't log in as postgres and its token can't read the Cluster's Secrets", func(ctx SpecContext) {
			for _, pod := range pods(Default, ctx) {
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
				Expect(rep.Postgres.SQLState).To(Equal(sqlStateInvalidAuthorization))
				Expect(rep.Postgres.Error).To(ContainSubstring(`Peer authentication failed for user "postgres"`))
			}

			for _, secret := range []string{
				cname + apiv1.SuperUserSecretSuffix,
				cname + apiv1.ApplicationUserSecretSuffix,
				cname + apiv1.DefaultServerCaSecretSuffix,
				cname + apiv1.ReplicationSecretSuffix,
			} {
				Expect(allowed(Default, ctx, "get", "", "secrets", secret)).To(BeFalse(), secret)
			}
			Expect(allowed(Default, ctx, "list", "", "secrets", "")).To(BeFalse())
		}, SpecTimeout(3*time.Minute))

		It("security: print-config names the headers file and shows no seeded Secret value", func(ctx SpecContext) {
			for _, pod := range pods(Default, ctx) {
				stdout, stderr, err := framework.Exec(ctx, cfg, types.NamespacedName{Namespace: ns, Name: pod.Name},
					metadata.SidecarContainerName, []string{"/plugin-telemetry", "instance", "print-config"})
				Expect(err).NotTo(HaveOccurred(), stderr)
				Expect(stdout).To(ContainSubstring("headersFile: secret/"+headersSecret+"/"+headersKey), "print-config shows no configuration")
				Expect(stdout).NotTo(ContainSubstring(headerName), "print-config lists no header key")

				for _, sentinel := range sentinels {
					Expect(stdout).NotTo(ContainSubstring(sentinel), "print-config of %s", pod.Name)
					Expect(stderr).NotTo(ContainSubstring(sentinel), "print-config stderr of %s", pod.Name)
				}
			}
		}, SpecTimeout(3*time.Minute))

		It("an invalid TelemetryExporter is reported, and the sidecars keep the configuration in force", func(ctx SpecContext) {
			// The CRD doesn't check glob syntax, so a bad exclude pattern fails Build.
			var exporter v1alpha1.TelemetryExporter
			Expect(c.Get(ctx, key(exporterName), &exporter)).To(Succeed())
			valid := exporter.DeepCopy()
			patch := client.MergeFrom(exporter.DeepCopy())
			exporter.Spec.Metrics.Exclude.Queries = []string{badGlob}
			invalidAt := time.Now()
			Expect(c.Patch(ctx, &exporter, patch)).To(Succeed())

			touch(ctx, "invalid") // a reconcile makes CloudNativePG ask for the status
			Eventually(func(g Gomega, ctx SpecContext) {
				st := status(g, ctx)
				g.Expect(st.ExporterState).To(Equal("invalid"))
				g.Expect(st.Message).To(ContainSubstring("exclude.queries[0]"))
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(Succeed())

			for _, pod := range pods(Default, ctx) {
				Eventually(func(ctx SpecContext) (string, error) {
					return sidecarLogs(ctx, cfg, ns, pod.Name)
				}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(
					ContainSubstring("invalid configuration, keeping the one in force"), pod.Name)
			}

			// The configuration was valid before, so it stays in force: the sidecars keep exporting every 10 s.
			Eventually(func(g Gomega, ctx SpecContext) {
				g.Expect(podsExporting(lines(g, ctx, invalidAt.Add(2*exportInterval)), "cnpg_e2e_base_one")).
					To(ConsistOf(instanceNames(g, ctx)))
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
			return parityTarget{env: env, c: c, cfg: cfg, reader: reader, ns: ns, cluster: cname, mode: m.name, sentinels: sentinels}
		})

		It("deleting the TelemetryExporter stops exports, and the status says missing after the next reconcile", func(ctx SpecContext) {
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
			awaitPluginExport(ctx, quiet)

			// CloudNativePG calls SetStatusInCluster on a reconcile, which an annotation edit triggers.
			touch(ctx, "after-delete")
			Eventually(func(g Gomega, ctx SpecContext) {
				st := status(g, ctx)
				g.Expect(st.Exporter).To(Equal(exporterName))
				g.Expect(st.ExporterState).To(Equal("missing"))
			}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(specPoll).Should(Succeed())

			// A Pod that a rollout deletes keeps its UID and annotations until it is gone, so look for a while.
			Consistently(func(g Gomega, ctx SpecContext) map[string]instance {
				return states(g, ctx)
			}).WithContext(ctx).WithTimeout(30*time.Second).WithPolling(specPoll).Should(Equal(baseline),
				"an instance Pod is being replaced, or its sidecar restarted")
		}, SpecTimeout(8*time.Minute))

		It("security: no seeded Secret value appears in the sidecar's logs", func(ctx SpecContext) {
			for _, pod := range pods(Default, ctx) {
				logs, err := sidecarLogs(ctx, cfg, ns, pod.Name)
				Expect(err).NotTo(HaveOccurred())
				Expect(logs).NotTo(BeEmpty())

				for _, sentinel := range sentinels {
					Expect(logs).NotTo(ContainSubstring(sentinel), "logs of %s", pod.Name)
				}
			}
		}, SpecTimeout(3*time.Minute))

		It("deleting the Cluster deletes the four objects", func(ctx SpecContext) {
			Expect(identityGone(ctx)).NotTo(Succeed(), "the identity is gone before the Cluster is deleted")

			Expect(c.Delete(ctx, getCluster(Default, ctx))).To(Succeed())

			Eventually(identityGone).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(specPoll).Should(Succeed())
		}, SpecTimeout(5*time.Minute))
	})
}
