//go:build e2e

package plugin_test

import (
	"os"
	"slices"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

const (
	clusterName = "env-peer"

	reconcilerHooks         = "TYPE_RECONCILER_HOOKS"
	setStatusInCluster      = "TYPE_SET_STATUS_IN_CLUSTER"
	hookCallsMetric         = "cnpg_otel_plugin_hook_calls_total"
	pluginServiceName       = "plugin-telemetry"
	environmentTimeout      = 3 * time.Minute
	environmentPollInterval = 2 * time.Second
)

var _ = Describe("the plugin environment", Ordered, func() {
	var (
		c      client.Client
		cfg    *rest.Config
		reader *framework.CollectorReader
		ns     string
		start  time.Time
	)

	BeforeAll(func(ctx SpecContext) {
		env, err := framework.LoadEnv(os.Getenv)
		Expect(err).NotTo(HaveOccurred())
		c, cfg, err = framework.NewClients(env.KubeContext)
		Expect(err).NotTo(HaveOccurred())
		reader = framework.NewCollectorReader(cfg)

		Eventually(framework.PluginAvailable).WithContext(ctx).WithArguments(c).
			WithTimeout(environmentTimeout).WithPolling(environmentPollInterval).Should(BeTrue())
		Expect(framework.CollectorReady(ctx, c)).To(Succeed())

		namespace, err := framework.CreateNamespace(ctx, c, "plugin")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, namespace))).To(Succeed())
		})
		ns = namespace.Name

		start = time.Now()
		Expect(c.Create(ctx, framework.ExporterFor(ns, podpatch.DefaultExporter))).To(Succeed())
		cluster := framework.PeerCluster(ns, clusterName, env.PostgresImage)
		Expect(c.Create(ctx, cluster)).To(Succeed())
		// Runs before the namespace's cleanup: a namespace deleted around a live Cluster stays Terminating.
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, cluster))).To(Succeed())
			Eventually(func(ctx SpecContext) bool {
				return apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(cluster), &apiv1.Cluster{}))
			}).WithContext(ctx).WithTimeout(3 * time.Minute).WithPolling(environmentPollInterval).Should(BeTrue())
		}, NodeTimeout(4*time.Minute))
	})

	It("CNPG reports the reconciler hooks and SetStatusInCluster for a Cluster using the plugin", func(ctx SpecContext) {
		Eventually(func(g Gomega, ctx SpecContext) {
			var cluster apiv1.Cluster
			g.Expect(c.Get(ctx, client.ObjectKey{Namespace: ns, Name: clusterName}, &cluster)).To(Succeed())

			i := slices.IndexFunc(cluster.Status.PluginStatus, func(ps apiv1.PluginStatus) bool {
				return ps.Name == metadata.PluginName
			})
			g.Expect(i).To(BeNumerically(">=", 0), "no pluginStatus entry for %s", metadata.PluginName)
			g.Expect(cluster.Status.PluginStatus[i].Capabilities).To(ContainElement(reconcilerHooks))
			g.Expect(cluster.Status.PluginStatus[i].OperatorCapabilities).To(ContainElement(setStatusInCluster))

			status, ok, err := framework.PluginStatus(&cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue())
			g.Expect(status.Exporter).To(Equal(podpatch.DefaultExporter))
		}).WithContext(ctx).WithTimeout(environmentTimeout).WithPolling(environmentPollInterval).Should(Succeed())
	})

	It("the plugin's self-metrics reach the collector with the plugin resource", func(ctx SpecContext) {
		Eventually(func(g Gomega, ctx SpecContext) {
			lines, err := reader.Since(ctx, start)
			g.Expect(err).NotTo(HaveOccurred())

			var plugin []map[string]string
			for _, r := range otlp.Resources(lines) {
				if r["service.name"] == pluginServiceName {
					plugin = append(plugin, r)
				}
			}
			g.Expect(plugin).NotTo(BeEmpty(), "no resource with service.name=%s", pluginServiceName)
			pods, err := framework.PluginPods(ctx, c)
			g.Expect(err).NotTo(HaveOccurred())
			for _, r := range plugin {
				g.Expect(r).To(HaveKeyWithValue("k8s.pod.name", Not(BeEmpty())))
			}
			g.Expect(plugin).To(ContainElement(HaveKeyWithValue("k8s.pod.name", BeElementOf(pods))))
			g.Expect(otlp.Points(lines, hookCallsMetric)).NotTo(BeEmpty())
		}).WithContext(ctx).WithTimeout(environmentTimeout).WithPolling(environmentPollInterval).Should(Succeed())
	})
})
