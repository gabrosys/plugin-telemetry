//go:build e2e

package plugin_test

import (
	"context"
	"os"
	"slices"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
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

	hookEvaluate = "EVALUATE"
	hookPatched  = "patched"
	hookTimeout  = 2 * time.Minute
)

// Serial: the restart spec rolls the plugin out, so no other spec may run beside it.
var _ = Describe("the plugin environment", Ordered, Serial, ContinueOnFailure, func() {
	var (
		env    framework.Env
		c      client.Client
		cfg    *rest.Config
		reader *framework.CollectorReader
		ns     string
		start  time.Time
	)

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

		namespace, err := framework.CreateNamespace(ctx, c, "plugin")
		Expect(err).NotTo(HaveOccurred())
		ns = namespace.Name
		DeferCleanup(func(ctx SpecContext) {
			Expect(framework.DeleteNamespace(ctx, c, ns, environmentPollInterval)).To(Succeed())
		}, NodeTimeout(4*time.Minute))

		start = time.Now()
		Expect(c.Create(ctx, framework.ExporterFor(ns, podpatch.DefaultExporter))).To(Succeed())
		cluster := framework.PeerCluster(ns, clusterName, env.PostgresImage)
		Expect(c.Create(ctx, cluster)).To(Succeed())
	})

	It("runs against the requested CNPG version", func(ctx SpecContext) {
		img, err := framework.OperatorImage(ctx, c)
		Expect(err).NotTo(HaveOccurred())
		AddReportEntry("cnpg-operator-image", img)
		Expect(framework.MatchesCNPGVersion(img, env.CNPGVersion)).To(BeTrue(), img)
	}, SpecTimeout(time.Minute))

	It("CNPG reports SetStatusInCluster and no reconciler hooks", func(ctx SpecContext) {
		Eventually(func(g Gomega, ctx SpecContext) {
			var cluster apiv1.Cluster
			g.Expect(c.Get(ctx, client.ObjectKey{Namespace: ns, Name: clusterName}, &cluster)).To(Succeed())

			i := slices.IndexFunc(cluster.Status.PluginStatus, func(ps apiv1.PluginStatus) bool {
				return ps.Name == metadata.PluginName
			})
			g.Expect(i).To(BeNumerically(">=", 0), "no pluginStatus entry for %s", metadata.PluginName)
			g.Expect(cluster.Status.PluginStatus[i].Capabilities).NotTo(ContainElement(reconcilerHooks))
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

	It("keeps the instance Pods across reconciles and a plugin restart", func(ctx SpecContext) {
		clusterKey := client.ObjectKey{Namespace: ns, Name: clusterName}

		Eventually(framework.ClusterReady).WithContext(ctx).WithArguments(c, clusterKey).
			WithTimeout(clusterTimeout).WithPolling(5 * time.Second).Should(BeTrue())

		type pod struct {
			UID     types.UID
			PodSpec string
		}
		instances := func(g Gomega, ctx context.Context) map[string]pod {
			list, err := framework.InstancePods(ctx, c, clusterKey)
			g.Expect(err).NotTo(HaveOccurred())

			out := map[string]pod{}
			for _, p := range list {
				out[p.Name] = pod{UID: p.UID, PodSpec: p.Annotations[framework.PodSpecAnnotation]}
			}

			return out
		}
		// awaitEvaluations waits until one of plugins logs, after since, an EVALUATE that patched each instance Pod.
		awaitEvaluations := func(ctx context.Context, plugins []string, want map[string]pod, since time.Time) {
			GinkgoHelper()

			Eventually(func(g Gomega, ctx context.Context) {
				found := map[string]bool{}
				for _, plugin := range plugins {
					calls, err := framework.HookCalls(ctx, cfg, plugin, since)
					g.Expect(err).NotTo(HaveOccurred())
					for _, call := range calls {
						if call.Namespace == ns && call.Cluster == clusterName &&
							call.Operation == hookEvaluate && call.Outcome == hookPatched {
							found[call.Pod] = true
						}
					}
				}
				for name := range want {
					g.Expect(found).To(HaveKey(name), "no %s of %s after %s by %v", hookEvaluate, name, since, plugins)
				}
			}).WithContext(ctx).WithTimeout(hookTimeout).WithPolling(5 * time.Second).Should(Succeed())
		}
		pluginPods := func(ctx context.Context) []string {
			GinkgoHelper()

			names, err := framework.PluginPods(ctx, c)
			Expect(err).NotTo(HaveOccurred())
			Expect(names).NotTo(BeEmpty())

			return names
		}

		var current apiv1.Cluster
		Expect(c.Get(ctx, clusterKey, &current)).To(Succeed())
		before := instances(Default, ctx)
		Expect(before).To(HaveLen(current.Spec.Instances))
		for name, in := range before {
			Expect(in.PodSpec).NotTo(BeEmpty(), "pod %s has no %s annotation", name, framework.PodSpecAnnotation)
		}

		oldPlugins := pluginPods(ctx)
		bumped := time.Now()
		touchCluster(ctx, c, clusterKey, "restart-"+time.Now().UTC().Format(time.RFC3339Nano))
		awaitEvaluations(ctx, oldPlugins, before, bumped)

		Expect(framework.RestartPlugin(ctx, c)).To(Succeed())
		Eventually(framework.PluginRolledOut).WithContext(ctx).WithArguments(c).
			WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(BeTrue())
		newPlugins := pluginPods(ctx)
		Expect(newPlugins).NotTo(ContainElement(BeElementOf(oldPlugins)), "an old plugin Pod is still there")

		bumped = time.Now()
		touchCluster(ctx, c, clusterKey, "restart-"+time.Now().UTC().Format(time.RFC3339Nano))
		awaitEvaluations(ctx, newPlugins, before, bumped)

		Consistently(func(g Gomega, ctx context.Context) {
			g.Expect(instances(g, ctx)).To(Equal(before))
			var cluster apiv1.Cluster
			g.Expect(c.Get(ctx, clusterKey, &cluster)).To(Succeed())
			g.Expect(cluster.Status.Phase).To(Equal(apiv1.PhaseHealthy))
		}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(10 * time.Second).Should(Succeed())
	}, SpecTimeout(22*time.Minute))
})
