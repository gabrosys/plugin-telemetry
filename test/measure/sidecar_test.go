//go:build e2e

package measure_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/podpatch"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

const (
	sidecarWorkingSetMax = 41943040 // 40 MiB
	sidecarCPUMax        = 10000000 // 10m
	usageEvery           = 15 * time.Second
	usageWindow          = 10 * time.Minute
	warmup               = 5 * time.Minute
	statePoll            = 2 * time.Second
)

// sidecarMeasurement is sidecar-<cnpg>.json.
type sidecarMeasurement struct {
	CNPGVersion      string         `json:"cnpgVersion"`
	WorkingSetBytes  framework.Stat `json:"workingSetBytes"`
	RSSBytes         framework.Stat `json:"rssBytes"`
	MeanCPUNanoCores uint64         `json:"meanCPUNanoCores"`
	Series           float64        `json:"series"`
	SkippedRuns      float64        `json:"skippedRuns"`
	Samples          int            `json:"samples"`
	WorkingSetSeries []uint64       `json:"workingSetSeries"`
	RSSSeries        []uint64       `json:"rssSeries"`
	CPUSeries        []uint64       `json:"cpuNanoCoresSeries"`
	RestartsBefore   int32          `json:"restartsBefore"`
	RestartsAfter    int32          `json:"restartsAfter"`
}

var _ = Describe("the sidecar with CNPG's default queries", Ordered, func() {
	It("keeps the sidecar's working set at most 40 MiB and its CPU at most 10m with CNPG's default queries", func(ctx SpecContext) {
		env, err := framework.LoadEnv(os.Getenv)
		Expect(err).NotTo(HaveOccurred())
		out := os.Getenv("MEASURE_OUT")
		Expect(out).NotTo(BeEmpty(), "MEASURE_OUT")
		c, cfg, err := framework.NewClients(env.KubeContext)
		Expect(err).NotTo(HaveOccurred())

		Eventually(framework.PluginAvailable).WithContext(ctx).WithArguments(c).
			WithTimeout(3 * time.Minute).WithPolling(statePoll).Should(BeTrue())
		Expect(framework.CollectorReady(ctx, c)).To(Succeed())

		ns, err := framework.CreateNamespace(ctx, c, "measure-sidecar")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, ns))).To(Succeed())
		})

		Expect(c.Create(ctx, measuredExporter(ns.Name, podpatch.DefaultExporter))).To(Succeed())
		key := client.ObjectKey{Namespace: ns.Name, Name: "sidecar"}
		cluster := framework.PeerCluster(ns.Name, key.Name, env.PostgresImage)
		Expect(c.Create(ctx, cluster)).To(Succeed())
		Eventually(func(ctx SpecContext) (bool, error) {
			return framework.ClusterReady(ctx, c, key)
		}).WithContext(ctx).WithTimeout(5*time.Minute).WithPolling(statePoll).Should(BeTrue(), "cluster ready")

		podKey := types.NamespacedName{Namespace: ns.Name, Name: key.Name + "-1"}
		reader := framework.NewCollectorReader(cfg)
		var firstOK time.Time
		Eventually(func(ctx SpecContext) (bool, error) {
			lines, err := reader.Since(ctx, time.Time{})
			if err != nil {
				return false, err
			}
			p, ok := otlp.Last(otlp.ForResource(lines, "k8s.pod.name", podKey.Name), "cnpg_otel_exports_total",
				map[string]string{"outcome": "ok"})
			firstOK = p.Time

			return ok, nil
		}).WithContext(ctx).WithTimeout(3*time.Minute).WithPolling(statePoll).Should(BeTrue(), "first export")
		Expect(framework.WaitUntil(ctx, firstOK.Add(warmup))).To(Succeed())

		var pod corev1.Pod
		Expect(c.Get(ctx, podKey, &pod)).To(Succeed())
		before := framework.SidecarRestarts(&pod)

		start := time.Now()
		readings, err := framework.SampleUsage(ctx, cfg, []types.NamespacedName{podKey}, metadata.SidecarContainerName, usageEvery, usageWindow)
		Expect(err).NotTo(HaveOccurred())
		Expect(c.Get(ctx, podKey, &pod)).To(Succeed())

		lines, err := reader.Since(ctx, start)
		Expect(err).NotTo(HaveOccurred())
		lines = otlp.ForResource(lines, "k8s.pod.name", podKey.Name)
		series, ok := otlp.Last(lines, "cnpg_otel_series", nil)
		Expect(ok).To(BeTrue(), "cnpg_otel_series")

		m := sidecarMeasurement{
			CNPGVersion: env.CNPGVersion, Series: series.Value, SkippedRuns: counterDelta(lines, "cnpg_otel_query_runs_total", "skipped"), Samples: len(readings),
			RestartsBefore: before, RestartsAfter: framework.SidecarRestarts(&pod),
		}
		var cpuSum uint64
		for _, u := range readings {
			m.WorkingSetSeries = append(m.WorkingSetSeries, u.WorkingSet)
			m.RSSSeries = append(m.RSSSeries, u.RSS)
			m.CPUSeries = append(m.CPUSeries, u.CPUNanoCores)
			cpuSum += u.CPUNanoCores
		}
		m.WorkingSetBytes, m.RSSBytes = framework.StatOf(m.WorkingSetSeries), framework.StatOf(m.RSSSeries)
		m.MeanCPUNanoCores = cpuSum / uint64(len(readings))

		data, err := json.MarshalIndent(m, "", "  ")
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(filepath.Join(out, "sidecar-"+env.CNPGVersion+".json"), append(data, '\n'), 0o644)).To(Succeed()) //nolint:gosec // a report file
		AddReportEntry("sidecar", string(data))

		Expect(m.WorkingSetBytes.Max).To(BeNumerically("<=", sidecarWorkingSetMax), "max working set")
		Expect(m.MeanCPUNanoCores).To(BeNumerically("<=", sidecarCPUMax), "mean CPU")
		Expect(m.RestartsAfter).To(Equal(m.RestartsBefore), "sidecar restarts")
	}, SpecTimeout(20*time.Minute))
})
