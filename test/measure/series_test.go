//go:build e2e

package measure_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/internal/metadata"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

const (
	seriesFloor       = 48000
	seriesWarmupExtra = 2 * time.Minute
	sidecarMemory     = 67108864 // 64Mi, the default sidecar limit
	syntheticQueries  = 5
	syntheticSQL      = "SELECT 'schema_' || (i % 50) AS schemaname, 'relation_' || lpad(i::text, 8, '0') AS relname, " +
		"i::float8 AS seq_scan, (i * 2)::float8 AS n_live_tup FROM generate_series(1, 4800) AS i"
)

// seriesMeasurement is series-<cnpg>.json.
type seriesMeasurement struct {
	CNPGVersion           string         `json:"cnpgVersion"`
	SeriesAtStart         float64        `json:"seriesAtStart"`
	SeriesAtEnd           float64        `json:"seriesAtEnd"`
	WorkingSetBytes       framework.Stat `json:"workingSetBytes"`
	RSSBytes              framework.Stat `json:"rssBytes"`
	MeanCPUNanoCores      uint64         `json:"meanCPUNanoCores"`
	ExportErrors          float64        `json:"exportErrors"`
	Samples               int            `json:"samples"`
	WorkingSetSeries      []uint64       `json:"workingSetSeries"`
	RSSSeries             []uint64       `json:"rssSeries"`
	CPUSeries             []uint64       `json:"cpuNanoCoresSeries"`
	RestartsBefore        int32          `json:"restartsBefore"`
	RestartsAfter         int32          `json:"restartsAfter"`
	LastTerminationReason string         `json:"lastTerminationReason"`
}

// syntheticSet is a MetricQuerySet of five queries of 9600 series each.
func syntheticSet(namespace string) *v1alpha1.MetricQuerySet {
	queries := map[string]v1alpha1.Query{}
	for i := range syntheticQueries {
		queries[fmt.Sprintf("synthetic_%d", i+1)] = v1alpha1.Query{
			Query: syntheticSQL,
			Metrics: []v1alpha1.MetricColumn{
				{"schemaname": {Usage: "LABEL"}},
				{"relname": {Usage: "LABEL"}},
				{"seq_scan": {Usage: "COUNTER"}},
				{"n_live_tup": {Usage: "GAUGE"}},
			},
		}
	}
	set := &v1alpha1.MetricQuerySet{Spec: v1alpha1.MetricQuerySetSpec{Queries: queries}}
	set.Namespace, set.Name = namespace, "synthetic"

	return set
}

// sidecarTermination is the last termination reason of the sidecar, empty when it never terminated.
func sidecarTermination(pod *corev1.Pod) string {
	for _, st := range pod.Status.InitContainerStatuses {
		if st.Name == metadata.SidecarContainerName && st.LastTerminationState.Terminated != nil {
			return st.LastTerminationState.Terminated.Reason
		}
	}

	return ""
}

var _ = Describe("the telemetry container at maxSeriesTotal 50000", Ordered, func() {
	It("holds 48000 to 50000 series within the default 64Mi limit", func(ctx SpecContext) {
		Expect(settings.Defaults().Limits.MaxSeriesTotal).To(Equal(50000))
		env, out, c, cfg := measureEnv(ctx)

		ns, err := framework.CreateNamespace(ctx, c, "measure-series")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			Expect(client.IgnoreNotFound(c.Delete(ctx, ns))).To(Succeed())
		})

		Expect(c.Create(ctx, syntheticSet(ns.Name))).To(Succeed())
		exporter := measuredExporter(ns.Name)
		exporter.Spec.Metrics.QuerySets = []string{"synthetic"}
		Expect(c.Create(ctx, exporter)).To(Succeed())
		key := client.ObjectKey{Namespace: ns.Name, Name: "series"}
		Expect(c.Create(ctx, framework.PeerCluster(ns.Name, key.Name, env.PostgresImage))).To(Succeed())
		Eventually(func(ctx SpecContext) (bool, error) {
			return framework.ClusterReady(ctx, c, key)
		}).WithContext(ctx).WithTimeout(5*time.Minute).WithPolling(statePoll).Should(BeTrue(), "cluster ready")

		podKey := types.NamespacedName{Namespace: ns.Name, Name: key.Name + "-1"}
		reader := framework.NewCollectorReader(cfg)
		Eventually(func(ctx SpecContext) (bool, error) {
			lines, err := reader.Since(ctx, time.Time{})
			if err != nil {
				return false, err
			}
			p, ok := otlp.Last(otlp.ForResource(lines, "k8s.pod.name", podKey.Name), "cnpg_otel_series", nil)

			return ok && p.Value >= seriesFloor, nil
		}).WithContext(ctx).WithTimeout(10*time.Minute).WithPolling(statePoll).Should(BeTrue(), "series reach 48000")
		Expect(framework.WaitUntil(ctx, time.Now().Add(seriesWarmupExtra))).To(Succeed())

		var pod corev1.Pod
		Expect(c.Get(ctx, podKey, &pod)).To(Succeed())
		before := framework.SidecarRestarts(&pod)

		start := time.Now()
		readings, sampleErr := framework.SampleUsage(ctx, cfg, []types.NamespacedName{podKey}, metadata.SidecarContainerName, usageEvery, usageWindow)
		Expect(c.Get(ctx, podKey, &pod)).To(Succeed())

		lines, err := reader.Since(ctx, start)
		Expect(err).NotTo(HaveOccurred())
		lines = otlp.ForResource(lines, "k8s.pod.name", podKey.Name)
		points := otlp.Points(lines, "cnpg_otel_series")

		m := seriesMeasurement{
			CNPGVersion:  env.CNPGVersion,
			ExportErrors: counterDelta(lines, "cnpg_otel_exports_total", "error"), Samples: len(readings),
			RestartsBefore: before, RestartsAfter: framework.SidecarRestarts(&pod), LastTerminationReason: sidecarTermination(&pod),
		}
		if len(points) > 0 {
			m.SeriesAtStart, m.SeriesAtEnd = points[0].Value, points[len(points)-1].Value
		}
		var cpuSum uint64
		for _, u := range readings {
			m.WorkingSetSeries = append(m.WorkingSetSeries, u.WorkingSet)
			m.RSSSeries = append(m.RSSSeries, u.RSS)
			m.CPUSeries = append(m.CPUSeries, u.CPUNanoCores)
			cpuSum += u.CPUNanoCores
		}
		if len(readings) > 0 {
			m.WorkingSetBytes, m.RSSBytes = framework.StatOf(m.WorkingSetSeries), framework.StatOf(m.RSSSeries)
			m.MeanCPUNanoCores = cpuSum / uint64(len(readings))
		}

		writeMeasurement(out, "series", env.CNPGVersion, m)

		Expect(sampleErr).NotTo(HaveOccurred(), "sampling usage")
		Expect(m.SeriesAtStart).To(BeNumerically(">=", seriesFloor), "series at the start")
		Expect(m.SeriesAtEnd).To(BeNumerically("~", 49000, 1000), "series at the end")
		Expect(m.WorkingSetBytes.Max).To(BeNumerically("<", sidecarMemory), "max working set")
		Expect(m.RestartsAfter).To(Equal(m.RestartsBefore), "sidecar restarts")
		Expect(m.LastTerminationReason).NotTo(Equal("OOMKilled"), "last termination reason")
		Expect(m.ExportErrors).To(BeZero(), "export errors")
	}, SpecTimeout(30*time.Minute))
})
