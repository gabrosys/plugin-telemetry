//go:build e2e

package measure_test

import (
	"bytes"
	"context"
	"fmt"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	machineryapi "github.com/cloudnative-pg/machinery/pkg/api"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gabrosys/plugin-telemetry/api/v1alpha1"
	"github.com/gabrosys/plugin-telemetry/test/e2e/framework"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

const (
	watchSidecars   = 10
	watchesPer      = 5 // the Cluster, the exporter, cnpg-default-monitoring, the Secret, the set
	kubeletWatches  = 1 // the kubelet's watch on the token Secret volume (watch is its default Secret detection strategy)
	watchWindow     = 30 * time.Minute
	watchOpenWithin = 5.0
	// A reflector's watch timeout is random in [5 min, 10 min] (client-go tools/cache/reflector.go:56-57).
	watchMeanRenewal = 7*time.Minute + 30*time.Second
	watchQueries     = "measure-queries"
	watchSet         = "measure-set"
	watchQueryText   = "measure_one:\n  query: SELECT 1 AS one\n  metrics:\n    - one:\n        usage: GAUGE\n        description: one\n"
)

// apiSnapshot is the API server's WATCH count and its LIST and WATCH request totals at one time.
type apiSnapshot struct {
	Open  float64 `json:"openWatches"`
	List  float64 `json:"listRequests"`
	Watch float64 `json:"watchRequests"`
}

// apiWindow is the growth of the request totals over one window, in requests.
type apiWindow struct {
	Start apiSnapshot `json:"start"`
	End   apiSnapshot `json:"end"`
	List  float64     `json:"list"`
	Watch float64     `json:"watch"`
}

// perHour is requests per sidecar per hour.
type perHour struct {
	List  float64 `json:"list"`
	Watch float64 `json:"watch"`
}

// watchesMeasurement is watches-<cnpg>.json.
type watchesMeasurement struct {
	CNPGVersion      string    `json:"cnpgVersion"`
	Sidecars         int       `json:"sidecars"`
	WindowMinutes    float64   `json:"windowMinutes"`
	Baseline         apiWindow `json:"baseline"`
	Plugin           apiWindow `json:"pluginWindow"`
	OpenIncrease     float64   `json:"openWatchIncrease"`
	ExpectedOpen     int       `json:"expectedOpenWatchIncrease"`
	ExpectedRenewals float64   `json:"expectedRenewalsPerWindow"`
	MeasuredWatch    float64   `json:"measuredWatchRequestsPerWindow"`
	PerSidecarHour   perHour   `json:"perSidecarPerHour"`
	At100            perHour   `json:"at100Instances"`
	At1000           perHour   `json:"at1000Instances"`
}

// apiMetrics reads the API server's /metrics and sums the stable request metrics by verb.
func apiMetrics(ctx context.Context, cs kubernetes.Interface) (apiSnapshot, error) {
	raw, err := cs.CoreV1().RESTClient().Get().AbsPath("/metrics").DoRaw(ctx)
	if err != nil {
		return apiSnapshot{}, fmt.Errorf("read /metrics: %w", err)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(raw))
	if err != nil {
		return apiSnapshot{}, fmt.Errorf("parse /metrics: %w", err)
	}

	var s apiSnapshot
	for _, m := range families["apiserver_longrunning_requests"].GetMetric() {
		if label(m.GetLabel(), "verb") == "WATCH" {
			s.Open += m.GetGauge().GetValue()
		}
	}
	for _, m := range families["apiserver_request_total"].GetMetric() {
		switch label(m.GetLabel(), "verb") {
		case "LIST":
			s.List += m.GetCounter().GetValue()
		case "WATCH":
			s.Watch += m.GetCounter().GetValue()
		}
	}

	return s, nil
}

// window takes a snapshot, waits watchWindow and takes the second one.
func window(ctx context.Context, cs kubernetes.Interface) apiWindow {
	GinkgoHelper()

	start, err := apiMetrics(ctx, cs)
	Expect(err).NotTo(HaveOccurred())
	Expect(framework.WaitUntil(ctx, time.Now().Add(watchWindow))).To(Succeed())
	end, err := apiMetrics(ctx, cs)
	Expect(err).NotTo(HaveOccurred())

	return apiWindow{Start: start, End: end, List: end.List - start.List, Watch: end.Watch - start.Watch}
}

var _ = Describe("the API requests of the exporters' watches", Ordered, func() {
	It("measures the API requests of ten exporters' 60 watches", func(ctx SpecContext) {
		env, out, c, cfg := measureEnv(ctx)
		cs, err := kubernetes.NewForConfig(cfg)
		Expect(err).NotTo(HaveOccurred())

		ns, err := framework.CreateNamespace(ctx, c, "measure-watches")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			Expect(framework.DeleteNamespace(ctx, c, ns.Name, 5*time.Second)).To(Succeed())
		}, NodeTimeout(15*time.Minute))

		Expect(c.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: watchQueries},
			StringData: map[string]string{"queries.yaml": watchQueryText},
		})).To(Succeed())
		Expect(c.Create(ctx, &v1alpha1.MetricQuerySet{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: watchSet},
			Spec: v1alpha1.MetricQuerySetSpec{Queries: map[string]v1alpha1.Query{"measure_one": {
				Query:   "SELECT 1 AS one",
				Metrics: []v1alpha1.MetricColumn{{"one": {Usage: "GAUGE"}}},
			}}},
		})).To(Succeed())
		exporter := measuredExporter(ns.Name)
		exporter.Spec.Metrics.QuerySets = []string{watchSet}
		Expect(c.Create(ctx, exporter)).To(Succeed())

		keys := make([]client.ObjectKey, watchSidecars)
		for i := range keys {
			keys[i] = client.ObjectKey{Namespace: ns.Name, Name: fmt.Sprintf("w-%02d", i)}
			cluster := framework.PeerCluster(ns.Name, keys[i].Name, env.PostgresImage)
			cluster.Spec.Plugins = nil
			cluster.Spec.Monitoring = &apiv1.MonitoringConfiguration{
				CustomQueriesSecret: []machineryapi.SecretKeySelector{{
					LocalObjectReference: machineryapi.LocalObjectReference{Name: watchQueries}, Key: "queries.yaml",
				}},
			}
			Expect(c.Create(ctx, cluster)).To(Succeed())
		}
		for _, key := range keys {
			Eventually(func(ctx SpecContext) (bool, error) {
				return framework.ClusterReady(ctx, c, key)
			}).WithContext(ctx).WithTimeout(10*time.Minute).WithPolling(statePoll).Should(BeTrue(), "cluster %s ready", key.Name)
		}

		baseline := window(ctx, cs)

		for _, key := range keys {
			var cluster apiv1.Cluster
			Expect(c.Get(ctx, key, &cluster)).To(Succeed())
			patch := client.MergeFrom(cluster.DeepCopy())
			cluster.Spec.Plugins = framework.PeerCluster(ns.Name, key.Name, env.PostgresImage).Spec.Plugins
			Expect(c.Patch(ctx, &cluster, patch)).To(Succeed())
		}
		reader := framework.NewCollectorReader(cfg)
		Eventually(func(ctx SpecContext) (int, error) {
			lines, err := reader.Since(ctx, time.Time{})
			if err != nil {
				return 0, err
			}
			exporting := 0
			for _, key := range keys {
				if _, ok := otlp.Last(otlp.ForResource(lines, "k8s.pod.name", key.Name+"-1"), "cnpg_otel_exports_total",
					map[string]string{"outcome": "ok"}); ok {
					exporting++
				}
			}

			return exporting, nil
		}).WithContext(ctx).WithTimeout(20*time.Minute).WithPolling(10*time.Second).Should(Equal(watchSidecars), "every sidecar exports")
		for _, key := range keys {
			Eventually(func(ctx SpecContext) (bool, error) {
				return framework.ClusterReady(ctx, c, key)
			}).WithContext(ctx).WithTimeout(5*time.Minute).WithPolling(statePoll).Should(BeTrue(), "cluster %s ready with the plugin", key.Name)
		}

		withPlugin := window(ctx, cs)

		m := watchesMeasurement{
			CNPGVersion: env.CNPGVersion, Sidecars: watchSidecars, WindowMinutes: watchWindow.Minutes(),
			Baseline: baseline, Plugin: withPlugin,
			OpenIncrease:     withPlugin.Start.Open - baseline.End.Open,
			ExpectedOpen:     watchSidecars * (watchesPer + kubeletWatches),
			ExpectedRenewals: float64(watchSidecars*(watchesPer+kubeletWatches)) * float64(watchWindow) / float64(watchMeanRenewal),
			MeasuredWatch:    withPlugin.Watch - baseline.Watch,
		}
		hours := float64(time.Hour / watchWindow)
		m.PerSidecarHour = perHour{
			List:  (withPlugin.List - baseline.List) * hours / watchSidecars,
			Watch: (withPlugin.Watch - baseline.Watch) * hours / watchSidecars,
		}
		m.At100 = perHour{List: m.PerSidecarHour.List * 100, Watch: m.PerSidecarHour.Watch * 100}
		m.At1000 = perHour{List: m.PerSidecarHour.List * 1000, Watch: m.PerSidecarHour.Watch * 1000}

		writeMeasurement(out, "watches", env.CNPGVersion, m)

		Expect(m.OpenIncrease).To(BeNumerically("~", m.ExpectedOpen, watchOpenWithin), "open watch increase")
	}, SpecTimeout(90*time.Minute))
})

// label returns the value of the label name, empty when absent.
func label(pairs []*dto.LabelPair, name string) string {
	for _, p := range pairs {
		if p.GetName() == name {
			return p.GetValue()
		}
	}

	return ""
}
