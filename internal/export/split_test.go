package export_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/gabrosys/plugin-telemetry/internal/export"
)

func gaugeMetric(name string, n int) metricdata.Metrics {
	dps := make([]metricdata.DataPoint[float64], n)
	for i := range dps {
		dps[i] = metricdata.DataPoint[float64]{Attributes: attribute.NewSet(attribute.Int("i", i)), Time: time.Unix(1, 0), Value: float64(i)}
	}

	return metricdata.Metrics{Name: name, Description: name + " d", Unit: "s", Data: metricdata.Gauge[float64]{DataPoints: dps}}
}

func gaugePoints(n int) []metricdata.DataPoint[float64] {
	g, ok := gaugeMetric("x", n).Data.(metricdata.Gauge[float64])
	if !ok {
		panic("gaugeMetric must return a Gauge")
	}

	return g.DataPoints
}

func countPoints(rm *metricdata.ResourceMetrics) int {
	n := 0
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Gauge[float64]:
				n += len(d.DataPoints)
			case metricdata.Sum[float64]:
				n += len(d.DataPoints)
			case metricdata.Histogram[float64]:
				n += len(d.DataPoints)
			}
		}
	}

	return n
}

var _ = Describe("Split", func() {
	res := resource.NewWithAttributes("", attribute.String("a", "b"))
	scope := instrumentation.Scope{Name: "s", Version: "v"}

	It("splits 2500 points into requests of 1000, 1000 and 500 without splitting a metric's identity", func() {
		rm := &metricdata.ResourceMetrics{Resource: res, ScopeMetrics: []metricdata.ScopeMetrics{{
			Scope: scope,
			Metrics: []metricdata.Metrics{
				gaugeMetric("m1", 1500),
				{Name: "m2", Description: "m2 d", Data: metricdata.Sum[float64]{Temporality: metricdata.CumulativeTemporality, IsMonotonic: true, DataPoints: gaugePoints(700)}},
				{Name: "m3", Data: metricdata.Histogram[float64]{Temporality: metricdata.CumulativeTemporality, DataPoints: make([]metricdata.HistogramDataPoint[float64], 300)}},
			},
		}}}

		out := export.Split(rm, 1000)

		Expect(out).To(HaveLen(3))
		var counts []int
		for _, r := range out {
			counts = append(counts, countPoints(r))
			Expect(r.Resource).To(BeIdenticalTo(res))
			Expect(r.ScopeMetrics).NotTo(BeEmpty())
			for _, sm := range r.ScopeMetrics {
				Expect(sm.Scope).To(Equal(scope))
			}
		}
		Expect(counts).To(Equal([]int{1000, 1000, 500}))

		first := out[0].ScopeMetrics[0].Metrics
		second := out[1].ScopeMetrics[0].Metrics
		Expect(first).To(HaveLen(1))
		Expect(second[0].Name).To(Equal("m1"))
		Expect(second[0].Description).To(Equal("m1 d"))
		Expect(second[0].Unit).To(Equal("s"))
		Expect(second[0].Data).To(BeAssignableToTypeOf(metricdata.Gauge[float64]{}))
		sum, ok := second[1].Data.(metricdata.Sum[float64])
		Expect(ok).To(BeTrue())
		Expect(sum.IsMonotonic).To(BeTrue())
		Expect(sum.Temporality).To(Equal(metricdata.CumulativeTemporality))
		Expect(fmt.Sprint(second[1].Name)).To(Equal("m2"))
	})

	It("returns nil for no data and one request when everything fits", func() {
		Expect(export.Split(&metricdata.ResourceMetrics{Resource: res}, 10)).To(BeNil())

		rm := &metricdata.ResourceMetrics{Resource: res, ScopeMetrics: []metricdata.ScopeMetrics{{Scope: scope, Metrics: []metricdata.Metrics{gaugeMetric("m", 5)}}}}
		Expect(export.Split(rm, 10)).To(HaveLen(1))
	})
})
