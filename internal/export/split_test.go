package export_test

import (
	"fmt"
	"reflect"
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

	It("keeps and splits every aggregation the SDK produces", func() {
		metrics := []metricdata.Metrics{
			{Name: "gauge-int", Data: metricdata.Gauge[int64]{DataPoints: make([]metricdata.DataPoint[int64], 2)}},
			{Name: "sum-int", Data: metricdata.Sum[int64]{
				Temporality: metricdata.CumulativeTemporality, IsMonotonic: true, DataPoints: make([]metricdata.DataPoint[int64], 3),
			}},
			{Name: "histogram-int", Data: metricdata.Histogram[int64]{DataPoints: make([]metricdata.HistogramDataPoint[int64], 2)}},
			{Name: "exponential-float", Data: metricdata.ExponentialHistogram[float64]{
				DataPoints: make([]metricdata.ExponentialHistogramDataPoint[float64], 2),
			}},
			{Name: "exponential-int", Data: metricdata.ExponentialHistogram[int64]{
				DataPoints: make([]metricdata.ExponentialHistogramDataPoint[int64], 2),
			}},
			{Name: "summary", Data: metricdata.Summary{DataPoints: make([]metricdata.SummaryDataPoint, 2)}},
		}
		rm := &metricdata.ResourceMetrics{Resource: res, ScopeMetrics: []metricdata.ScopeMetrics{{Scope: scope, Metrics: metrics}}}

		whole := export.Split(rm, 100)
		Expect(whole).To(HaveLen(1))
		Expect(whole[0].ScopeMetrics[0].Metrics).To(Equal(metrics))

		var names []string
		total := 0
		for _, r := range export.Split(rm, 2) {
			for _, m := range r.ScopeMetrics[0].Metrics {
				names = append(names, m.Name)
				n := reflect.ValueOf(m.Data).FieldByName("DataPoints").Len()
				Expect(n).To(BeNumerically("<=", 2), m.Name)
				total += n
			}
		}
		// With a cap of 2 every metric after the 3-point sum straddles two requests.
		Expect(names).To(Equal([]string{
			"gauge-int", "sum-int", "sum-int", "histogram-int", "histogram-int", "exponential-float",
			"exponential-float", "exponential-int", "exponential-int", "summary", "summary",
		}))
		Expect(total).To(Equal(13))
	})

	It("returns nil for no data and one request when everything fits", func() {
		Expect(export.Split(&metricdata.ResourceMetrics{Resource: res}, 10)).To(BeNil())

		rm := &metricdata.ResourceMetrics{Resource: res, ScopeMetrics: []metricdata.ScopeMetrics{{Scope: scope, Metrics: []metricdata.Metrics{gaugeMetric("m", 5)}}}}
		Expect(export.Split(rm, 10)).To(HaveLen(1))
	})
})
