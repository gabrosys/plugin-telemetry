package export_test

import (
	"context"
	"math"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/snapshot"
	"github.com/gabrosys/plugin-telemetry/internal/version"
)

func drops(query string, reason queries.DropReason, n int) map[snapshot.Key]map[queries.DropReason]int {
	return map[snapshot.Key]map[queries.DropReason]int{{Query: query, Database: "app"}: {reason: n}}
}

var _ = Describe("Producer", func() {
	var (
		store *snapshot.Store
		now   time.Time
		seen  map[snapshot.Key]map[queries.DropReason]int
		prod  *export.Producer
		obs   time.Time
	)

	put := func(q string, samples ...queries.Sample) {
		Expect(store.Put(snapshot.Key{Query: q, Database: "app"}, "fp", samples, obs)).To(Succeed())
	}
	produce := func() []metricdata.Metrics {
		sms, err := prod.Produce(context.Background())
		Expect(err).NotTo(HaveOccurred())
		if len(sms) == 0 {
			return nil
		}
		Expect(sms).To(HaveLen(1))

		return sms[0].Metrics
	}

	BeforeEach(func() {
		store = snapshot.New(0)
		obs = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		now = obs.Add(time.Minute)
		seen = nil
		prod = export.NewProducer(store, clocktesting.NewFakePassiveClock(now), func(d map[snapshot.Key]map[queries.DropReason]int) { seen = d }, nil)
	})

	It("exports COUNTER as a monotonic cumulative Sum, GAUGE, MAPPEDMETRIC and DURATION as a Gauge, HISTOGRAM as a cumulative Histogram", func() {
		put("q",
			queries.Sample{Metric: "c_total", Kind: queries.KindCounter, Value: 4, Labels: []queries.Label{{Key: "k", Value: "v"}}},
			queries.Sample{Metric: "g", Kind: queries.KindGauge, Value: 2.5},
			queries.Sample{Metric: "mapped", Kind: queries.KindGauge, Value: 1},
			queries.Sample{Metric: "elapsed_milliseconds", Kind: queries.KindGauge, Value: 1500},
			queries.Sample{Metric: "h", Kind: queries.KindHistogram, Histogram: &queries.Histogram{Bounds: []float64{1}, Cumulative: []uint64{1}, Count: 2, Sum: 3}},
		)

		ms := produce()

		Expect(ms).To(HaveLen(5))
		byName := map[string]metricdata.Metrics{}
		for _, m := range ms {
			byName[m.Name] = m
		}
		sum, ok := byName["c_total"].Data.(metricdata.Sum[float64])
		Expect(ok).To(BeTrue())
		Expect(sum.IsMonotonic).To(BeTrue())
		Expect(sum.Temporality).To(Equal(metricdata.CumulativeTemporality))
		Expect(sum.DataPoints).To(HaveLen(1))
		Expect(sum.DataPoints[0].Value).To(Equal(4.0))
		v, found := sum.DataPoints[0].Attributes.Value("k")
		Expect(found).To(BeTrue())
		Expect(v.AsString()).To(Equal("v"))

		g, ok := byName["g"].Data.(metricdata.Gauge[float64])
		Expect(ok).To(BeTrue())
		Expect(g.DataPoints[0].Value).To(Equal(2.5))
		for _, name := range []string{"mapped", "elapsed_milliseconds"} {
			_, ok := byName[name].Data.(metricdata.Gauge[float64])
			Expect(ok).To(BeTrue(), name)
		}

		h, ok := byName["h"].Data.(metricdata.Histogram[float64])
		Expect(ok).To(BeTrue())
		Expect(h.Temporality).To(Equal(metricdata.CumulativeTemporality))
		Expect(h.DataPoints[0].Count).To(Equal(uint64(2)))
		Expect(h.DataPoints[0].Sum).To(Equal(3.0))
	})

	It("stamps every point with the collection time and counters with their snapshot Start", func() {
		put("q",
			queries.Sample{Metric: "c_total", Kind: queries.KindCounter, Value: 1},
			queries.Sample{Metric: "g", Kind: queries.KindGauge, Value: 1},
			queries.Sample{Metric: "h", Kind: queries.KindHistogram, Histogram: &queries.Histogram{Count: 0}},
		)

		for _, m := range produce() {
			switch d := m.Data.(type) {
			case metricdata.Sum[float64]:
				Expect(d.DataPoints[0].Time).To(Equal(now))
				Expect(d.DataPoints[0].StartTime).To(Equal(obs))
			case metricdata.Gauge[float64]:
				Expect(d.DataPoints[0].Time).To(Equal(now))
				Expect(d.DataPoints[0].StartTime.IsZero()).To(BeTrue())
			case metricdata.Histogram[float64]:
				Expect(d.DataPoints[0].Time).To(Equal(now))
				Expect(d.DataPoints[0].StartTime).To(Equal(obs))
			}
		}
	})

	DescribeTable("converts histograms",
		func(h queries.Histogram, bounds []float64, buckets []uint64) {
			put("q", queries.Sample{Metric: "h", Kind: queries.KindHistogram, Histogram: &h})

			hd, ok := produce()[0].Data.(metricdata.Histogram[float64])
			Expect(ok).To(BeTrue())
			dp := hd.DataPoints[0]

			Expect(dp.Bounds).To(Equal(bounds))
			Expect(dp.BucketCounts).To(Equal(buckets))
			Expect(dp.BucketCounts).To(HaveLen(len(dp.Bounds) + 1))
			Expect(dp.Count).To(Equal(h.Count))
		},
		Entry("converts cumulative bucket counts to per-bucket counts",
			queries.Histogram{Bounds: []float64{1, 2, 5}, Cumulative: []uint64{3, 5, 6}, Count: 7, Sum: 1},
			[]float64{1, 2, 5}, []uint64{3, 2, 1, 1}),
		Entry("drops an explicit +Inf bound",
			queries.Histogram{Bounds: []float64{1, math.Inf(1)}, Cumulative: []uint64{3, 7}, Count: 7},
			[]float64{1}, []uint64{3, 4}),
		Entry("clamps the overflow bucket at 0",
			queries.Histogram{Bounds: []float64{1, math.Inf(1)}, Cumulative: []uint64{3, 7}, Count: 2},
			[]float64{1}, []uint64{3, 0}),
		Entry("clamps a decreasing cumulative at 0",
			queries.Histogram{Bounds: []float64{1, 2}, Cumulative: []uint64{5, 3}, Count: 5},
			[]float64{1, 2}, []uint64{5, 0, 2}),
	)

	It("keeps empty units and the YAML descriptions", func() {
		put("q", queries.Sample{Metric: "g", Description: "a gauge", Kind: queries.KindGauge, Value: 1})

		ms := produce()

		Expect(ms[0].Unit).To(BeEmpty())
		Expect(ms[0].Description).To(Equal("a gauge"))
	})

	It("puts the snapshot under scope github.com/gabrosys/plugin-telemetry with the build version", func() {
		put("q", queries.Sample{Metric: "g", Kind: queries.KindGauge, Value: 1})

		sms, err := prod.Produce(context.Background())

		Expect(err).NotTo(HaveOccurred())
		Expect(sms).To(HaveLen(1))
		Expect(sms[0].Scope.Name).To(Equal("github.com/gabrosys/plugin-telemetry"))
		Expect(sms[0].Scope.Name).To(Equal(export.ScopeName))
		Expect(sms[0].Scope.Version).To(Equal(version.Version))
	})

	It("keeps the first kind when two queries give one metric name two kinds, and reports the rest", func() {
		put("a", queries.Sample{Metric: "m", Description: "first", Kind: queries.KindGauge, Value: 1})
		put("b", queries.Sample{Metric: "m", Description: "second", Kind: queries.KindCounter, Value: 2, Labels: []queries.Label{{Key: "x", Value: "1"}}},
			queries.Sample{Metric: "m", Kind: queries.KindGauge, Value: 3, Labels: []queries.Label{{Key: "x", Value: "2"}}})

		ms := produce()

		Expect(ms).To(HaveLen(1))
		Expect(ms[0].Description).To(Equal("first"))
		g, ok := ms[0].Data.(metricdata.Gauge[float64])
		Expect(ok).To(BeTrue())
		Expect(g.DataPoints).To(HaveLen(2))
		Expect(seen).To(Equal(drops("b", queries.DropDuplicate, 1)))
	})

	It("merges snapshot duplicates and kind conflicts into one onDrops call", func() {
		put("a", queries.Sample{Metric: "m", Kind: queries.KindGauge, Value: 1})
		put("b", queries.Sample{Metric: "m", Kind: queries.KindGauge, Value: 1}, queries.Sample{Metric: "m", Kind: queries.KindCounter, Value: 1, Labels: []queries.Label{{Key: "x", Value: "1"}}})

		produce()

		Expect(seen).To(Equal(drops("b", queries.DropDuplicate, 2)))
	})

	It("counts a histogram without data and an unknown kind as conversion drops", func() {
		put("q",
			queries.Sample{Metric: "h", Kind: queries.KindHistogram},
			queries.Sample{Metric: "odd", Kind: queries.Kind(99), Value: 1},
			queries.Sample{Metric: "g", Kind: queries.KindGauge, Value: 1},
		)

		ms := produce()

		Expect(ms).To(HaveLen(1))
		Expect(ms[0].Name).To(Equal("g"))
		Expect(seen).To(Equal(drops("q", queries.DropConversion, 2)))
	})

	It("accepts a nil onDrops", func() {
		p := export.NewProducer(store, clocktesting.NewFakePassiveClock(now), nil, nil)
		put("a", queries.Sample{Metric: "m", Kind: queries.KindGauge, Value: 1})
		put("b", queries.Sample{Metric: "m", Kind: queries.KindGauge, Value: 1})

		_, err := p.Produce(context.Background())

		Expect(err).NotTo(HaveOccurred())
	})

	It("returns no error and no data for an empty snapshot", func() {
		sms, err := prod.Produce(context.Background())

		Expect(err).NotTo(HaveOccurred())
		Expect(sms).To(BeEmpty())
		Expect(seen).To(BeNil())
	})

	It("builds one attribute set per point from its labels", func() {
		put("q", queries.Sample{Metric: "g", Kind: queries.KindGauge, Value: 1, Labels: []queries.Label{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}})

		gd, ok := produce()[0].Data.(metricdata.Gauge[float64])
		Expect(ok).To(BeTrue())
		dp := gd.DataPoints[0]

		Expect(dp.Attributes).To(Equal(attribute.NewSet(attribute.String("a", "1"), attribute.String("b", "2"))))
	})

	Describe("with extra samples", func() {
		pgWAL := func(value string, v float64) queries.Sample {
			return queries.Sample{
				Metric: "cnpg_collector_pg_wal", Description: queries.PGWALHelp, Kind: queries.KindGauge, Value: v,
				Labels: []queries.Label{{Key: "value", Value: value}},
			}
		}
		withExtra := func(extra func() []queries.Sample) *export.Producer {
			return export.NewProducer(store, clocktesting.NewFakePassiveClock(now), func(d map[snapshot.Key]map[queries.DropReason]int) { seen = d }, extra)
		}

		It("joins them to the snapshot's points of the same name", func() {
			var samples []queries.Sample
			for i, v := range []string{"count", "size", "min", "max", "keep", "slots_max"} {
				samples = append(samples, pgWAL(v, float64(i)))
			}
			put("collector_pg_wal", samples...)
			prod = withExtra(func() []queries.Sample {
				return []queries.Sample{pgWAL("volume_size", 1073741824), pgWAL("volume_max", 64)}
			})

			ms := produce()

			Expect(ms).To(HaveLen(1))
			Expect(ms[0].Name).To(Equal("cnpg_collector_pg_wal"))
			Expect(ms[0].Description).To(Equal(queries.PGWALHelp))
			g, ok := ms[0].Data.(metricdata.Gauge[float64])
			Expect(ok).To(BeTrue())
			Expect(g.DataPoints).To(HaveLen(8))
			values := map[string]float64{}
			for _, dp := range g.DataPoints {
				Expect(dp.Time).To(Equal(now))
				v, _ := dp.Attributes.Value("value")
				values[v.AsString()] = dp.Value
			}
			Expect(values).To(HaveKeyWithValue("volume_size", 1073741824.0))
			Expect(values).To(HaveKeyWithValue("volume_max", 64.0))
			Expect(values).To(HaveKeyWithValue("count", 0.0))
			Expect(seen).To(BeNil())
		})

		It("exports them without a snapshot", func() {
			prod = withExtra(func() []queries.Sample { return []queries.Sample{pgWAL("volume_size", 1)} })

			ms := produce()

			Expect(ms).To(HaveLen(1))
			Expect(ms[0].Name).To(Equal("cnpg_collector_pg_wal"))
		})

		It("keeps the first of repeated extra samples", func() {
			prod = withExtra(func() []queries.Sample {
				return []queries.Sample{pgWAL("volume_size", 1), pgWAL("volume_size", 2), pgWAL("volume_max", 3)}
			})

			ms := produce()

			Expect(ms).To(HaveLen(1))
			g, ok := ms[0].Data.(metricdata.Gauge[float64])
			Expect(ok).To(BeTrue())
			Expect(g.DataPoints).To(HaveLen(2))
			values := map[string]float64{}
			for _, dp := range g.DataPoints {
				v, _ := dp.Attributes.Value("value")
				values[v.AsString()] = dp.Value
			}
			Expect(values).To(Equal(map[string]float64{"volume_size": 1, "volume_max": 3}))
		})

		It("calls extra once per Produce", func() {
			calls := 0
			prod = withExtra(func() []queries.Sample { calls++; return nil })

			produce()
			produce()

			Expect(calls).To(Equal(2))
		})

		It("drops only extra's samples when it panics", func() {
			put("q", queries.Sample{Metric: "g", Kind: queries.KindGauge, Value: 1})
			prod = withExtra(func() []queries.Sample { panic("extra broke") })

			ms := produce()

			Expect(ms).To(HaveLen(1))
			Expect(ms[0].Name).To(Equal("g"))
		})

		It("keeps an extra sample over a snapshot series with its name and labels, or its name and another kind, and reports the snapshot's", func() {
			put("mine", queries.Sample{
				Metric: "cnpg_collector_pg_wal", Kind: queries.KindGauge, Value: 9,
				Labels: []queries.Label{{Key: "value", Value: "volume_size"}},
			})
			put("other", queries.Sample{Metric: "cnpg_collector_fencing_on", Kind: queries.KindCounter, Value: 9})
			prod = withExtra(func() []queries.Sample {
				return []queries.Sample{
					pgWAL("volume_size", 1),
					{Metric: "cnpg_collector_fencing_on", Kind: queries.KindGauge, Value: 0},
				}
			})

			ms := produce()

			Expect(ms).To(HaveLen(2))
			for _, m := range ms {
				g, ok := m.Data.(metricdata.Gauge[float64])
				Expect(ok).To(BeTrue(), m.Name)
				Expect(g.DataPoints).To(HaveLen(1), m.Name)
				Expect(g.DataPoints[0].Value).NotTo(Equal(9.0), m.Name)
			}
			Expect(seen).To(Equal(map[snapshot.Key]map[queries.DropReason]int{
				{Query: "mine", Database: "app"}:  {queries.DropDuplicate: 1},
				{Query: "other", Database: "app"}: {queries.DropDuplicate: 1},
			}))
		})
	})
})
