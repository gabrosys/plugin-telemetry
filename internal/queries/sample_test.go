package queries_test

import (
	"math"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
)

func rowDef(metrics string) queries.Definition {
	GinkgoHelper()

	return defineOne("stats", "stats:\n  query: SELECT 1\n  metrics:\n"+metrics, opts)
}

var _ = Describe("RowSamples", func() {
	It("gives NaN for a NULL COUNTER or GAUGE", func() {
		def := rowDef("    - c: {usage: COUNTER}\n    - g: {usage: GAUGE}\n")
		samples, dropped := def.RowSamples([]string{"c", "g"}, []any{nil, nil}, nil, 0)
		Expect(dropped).To(BeEmpty())
		Expect(samples).To(HaveLen(2))
		for _, s := range samples {
			Expect(math.IsNaN(s.Value)).To(BeTrue())
		}
	})

	It("fills metric, description, kind and value", func() {
		def := rowDef("    - c: {usage: COUNTER, description: total}\n    - g: {usage: GAUGE, name: gg}\n")
		samples, dropped := def.RowSamples([]string{"c", "g"}, []any{int64(3), 2.5}, nil, 0)
		Expect(dropped).To(BeEmpty())
		Expect(samples).To(ConsistOf(
			queries.Sample{Metric: "cnpg_stats_c", Description: "total", Kind: queries.KindCounter, Value: 3},
			queries.Sample{Metric: "cnpg_stats_gg", Kind: queries.KindGauge, Value: 2.5},
		))
	})

	It("drops a value that can't be converted and keeps the other samples", func() {
		def := rowDef("    - c: {usage: COUNTER}\n    - g: {usage: GAUGE}\n")
		samples, dropped := def.RowSamples([]string{"c", "g"}, []any{"abc", 1.0}, nil, 0)
		Expect(samples).To(HaveLen(1))
		Expect(samples[0].Metric).To(Equal("cnpg_stats_g"))
		Expect(dropped).To(Equal(map[queries.DropReason]int{queries.DropConversion: 1}))
	})

	It("maps MAPPEDMETRIC text through the mapping and drops unmapped text or a non-string", func() {
		def := rowDef("    - m: {usage: MAPPEDMETRIC, metric_mapping: {up: 1, down: 0}}\n")
		columns := []string{"m"}

		samples, dropped := def.RowSamples(columns, []any{"up"}, nil, 0)
		Expect(dropped).To(BeEmpty())
		Expect(samples).To(HaveLen(1))
		Expect(samples[0].Value).To(Equal(1.0))
		Expect(samples[0].Kind).To(Equal(queries.KindGauge))

		for _, value := range []any{"sideways", int64(1), nil} {
			samples, dropped = def.RowSamples(columns, []any{value}, nil, 0)
			Expect(samples).To(BeEmpty())
			Expect(dropped).To(Equal(map[queries.DropReason]int{queries.DropConversion: 1}))
		}
	})

	It("converts DURATION Go duration text to whole milliseconds", func() {
		def := rowDef("    - t: {usage: DURATION}\n")
		for value, want := range map[any]float64{"2s": 2000, "1.5ms": 1, "0s": 0} {
			samples, dropped := def.RowSamples([]string{"t"}, []any{value}, nil, 0)
			Expect(dropped).To(BeEmpty())
			Expect(samples).To(HaveLen(1))
			Expect(samples[0].Metric).To(Equal("cnpg_stats_t_milliseconds"))
			Expect(samples[0].Value).To(Equal(want))
		}

		samples, dropped := def.RowSamples([]string{"t"}, []any{[]byte("2s")}, nil, 0)
		Expect(dropped).To(BeEmpty())
		Expect(samples[0].Value).To(Equal(2000.0))
	})

	It("drops DURATION NULL, -1, and interval text such as 00:00:01", func() {
		def := rowDef("    - t: {usage: DURATION}\n")
		for _, value := range []any{nil, "-1", "00:00:01", int64(5)} {
			samples, dropped := def.RowSamples([]string{"t"}, []any{value}, nil, 0)
			Expect(samples).To(BeEmpty())
			Expect(dropped).To(Equal(map[queries.DropReason]int{queries.DropConversion: 1}))
		}
	})

	It("matches labels to columns by name in any column order", func() {
		def := rowDef("    - b: {usage: LABEL}\n    - v: {usage: GAUGE}\n    - a: {usage: LABEL}\n")
		samples, dropped := def.RowSamples([]string{"a", "v", "b"}, []any{"x", 1.0, int64(7)}, nil, 0)
		Expect(dropped).To(BeEmpty())
		Expect(samples).To(HaveLen(1))
		Expect(samples[0].Labels).To(Equal([]queries.Label{{Key: "a", Value: "x"}, {Key: "b", Value: "7"}}))
	})

	It("drops the row when a label can't become text", func() {
		def := rowDef("    - l: {usage: LABEL}\n    - v: {usage: GAUGE}\n")
		samples, dropped := def.RowSamples([]string{"l", "v"}, []any{struct{}{}, 1.0}, nil, 0)
		Expect(samples).To(BeEmpty())
		Expect(dropped).To(Equal(map[queries.DropReason]int{queries.DropLabel: 1}))
	})

	It("drops the row when a label exceeds maxLabelValueLength", func() {
		def := rowDef("    - l: {usage: LABEL}\n    - v: {usage: GAUGE}\n")
		long := strings.Repeat("x", 5)

		samples, dropped := def.RowSamples([]string{"l", "v"}, []any{long, 1.0}, nil, 4)
		Expect(samples).To(BeEmpty())
		Expect(dropped).To(Equal(map[queries.DropReason]int{queries.DropLabelTooLong: 1}))

		samples, dropped = def.RowSamples([]string{"l", "v"}, []any{long, 1.0}, nil, 5)
		Expect(samples).To(HaveLen(1))
		Expect(dropped).To(BeEmpty())

		samples, _ = def.RowSamples([]string{"l", "v"}, []any{long, 1.0}, nil, 0)
		Expect(samples).To(HaveLen(1))
	})

	It("emits nothing for LABEL and DISCARD columns", func() {
		def := rowDef("    - l: {usage: LABEL}\n    - d: {usage: DISCARD}\n")
		samples, dropped := def.RowSamples([]string{"l", "d"}, []any{"x", int64(1)}, nil, 0)
		Expect(samples).To(BeEmpty())
		Expect(dropped).To(BeEmpty())
	})

	It("emits nothing for a column outside its pg_version range, and still counts it in CheckColumns", func() {
		def := rowDef("    - old: {usage: GAUGE, pg_version: \"<17.0.0\"}\n    - new: {usage: GAUGE, pg_version: \">=17.0.0\"}\n")
		columns := []string{"old", "new"}
		Expect(def.CheckColumns(columns)).To(Succeed())

		samples, _ := def.RowSamples(columns, []any{1.0, 2.0}, semver.MustParse("16.4.0"), 0)
		Expect(samples).To(HaveLen(1))
		Expect(samples[0].Metric).To(Equal("cnpg_stats_old"))

		samples, _ = def.RowSamples(columns, []any{1.0, 2.0}, semver.MustParse("17.0.0"), 0)
		Expect(samples).To(HaveLen(1))
		Expect(samples[0].Metric).To(Equal("cnpg_stats_new"))

		samples, _ = def.RowSamples(columns, []any{1.0, 2.0}, nil, 0)
		Expect(samples).To(HaveLen(2))
	})

	It("reports ErrColumnMismatch when the result has a different number of columns", func() {
		def := rowDef("    - l: {usage: LABEL}\n    - v: {usage: GAUGE}\n")
		Expect(def.CheckColumns([]string{"l", "v"})).To(Succeed())
		Expect(def.CheckColumns([]string{"l"})).To(MatchError(queries.ErrColumnMismatch))
		Expect(def.CheckColumns([]string{"l", "v", "extra"})).To(MatchError(queries.ErrColumnMismatch))
	})

	Describe("HISTOGRAM", func() {
		def := func() queries.Definition {
			return rowDef("    - x: {usage: HISTOGRAM, description: latency}\n    - l: {usage: LABEL}\n")
		}
		columns := []string{"l", "x", "x_bucket", "x_sum", "x_count"}

		It("builds a histogram from text arrays as CNPG does", func() {
			d := def()
			samples, dropped := d.RowSamples(columns, []any{"a", "{1,2,5}", "{3,5,6}", 10.5, int64(7)}, nil, 0)
			Expect(dropped).To(BeEmpty())
			Expect(samples).To(HaveLen(1))
			Expect(samples[0].Metric).To(Equal("cnpg_stats_x"))
			Expect(samples[0].Description).To(Equal("latency"))
			Expect(samples[0].Kind).To(Equal(queries.KindHistogram))
			Expect(samples[0].Labels).To(Equal([]queries.Label{{Key: "l", Value: "a"}}))
			Expect(samples[0].Histogram).To(Equal(&queries.Histogram{
				Bounds: []float64{1, 2, 5}, Cumulative: []uint64{3, 5, 6}, Count: 7, Sum: 10.5,
			}))
		})

		It("accepts []byte arrays and sorts bounds ascending with their counts", func() {
			d := def()
			samples, _ := d.RowSamples(columns, []any{"a", []byte("{5,1,2}"), []byte("{6,3,5}"), 10.5, int64(7)}, nil, 0)
			Expect(samples).To(HaveLen(1))
			Expect(samples[0].Histogram.Bounds).To(Equal([]float64{1, 2, 5}))
			Expect(samples[0].Histogram.Cumulative).To(Equal([]uint64{3, 5, 6}))
		})

		It("ignores bounds without a count and turns a negative count into 0", func() {
			d := def()
			samples, dropped := d.RowSamples(columns, []any{"a", "{1,2,5}", "{3,-4}", 1.0, int64(7)}, nil, 0)
			Expect(dropped).To(BeEmpty())
			Expect(samples[0].Histogram.Bounds).To(Equal([]float64{1, 2}))
			Expect(samples[0].Histogram.Cumulative).To(Equal([]uint64{3, 0}))
		})

		It("ignores counts without a bound", func() {
			d := def()
			samples, _ := d.RowSamples(columns, []any{"a", "{1}", "{3,4}", 1.0, int64(7)}, nil, 0)
			Expect(samples[0].Histogram.Bounds).To(Equal([]float64{1}))
			Expect(samples[0].Histogram.Cumulative).To(Equal([]uint64{3}))
		})

		It("takes Sum and Count the way CNPG converts them", func() {
			d := def()
			samples, dropped := d.RowSamples(columns, []any{"a", "{1}", "{3}", nil, nil}, nil, 0)
			Expect(dropped).To(BeEmpty())
			Expect(math.IsNaN(samples[0].Histogram.Sum)).To(BeTrue())
			Expect(samples[0].Histogram.Count).To(BeZero())
		})

		DescribeTable("drops the sample as a conversion failure",
			func(values []any) {
				d := def()
				samples, dropped := d.RowSamples(columns, values, nil, 0)
				Expect(samples).To(BeEmpty())
				Expect(dropped).To(Equal(map[queries.DropReason]int{queries.DropConversion: 1}))
			},
			Entry("NULL bounds", []any{"a", nil, "{3}", 1.0, int64(1)}),
			Entry("NULL counts", []any{"a", "{1}", nil, 1.0, int64(1)}),
			Entry("malformed bounds", []any{"a", "{1,", "{3}", 1.0, int64(1)}),
			Entry("malformed counts", []any{"a", "{1}", "{x}", 1.0, int64(1)}),
			Entry("bad sum", []any{"a", "{1}", "{3}", "abc", int64(1)}),
			Entry("bad count", []any{"a", "{1}", "{3}", 1.0, "abc"}),
			Entry("NaN bound", []any{"a", "{1,NaN}", "{3,4}", 1.0, int64(1)}),
			Entry("repeated NaN bounds", []any{"a", "{NaN,NaN}", "{3,4}", 1.0, int64(1)}),
			Entry("two-dimensional bounds", []any{"a", "{{1,2},{3,4}}", "{1,2,3,4}", 1.0, int64(1)}),
			Entry("two-dimensional counts", []any{"a", "{1,2,3,4}", "{{1,2},{3,4}}", 1.0, int64(1)}),
		)

		It("drops the sample when x or x_bucket is not in the result", func() {
			d := def()
			samples, dropped := d.RowSamples([]string{"l", "x", "x_sum", "x_count"}, []any{"a", "{1}", 1.0, int64(1)}, nil, 0)
			Expect(samples).To(BeEmpty())
			Expect(dropped).To(Equal(map[queries.DropReason]int{queries.DropConversion: 1}))

			samples, dropped = d.RowSamples([]string{"l", "x_bucket", "x_sum", "x_count"}, []any{"a", "{1}", 1.0, int64(1)}, nil, 0)
			Expect(samples).To(BeEmpty())
			Expect(dropped).To(Equal(map[queries.DropReason]int{queries.DropConversion: 1}))
		})
	})

	It("accepts time values as labels and values", func() {
		def := rowDef("    - l: {usage: LABEL}\n    - v: {usage: GAUGE}\n")
		at := time.Unix(1700000000, 0)
		samples, _ := def.RowSamples([]string{"l", "v"}, []any{at, at}, nil, 0)
		Expect(samples[0].Labels[0].Value).To(Equal("1700000000"))
		Expect(samples[0].Value).To(Equal(1700000000.0))
	})
})
