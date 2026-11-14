package plugin_test

import (
	"bytes"
	"math"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
	"github.com/gabrosys/plugin-telemetry/test/e2e/plugin"
)

// The fixtures were recorded on kind with CNPG main: the primary plugin-peer-1's endpoint, scraped
// right after the last of its three exports in ours.jsonl.
const (
	parityPod    = "plugin-peer-1"
	parityOther  = "plugin-peer-2"
	podAttribute = `{"key":"k8s.pod.name","value":{"stringValue":"`

	xactCommit   = "cnpg_pg_stat_database_xact_commit"
	baseOne      = "cnpg_e2e_base_one"
	primaryOne   = "cnpg_e2e_primary_one"
	histogram    = "cnpg_parity_latency"
	histogramCNP = `# HELP cnpg_parity_latency Query latency.
# TYPE cnpg_parity_latency histogram
cnpg_parity_latency_bucket{op="read",le="1"} 2
cnpg_parity_latency_bucket{op="read",le="+Inf"} 5
cnpg_parity_latency_sum{op="read"} 2.5
cnpg_parity_latency_count{op="read"} 5
`
)

func parityFixture(name string) []byte {
	GinkgoHelper()

	root, err := os.OpenRoot("testdata/parity")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(root.Close)

	data, err := root.ReadFile(name)
	Expect(err).NotTo(HaveOccurred())

	return data
}

func decodeLines(data []byte) []otlp.Line {
	GinkgoHelper()

	var l otlp.Lines
	Expect(l.Update(data)).To(Succeed())

	return l.All()
}

// setSample sets the value of the one sample line of text that starts with series and a space.
func setSample(text []byte, series string, value float64) []byte {
	GinkgoHelper()

	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(series) + ` \S+$`)
	Expect(re.FindAll(text, -1)).To(HaveLen(1), "sample %s", series)

	return re.ReplaceAllLiteral(text, []byte(series+" "+strconv.FormatFloat(value, 'g', -1, 64)))
}

// replaceOnce replaces old, which must occur exactly once in text.
func replaceOnce(text []byte, old, repl string) []byte {
	GinkgoHelper()

	Expect(bytes.Count(text, []byte(old))).To(Equal(1), "%q", old)

	return bytes.Replace(text, []byte(old), []byte(repl), 1)
}

// numberPoints returns the data points of the gauge or sum called name, for editing in place.
func numberPoints(lines []otlp.Line, name string) []*metricpb.NumberDataPoint {
	GinkgoHelper()

	var out []*metricpb.NumberDataPoint
	for _, m := range metricsNamed(lines, name) {
		out = append(out, m.GetGauge().GetDataPoints()...)
		out = append(out, m.GetSum().GetDataPoints()...)
	}
	Expect(out).NotTo(BeEmpty(), name)

	return out
}

func metricsNamed(lines []otlp.Line, name string) []*metricpb.Metric {
	var out []*metricpb.Metric
	for _, l := range lines {
		for _, rm := range l.Request.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					if m.GetName() == name {
						out = append(out, m)
					}
				}
			}
		}
	}

	return out
}

// newestValue is our latest value of the series of name whose attributes include attrs.
func newestValue(lines []otlp.Line, name string, attrs map[string]string) float64 {
	GinkgoHelper()

	p, ok := otlp.Last(lines, name, attrs)
	Expect(ok).To(BeTrue(), name)

	return p.Value
}

// addHistogram adds the histogram cnpg_parity_latency{op="read"} to the last line, under our scope.
func addHistogram(lines []otlp.Line, count uint64, sum float64, attrs map[string]string) {
	GinkgoHelper()

	scopes := lines[len(lines)-1].Request.GetResourceMetrics()[0].GetScopeMetrics()
	i := slices.IndexFunc(scopes, func(sm *metricpb.ScopeMetrics) bool { return sm.GetScope().GetName() == export.ScopeName })
	Expect(i).To(BeNumerically(">=", 0), "no %s scope", export.ScopeName)
	var kvs []*commonpb.KeyValue
	for k, v := range attrs {
		kvs = append(kvs, &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}})
	}
	sm := scopes[i]
	sm.Metrics = append(sm.Metrics, &metricpb.Metric{
		Name: histogram,
		Data: &metricpb.Metric_Histogram{Histogram: &metricpb.Histogram{
			AggregationTemporality: metricpb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			DataPoints: []*metricpb.HistogramDataPoint{{
				TimeUnixNano:   uint64(lines[len(lines)-1].Time.UnixNano()),
				Count:          count,
				Sum:            &sum,
				BucketCounts:   []uint64{2, count - 2},
				ExplicitBounds: []float64{1},
				Attributes:     kvs,
			}},
		}},
	})
}

func diffMetrics(diffs []plugin.Diff) []string {
	out := make([]string, 0, len(diffs))
	for _, d := range diffs {
		out = append(out, d.Metric)
	}

	return out
}

var _ = Describe("the parity report", func() {
	var (
		text  []byte
		lines []otlp.Line
	)

	BeforeEach(func() {
		text = parityFixture("cnpg.prom")
		lines = decodeLines(parityFixture("ours.jsonl"))
		Expect(lines).To(HaveLen(3))
	})

	compare := func() plugin.Parity {
		GinkgoHelper()

		p, err := plugin.Compare(text, lines, parityPod)
		Expect(err).NotTo(HaveOccurred())

		return p
	}

	It("is built from fixtures that hold no seeded Secret value", func() {
		for _, name := range []string{"cnpg.prom", "ours.jsonl"} {
			Expect(string(parityFixture(name))).NotTo(ContainSubstring("sentinel-"), name)
		}
	})

	Describe("on the recorded instance", func() {
		It("expects CNPG's Go and built-in collector metrics, and our runtime and self-metrics, on one side", func() {
			p := compare()

			Expect(p.ExpectedCNPG).To(ContainElements(
				"go_goroutines", "go_gc_duration_seconds", "cnpg_collector_pg_wal", "cnpg_collector_collections_total"))
			Expect(p.ExpectedCNPG).NotTo(ContainElements("cnpg_collector_up", "cnpg_collector_lo_pages"))
			Expect(p.ExpectedOurs).To(ContainElements(
				"go.goroutine.count", "go.memory.used", "cnpg_otel_query_runs_total", "cnpg_otel_config_info",
				"otel.sdk.exporter.operation.duration"))
		})

		It("lists CNPG's query-collector metrics as only in CNPG, and nothing as only in ours", func() {
			p := compare()

			Expect(p.OnlyCNPG).To(Equal([]string{
				"cnpg_cache_hits", "cnpg_cache_miss", "cnpg_last_error", "cnpg_last_update_timestamp",
			}))
			Expect(p.OnlyOurs).To(BeEmpty())
			Expect(p.LabelDiffs).To(BeEmpty())
		})

		It("compares cnpg_collector_up and cnpg_collector_lo_pages like any other metric", func() {
			p := compare()

			for _, name := range []string{"cnpg_collector_up", "cnpg_collector_lo_pages"} {
				Expect(p.OnlyCNPG).NotTo(ContainElement(name))
				Expect(p.OnlyOurs).NotTo(ContainElement(name))
				Expect(p.ExpectedCNPG).NotTo(ContainElement(name))
				Expect(p.ExpectedOurs).NotTo(ContainElement(name))
			}
			Expect(diffMetrics(p.ValueDiffs)).NotTo(ContainElement(HavePrefix("cnpg_collector_up")))
			Expect(diffMetrics(p.Gauges)).NotTo(ContainElement(HavePrefix("cnpg_collector_up")),
				"a series whose points all hold one value is a constant, not a gauge")
		})

		It("passes a counter within 10 % and fails one further below CNPG's", func() {
			p := compare()

			// postgres: ours 964, CNPG 980. app: ours 168, CNPG 199.
			Expect(diffMetrics(p.ValueDiffs)).NotTo(ContainElement(xactCommit + `{datname="postgres"}`))
			Expect(p.ValueDiffs).To(ContainElement(SatisfyAll(
				HaveField("Metric", xactCommit+`{datname="app"}`),
				HaveField("Detail", SatisfyAll(ContainSubstring("168"), ContainSubstring("199"))),
			)))
		})

		It("passes the constants that match exactly", func() {
			p := compare()

			Expect(diffMetrics(p.ValueDiffs)).NotTo(ContainElements(baseOne, primaryOne))
			Expect(diffMetrics(p.Gauges)).NotTo(ContainElements(baseOne, primaryOne))
		})

		It("lists the series of a gauge that only one side reports with the other side absent", func() {
			p := compare()

			// CNPG sees the engine's session idle, the engine sees itself active.
			idle := `cnpg_backends_total{application_name="cnpg_otel_engine",datname="app",state="idle",usename="cnpg_metrics_exporter"}`
			active := `cnpg_backends_total{application_name="cnpg_otel_engine",datname="app",state="active",usename="cnpg_metrics_exporter"}`
			Expect(p.Gauges).To(ContainElements(
				plugin.Diff{Metric: idle, Detail: "CNPG 1, ours absent"},
				plugin.Diff{Metric: active, Detail: "CNPG absent, ours 1"},
			))
			Expect(diffMetrics(p.ValueDiffs)).NotTo(ContainElements(idle, active))
		})
	})

	Describe("counters", func() {
		var ours float64
		series := xactCommit + `{datname="postgres"}`

		BeforeEach(func() {
			ours = newestValue(lines, xactCommit, map[string]string{"datname": "postgres"})
		})

		DescribeTable("compare ours with CNPG's",
			func(cnpg func(ours float64) float64, diff bool) {
				text = setSample(text, series, cnpg(ours))
				p := compare()

				if diff {
					Expect(diffMetrics(p.ValueDiffs)).To(ContainElement(series))
				} else {
					Expect(diffMetrics(p.ValueDiffs)).NotTo(ContainElement(series))
				}
				Expect(diffMetrics(p.Gauges)).NotTo(ContainElement(series), "a counter is never a gauge")
			},
			Entry("equal", func(o float64) float64 { return o }, false),
			Entry("ours 8 % below", func(o float64) float64 { return o / 0.92 }, false),
			Entry("ours above", func(o float64) float64 { return o - 1 }, true),
			Entry("ours 20 % below", func(o float64) float64 { return o / 0.8 }, true),
			Entry("CNPG NaN", func(float64) float64 { return math.NaN() }, true),
		)

		It("counts NaN on both sides as equal", func() {
			for _, dp := range numberPoints(lines, xactCommit) {
				dp.Value = &metricpb.NumberDataPoint_AsDouble{AsDouble: math.NaN()}
			}
			text = setSample(text, series, math.NaN())

			Expect(diffMetrics(compare().ValueDiffs)).NotTo(ContainElement(series))
		})

		It("fails a series only one side reports", func() {
			text = replaceOnce(text, series+" ", xactCommit+`{datname="renamed"} `)
			p := compare()

			Expect(diffMetrics(p.ValueDiffs)).To(ContainElements(series, xactCommit+`{datname="renamed"}`))
		})
	})

	Describe("constants and gauges", func() {
		It("fails a constant that differs", func() {
			text = setSample(text, baseOne, 2)
			p := compare()

			Expect(p.ValueDiffs).To(ContainElement(SatisfyAll(
				HaveField("Metric", baseOne),
				HaveField("Detail", SatisfyAll(ContainSubstring("CNPG 2"), ContainSubstring("ours 1"))),
			)))
			Expect(diffMetrics(p.Gauges)).NotTo(ContainElement(baseOne))
		})

		It("counts NaN on both sides as equal and fails NaN against a number", func() {
			for _, dp := range numberPoints(lines, baseOne) {
				dp.Value = &metricpb.NumberDataPoint_AsDouble{AsDouble: math.NaN()}
			}
			text = setSample(text, baseOne, math.NaN())
			Expect(diffMetrics(compare().ValueDiffs)).NotTo(ContainElement(baseOne))

			text = setSample(text, baseOne, 1)
			Expect(diffMetrics(compare().ValueDiffs)).To(ContainElement(baseOne))
		})

		It("lists a gauge whose points vary in Gauges with both values, and never fails it", func() {
			numberPoints(lines, baseOne)[0].Value = &metricpb.NumberDataPoint_AsDouble{AsDouble: 3}
			text = setSample(text, baseOne, 2)
			p := compare()

			Expect(p.Gauges).To(ContainElement(plugin.Diff{Metric: baseOne, Detail: "CNPG 2, ours 1"}))
			Expect(diffMetrics(p.ValueDiffs)).NotTo(ContainElement(baseOne))
		})

		It("treats a series with one point of ours as a gauge", func() {
			text = setSample(text, baseOne, 2)
			p, err := plugin.Compare(text, lines[2:], parityPod)
			Expect(err).NotTo(HaveOccurred())

			Expect(p.Gauges).To(ContainElement(plugin.Diff{Metric: baseOne, Detail: "CNPG 2, ours 1"}))
			Expect(diffMetrics(p.ValueDiffs)).NotTo(ContainElement(baseOne))
		})
	})

	Describe("names, types and labels", func() {
		It("lists a metric only one side reports", func() {
			text = replaceOnce(text, "cnpg_e2e_base_one 1\n", "")
			text = append(text, "# TYPE cnpg_parity_only gauge\ncnpg_parity_only 1\n"...)
			p := compare()

			Expect(p.OnlyOurs).To(Equal([]string{baseOne}))
			Expect(p.OnlyCNPG).To(ContainElement("cnpg_parity_only"))
		})

		It("expects CNPG's process metrics on its side only", func() {
			text = append(text, "# TYPE process_cpu_seconds_total counter\nprocess_cpu_seconds_total 1.5\n"...)

			Expect(compare().ExpectedCNPG).To(ContainElement("process_cpu_seconds_total"))
		})

		It("names a label key that only CNPG has, and compares no values of that metric", func() {
			text = replaceOnce(text, "cnpg_e2e_base_one 1\n", `cnpg_e2e_base_one{extra="x"} 2`+"\n")
			p := compare()

			Expect(p.LabelDiffs).To(ConsistOf(SatisfyAll(
				HaveField("Metric", baseOne),
				HaveField("Detail", SatisfyAll(ContainSubstring("extra"), ContainSubstring("CNPG"))),
			)))
			Expect(diffMetrics(p.ValueDiffs)).NotTo(ContainElement(HavePrefix(baseOne)))
			Expect(diffMetrics(p.Gauges)).NotTo(ContainElement(HavePrefix(baseOne)))
		})

		It("names a label key that only we have", func() {
			for _, dp := range numberPoints(lines, primaryOne) {
				dp.Attributes = append(dp.Attributes, &commonpb.KeyValue{
					Key: "ours_only", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "y"}},
				})
			}

			Expect(compare().LabelDiffs).To(ConsistOf(SatisfyAll(
				HaveField("Metric", primaryOne),
				HaveField("Detail", SatisfyAll(ContainSubstring("ours_only"), ContainSubstring("ours"))),
			)))
		})

		It("reports a metric whose type differs", func() {
			text = replaceOnce(text, "# TYPE cnpg_e2e_base_one gauge", "# TYPE cnpg_e2e_base_one counter")

			Expect(compare().ValueDiffs).To(ContainElement(SatisfyAll(
				HaveField("Metric", baseOne),
				HaveField("Detail", SatisfyAll(ContainSubstring("type"), ContainSubstring("counter"), ContainSubstring("gauge"))),
			)))
		})
	})

	Describe("histograms", func() {
		BeforeEach(func() {
			text = append(text, histogramCNP...)
		})

		It("maps CNPG's family with _bucket, _sum and _count to our histogram and passes equal ones", func() {
			addHistogram(lines, 5, 2.5, map[string]string{"op": "read"})
			p := compare()

			Expect(p.OnlyCNPG).NotTo(ContainElement(HavePrefix(histogram)))
			Expect(p.OnlyOurs).NotTo(ContainElement(HavePrefix(histogram)))
			Expect(diffMetrics(p.ValueDiffs)).NotTo(ContainElement(HavePrefix(histogram)))
			Expect(diffMetrics(p.LabelDiffs)).NotTo(ContainElement(HavePrefix(histogram)))
		})

		It("compares the count and the sum with the counter rule", func() {
			addHistogram(lines, 6, 2.0, map[string]string{"op": "read"})
			p := compare()

			Expect(p.ValueDiffs).To(ContainElements(
				SatisfyAll(HaveField("Metric", histogram+`{op="read"}`), HaveField("Detail", ContainSubstring("count"))),
				SatisfyAll(HaveField("Metric", histogram+`{op="read"}`), HaveField("Detail", ContainSubstring("sum"))),
			))
		})

		It("names a label key on one side", func() {
			addHistogram(lines, 5, 2.5, map[string]string{"op": "read", "kind": "seq"})

			Expect(compare().LabelDiffs).To(ConsistOf(SatisfyAll(
				HaveField("Metric", histogram), HaveField("Detail", ContainSubstring("kind")),
			)))
		})
	})

	It("reads only the pod's series", func() {
		other := decodeLines(bytes.ReplaceAll(parityFixture("ours.jsonl"),
			[]byte(podAttribute+parityPod+`"}}`), []byte(podAttribute+parityOther+`"}}`)))
		for _, dp := range numberPoints(other, baseOne) {
			dp.Value = &metricpb.NumberDataPoint_AsDouble{AsDouble: 7}
		}

		mixed, err := plugin.Compare(text, append(other, lines...), parityPod)
		Expect(err).NotTo(HaveOccurred())
		Expect(mixed).To(Equal(compare()))

		p, err := plugin.Compare(text, other, parityOther)
		Expect(err).NotTo(HaveOccurred())
		Expect(diffMetrics(p.ValueDiffs)).To(ContainElement(baseOne))
	})

	Describe("inputs it can't compare", func() {
		It("fails on a CNPG text that doesn't parse", func() {
			_, err := plugin.Compare([]byte("cnpg_broken{label=\n"), lines, parityPod)
			Expect(err).To(MatchError(ContainSubstring("parse the CNPG text")))
		})

		It("fails on a CNPG text without metrics", func() {
			_, err := plugin.Compare([]byte("# HELP nothing here\n"), lines, parityPod)
			Expect(err).To(MatchError(plugin.ErrNoCNPGMetrics))
		})

		It("fails when no line holds the pod", func() {
			_, err := plugin.Compare(text, lines, "absent-1")
			Expect(err).To(MatchError(plugin.ErrNoPodLines))
			Expect(err).To(MatchError(ContainSubstring("absent-1")))

			_, err = plugin.Compare(text, nil, parityPod)
			Expect(err).To(MatchError(plugin.ErrNoPodLines))
		})
	})

	Describe("Markdown", func() {
		const title = "Parity: CNPG main, peer mode, plugin-peer-1"

		It("has every section, in order, under the title", func() {
			md := compare().Markdown(title)

			headings := regexp.MustCompile(`(?m)^#+ .*$`).FindAllString(md, -1)
			Expect(headings).To(Equal([]string{
				"# " + title,
				"## Summary",
				"## Only in CNPG",
				"## Only in ours",
				"## Expected on one side",
				"### CNPG",
				"### Ours",
				"## Label differences",
				"## Value differences",
				"## Gauges",
			}))
		})

		It("names the tolerance rules and says CNPG can't see MetricQuerySets", func() {
			md := compare().Markdown(title)

			Expect(md).To(ContainSubstring("10 %"))
			Expect(md).To(ContainSubstring("NaN"))
			Expect(md).To(ContainSubstring("MetricQuerySet"))
		})

		It("counts each section, lists its entries and says none when it is empty", func() {
			p := compare()
			md := p.Markdown(title)

			Expect(md).To(ContainSubstring("| Only in CNPG | 4 |"))
			Expect(md).To(ContainSubstring("| Only in ours | 0 |"))
			Expect(md).To(ContainSubstring("| Value differences | " + strconv.Itoa(len(p.ValueDiffs)) + " |"))
			Expect(md).To(ContainSubstring("- `cnpg_cache_hits`\n"))
			Expect(md).To(MatchRegexp(`(?m)^## Only in ours\n\nnone\n`))
			Expect(md).To(MatchRegexp(`(?m)^## Label differences\n\nnone\n`))
			Expect(md).To(ContainSubstring("- `" + xactCommit + `{datname="app"}` + "`: "))
		})

		It("is the same on every run", func() {
			md := compare().Markdown(title)
			for range 5 {
				Expect(compare().Markdown(title)).To(Equal(md))
			}
		})

		It("dumps no resource attribute", func() {
			md := compare().Markdown(title)

			for _, s := range []string{"k8s.pod.uid", "8f80ae36", "plugin.e2e.extra", "extra-value", "service.instance.id"} {
				Expect(md).NotTo(ContainSubstring(s))
			}
		})

		It("puts a label value holding a backtick in a longer code span", func() {
			p := plugin.Parity{OnlyCNPG: []string{"a`b"}}

			Expect(p.Markdown(title)).To(ContainSubstring("- `` a`b ``\n"))
			Expect(strings.Count(p.Markdown(title), "none")).To(Equal(6))
		})
	})
})
