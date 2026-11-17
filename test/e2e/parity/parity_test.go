package parity_test

import (
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp/cmpopts"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
	"github.com/gabrosys/plugin-telemetry/test/e2e/parity"
	"github.com/gabrosys/plugin-telemetry/test/fixture"
)

// The fixtures were recorded on kind with CNPG main: the primary plugin-peer-1's endpoint, scraped
// right after the last of its three exports in ours.jsonl.
const (
	pod         = "plugin-peer-1"
	lastUpdate  = "cnpg_last_update_timestamp"
	xactApp     = `cnpg_pg_stat_database_xact_commit{datname="app"}`
	baseOne     = "cnpg_e2e_base_one"
	peerBackend = `cnpg_backends_total{application_name="plugin-peer-2",datname="",state="active",usename="streaming_replica"}`
	walCount    = `cnpg_collector_pg_wal{value="count"}`
	walMin      = `cnpg_collector_pg_wal{value="min"}`
	syncMax     = `cnpg_collector_sync_replicas{value="max"}`
	syncSeen    = `cnpg_collector_sync_replicas{value="observed"}`
	// The edited copy's cnpg_last_update_timestamp; the fixture's is afterUpdate.
	beforeUpdate = 1794828070
	afterUpdate  = 1794828095
	// Our last success of pg_stat_database in the newest export.
	xactSuccess      = 1794828077.9944074
	xactSuccessMicro = int64(1794828077994407)
	latency          = "cnpg_e2e_base_latency"
)

// testQueries are the queries behind the fixture's series that the specs time, plus a name override.
const testQueries = `
pg_stat_database:
  query: SELECT datname, xact_commit FROM pg_catalog.pg_stat_database
  metrics:
    - datname: {usage: LABEL, description: database}
    - xact_commit: {usage: COUNTER, description: commits}
pg_stat_archiver:
  query: SELECT archived_count FROM pg_catalog.pg_stat_archiver
  metrics:
    - archived_count: {usage: COUNTER, description: archived}
pg_stat_bgwriter:
  runonserver: "<17.0.0"
  query: SELECT buffers_alloc FROM pg_catalog.pg_stat_bgwriter
  metrics:
    - buffers_alloc: {usage: COUNTER, description: allocated}
pg_stat_bgwriter_17:
  runonserver: ">=17.0.0"
  name: pg_stat_bgwriter
  query: SELECT buffers_alloc FROM pg_catalog.pg_stat_bgwriter
  metrics:
    - buffers_alloc: {usage: COUNTER, description: allocated}
backends:
  query: SELECT datname, usename, application_name, state, total FROM b
  metrics:
    - datname: {usage: LABEL, description: database}
    - usename: {usage: LABEL, description: user}
    - application_name: {usage: LABEL, description: application}
    - state: {usage: LABEL, description: state}
    - total: {usage: GAUGE, description: backends}
e2e_base:
  query: SELECT 1 AS one, 2 AS two
  metrics:
    - one: {usage: GAUGE, description: one}
    - two: {usage: GAUGE, description: two, name: renamed}
`

var (
	beforeScrape = time.Unix(beforeUpdate, 0)
	afterScrape  = time.Unix(afterUpdate, 0)
)

// setSample sets the value of the one sample line of text that starts with series and a space.
func setSample(text []byte, series string, value float64) []byte {
	GinkgoHelper()

	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(series) + ` \S+$`)
	Expect(re.FindAll(text, -1)).To(HaveLen(1), "sample %s", series)

	return re.ReplaceAllLiteral(text, []byte(series+" "+strconv.FormatFloat(value, 'g', -1, 64)))
}

func parseCNPG(text []byte, at time.Time) []parity.Series {
	GinkgoHelper()

	series, err := parity.ParseCNPG(text, at)
	Expect(err).NotTo(HaveOccurred())

	return series
}

// beforeText is the fixture computed earlier: its cnpg_last_update_timestamp is beforeUpdate.
func beforeText() []byte {
	GinkgoHelper()

	return setSample(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), lastUpdate, beforeUpdate)
}

func index() map[string][]string {
	GinkgoHelper()

	qs, err := queries.Parse([]byte(testQueries))
	Expect(err).NotTo(HaveOccurred())

	return parity.QueryIndex(queries.Builtin(), qs)
}

func ourLines() []otlp.Line {
	GinkgoHelper()

	var l otlp.Lines
	Expect(l.Update(fixture.Read(GinkgoTB(), "testdata", "ours.jsonl"))).To(Succeed())

	return l.All()
}

func ours() []parity.Series {
	GinkgoHelper()

	return parity.Ours(ourLines(), pod, index())
}

func named(series []parity.Series, name string) parity.Series {
	GinkgoHelper()

	i := slices.IndexFunc(series, func(s parity.Series) bool { return s.Name == name })
	Expect(i).To(BeNumerically(">=", 0), "no series %s", name)

	return series[i]
}

func ofMetric(series []parity.Series, metric string) parity.Series {
	GinkgoHelper()

	i := slices.IndexFunc(series, func(s parity.Series) bool { return s.Metric == metric })
	Expect(i).To(BeNumerically(">=", 0), "no metric %s", metric)

	return series[i]
}

// with returns a copy of series with the series called name edited.
func with(series []parity.Series, name string, edit func(*parity.Series)) []parity.Series {
	GinkgoHelper()

	out := slices.Clone(series)
	i := slices.IndexFunc(out, func(s parity.Series) bool { return s.Name == name })
	Expect(i).To(BeNumerically(">=", 0), "no series %s", name)
	edit(&out[i])

	return out
}

func without(series []parity.Series, name string) []parity.Series {
	return slices.DeleteFunc(slices.Clone(series), func(s parity.Series) bool { return s.Name == name })
}

func checkOf(checks []parity.Check, series string) parity.Check {
	GinkgoHelper()

	var found []parity.Check
	for _, c := range checks {
		if c.Series == series {
			found = append(found, c)
		}
	}
	Expect(found).To(HaveLen(1), "checks of %s", series)

	return found[0]
}

func seriesOf(checks []parity.Check) []string {
	out := make([]string, 0, len(checks))
	for _, c := range checks {
		out = append(out, c.Series)
	}

	return out
}

// collectorSeries are the fixture's cnpg_collector_* series but lo_pages, scraped at at.
func collectorSeries(at time.Time) []parity.Series {
	GinkgoHelper()

	return slices.DeleteFunc(parseCNPG(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), at), func(s parity.Series) bool {
		return !strings.HasPrefix(s.Metric, "cnpg_collector_") || s.Metric == "cnpg_collector_lo_pages"
	})
}

func retimed(series []parity.Series, at time.Time) []parity.Series {
	out := slices.Clone(series)
	for i := range out {
		out[i].At = at
	}

	return out
}

// asPod returns the fixture's lines moved to the resource of pod name, with every xact_commit point set to v.
func asPod(name string, v float64) []otlp.Line {
	GinkgoHelper()

	lines := ourLines()
	for _, l := range lines {
		for _, rm := range l.Request.GetResourceMetrics() {
			for _, kv := range rm.GetResource().GetAttributes() {
				if kv.GetKey() == "k8s.pod.name" {
					kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: name}}
				}
			}
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					if m.GetName() == "cnpg_pg_stat_database_xact_commit" {
						for _, dp := range m.GetSum().GetDataPoints() {
							dp.Value = &metricpb.NumberDataPoint_AsDouble{AsDouble: v}
						}
					}
				}
			}
		}
	}

	return lines
}

// row returns the Markdown table row of series, written in a code span with single backticks.
func row(md, series string) string {
	GinkgoHelper()

	for line := range strings.Lines(md) {
		if strings.HasPrefix(line, "| `"+series+"` |") {
			return line
		}
	}
	Fail("no row for " + series + " in\n" + md)

	return ""
}

var _ = Describe("ParseCNPG", func() {
	at := time.Unix(1794828100, 500_000_000)

	It("times the built-ins by the scrape and lo_pages and the queries by cnpg_last_update_timestamp", func() {
		series := parseCNPG(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), at)

		Expect(named(series, `cnpg_collector_up{cluster="plugin-peer"}`).At).To(Equal(at))
		Expect(named(series, walCount).At).To(Equal(at))
		Expect(named(series, "cnpg_collector_collections_total").At).To(Equal(at))
		Expect(named(series, `cnpg_collector_lo_pages{datname="app"}`).At).To(Equal(afterScrape))
		Expect(named(series, xactApp).At).To(Equal(afterScrape))
	})

	It("names each series with its labels sorted and gives its kind and value", func() {
		series := parseCNPG(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), at)

		s := named(series, peerBackend)
		Expect(s.Metric).To(Equal("cnpg_backends_total"))
		Expect(s.Kind).To(Equal("gauge"))
		Expect(s.Value).To(Equal(1.0))
		Expect(s.Labels).To(Equal(map[string]string{
			"application_name": "plugin-peer-2", "datname": "", "state": "active", "usename": "streaming_replica",
		}))
		Expect(named(series, xactApp).Kind).To(Equal("counter"))
		Expect(named(series, xactApp).Value).To(Equal(255.0))
		Expect(math.IsNaN(named(series, `cnpg_collector_pg_wal{value="slots_max"}`).Value)).To(BeTrue())
	})

	It("reads a histogram's count and sum", func() {
		text := append(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), []byte("# TYPE "+latency+" histogram\n"+
			latency+`_bucket{le="1"} 2`+"\n"+latency+`_bucket{le="+Inf"} 5`+"\n"+
			latency+"_sum 2.5\n"+latency+"_count 5\n")...)

		s := named(parseCNPG(text, at), latency)
		Expect(s.Kind).To(Equal("histogram"))
		Expect(s.Value).To(Equal(5.0))
		Expect(s.Sum).To(Equal(2.5))
	})

	It("fails without cnpg_last_update_timestamp", func() {
		text := regexp.MustCompile(`(?m)^cnpg_last_update_timestamp \S+\n`).ReplaceAllLiteral(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), nil)

		_, err := parity.ParseCNPG(text, at)
		Expect(err).To(MatchError(parity.ErrNoUpdateTime))
	})

	It("fails on text that does not parse", func() {
		_, err := parity.ParseCNPG([]byte("cnpg_x{ 1\n"), at)
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("QueryIndex", func() {
	It("maps each metric to its query key, name overrides included", func() {
		idx := index()

		Expect(idx).To(HaveKeyWithValue("cnpg_pg_stat_bgwriter_buffers_alloc", []string{"pg_stat_bgwriter", "pg_stat_bgwriter_17"}))
		Expect(idx).To(HaveKeyWithValue("cnpg_pg_stat_database_xact_commit", []string{"pg_stat_database"}))
		Expect(idx).To(HaveKeyWithValue("cnpg_e2e_base_one", []string{"e2e_base"}))
		Expect(idx).To(HaveKeyWithValue("cnpg_e2e_base_renamed", []string{"e2e_base"}))
		Expect(idx).To(HaveKeyWithValue("cnpg_collector_lo_pages", []string{queries.KeyLOPages}))
		Expect(idx).To(HaveKeyWithValue("cnpg_collector_pg_wal", []string{queries.KeyPGWAL}))
		Expect(idx).NotTo(HaveKey("cnpg_pg_stat_database_datname"))
	})
})

var _ = Describe("Ours", func() {
	It("reads the pod's newest export with each series' kind", func() {
		series := ours()

		Expect(named(series, xactApp).Value).To(Equal(211.0))
		Expect(named(series, xactApp).Kind).To(Equal("counter"))
		Expect(named(series, xactApp).Labels).To(Equal(map[string]string{"datname": "app"}))
		Expect(named(series, baseOne).Kind).To(Equal("gauge"))
		h := ofMetric(series, "otel.sdk.exporter.operation.duration")
		Expect(h.Kind).To(Equal("histogram"))
		Expect(h.Value).To(Equal(22.0))
		Expect(h.Sum).To(Equal(0.15783348799999997))
		Expect(h.Name).To(HavePrefix(`otel.sdk.exporter.operation.duration{http.response.status_code="200",otel.component.name=`))
	})

	It("returns nothing for a pod without an export", func() {
		Expect(parity.Ours(ourLines(), "plugin-peer-9", index())).To(BeEmpty())
	})

	DescribeTable("times each query by its own last success (PAR-03, PAR-04)",
		func(series string, success int64) {
			Expect(named(ours(), series).At).To(BeTemporally("~", time.UnixMicro(success), time.Microsecond))
		},
		Entry("pg_stat_database", xactApp, xactSuccessMicro),
		Entry("pg_stat_database on a database it did not run on, by the oldest",
			`cnpg_pg_stat_database_xact_commit{datname="template1"}`, xactSuccessMicro),
		Entry("pg_stat_archiver", "cnpg_pg_stat_archiver_archived_count", int64(1794828081949583)),
		Entry("pg_stat_bgwriter through pg_stat_bgwriter_17", "cnpg_pg_stat_bgwriter_buffers_alloc", int64(1794828085524662)),
		Entry("lo_pages on postgres, by datname", `cnpg_collector_lo_pages{datname="postgres"}`, int64(1794828081890259)),
		Entry("lo_pages on app, by datname", `cnpg_collector_lo_pages{datname="app"}`, int64(1794828084159439)),
	)

	It("takes the oldest database's success when the series names none of them", func() {
		series := parity.Ours(ourLines(), pod, map[string][]string{"cnpg_pg_stat_database_xact_commit": {"pg_extensions"}})

		Expect(named(series, `cnpg_pg_stat_database_xact_commit{datname="template1"}`).At).
			To(BeTemporally("~", time.UnixMicro(1794828078284518), time.Microsecond))
	})

	It("takes the point time without a query or its success", func() {
		series := parity.Ours(ourLines(), pod, map[string][]string{baseOne: {"missing"}})

		Expect(named(series, `cnpg_collector_up{cluster="plugin-peer"}`).At).To(Equal(time.Unix(0, 1794828092351454488)))
		Expect(named(series, baseOne).At).To(Equal(time.Unix(0, 1794828092352470342)))
	})

	DescribeTable("times a series two queries define by the one that succeeded, whichever sorts last",
		func(keys ...string) {
			series := parity.Ours(ourLines(), pod, map[string][]string{"cnpg_pg_stat_bgwriter_buffers_alloc": keys})

			Expect(named(series, "cnpg_pg_stat_bgwriter_buffers_alloc").At).
				To(BeTemporally("~", time.UnixMicro(1794828085524662), time.Microsecond))
		},
		Entry("the succeeding one sorts first", "pg_stat_bgwriter_17", "zz_missing"),
		Entry("the succeeding one sorts last", "aa_missing", "pg_stat_bgwriter_17"),
	)

	It("reads only its pod's lines", func() {
		lines := append(ourLines(), asPod("plugin-peer-2", 999)...)

		mine := parity.Ours(lines, pod, index())
		Expect(named(mine, xactApp).Value).To(Equal(211.0))
		Expect(named(parity.Ours(lines, "plugin-peer-2", index()), xactApp).Value).To(Equal(999.0))
		Expect(mine).To(BeComparableTo(ours(), cmpopts.EquateNaNs())) // the built-ins hold NaN
	})
})

var _ = Describe("the fixtures", func() {
	It("hold no seeded Secret value", func() {
		for _, name := range []string{"cnpg.prom", "ours.jsonl"} {
			Expect(string(fixture.Read(GinkgoTB(), "testdata", name))).NotTo(ContainSubstring("sentinel-"), name)
		}
	})
})

var _ = Describe("Bracket", func() {
	DescribeTable("the bracket rule",
		func(series string, before, our, after float64, verdict string) {
			bs := parseCNPG(setSample(beforeText(), series, before), beforeScrape)
			as := parseCNPG(setSample(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), series, after), afterScrape)
			mine := with(ours(), series, func(s *parity.Series) { s.Value = our })

			c := checkOf(parity.Bracket(bs, as, mine, parity.Options{}), series)
			Expect(c.Verdict).To(Equal(verdict), c.Detail)
			Expect(c.Rule).To(Equal("bracket"))
		},
		Entry("a counter between both samples passes", xactApp, 150.0, 168.0, 199.0, "pass"),
		Entry("a fresh small counter 30 % over the first sample passes under the second (PAR-02)",
			xactApp, 10.0, 13.0, 14.0, "pass"),
		Entry("a counter equal to a sample passes", xactApp, 168.0, 168.0, 199.0, "pass"),
		Entry("a counter above the second sample fails", xactApp, 150.0, 200.0, 199.0, "fail"),
		Entry("a counter under the first sample fails", xactApp, 170.0, 168.0, 199.0, "fail"),
		Entry("a constant equal on both sides passes", baseOne, 1.0, 1.0, 1.0, "pass"),
		Entry("a constant different from ours fails", baseOne, 1.0, 2.0, 1.0, "fail"),
		Entry("NaN on both sides passes", baseOne, math.NaN(), math.NaN(), math.NaN(), "pass"),
		Entry("NaN on ours alone fails", baseOne, 1.0, math.NaN(), 1.0, "fail"),
		Entry("NaN on CNPG's side alone fails", baseOne, math.NaN(), 1.0, math.NaN(), "fail"),
	)

	DescribeTable("the bracket times",
		func(before, after float64, verdict string) {
			bs := parseCNPG(setSample(setSample(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), lastUpdate, before), xactApp, 150), beforeScrape)
			as := parseCNPG(setSample(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), lastUpdate, after), afterScrape)

			c := checkOf(parity.Bracket(bs, as, ours(), parity.Options{}), xactApp)
			Expect(c.Verdict).To(Equal(verdict), c.Detail)
		},
		Entry("samples a margin or more away bracket ours", float64(beforeUpdate), float64(afterUpdate), "pass"),
		Entry("a CNPG sample 0.5 s before ours is unused", xactSuccess-0.5, float64(afterUpdate), "not compared"),
		Entry("ours newer than after − margin is not compared (PAR-05)", float64(beforeUpdate), xactSuccess+0.5, "not compared"),
		Entry("ours newer than both samples is not compared", float64(beforeUpdate), xactSuccess-1.5, "not compared"),
	)

	It("uses CNPG samples exactly one margin away", func() {
		t := time.Unix(1794828100, 0)
		up := `cnpg_collector_up{cluster="plugin-peer"}`
		mine := retimed(collectorSeries(t), t)

		c := checkOf(parity.Bracket(collectorSeries(t.Add(-parity.Margin)), collectorSeries(t.Add(parity.Margin)), mine, parity.Options{}), up)
		Expect(c.Verdict).To(Equal("pass"), c.Detail)

		c = checkOf(parity.Bracket(collectorSeries(t.Add(-parity.Margin+time.Millisecond)), collectorSeries(t.Add(parity.Margin)), mine,
			parity.Options{}), up)
		Expect(c.Verdict).To(Equal("not compared"), c.Detail)
	})

	DescribeTable("a histogram passes only with count and sum between",
		func(count, sum float64, verdict string) {
			hist := func(text []byte, count, sum float64) []byte {
				return append(text, []byte("# TYPE "+latency+" histogram\n"+latency+`_bucket{le="+Inf"} `+
					strconv.FormatFloat(count, 'g', -1, 64)+"\n"+latency+"_sum "+strconv.FormatFloat(sum, 'g', -1, 64)+"\n"+
					latency+"_count "+strconv.FormatFloat(count, 'g', -1, 64)+"\n")...)
			}
			bs := parseCNPG(hist(beforeText(), 5, 2.5), beforeScrape)
			as := parseCNPG(hist(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), 9, 4.5), afterScrape)
			mine := append(ours(), parity.Series{
				Name: latency, Metric: latency, Kind: "histogram", Labels: map[string]string{},
				Value: count, Sum: sum, At: time.UnixMicro(xactSuccessMicro),
			})

			c := checkOf(parity.Bracket(bs, as, mine, parity.Options{}), latency)
			Expect(c.Verdict).To(Equal(verdict), c.Detail)
		},
		Entry("count and sum between", 7.0, 3.5, "pass"),
		Entry("sum out", 7.0, 5.0, "fail"),
		Entry("count out", 10.0, 3.5, "fail"),
	)

	It("fails a series on one side only and names both when a label value differs", func() {
		reset := `cnpg_collector_wal_records{stats_reset="2026-11-16T11:15:53.188373Z"}`
		local := `cnpg_collector_wal_records{stats_reset="2026-11-16T12:15:53.188373+01:00"}`
		t := time.Unix(1794828100, 0)
		mine := with(retimed(collectorSeries(t), t), reset, func(s *parity.Series) {
			s.Name, s.Labels = local, map[string]string{"stats_reset": "2026-11-16T12:15:53.188373+01:00"}
		})

		checks := parity.Bracket(collectorSeries(t.Add(-5*time.Second)), collectorSeries(t.Add(5*time.Second)), mine, parity.Options{})
		Expect(checkOf(checks, reset).Verdict).To(Equal("fail"))
		Expect(checkOf(checks, reset).Detail).To(ContainSubstring("only in CNPG"))
		Expect(checkOf(checks, local).Verdict).To(Equal("fail"))
		Expect(checkOf(checks, local).Detail).To(ContainSubstring("only in ours"))
	})

	It("expects the query-cache metrics on CNPG's side and names our equivalents (PAR-01)", func() {
		checks := parity.Bracket(parseCNPG(beforeText(), beforeScrape), parseCNPG(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), afterScrape), ours(), parity.Options{})

		for metric, equivalent := range map[string]string{
			"cnpg_cache_hits":            "none",
			"cnpg_cache_miss":            "none",
			"cnpg_last_error":            "cnpg_otel_query_runs_total",
			"cnpg_last_update_timestamp": "cnpg_otel_query_last_success_timestamp_seconds",
		} {
			c := checkOf(checks, metric)
			Expect(c.Verdict).To(Equal("expected"), metric)
			Expect(c.Rule).To(Equal("cnpg-query-cache"), metric)
			Expect(c.Detail).To(ContainSubstring("ours: "+equivalent), metric)
		}
		Expect(checkOf(checks, "go_goroutines").Verdict).To(Equal("expected"))
		Expect(checkOf(checks, "cnpg_otel_series").Verdict).To(Equal("expected"))
		Expect(checkOf(checks, "go.memory.used").Verdict).To(Equal("expected"))
		Expect(checkOf(checks, "otel.sdk.exporter.operation.duration").Verdict).To(Equal("expected"))
	})

	Context("self-measuring", func() {
		t := time.Unix(1794828100, 0)
		duration := "cnpg_collector_collection_duration_seconds"

		check := func(mine []parity.Series, metric string) parity.Check {
			GinkgoHelper()

			checks := parity.Bracket(collectorSeries(t.Add(-5*time.Second)), collectorSeries(t.Add(5*time.Second)), mine, parity.Options{})
			c := checkOf(checks, metric)
			Expect(c.Rule).To(Equal("self-measuring"))

			return c
		}

		It("passes by presence and label keys, whatever the values and label values", func() {
			mine := without(retimed(collectorSeries(t), t), duration+`{collector="Collect.cnpg"}`)
			mine = with(mine, duration+`{collector="Collect.up"}`, func(s *parity.Series) { s.Value = 99 })
			mine = with(mine, "cnpg_collector_collections_total", func(s *parity.Series) { s.Value = 1e6 })

			Expect(check(mine, duration).Verdict).To(Equal("pass"))
			Expect(check(mine, "cnpg_collector_collections_total").Verdict).To(Equal("pass"))
		})

		It("fails when ours lacks it or has other label keys", func() {
			mine := without(retimed(collectorSeries(t), t), "cnpg_collector_collections_total")
			mine = with(mine, "cnpg_collector_last_collection_error", func(s *parity.Series) {
				s.Name, s.Labels = `cnpg_collector_last_collection_error{collector="Collect.up"}`, map[string]string{"collector": "Collect.up"}
			})

			Expect(check(mine, "cnpg_collector_collections_total").Verdict).To(Equal("fail"))
			Expect(check(mine, "cnpg_collector_last_collection_error").Verdict).To(Equal("fail"))
		})

		It("does not fail a series CNPG never has, such as collection_errors_total{collector=\"Collect.up\"}", func() {
			errs := "cnpg_collector_collection_errors_total"
			mine := append(retimed(collectorSeries(t), t), parity.Series{
				Name: errs + `{collector="Collect.up"}`, Metric: errs, Kind: "counter",
				Labels: map[string]string{"collector": "Collect.up"}, Value: 3, At: t,
			})

			c := check(mine, errs)
			Expect(c.Verdict).To(Equal("expected"))
			Expect(parity.Failures([]parity.Check{c})).To(BeEmpty())
		})

		It("honours a skip of one of its series or of the whole metric", func() {
			cnpgOnly := duration + `{collector="Collect.cnpg"}`
			mine := without(retimed(collectorSeries(t), t), "cnpg_collector_collections_total")

			checks := parity.Bracket(collectorSeries(t.Add(-5*time.Second)), collectorSeries(t.Add(5*time.Second)), mine,
				parity.Options{Skip: map[string]string{cnpgOnly: "one-series", "cnpg_collector_collections_total": "whole"}})
			Expect(checkOf(checks, cnpgOnly).Verdict).To(Equal("skipped"))
			Expect(checkOf(checks, cnpgOnly).Rule).To(Equal("one-series"))
			Expect(checkOf(checks, duration).Verdict).To(Equal("pass"))
			Expect(checkOf(checks, "cnpg_collector_collections_total").Verdict).To(Equal("skipped"))
		})
	})

	Context("metrics expected on one side", func() {
		bracket := func(mine []parity.Series, o parity.Options) []parity.Check {
			GinkgoHelper()

			return parity.Bracket(parseCNPG(beforeText(), beforeScrape), parseCNPG(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), afterScrape), mine, o)
		}

		It("fails a CNPG-side metric found in ours", func() {
			mine := append(ours(), parity.Series{Name: "cnpg_cache_hits", Metric: "cnpg_cache_hits", Kind: "gauge", At: afterScrape})

			c := checkOf(bracket(mine, parity.Options{}), "cnpg_cache_hits")
			Expect(c.Verdict).To(Equal("fail"))
			Expect(c.Detail).To(ContainSubstring("found in ours"))
		})

		It("fails an ours-side metric found in CNPG", func() {
			text := append(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), []byte("# TYPE cnpg_otel_series gauge\ncnpg_otel_series 1\n")...)
			checks := parity.Bracket(parseCNPG(beforeText(), beforeScrape), parseCNPG(text, afterScrape), ours(), parity.Options{})

			c := checkOf(checks, "cnpg_otel_series")
			Expect(c.Verdict).To(Equal("fail"))
			Expect(c.Detail).To(ContainSubstring("found in CNPG"))
		})

		It("honours a skip", func() {
			c := checkOf(bracket(ours(), parity.Options{Skip: map[string]string{"go_goroutines": "named"}}), "go_goroutines")
			Expect(c.Verdict).To(Equal("skipped"))
			Expect(c.Rule).To(Equal("named"))
		})
	})

	DescribeTable("fails a series whose kind differs, naming the CNPG sample that differs",
		func(before, after, detail string) {
			kind := func(series []parity.Series, k string) []parity.Series {
				return with(series, xactApp, func(s *parity.Series) { s.Kind = k })
			}
			bs := kind(parseCNPG(setSample(beforeText(), xactApp, 150), beforeScrape), before)
			as := kind(parseCNPG(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), afterScrape), after)

			c := checkOf(parity.Bracket(bs, as, ours(), parity.Options{}), xactApp)
			Expect(c.Verdict).To(Equal("fail"))
			Expect(c.Detail).To(Equal(detail))
		},
		Entry("before", "gauge", "counter", "kind: CNPG before gauge, ours counter"),
		Entry("after", "counter", "gauge", "kind: CNPG after gauge, ours counter"),
	)

	Context("built-ins", func() {
		t := time.Unix(1794828100, 0)
		bracket := func(mine []parity.Series, o parity.Options) []parity.Check {
			return parity.Bracket(collectorSeries(t.Add(-5*time.Second)), collectorSeries(t.Add(5*time.Second)), mine, o)
		}
		set := func(name string, v float64) []parity.Series {
			return with(retimed(collectorSeries(t), t), name, func(s *parity.Series) { s.Value = v })
		}

		It("skips the series a skip names and no other", func() {
			mine := with(set(syncMax, 1), syncSeen, func(s *parity.Series) { s.Value = 1 })

			checks := bracket(mine, parity.Options{Skip: map[string]string{
				syncSeen:            "primary-only-on-standby",
				"cnpg_collector_up": "fenced-frozen",
			}})
			Expect(checkOf(checks, syncSeen).Verdict).To(Equal("skipped"))
			Expect(checkOf(checks, syncSeen).Rule).To(Equal("primary-only-on-standby"))
			Expect(checkOf(checks, `cnpg_collector_up{cluster="plugin-peer"}`).Verdict).To(Equal("skipped"))
			Expect(checkOf(checks, `cnpg_collector_up{cluster="plugin-peer"}`).Rule).To(Equal("fenced-frozen"))
			Expect(checkOf(checks, syncMax).Verdict).To(Equal("fail"))
		})

		DescribeTable("the v1.30.1 pg_wal rule applies only when asked",
			func(series string, our float64, loose bool, verdict, rule string) {
				c := checkOf(bracket(set(series, our), parity.Options{LooseWALCount: loose}), series)
				Expect(c.Verdict).To(Equal(verdict), c.Detail)
				Expect(c.Rule).To(Equal(rule))
			},
			Entry("count under CNPG's, not asked", walCount, 3.0, false, "fail", "bracket"),
			Entry("count under CNPG's, asked", walCount, 3.0, true, "pass", "v1.30.1-wal-regex"),
			Entry("count equal to CNPG's, asked", walCount, 5.0, true, "pass", "v1.30.1-wal-regex"),
			Entry("count over CNPG's, asked", walCount, 6.0, true, "fail", "v1.30.1-wal-regex"),
			Entry("size under CNPG's, asked", `cnpg_collector_pg_wal{value="size"}`, 1.0, true, "pass", "v1.30.1-wal-regex"),
			Entry("min under CNPG's, asked", walMin, 4.0, true, "fail", "bracket"),
		)

		It("fails a gated series when unbracketable or one-sided, and only a gated one", func() {
			gate := func(metric string) bool { return strings.HasPrefix(metric, "cnpg_collector_") }
			nodes := "cnpg_collector_nodes_used"
			mine := with(without(retimed(collectorSeries(t), t), "cnpg_collector_fencing_on"), nodes, func(s *parity.Series) {
				s.At = t.Add(5 * time.Second)
			})

			checks := bracket(mine, parity.Options{Gate: gate})
			Expect(checkOf(checks, nodes).Verdict).To(Equal("not compared"))
			Expect(checkOf(checks, nodes).Gated).To(BeTrue())
			Expect(seriesOf(parity.Failures(checks))).To(ConsistOf(nodes, "cnpg_collector_fencing_on"))

			Expect(parity.Failures(bracket(mine, parity.Options{}))).To(BeEmpty())
		})
	})

	It("keeps a failing counter and a gauge failing again, and drops a gauge that passes (Retry)", func() {
		first := []parity.Check{
			{Series: "c", Metric: "c", Kind: "counter", Verdict: "fail", Gated: true},
			{Series: "g1", Metric: "g1", Kind: "gauge", Verdict: "fail", Gated: true},
			{Series: "g2", Metric: "g2", Kind: "gauge", Verdict: "not compared", Gated: true},
			{Series: "p", Metric: "p", Kind: "gauge", Verdict: "pass", Gated: true},
		}
		second := []parity.Check{
			{Series: "c", Metric: "c", Kind: "counter", Verdict: "pass", Gated: true},
			{Series: "g1", Metric: "g1", Kind: "gauge", Verdict: "fail", Gated: true, Detail: "again"},
			{Series: "g2", Metric: "g2", Kind: "gauge", Verdict: "pass", Gated: true},
			{Series: "p", Metric: "p", Kind: "gauge", Verdict: "fail", Gated: true},
		}

		failures := parity.Retry(first, second)
		Expect(seriesOf(failures)).To(ConsistOf("c", "g1"))
		Expect(checkOf(failures, "g1").Detail).To(Equal("again"))
	})
})

var _ = Describe("Markdown", func() {
	checks := func(series string, our float64) []parity.Check {
		GinkgoHelper()

		bs := parseCNPG(setSample(beforeText(), xactApp, 150), beforeScrape)
		as := parseCNPG(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), afterScrape)
		mine := with(ours(), series, func(s *parity.Series) { s.Value = our })

		return parity.Bracket(bs, as, mine, parity.Options{Gate: func(m string) bool { return m == "cnpg_backends_total" }})
	}

	It("prints each check's gate inputs (PAR-10)", func() {
		md := parity.Markdown("Parity", checks(xactApp, 168))

		Expect(md).To(HavePrefix("# Parity\n"))
		r := row(md, xactApp)
		for _, part := range []string{
			"| bracket |", "| no |", "150 at 2026-11-16T11:21:10.000Z", "168 at 2026-11-16T11:21:17.994Z",
			"255 at 2026-11-16T11:21:35.000Z", "| 1s |", "| pass |",
		} {
			Expect(r).To(ContainSubstring(part))
		}
	})

	It("names the metric, labels, values and times of a failing comparison", func() {
		r := row(parity.Markdown("Parity", checks(peerBackend, 2)), peerBackend)

		for _, part := range []string{
			"| yes |", "1 at 2026-11-16T11:21:10.000Z", "2 at 2026-11-16T11:21:16.994Z", "1 at 2026-11-16T11:21:35.000Z",
			"| 1s |", "| fail |",
		} {
			Expect(r).To(ContainSubstring(part))
		}
	})

	It("prints absent for a side without the series", func() {
		r := row(parity.Markdown("Parity", checks(xactApp, 168)),
			`cnpg_backends_total{application_name="cnpg_otel_engine",datname="app",state="active",usename="cnpg_metrics_exporter"}`)

		Expect(r).To(ContainSubstring("| absent |"))
		Expect(r).To(ContainSubstring("| fail |"))
	})

	It("renders the same input the same way, whatever its order", func() {
		bs, as, mine := parseCNPG(beforeText(), beforeScrape), parseCNPG(fixture.Read(GinkgoTB(), "testdata", "cnpg.prom"), afterScrape), ours()
		md := parity.Markdown("Parity", parity.Bracket(bs, as, mine, parity.Options{}))

		reorders := []func([]parity.Series) []parity.Series{
			func(s []parity.Series) []parity.Series { return s },
			func(s []parity.Series) []parity.Series { out := slices.Clone(s); slices.Reverse(out); return out },
			func(s []parity.Series) []parity.Series { return append(slices.Clone(s[len(s)/2:]), s[:len(s)/2]...) },
		}
		for _, reorder := range reorders {
			Expect(parity.Markdown("Parity", parity.Bracket(reorder(bs), reorder(as), reorder(mine), parity.Options{}))).To(Equal(md))
		}
	})

	It("puts a series holding a backtick in a longer code span", func() {
		name := "cnpg_x{l=\"a``b\"}"
		md := parity.Markdown("Parity", []parity.Check{{Series: name, Metric: "cnpg_x", Rule: "bracket", Verdict: "fail"}})

		Expect(md).To(ContainSubstring("| ``` " + name + " ``` |"))
	})
})
