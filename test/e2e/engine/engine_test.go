//go:build e2e

package engine_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	otelruntime "go.opentelemetry.io/contrib/instrumentation/runtime"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/scheduler"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/test/e2e/otlp"
)

// The intervals of testdata/engine.yaml and testdata/collector.yaml.
const (
	exportInterval  = 5 * time.Second
	fastInterval    = 2 * time.Second
	slowInterval    = 20 * time.Second
	defaultInterval = 10 * time.Second
	flushInterval   = time.Second
)

const (
	// engineScope is the instrumentation scope of the engine's metrics.
	engineScope = "github.com/gabrosys/plugin-telemetry"
	// slack covers the work between the timers: a reload, a run, a copy of the collector's file.
	slack = 3 * time.Second

	defaultDatabase = "postgres"
	goldenFile      = "metrics.golden.json"
	probeMetric     = "cnpg_e2e_probe_value"
	probeQuery      = `e2e_probe:
  query: SELECT 42 AS value
  metrics:
    - value:
        usage: GAUGE
        description: A query added while the engine runs.
`

	runsTotal   = "cnpg_otel_query_runs_total"
	configInfo  = otlp.ConfigInfo
	configLoads = "cnpg_otel_config_loads_total"
	up          = "cnpg_collector_up"
)

// failureOnly are the catalogue metrics reported only once a run is capped or series are dropped.
var failureOnly = map[string]bool{
	"cnpg_otel_query_capped_series":        true,
	"cnpg_otel_query_dropped_series_total": true,
}

func testdata() *os.Root {
	GinkgoHelper()
	root, err := os.OpenRoot("testdata")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(root.Close)
	return root
}

// marshalShapes writes one metric per line, so the golden file diffs by metric.
func marshalShapes(shapes []otlp.Shape) []byte {
	GinkgoHelper()
	rows := make([]string, 0, len(shapes))
	for _, s := range shapes {
		row, err := json.Marshal(s)
		Expect(err).NotTo(HaveOccurred())
		rows = append(rows, "  "+string(row))
	}
	return []byte("[\n" + strings.Join(rows, ",\n") + "\n]\n")
}

// latestExport returns the lines of the last export.
func latestExport(lines []otlp.Line) []otlp.Line {
	times := otlp.ExportTimes(lines)
	if len(times) == 0 {
		return nil
	}
	return otlp.ExportAt(lines, times[len(times)-1])
}

// increase returns how much the counter series matching want grew over the window that ends at its last point.
func increase(g Gomega, lines []otlp.Line, name string, want map[string]string, window time.Duration) float64 {
	points := otlp.Matching(otlp.Points(lines, name), want)
	g.Expect(points).NotTo(BeEmpty(), "%s%v", name, want)
	end := points[len(points)-1]
	start := slices.IndexFunc(points, func(p otlp.Point) bool {
		d := end.Time.Sub(p.Time) - window
		return d > -time.Second && d < time.Second
	})
	g.Expect(start).To(BeNumerically(">=", 0), "no point of %s%v %s before %s", name, want, window, end.Time)
	return end.Value - points[start].Value
}

// lastValue returns the latest value of the series of name matching want, failing g when there is none.
func lastValue(g Gomega, lines []otlp.Line, name string, want map[string]string) float64 {
	p, ok := otlp.Last(lines, name, want)
	g.Expect(ok).To(BeTrue(), "%s%v was not exported", name, want)
	return p.Value
}

var _ = Describe("the engine", Ordered, ContinueOnFailure, func() {
	It("exports CNPG's default metrics with the golden names, kinds and attribute keys", func(ctx SpecContext) {
		// Every job of the first plan runs within MaxInitialDelay of the start.
		settled := proc.started.Add(scheduler.MaxInitialDelay + slack)
		var lines []otlp.Line
		Eventually(func(g Gomega) {
			lines = otlp.Since(exported(ctx, g), settled)
			intervals := otlp.Points(lines, "cnpg_otel_query_interval_seconds")
			g.Expect(intervals).NotTo(BeEmpty())
			for _, p := range intervals {
				_, ok := otlp.Last(lines, runsTotal, map[string]string{"query": p.Attrs["query"], "outcome": "ok"})
				g.Expect(ok).To(BeTrue(), "query %s has no ok run", p.Attrs["query"])
			}
			g.Expect(otlp.Points(lines, "cnpg_otel_last_export_success_timestamp_seconds")).NotTo(BeEmpty())
		}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(time.Second).Should(Succeed())

		shapes, err := otlp.Shapes(lines, engineScope)
		Expect(err).NotTo(HaveOccurred())
		got := marshalShapes(shapes)
		root := testdata()
		if os.Getenv("UPDATE_GOLDEN") != "" {
			Expect(root.WriteFile(goldenFile, got, 0o600)).To(Succeed())
		}
		want, err := root.ReadFile(goldenFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(got)).To(Equal(string(want)))
	}, SpecTimeout(2*time.Minute))

	It("sets the resource attributes of design 6.6", func(ctx SpecContext) {
		var lines []otlp.Line
		Eventually(func(g Gomega) {
			lines = exported(ctx, g)
			g.Expect(lines).NotTo(BeEmpty())
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		want := map[string]string{
			"service.name":        "e2e",
			"service.namespace":   "e2e",
			"service.instance.id": "e2e.e2e-1.otel-metrics",
			"k8s.namespace.name":  namespace,
			"k8s.pod.name":        podName,
			"k8s.pod.uid":         podUID,
			"k8s.node.name":       nodeName,
			"k8s.container.name":  "otel-metrics",
			"e2e.suite":           "engine",
		}
		resources := otlp.Resources(lines)
		Expect(resources).NotTo(BeEmpty())
		for _, res := range resources {
			Expect(res).To(Equal(want))
		}
		var engineScopes []otlp.Scope
		for _, s := range otlp.Scopes(lines) {
			if s.Name == engineScope {
				engineScopes = append(engineScopes, s)
			}
		}
		Expect(engineScopes).To(ConsistOf(otlp.Scope{Name: engineScope, Version: proc.version}))
	}, SpecTimeout(time.Minute))

	It("exports every 5s and runs each group at its own interval", func(ctx SpecContext) {
		slow := map[string]string{"query": "pg_database", "database": defaultDatabase, "outcome": "ok"}
		fast := map[string]string{"query": "backends", "database": defaultDatabase, "outcome": "ok"}
		slowWindow, fastWindow := 2*slowInterval, 10*fastInterval
		var lines []otlp.Line
		Eventually(func(g Gomega) {
			lines = exported(ctx, g)
			points := otlp.Matching(otlp.Points(lines, runsTotal), slow)
			g.Expect(points).NotTo(BeEmpty())
			g.Expect(points[len(points)-1].Time.Sub(points[0].Time)).To(BeNumerically(">=", slowWindow))
		}).WithContext(ctx).WithTimeout(2 * time.Minute).WithPolling(time.Second).Should(Succeed())

		times := otlp.ExportTimes(lines)
		Expect(len(times)).To(BeNumerically(">=", 7), "fewer than 6 export gaps")
		gaps := make([]time.Duration, 0, len(times)-1)
		for i := 1; i < len(times); i++ {
			gaps = append(gaps, times[i].Sub(times[i-1]))
		}
		AddReportEntry("export gaps", gaps)
		for _, gap := range gaps {
			Expect(gap).To(BeNumerically("~", exportInterval, 500*time.Millisecond))
		}

		fastRuns := increase(Default, lines, runsTotal, fast, fastWindow)
		slowRuns := increase(Default, lines, runsTotal, slow, slowWindow)
		AddReportEntry("backends runs in "+fastWindow.String(), fastRuns)
		AddReportEntry("pg_database runs in "+slowWindow.String(), slowRuns)
		Expect(fastRuns).To(And(BeNumerically(">=", 9), BeNumerically("<=", 11)))
		Expect(slowRuns).To(And(BeNumerically(">=", 1), BeNumerically("<=", 2)))

		for query, interval := range map[string]time.Duration{
			"backends": fastInterval, "pg_database": slowInterval, "pg_postmaster": defaultInterval,
		} {
			got := lastValue(Default, lines, "cnpg_otel_query_interval_seconds", map[string]string{"query": query})
			Expect(got).To(Equal(interval.Seconds()), query)
		}
	}, SpecTimeout(3*time.Minute))

	It("exports a query added to a source file, and keeps it when the file stops parsing", func(ctx SpecContext) {
		extra := filepath.Join(configDir, "extra.yaml")
		var lines []otlp.Line
		Eventually(func(g Gomega) {
			lines = exported(ctx, g)
			g.Expect(lines).NotTo(BeEmpty())
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
		oldGeneration := mustLast(lines, configInfo, nil).Attrs["generation"]

		// A new query waits for its initial delay, below its interval and MaxInitialDelay, before its first run.
		bound := engine.Debounce + min(defaultInterval, scheduler.MaxInitialDelay) + exportInterval + flushInterval + slack
		start := time.Now()
		writeFile("extra.yaml", probeQuery)
		Eventually(func(g Gomega) []otlp.Point {
			return otlp.Points(otlp.Since(exported(ctx, g), start), probeMetric)
		}).WithContext(ctx).WithTimeout(bound).WithPolling(250 * time.Millisecond).Should(
			ContainElement(HaveField("Value", 42.0)))
		AddReportEntry("probe exported after", time.Since(start))

		lines, err := otelcol.read(ctx)
		Expect(err).NotTo(HaveOccurred())
		generation := mustLast(lines, configInfo, nil).Attrs["generation"]
		Expect(generation).NotTo(Equal(oldGeneration))

		loadErrors := func(ls []otlp.Line) float64 {
			p, ok := otlp.Last(ls, configLoads, map[string]string{"source": extra, "outcome": "error"})
			if !ok {
				return 0
			}
			return p.Value
		}
		errorsBefore := loadErrors(lines)
		writeFile("extra.yaml", probeQuery+"broken: [\n")
		var broken time.Time
		Eventually(func(g Gomega) {
			ls := exported(ctx, g)
			g.Expect(loadErrors(ls)).To(BeNumerically(">", errorsBefore))
			p, _ := otlp.Last(ls, configLoads, map[string]string{"source": extra, "outcome": "error"})
			broken = p.Time
		}).WithContext(ctx).WithTimeout(engine.Debounce + exportInterval + flushInterval + slack).
			WithPolling(250 * time.Millisecond).Should(Succeed())

		// The exports after the failed load still carry the probe, under the same generation.
		var after []otlp.Line
		Eventually(func(g Gomega) {
			after = otlp.Since(exported(ctx, g), broken)
			g.Expect(len(otlp.ExportTimes(after))).To(BeNumerically(">=", 3))
		}).WithContext(ctx).WithTimeout(3*exportInterval + slack).WithPolling(time.Second).Should(Succeed())
		for _, at := range otlp.ExportTimes(after) {
			export := otlp.ExportAt(after, at)
			Expect(otlp.Points(export, probeMetric)).To(ContainElement(HaveField("Value", 42.0)), "export at %s", at)
			Expect(mustLast(export, configInfo, nil).Attrs).To(HaveKeyWithValue("generation", generation), "export at %s", at)
		}
	}, SpecTimeout(2*time.Minute))

	It("sets cnpg_collector_up to 0 while PostgreSQL is down and back to 1 after", func(ctx SpecContext) {
		upSince := func(since time.Time) func(Gomega) float64 {
			return func(g Gomega) float64 {
				return lastValue(g, otlp.Since(exported(ctx, g), since), up, map[string]string{"cluster": "e2e"})
			}
		}
		Eventually(upSince(time.Now())).WithContext(ctx).WithTimeout(exportInterval + flushInterval + slack).
			WithPolling(250 * time.Millisecond).Should(Equal(1.0))

		Expect(pgServer.StopServer(ctx)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) { Expect(pgServer.StartServer(ctx)).To(Succeed()) })
		stopped := time.Now()
		Eventually(upSince(stopped)).WithContext(ctx).WithTimeout(15 * time.Second).
			WithPolling(250 * time.Millisecond).Should(Equal(0.0))
		AddReportEntry("up 0 after stop", time.Since(stopped))

		Expect(pgServer.StartServer(ctx)).To(Succeed())
		started := time.Now()
		Eventually(upSince(started)).WithContext(ctx).WithTimeout(15 * time.Second).
			WithPolling(250 * time.Millisecond).Should(Equal(1.0))
		AddReportEntry("up 1 after start", time.Since(started))
	}, SpecTimeout(2*time.Minute))

	It("exports the self-metrics that need no failure", func(ctx SpecContext) {
		var want []string
		for _, m := range selfmetrics.ByComponent(selfmetrics.ComponentEngine) {
			if !failureOnly[m.Name] {
				want = append(want, m.Name)
			}
		}
		Expect(want).NotTo(BeEmpty())
		Eventually(func(g Gomega) {
			current := latestExport(exported(ctx, g))
			g.Expect(otlp.Names(current, engineScope)).To(ContainElements(want))
			g.Expect(otlp.Names(current, otelruntime.ScopeName)).To(ContainElements("go.goroutine.count", "go.memory.used"))
		}).WithContext(ctx).WithTimeout(exportInterval + flushInterval + slack).WithPolling(time.Second).Should(Succeed())
	}, SpecTimeout(time.Minute))
})

// mustLast returns the latest point of the series of name matching want.
func mustLast(lines []otlp.Line, name string, want map[string]string) otlp.Point {
	GinkgoHelper()
	p, ok := otlp.Last(lines, name, want)
	Expect(ok).To(BeTrue(), "%s%v was not exported", name, want)
	return p
}
