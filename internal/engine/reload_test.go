package engine_test

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/snapshot"
)

const unitSettings = `cluster: unit
postgres:
  host: 127.0.0.1
  port: 5432
defaultInterval: 30s
sources:
  - queries.yaml
exporter:
  endpoint: http://127.0.0.1:4318/v1/metrics
`

const alphaBeta = `alpha:
  query: SELECT 1 AS value
  metrics:
    - value:
        usage: GAUGE
        description: Alpha.
beta:
  query: SELECT 2 AS value
  metrics:
    - value:
        usage: GAUGE
        description: Beta.
`

const gamma = `gamma:
  query: SELECT 3 AS value
  metrics:
    - value:
        usage: GAUGE
        description: Gamma.
`

var _ = Describe("Replanner", func() {
	var (
		config  string
		source  string
		store   *snapshot.Store
		stats   *selfmetrics.Stats
		plans   []planner.Plan
		r       *engine.Replanner
		primary planner.Facts
	)

	write := func(path, content string) {
		GinkgoHelper()
		Expect(os.WriteFile(path, []byte(content), 0o600)).To(Succeed())
	}

	jobs := func(p planner.Plan) []string {
		out := make([]string, 0, len(p.Jobs))
		for _, j := range p.Jobs {
			out = append(out, j.Query+"@"+j.Database)
		}
		return out
	}

	job := func(p planner.Plan, query string) planner.Job {
		GinkgoHelper()
		for _, j := range p.Jobs {
			if j.Query == query && j.Database == "postgres" {
				return j
			}
		}
		Fail("no job for " + query)
		return planner.Job{}
	}

	BeforeEach(func() {
		dir := GinkgoT().TempDir()
		config = filepath.Join(dir, "engine.yaml")
		source = filepath.Join(dir, "queries.yaml")
		write(config, unitSettings)
		write(source, alphaBeta)
		store = snapshot.New(0)
		stats = selfmetrics.NewStats()
		plans = nil
		r = engine.NewReplanner(config, stats, store, func(p planner.Plan) { plans = append(plans, p) })
		primary = planner.Facts{Version: *semver.MustParse("18.0.7"), Databases: []string{"app", "postgres"}}
	})

	start := func(ctx SpecContext) {
		GinkgoHelper()
		Expect(r.Reload(ctx)).To(Succeed())
		r.SetFacts(ctx, primary)
		Expect(plans).To(HaveLen(1))
	}

	It("plans nothing before the first facts", func(ctx SpecContext) {
		Expect(r.Reload(ctx)).To(Succeed())
		Expect(plans).To(BeEmpty())

		r.SetFacts(ctx, primary)
		Expect(plans).To(HaveLen(1))
		Expect(jobs(plans[0])).To(ConsistOf("alpha@postgres", "beta@postgres", "collector@app", "collector@postgres"))
	})

	It("replans when the sources hash changes and not when it stays", func(ctx SpecContext) {
		start(ctx)
		Expect(r.Reload(ctx)).To(Succeed())
		Expect(plans).To(HaveLen(1))

		write(source, alphaBeta+gamma)
		Expect(r.Reload(ctx)).To(Succeed())
		Expect(plans).To(HaveLen(2))
		Expect(jobs(plans[1])).To(ContainElement("gamma@postgres"))
	})

	It("doesn't replan when the settings file is rewritten with identical content", func(ctx SpecContext) {
		start(ctx)
		for range 3 {
			write(config, unitSettings)
			Expect(r.Reload(ctx)).To(Succeed())
		}
		Expect(plans).To(HaveLen(1))
	})

	It("replans when the settings change", func(ctx SpecContext) {
		start(ctx)
		write(config, strings.Replace(unitSettings, "defaultInterval: 30s", "defaultInterval: 45s", 1))
		Expect(r.Reload(ctx)).To(Succeed())
		Expect(plans).To(HaveLen(2))
		Expect(job(plans[1], "alpha").Interval).To(Equal(45 * time.Second))
	})

	It("replans when the facts change and not when they repeat", func(ctx SpecContext) {
		start(ctx)
		r.SetFacts(ctx, planner.Facts{Version: primary.Version, Databases: []string{"app", "postgres"}})
		Expect(plans).To(HaveLen(1))

		r.SetFacts(ctx, planner.Facts{Version: primary.Version, Databases: []string{"postgres"}})
		Expect(plans).To(HaveLen(2))
		Expect(jobs(plans[1])).To(ConsistOf("alpha@postgres", "beta@postgres", "collector@postgres"))
	})

	It("keeps the snapshot entries of unchanged queries across a replan", func(ctx SpecContext) {
		start(ctx)
		for _, q := range []string{"alpha", "beta"} {
			sample := queries.Sample{Metric: "cnpg_" + q + "_value", Kind: queries.KindGauge, Value: 1}
			Expect(store.Put(snapshot.Key{Query: q, Database: "postgres"}, job(plans[0], q).Fingerprint,
				[]queries.Sample{sample}, time.Now())).To(Succeed())
		}

		write(source, strings.Replace(alphaBeta, "SELECT 2", "SELECT 20", 1))
		Expect(r.Reload(ctx)).To(Succeed())
		Expect(plans).To(HaveLen(2))
		Expect(job(plans[1], "alpha").Fingerprint).To(Equal(job(plans[0], "alpha").Fingerprint))
		Expect(job(plans[1], "beta").Fingerprint).NotTo(Equal(job(plans[0], "beta").Fingerprint))

		points, _ := store.Points()
		Expect(points).To(HaveLen(1))
		Expect(points[0].Key).To(Equal(snapshot.Key{Query: "alpha", Database: "postgres"}))
	})

	It("stops reporting the query self-metrics of a database that leaves the plan", func(ctx SpecContext) {
		reader := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		DeferCleanup(mp.Shutdown)
		Expect(selfmetrics.Register(mp.Meter("test"), stats)).To(Succeed())
		databases := func() []string {
			GinkgoHelper()
			var rm metricdata.ResourceMetrics
			Expect(reader.Collect(ctx, &rm)).To(Succeed())
			var out []string
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					if m.Name != "cnpg_otel_query_last_success_timestamp_seconds" {
						continue
					}
					gauge, ok := m.Data.(metricdata.Gauge[float64])
					Expect(ok).To(BeTrue())
					for _, dp := range gauge.DataPoints {
						db, _ := dp.Attributes.Value(attribute.Key("database"))
						out = append(out, db.AsString())
					}
				}
			}
			return out
		}

		start(ctx)
		for _, db := range primary.Databases {
			stats.QueryRun("collector", db, selfmetrics.OutcomeOK, time.Millisecond, time.Now())
		}
		Expect(databases()).To(ConsistOf("app", "postgres"))

		r.SetFacts(ctx, planner.Facts{Version: primary.Version, Databases: []string{"postgres"}})
		Expect(plans).To(HaveLen(2))
		Expect(databases()).To(ConsistOf("postgres"))
	})

	It("keeps the last good configuration when the settings become invalid", func(ctx SpecContext) {
		start(ctx)
		write(config, "cluster: \"\"\n")
		Expect(r.Reload(ctx)).To(MatchError(ContainSubstring("cluster")))
		Expect(plans).To(HaveLen(1))

		r.SetFacts(ctx, planner.Facts{Version: primary.Version, InRecovery: true, Databases: primary.Databases})
		Expect(plans).To(HaveLen(2))
		Expect(jobs(plans[1])).To(Equal(jobs(plans[0])))
	})
})
