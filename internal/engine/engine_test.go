package engine_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-logr/logr/funcr"
	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/executor"
	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

var _ = Describe("A run result", func() {
	DescribeTable("asks for an early facts round only when the run lost its connection",
		func(ctx SpecContext, res executor.Result, early bool) {
			res.Job = planner.Job{Query: "backends", Database: "app", Fingerprint: "f"}
			got, err := engine.EarlyFactsAfter(ctx, res)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(early))
		},
		Entry("a refused password", executor.Result{
			Outcome: selfmetrics.OutcomeError, Err: &pgconn.PgError{Code: "28P01"}, ConnectionLost: true,
		}, true),
		Entry("a backoff", executor.Result{
			Outcome: selfmetrics.OutcomeError, Err: fmt.Errorf("acquire: %w", pg.ErrBackoff), ConnectionLost: true,
		}, true),
		Entry("a column mismatch", executor.Result{
			Outcome: selfmetrics.OutcomeError, Err: queries.ErrColumnMismatch,
		}, false),
		Entry("a statement timeout", executor.Result{
			Outcome: selfmetrics.OutcomeTimeout, Err: &pgconn.PgError{Code: "57014"},
		}, false),
		Entry("a success", executor.Result{Outcome: selfmetrics.OutcomeOK}, false),
	)
})

var _ = Describe("The meter provider", func() {
	It("exports 3000 series of one self-metric without the overflow attribute", func(ctx SpecContext) {
		reader := sdkmetric.NewManualReader()
		mp := engine.NewMeterProvider(reader)
		DeferCleanup(mp.Shutdown)
		stats := selfmetrics.NewStats()
		Expect(selfmetrics.Register(mp.Meter("test"), stats)).To(Succeed())
		for i := range 3000 {
			stats.QueryRun(fmt.Sprintf("q%04d", i), "app", selfmetrics.OutcomeOK, time.Millisecond, time.Now())
		}

		var rm metricdata.ResourceMetrics
		Expect(reader.Collect(ctx, &rm)).To(Succeed())
		queries := map[string]bool{}
		for _, req := range export.Split(&rm, 2000) {
			for _, sm := range req.ScopeMetrics {
				for _, m := range sm.Metrics {
					if m.Name != "cnpg_otel_query_runs_total" {
						continue
					}
					sum, ok := m.Data.(metricdata.Sum[int64])
					Expect(ok).To(BeTrue())
					for _, dp := range sum.DataPoints {
						_, overflow := dp.Attributes.Value("otel.metric.overflow")
						Expect(overflow).To(BeFalse())
						q, _ := dp.Attributes.Value("query")
						queries[q.AsString()] = true
					}
				}
			}
		}
		Expect(queries).To(HaveLen(3000))
	})
})

var _ = Describe("cnpg_collector_up", func() {
	It("isn't reported before the first facts round", func(ctx SpecContext) {
		_, found, err := engine.CollectorUp(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeFalse())
	})

	It("follows the last facts round and has no unit", func(ctx SpecContext) {
		value := func(rounds ...bool) int64 {
			GinkgoHelper()
			m, found, err := engine.CollectorUp(ctx, rounds...)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(m.Unit).To(BeEmpty())
			Expect(m.Description).To(Equal("1 if PostgreSQL is up, 0 otherwise."))
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			Expect(ok).To(BeTrue())
			Expect(gauge.DataPoints).To(HaveLen(1))
			Expect(gauge.DataPoints[0].Attributes.ToSlice()).To(Equal([]attribute.KeyValue{attribute.String("cluster", "c")}))
			return gauge.DataPoints[0].Value
		}
		Expect(value(false)).To(Equal(int64(0)))
		Expect(value(false, true)).To(Equal(int64(1)))
		Expect(value(true, false)).To(Equal(int64(0)))
	})
})

var _ = Describe("Run bookkeeping", func() {
	It("survives a panic, logs it once and counts each run as an error", func(ctx SpecContext) {
		var lines []string
		logger := funcr.New(func(_, args string) { lines = append(lines, args) }, funcr.Options{})
		job := planner.Job{Query: "backends", Database: "app", Fingerprint: "f"}

		runs, err := engine.RunWithBrokenBookkeeping(ctrllog.IntoContext(ctx, logger), job, 3)
		Expect(err).NotTo(HaveOccurred())
		Expect(runs).To(Equal(int64(3)))
		var panics []string
		for _, l := range lines {
			if strings.Contains(l, "panic") {
				panics = append(panics, l)
			}
		}
		Expect(panics).To(HaveLen(1))
		Expect(panics[0]).To(And(ContainSubstring(`"query"="backends"`), ContainSubstring(`"database"="app"`)))
	})
})

var _ = Describe("Pool replacement", func() {
	It("closes the old pool only after every run started on it ends, across runners that share it", func(ctx SpecContext) {
		p, err := engine.NewPools(ctx)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(p.Close)

		s := engine.Unreachable()
		Expect(p.Apply(ctx, s)).To(BeTrue())
		first, end := p.Begin()
		s.Limits.MaxRowsPerQuery = 5
		Expect(p.Apply(ctx, s)).To(BeFalse(), "a limits-only change keeps the pool")
		s.Postgres.MaxConnections = 3
		Expect(p.Apply(ctx, s)).To(BeTrue())

		closed := func() bool {
			_, _, err := first.Acquire(ctx, "app")
			return errors.Is(err, pg.ErrPoolClosed)
		}
		Consistently(closed).WithTimeout(300 * time.Millisecond).Should(BeFalse())
		end()
		Eventually(closed).Should(BeTrue())
	})
})

var _ = Describe("The watcher", func() {
	It("looks at the files again after a swap", func(ctx SpecContext) {
		dir := GinkgoT().TempDir()
		a, b := filepath.Join(dir, "a.yaml"), filepath.Join(dir, "b.yaml")
		notified, err := engine.WatchSwapNotifies(ctx, []string{a}, []string{a, b})
		Expect(err).NotTo(HaveOccurred())
		Expect(notified).To(BeTrue())
	})
})

var _ = DescribeTable("The wait before the next facts round",
	func(failures int, want time.Duration) {
		Expect(engine.FactsWait(failures, 30*time.Second)).To(Equal(want))
	},
	Entry("after a success, the planning cycle", 0, 30*time.Second),
	Entry("after one failure", 1, time.Second),
	Entry("after two", 2, 2*time.Second),
	Entry("after three", 3, 4*time.Second),
	Entry("after five", 5, 16*time.Second),
	Entry("after six, capped at the planning cycle", 6, 30*time.Second),
	Entry("after very many", 1000, 30*time.Second),
)

var _ = Describe("PostgresConfig", func() {
	It("maps the connection settings and the timeouts", func() {
		s := settings.Settings{
			Postgres: settings.Postgres{Host: "/run", Port: 5433, User: "u", PasswordFile: "/pw"},
			Limits:   settings.Limits{QueryTimeout: 3 * time.Second, LockTimeout: time.Second},
		}
		c := engine.PostgresConfig(s)
		Expect(c.Host).To(Equal("/run"))
		Expect(c.Port).To(Equal(uint16(5433)))
		Expect(c.User).To(Equal("u"))
		Expect(c.PasswordFile).To(Equal("/pw"))
		Expect(c.QueryTimeout).To(Equal(3 * time.Second))
		Expect(c.LockTimeout).To(Equal(time.Second))
		Expect(c.ConnectTimeout).To(Equal(5 * time.Second))
	})
})
