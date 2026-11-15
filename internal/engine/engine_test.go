package engine_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/executor"
	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/export/otlptest"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

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
	It("is absent before the first facts round, then follows the last one", func() {
		release := make(chan struct{})
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) },
			func(ctx context.Context, round int) (planner.Facts, error) {
				switch round {
				case 0:
					select {
					case <-release:
					case <-ctx.Done():
						return planner.Facts{}, ctx.Err()
					}
					return memPrimary, nil
				default:
					return unreachable(ctx, round)
				}
			})
		h.advance(func() bool { return len(h.requests()) >= 2 })
		Expect(points(h.srv, "cnpg_collector_up", time.Time{})).To(BeEmpty())

		close(release)
		h.advance(h.has("cnpg_collector_up", map[string]string{"cluster": "mem"}, 1))
		h.advance(h.has("cnpg_collector_up", map[string]string{"cluster": "mem"}, 0))
		up := points(h.srv, "cnpg_collector_up", time.Time{})
		Expect(up[len(up)-1].attrs).To(Equal(map[string]string{"cluster": "mem"}))
		unit, description, found := metricMeta(h.srv, "cnpg_collector_up")
		Expect(found).To(BeTrue())
		Expect(unit).To(BeEmpty())
		Expect(description).To(Equal("1 if PostgreSQL is up, 0 otherwise."))
	})
})

// gaps returns the time between consecutive entries of times.
func gaps(times []time.Time) []time.Duration {
	out := make([]time.Duration, 0, len(times))
	for i := 1; i < len(times); i++ {
		out = append(out, times[i].Sub(times[i-1]))
	}
	return out
}

var _ = Describe("Facts rounds", func() {
	It("back off 1s, 2s, 4s ... capped at the planning cycle whatever the early rounds, and start over on a new pool", func() {
		// The first round plans the queries; after it PostgreSQL is gone, and every run that fails asks for an
		// early round. The planning cycle is 30 s, the queries run every second.
		h := run(func(url string) *memConfig {
			return newMemConfig(strings.Replace(httpSettings(url), "defaultInterval: 30s\n", "defaultInterval: 1s\n", 1), httpFiles())
		}, func(ctx context.Context, round int) (planner.Facts, error) {
			if round == 0 {
				return memPrimary, nil
			}
			return unreachable(ctx, round)
		})
		h.advanceBy(time.Second, func() bool { return len(h.timersArmed()) >= 8 })
		Expect(h.timersArmed()[:8]).To(Equal([]time.Duration{
			0, 30 * time.Second, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second,
		}))
		Expect(len(h.roundTimes())).To(BeNumerically(">", len(h.timersArmed())), "runs asked for early rounds in between")

		before := len(h.timersArmed())
		h.cfg.set(func() { h.cfg.doc = strings.Replace(h.cfg.doc, "port: 1\n", "port: 2\n", 1) })
		h.reload()
		Expect(h.logged(`"msg"="configuration applied"`)).To(Equal(2))
		h.advanceBy(time.Second, func() bool { return len(h.timersArmed()) >= before+3 })
		Expect(h.timersArmed()[before : before+3]).To(Equal([]time.Duration{0, time.Second, 2 * time.Second}))
	})

	It("keep earlyFactsGap after a round when runs that lose their connection ask for one", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) }, primary)
		h.advance(func() bool { return len(h.roundTimes()) >= 5 })
		rounds := h.roundTimes()
		Expect(rounds[len(rounds)-1].Sub(rounds[0])).To(BeNumerically("<", 30*time.Second), "rounds came early, not on the timer")
		for _, gap := range gaps(rounds) {
			Expect(gap).To(BeNumerically(">=", time.Second))
		}
	})

	It("stop writing a job's self-metrics once it leaves the plan", func() {
		var dropApp atomic.Bool
		h := run(func(url string) *memConfig { return newMemConfig(fastSettings(url), httpFiles()) },
			func(context.Context, int) (planner.Facts, error) {
				if dropApp.Load() {
					return planner.Facts{Version: memPrimary.Version, Databases: []string{"postgres"}}, nil
				}
				return memPrimary, nil
			})
		onApp := func(req otlptest.Request) bool {
			for _, p := range requestPoints(req, "cnpg_otel_query_runs_total") {
				if p.attrs["database"] == "app" {
					return true
				}
			}
			return false
		}
		h.advance(func() bool { reqs := h.requests(); return len(reqs) > 0 && onApp(reqs[len(reqs)-1]) })

		dropApp.Store(true)
		h.advance(func() bool { return h.logged(`"msg"="plan updated"`) >= 2 })
		h.advance(func() bool { reqs := h.requests(); return len(reqs) > 0 && !onApp(reqs[len(reqs)-1]) })
		since := len(h.requests())
		h.advance(func() bool { return len(h.requests()) >= since+5 })
		for _, req := range h.requests()[since:] {
			Expect(onApp(req)).To(BeFalse())
		}
	})
})

var _ = Describe("A run caught in a pool swap", func() {
	It("keeps its job's snapshot entry, counts skipped and logs nothing", func(ctx SpecContext) {
		var lines []string
		logger := funcr.New(func(_, args string) { lines = append(lines, args) }, funcr.Options{})
		res := executor.Result{
			Job:     planner.Job{Query: "backends", Database: "app", Fingerprint: "f"},
			Outcome: selfmetrics.OutcomeSkipped,
			Err:     fmt.Errorf("acquire connection to app: %w", pg.ErrPoolClosed),
		}

		kept, runs, err := engine.RecordResult(ctrllog.IntoContext(ctx, logger), res)
		Expect(err).NotTo(HaveOccurred())
		Expect(kept).To(BeTrue())
		Expect(runs).To(Equal(int64(1)))
		Expect(lines).To(BeEmpty())
	})
})

var _ = Describe("Run bookkeeping", func() {
	It("logs a bookkeeping panic with its stack and keeps running the job", func(ctx SpecContext) {
		var (
			mu    sync.Mutex // guards lines
			lines []string
		)
		logger := funcr.New(func(_, args string) {
			mu.Lock()
			defer mu.Unlock()
			lines = append(lines, args)
		}, funcr.Options{})
		job := planner.Job{Query: "backends", Database: "app", Fingerprint: "f"}

		errorRuns, kept, err := engine.RunWithBrokenBookkeeping(ctrllog.IntoContext(ctx, logger), job, 3)
		Expect(err).NotTo(HaveOccurred())
		Expect(errorRuns).To(BeZero())
		Expect(kept).To(BeTrue())
		mu.Lock()
		defer mu.Unlock()
		var panics []string
		for _, l := range lines {
			if strings.Contains(l, "recovered from a panic") {
				panics = append(panics, l)
			}
		}
		Expect(panics).To(HaveLen(1), "the same panic of a run is logged once")
		Expect(panics[0]).To(And(ContainSubstring("run of backends on app"), ContainSubstring(`"stack"=`), ContainSubstring("panic:")))
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
