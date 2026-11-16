package engine_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Masterminds/semver/v3"
	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
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

var _ = DescribeTable("Databases whose names are not valid UTF-8",
	func(lists [][]string, warnings, recoveries int) {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) },
			func(_ context.Context, round int) (planner.Facts, error) {
				f := memPrimary
				f.SkippedDatabases = lists[min(round, len(lists)-1)]
				return f, nil
			})
		h.advance(func() bool { return len(h.roundTimes()) >= len(lists)+3 })
		Expect(h.logged("databases not monitored, their names are not valid UTF-8")).To(Equal(warnings))
		Expect(h.logged("all databases are monitored again")).To(Equal(recoveries))
	},
	Entry("the same list three times, then none: one warning, one recovery",
		[][]string{{`"a"`}, {`"a"`}, {`"a"`}, nil}, 1, 1),
	Entry("a different list warns again", [][]string{{`"a"`}, {`"b"`}}, 2, 0),
	Entry("a list, none, the list again: two warnings, one recovery", [][]string{{`"a"`}, nil, {`"a"`}}, 2, 1),
	Entry("never a list: nothing logged", [][]string{nil}, 0, 0),
)

var _ = Describe("The databases not monitored warning", func() {
	It("names the databases", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) },
			func(context.Context, int) (planner.Facts, error) {
				f := memPrimary
				f.SkippedDatabases = []string{`"bad\xffname"`}
				return f, nil
			})
		h.advance(func() bool { return h.logged("databases not monitored") >= 1 })
		Expect(h.loggedAll("databases not monitored", `"databases"=["\"bad\\xffname\""]`)).To(Equal(1))
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

	It("keeps earlyFactsGap between the end of one round and the start of the next while each connect hangs for 5s", func() {
		var mu sync.Mutex
		var starts, ends []time.Time
		var h *harness
		h = run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) },
			func(_ context.Context, round int) (planner.Facts, error) {
				if round == 0 {
					return memPrimary, nil
				}
				mu.Lock()
				starts = append(starts, h.clk.Now())
				mu.Unlock()
				h.clk.Step(5 * time.Second)
				mu.Lock()
				ends = append(ends, h.clk.Now())
				mu.Unlock()
				return planner.Facts{}, fmt.Errorf("acquire a connection: %w", context.DeadlineExceeded)
			})
		begin := h.clk.Now()
		h.advanceBy(time.Second, func() bool { return h.clk.Now().Sub(begin) >= 60*time.Second })

		mu.Lock()
		defer mu.Unlock()
		Expect(len(ends)).To(BeNumerically(">=", 3))
		for i := 1; i < len(ends) && i < len(starts); i++ {
			Expect(starts[i].Sub(ends[i-1])).To(BeNumerically(">=", time.Second))
		}
	})

	It("leaves cnpg_collector_up as it is when a round fails because the engine stops", func(ctx SpecContext) {
		Expect(engine.FactsRoundOnStop(ctx)).To(BeEquivalentTo(1))
	})

	It("keeps the failure count when a round panics while the engine stops", func(ctx SpecContext) {
		Expect(engine.FactsFailuresAfterPanicOnStop(ctx)).To(Equal(3))
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

var _ = DescribeTable("A failed run",
	func(ctx SpecContext, outcome selfmetrics.Outcome) {
		res := executor.Result{Job: planner.Job{Query: "backends", Database: "app", Fingerprint: "f"}, Outcome: outcome, WouldBe: 5}
		exported, start, t0, err := engine.RecordFailureBetweenSuccesses(ctx, res)
		Expect(err).NotTo(HaveOccurred())
		Expect(exported).To(BeFalse(), "the series stop exporting")
		Expect(start).To(Equal(t0), "the next run keeps the counter's start time")
	},
	Entry("on a timeout", selfmetrics.OutcomeTimeout),
	Entry("when denied", selfmetrics.OutcomeDenied),
	Entry("over the series cap", selfmetrics.OutcomeCapped),
)

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

var _ = DescribeTable("FactsTimeout",
	func(query time.Duration, want time.Duration) {
		Expect(engine.FactsTimeout(settings.Limits{QueryTimeout: query})).To(Equal(want))
	},
	Entry("10 s query timeout", 10*time.Second, 29*time.Second),
	Entry("100 ms query timeout", 100*time.Millisecond, 9200*time.Millisecond),
)

// clusterConfig is a memConfig that watches the instance's Cluster, as kubesource.Source does.
type clusterConfig struct {
	*memConfig
	cluster atomic.Pointer[apiv1.Cluster]
}

func (c *clusterConfig) Cluster() (*apiv1.Cluster, bool) {
	cl := c.cluster.Load()
	return cl, cl != nil
}

var _ engine.ClusterReader = (*clusterConfig)(nil)

// builtinCluster is Cluster c: 2 instances, sync 1/1, c-1 the current primary, a 1Gi WAL volume, both
// instances on one node.
func builtinCluster() *apiv1.Cluster {
	c := &apiv1.Cluster{}
	c.Name = "c"
	c.Spec.Instances = 2
	c.Spec.MinSyncReplicas = 1
	c.Spec.MaxSyncReplicas = 1
	c.Spec.WalStorage = &apiv1.StorageConfiguration{Size: "1Gi"}
	c.Status.CurrentPrimary = "c-1"
	c.Status.InstancesStatus = map[apiv1.PodStatus][]string{apiv1.PodHealthy: {"c-1", "c-2"}}
	c.Status.Topology = apiv1.Topology{SuccessfullyExtracted: true, NodesUsed: 1}
	return c
}

// builtinFacts is a PostgreSQL 18.0.7 primary with 16 MiB WAL segments.
var builtinFacts = planner.Facts{Version: *semver.MustParse("18.0.7"), Databases: memPrimary.Databases, WALSegmentSize: 16777216}

// series is one exported series: a metric name and its exact attributes.
type series struct {
	name  string
	attrs map[string]string
}

func (s series) String() string { return fmt.Sprintf("%s%v", s.name, s.attrs) }

// valueIn returns the value of s in req.
func valueIn(req otlptest.Request, s series) (float64, bool) {
	for _, p := range requestPoints(req, s.name) {
		if maps.Equal(p.attrs, s.attrs) {
			return p.value, true
		}
	}
	return 0, false
}

var (
	timestamps = []series{
		{"cnpg_collector_first_recoverability_point", map[string]string{}},
		{"cnpg_collector_last_available_backup_timestamp", map[string]string{}},
		{"cnpg_collector_last_failed_backup_timestamp", map[string]string{}},
	}
	fencingOn     = series{"cnpg_collector_fencing_on", map[string]string{}}
	walVolumeSize = series{"cnpg_collector_pg_wal", map[string]string{"value": "volume_size"}}
	walVolumeMax  = series{"cnpg_collector_pg_wal", map[string]string{"value": "volume_max"}}
	pgVersion     = series{"cnpg_collector_postgres_version", map[string]string{"full": "18.7", "cluster": "c"}}
)

// builtinValue is the value of one built-in series.
type builtinValue struct {
	series
	value float64
}

// builtinValues are the built-ins of builtinCluster and builtinFacts on c-1.
func builtinValues() []builtinValue {
	none := map[string]string{}
	want := []builtinValue{
		{series{"cnpg_collector_replica_mode", none}, 0},
		{fencingOn, 0},
		{series{"cnpg_collector_nodes_used", none}, 1},
		{series{"cnpg_collector_sync_replicas", map[string]string{"value": "min"}}, 1},
		{series{"cnpg_collector_sync_replicas", map[string]string{"value": "max"}}, 1},
		{series{"cnpg_collector_sync_replicas", map[string]string{"value": "expected"}}, 1},
		{series{"cnpg_collector_manual_switchover_required", none}, 0},
		{walVolumeSize, 1073741824},
		{walVolumeMax, 64},
		{pgVersion, 18.7},
	}
	for _, t := range timestamps {
		want = append(want, builtinValue{t, 0})
	}
	return want
}

var _ = Describe("The built-ins computed outside SQL", func() {
	var (
		h       *harness
		cfg     *clusterConfig
		failing atomic.Bool
		standby atomic.Bool
	)

	BeforeEach(func() {
		failing.Store(false)
		standby.Store(false)
		h = runWith(func(url string) *memConfig {
			return newMemConfig(strings.Replace(httpSettings(url), "cluster: mem\n", "cluster: c\n", 1), httpFiles())
		}, func(context.Context, int) (planner.Facts, error) {
			if failing.Load() {
				return planner.Facts{}, errors.New("acquire a connection: connection refused")
			}
			f := builtinFacts
			f.InRecovery = standby.Load()
			return f, nil
		}, runOptions{
			env: map[string]string{"POD_NAME": "c-1"},
			config: func(m *memConfig) engine.Config {
				cfg = &clusterConfig{memConfig: m}
				cfg.cluster.Store(builtinCluster())
				return cfg
			},
		})
	})

	// exportsAfter waits for n more requests than now and returns them.
	exportsAfter := func(n int) []otlptest.Request {
		GinkgoHelper()
		since := len(h.requests())
		h.advance(func() bool { return len(h.requests()) >= since+n })
		return h.requests()[since:]
	}

	It("follow the Cluster at each export and the newest facts round", func() {
		h.advance(func() bool {
			reqs := h.requests()
			if len(reqs) == 0 {
				return false
			}
			_, ok := valueIn(reqs[len(reqs)-1], pgVersion)
			return ok
		})
		for _, req := range exportsAfter(3) {
			for _, b := range builtinValues() {
				got, ok := valueIn(req, b.series)
				Expect(ok).To(BeTrue(), "%v missing", b.series)
				Expect(got).To(Equal(b.value), "%v", b.series)
			}
		}

		By("fencing c-1, with the configuration generation unchanged")
		fenced := builtinCluster()
		fenced.Annotations = map[string]string{"cnpg.io/fencedInstances": `["c-1"]`}
		cfg.cluster.Store(fenced)
		req := exportsAfter(1)[0]
		got, ok := valueIn(req, fencingOn)
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal(1.0))
		for _, p := range points(h.srv, "cnpg_otel_config_info", time.Time{}) {
			Expect(p.attrs).To(HaveKeyWithValue("generation", "1"))
		}

		By("failing the facts rounds")
		failing.Store(true)
		h.advance(h.has("cnpg_collector_up", map[string]string{"cluster": "c"}, 0))
		for _, req := range exportsAfter(2) {
			for _, s := range append([]series{walVolumeSize, walVolumeMax, pgVersion}, timestamps...) {
				_, ok := valueIn(req, s)
				Expect(ok).To(BeFalse(), "%v exported while the facts fail", s)
			}
			got, ok := valueIn(req, fencingOn)
			Expect(ok).To(BeTrue())
			Expect(got).To(Equal(1.0))
		}

		By("gathering the facts of a server in recovery")
		standby.Store(true)
		failing.Store(false)
		h.advance(h.has("cnpg_collector_up", map[string]string{"cluster": "c"}, 1))
		for _, req := range exportsAfter(2) {
			for _, s := range timestamps {
				_, ok := valueIn(req, s)
				Expect(ok).To(BeFalse(), "%v exported in recovery", s)
			}
			for _, s := range []series{walVolumeSize, walVolumeMax, pgVersion} {
				_, ok := valueIn(req, s)
				Expect(ok).To(BeTrue(), "%v missing in recovery", s)
			}
		}
	})
})

var _ = Describe("The self-measuring metrics", func() {
	It("count the facts rounds under collector=Collect.up", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) },
			func(ctx context.Context, round int) (planner.Facts, error) {
				switch round {
				case 0:
					return memPrimary, nil
				case 1:
					return unreachable(ctx, round)
				default:
					<-ctx.Done()
					return planner.Facts{}, ctx.Err()
				}
			})
		h.advance(func() bool { return len(h.roundTimes()) >= 3 })
		up := map[string]string{"collector": "Collect.up"}
		h.advance(h.has("cnpg_collector_collections_total", map[string]string{}, 2))
		h.advance(h.has("cnpg_collector_collection_errors_total", up, 1))
		h.advance(h.has("cnpg_collector_last_collection_error", map[string]string{}, 1))
		_, ok := h.lastValue("cnpg_collector_collection_duration_seconds", up)
		Expect(ok).To(BeTrue())
	})

	It("report nothing before the first facts round", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) },
			func(ctx context.Context, _ int) (planner.Facts, error) {
				<-ctx.Done()
				return planner.Facts{}, ctx.Err()
			})
		h.advance(func() bool { return len(h.requests()) >= 2 })
		for _, name := range []string{
			"cnpg_collector_collections_total", "cnpg_collector_collection_errors_total",
			"cnpg_collector_last_collection_error", "cnpg_collector_collection_duration_seconds",
		} {
			Expect(points(h.srv, name, time.Time{})).To(BeEmpty(), name)
		}
	})
})

var _ = Describe("A superuser session role", func() {
	// fromSQL are the series the database gives: the superuser refusal exports none of them.
	fromSQL := func(name string) bool {
		return slices.Contains([]string{
			"cnpg_collector_up", "cnpg_collector_pg_wal", "cnpg_collector_pg_wal_archive_status",
			"cnpg_collector_lo_pages", "cnpg_collector_postgres_version", "cnpg_otel_query_runs_total",
		}, name) || strings.HasPrefix(name, "cnpg_collector_wal_") || strings.HasPrefix(name, "cnpg_alpha_") ||
			strings.HasPrefix(name, "cnpg_beta_")
	}
	// refusedIn checks that req has no series from the database and the collection metrics and the Cluster's built-ins.
	refusedIn := func(req otlptest.Request) {
		GinkgoHelper()
		var names []string
		for _, rm := range req.Metrics.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					names = append(names, m.GetName())
				}
			}
		}
		for _, name := range names {
			Expect(fromSQL(name)).To(BeFalse(), "%s exported to a superuser session", name)
		}
		for _, p := range requestPoints(req, "cnpg_collector_sync_replicas") {
			Expect(p.attrs["value"]).NotTo(Equal("observed"))
		}
		for _, p := range requestPoints(req, "cnpg_otel_check") {
			Expect(p.attrs["check"]).To(Equal("superuser"))
		}
	}

	var (
		h     *harness
		super atomic.Bool
	)

	// start runs the engine on a role that is a superuser or not.
	start := func(superuser bool) {
		GinkgoHelper()
		super.Store(superuser)
		h = runWith(func(url string) *memConfig {
			return newMemConfig(strings.Replace(httpSettings(url), "cluster: mem\n", "cluster: c\n", 1), httpFiles())
		}, func(context.Context, int) (planner.Facts, error) {
			if super.Load() {
				return planner.Facts{Checks: map[string]bool{"superuser": true}},
					fmt.Errorf("gather facts: %w", planner.ErrSuperuser)
			}
			f := builtinFacts
			f.Checks = map[string]bool{"superuser": false}
			return f, nil
		}, runOptions{
			env: map[string]string{"POD_NAME": "c-1"},
			config: func(m *memConfig) engine.Config {
				cfg := &clusterConfig{memConfig: m}
				cfg.cluster.Store(builtinCluster())
				return cfg
			},
		})
	}

	It("runs no query, exports the check and the Cluster's built-ins, waits a planning cycle and logs the start and the end", func() {
		start(true)
		h.advanceBy(time.Second, func() bool { return len(h.roundTimes()) >= 4 })
		for _, gap := range gaps(h.roundTimes()) {
			Expect(gap).To(BeNumerically("~", 30*time.Second, time.Second))
		}
		h.advance(h.has("cnpg_otel_check", map[string]string{"check": "superuser"}, 1))
		reqs := h.requests()
		for _, req := range reqs {
			refusedIn(req)
		}
		latest := reqs[len(reqs)-1]
		Expect(requestPoints(latest, "cnpg_collector_collection_errors_total")).NotTo(BeEmpty())
		Expect(requestPoints(latest, "cnpg_collector_collections_total")).NotTo(BeEmpty())
		Expect(requestPoints(latest, "cnpg_collector_replica_mode")).NotTo(BeEmpty())
		Expect(h.logged(`"msg"="plan updated" "jobs"=0`)).To(Equal(1))
		Expect(h.loggedAll(`"msg"="query failed"`, `"query"="facts"`, `"outcome"="denied"`, "the session role is a superuser", "cnpg_metrics_exporter")).To(Equal(1))
		Expect(h.loggedAll(`"msg"="query recovered"`, `"query"="facts"`)).To(BeZero())

		By("the role losing the attribute")
		super.Store(false)
		h.advanceBy(time.Second, h.has("cnpg_collector_up", map[string]string{"cluster": "c"}, 1))
		h.advance(h.has("cnpg_otel_check", map[string]string{"check": "superuser"}, 0))
		Expect(h.loggedAll(`"msg"="query recovered"`, `"query"="facts"`)).To(Equal(1))
		Expect(h.loggedAll(`"msg"="plan updated"`, `"jobs"=8`)).To(Equal(1))
		h.advance(func() bool {
			_, ok := h.lastValue("cnpg_otel_query_runs_total", map[string]string{"query": "alpha"})
			return ok
		})
	})

	It("empties the plan of a session that turns superuser and plans it again after", func() {
		start(false)
		h.advance(h.has("cnpg_otel_query_runs_total", map[string]string{"query": "alpha"}, 1))
		Expect(h.loggedAll(`"msg"="plan updated"`, `"jobs"=8`)).To(Equal(1))

		super.Store(true)
		h.advanceBy(time.Second, func() bool { return h.loggedAll(`"msg"="query failed"`, `"query"="facts"`) >= 1 })
		Expect(h.logged(`"msg"="plan updated" "jobs"=0`)).To(Equal(1))
		h.advance(func() bool {
			reqs := h.requests()
			return len(reqs) > 0 && len(requestPoints(reqs[len(reqs)-1], "cnpg_otel_query_runs_total")) == 0
		})
		since := len(h.requests())
		h.advance(func() bool { return len(h.requests()) >= since+3 })
		for _, req := range h.requests()[since:] {
			refusedIn(req)
		}

		super.Store(false)
		h.advanceBy(time.Second, h.has("cnpg_collector_up", map[string]string{"cluster": "c"}, 1))
		h.advance(func() bool { return h.loggedAll(`"msg"="plan updated"`, `"jobs"=8`) == 2 })
	})
})
