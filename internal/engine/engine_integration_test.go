//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.yaml.in/yaml/v4"
	"k8s.io/utils/clock"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/export"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/test/otlptest"
	"github.com/gabrosys/plugin-telemetry/test/pgtest"
)

var server *pgtest.Postgres

var _ = BeforeSuite(func(ctx SpecContext) {
	image := os.Getenv("POSTGRES_IMAGE")
	Expect(image).NotTo(BeEmpty(), "POSTGRES_IMAGE must name the PostgreSQL image (make test-integration sets it)")
	pg.ClearEnv()
	export.ClearEnv()

	var err error
	server, err = pgtest.Start(ctx, image)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func(ctx SpecContext) {
		Expect(server.Terminate(ctx)).To(Succeed())
	})
}, NodeTimeout(5*time.Minute))

const (
	queryInterval  = time.Second
	exportInterval = time.Second
	// slack covers the work between the timers: the reload, the run and the request.
	slack = time.Second
)

// defaultKeys are CNPG v1.30.1's default queries planned on a PostgreSQL 18 primary, plus the builtin one.
var defaultKeys = []string{
	"backends", "backends_waiting", "collector", "pg_database", "pg_extensions", "pg_postmaster",
	"pg_replication", "pg_replication_slots", "pg_settings", "pg_stat_archiver", "pg_stat_bgwriter_17",
	"pg_stat_checkpointer", "pg_stat_database", "pg_stat_replication",
}

const extraQuery = `extra_probe:
  query: SELECT 42 AS value
  metrics:
    - value:
        usage: GAUGE
        description: A probe.
`

func writeFile(path, content string) {
	GinkgoHelper()
	Expect(os.WriteFile(path, []byte(content), 0o600)).To(Succeed())
}

// defaultQueries returns data.queries of CNPG v1.30.1's default-monitoring ConfigMap.
func defaultQueries() string {
	GinkgoHelper()
	var cm struct {
		Data map[string]string `yaml:"data"`
	}
	raw, err := os.ReadFile("../queries/testdata/default-monitoring-v1.30.1.yaml")
	Expect(err).NotTo(HaveOccurred())
	Expect(yaml.Unmarshal(raw, &cm)).To(Succeed())
	Expect(cm.Data).To(HaveKey("queries"))
	return cm.Data["queries"]
}

// fixture is a directory with an engine configuration, CNPG's default queries and an extra
// source file, next to an OTLP/HTTP receiver.
type fixture struct {
	config, extra string
	valid         string // a valid configuration for the receiver
	srv           *otlptest.Server
	done          chan error
	gather        func(context.Context, settings.Settings) (planner.Facts, error) // nil reads the facts from PostgreSQL
}

func newFixture(planInterval time.Duration) *fixture {
	GinkgoHelper()
	dir := GinkgoT().TempDir()
	f := &fixture{
		config: filepath.Join(dir, "engine.yaml"),
		extra:  filepath.Join(dir, "extra.yaml"),
		srv:    otlptest.NewHTTP(nil),
	}
	DeferCleanup(f.srv.Close)
	writeFile(filepath.Join(dir, "default-queries.yaml"), defaultQueries())
	writeFile(f.extra, "")
	f.valid = fmt.Sprintf(`cluster: it
namespace: ns
postgres:
  host: %s
  port: %d
  user: %s
  passwordFile: %s
planInterval: %s
defaultInterval: %s
sources:
  - default-queries.yaml
  - extra.yaml
exporter:
  endpoint: %s/v1/metrics
  interval: %s
`, server.Host, server.Port, pgtest.MonitorUser, server.MonitorConfig(dir).PasswordFile,
		planInterval, queryInterval, f.srv.URL, exportInterval)
	return f
}

// run starts the engine and stops it at cleanup.
func (f *fixture) run() {
	GinkgoHelper()
	f.runConfig(engine.FileConfig(f.config))
}

// runConfig starts the engine on cfg and stops it at cleanup.
func (f *fixture) runConfig(cfg engine.Config) {
	GinkgoHelper()
	ctx, cancel := context.WithCancel(ctrllog.IntoContext(context.Background(), GinkgoLogr))
	f.done = make(chan error, 1)
	go func() {
		f.done <- engine.RunWith(ctx, engine.Options{
			Config: cfg,
			Clock:  clock.RealClock{},
			Rand:   rand.New(rand.NewPCG(1, 2)), //nolint:gosec // seeded jitter for the test
			Getenv: func(string) string { return "" },
		}, f.gather)
	}()
	DeferCleanup(func() {
		cancel()
		Eventually(f.done).WithTimeout(20 * time.Second).Should(Receive(BeNil()))
	})
}

func expectUp(srv *otlptest.Server, want float64) func(Gomega) {
	return func(g Gomega) {
		up, ok := last(srv, "cnpg_collector_up", map[string]string{"cluster": "it"})
		g.Expect(ok).To(BeTrue(), "cnpg_collector_up{cluster=it} was not exported; got %d requests with %v",
			len(srv.Requests()), metricNames(srv))
		g.Expect(up).To(Equal(want))
	}
}

var _ = Describe("Run", Ordered, func() {
	var f *fixture

	BeforeAll(func() {
		f = newFixture(time.Second)
		writeFile(f.config, f.valid)
		f.run()
	})

	It("exports CNPG's default queries, cnpg_collector_up and the self-metrics", func() {
		Eventually(func(g Gomega) {
			expectUp(f.srv, 1)(g)
			for _, name := range []string{
				"cnpg_backends_total", "cnpg_pg_database_size_bytes", "cnpg_pg_postmaster_start_time",
				"cnpg_pg_settings_setting", "cnpg_pg_stat_database_xact_commit", "cnpg_pg_replication_in_recovery",
				"cnpg_collector_lo_pages",
			} {
				g.Expect(points(f.srv, name, time.Time{})).NotTo(BeEmpty(), name)
			}
			for _, q := range defaultKeys {
				runs, ok := last(f.srv, "cnpg_otel_query_runs_total", map[string]string{"query": q, "outcome": "ok"})
				g.Expect(ok).To(BeTrue(), q)
				g.Expect(runs).To(BeNumerically(">=", 1), q)
			}
			for _, name := range []string{"cnpg_otel_config_info", "cnpg_otel_series", "cnpg_otel_query_interval_seconds", "go.goroutine.count"} {
				g.Expect(points(f.srv, name, time.Time{})).NotTo(BeEmpty(), name)
			}
		}).WithTimeout(30 * time.Second).WithPolling(250 * time.Millisecond).Should(Succeed())

		for _, p := range points(f.srv, "cnpg_otel_query_runs_total", time.Time{}) {
			Expect(p.attrs["outcome"]).NotTo(BeElementOf("error", "timeout"), "%v", p.attrs)
		}
		info := points(f.srv, "cnpg_otel_config_info", time.Time{})
		Expect(info[len(info)-1].attrs).To(HaveKeyWithValue("generation", "1"))
		req := f.srv.Requests()[0]
		Expect(attrMap(req.Metrics.GetResourceMetrics()[0].GetResource().GetAttributes())).To(And(
			HaveKeyWithValue("service.name", "it"), HaveKeyWithValue("service.namespace", "ns")))
	})

	It("exports cnpg_otel_check for the monitoring role after the facts round", func() {
		Eventually(func(g Gomega) {
			for check, want := range map[string]float64{
				"pg_monitor": 1, "read_only": 1, "track_io_timing": 0, "track_wal_io_timing": 0, "track_functions": 0,
			} {
				got, ok := last(f.srv, "cnpg_otel_check", map[string]string{"check": check})
				g.Expect(ok).To(BeTrue(), check)
				g.Expect(got).To(Equal(want), check)
			}
		}).WithTimeout(30 * time.Second).WithPolling(250 * time.Millisecond).Should(Succeed())
	})

	It("exports every built-in a file configuration has, which has no Cluster", func() {
		Eventually(func(g Gomega) {
			reqs := f.srv.Requests()
			g.Expect(reqs).NotTo(BeEmpty())
			req := reqs[len(reqs)-1]
			g.Expect(attrValues(requestPoints(req, "cnpg_collector_pg_wal"), "value")).To(
				ConsistOf("count", "size", "min", "max", "keep", "slots_max"))
			g.Expect(attrValues(requestPoints(req, "cnpg_collector_pg_wal_archive_status"), "value")).To(ConsistOf("ready", "done"))
			for _, name := range []string{
				"cnpg_collector_wal_records", "cnpg_collector_wal_fpi", "cnpg_collector_wal_bytes", "cnpg_collector_wal_buffers_full",
			} {
				g.Expect(requestPoints(req, name)).To(HaveLen(1), name)
			}
			g.Expect(attrValues(requestPoints(req, "cnpg_collector_sync_replicas"), "value")).To(ConsistOf("observed"))
			version := requestPoints(req, "cnpg_collector_postgres_version")
			g.Expect(version).To(HaveLen(1))
			g.Expect(version[0].attrs).To(Equal(map[string]string{"full": "18.7", "cluster": "it"}))
			g.Expect(version[0].value).To(Equal(18.7))
			g.Expect(requestPoints(req, "cnpg_collector_collections_total")).To(HaveLen(1))
			g.Expect(requestPoints(req, "cnpg_collector_fencing_on")).To(BeEmpty())
		}).WithTimeout(30 * time.Second).WithPolling(250 * time.Millisecond).Should(Succeed())
	})

	It("exports a query appended to a source file within debounce + interval + export interval", func() {
		bound := engine.Debounce + queryInterval + exportInterval + slack
		start := time.Now()
		writeFile(f.extra, extraQuery)

		Eventually(func() []point { return points(f.srv, "cnpg_extra_probe_value", start) }).
			WithTimeout(bound).WithPolling(50 * time.Millisecond).ShouldNot(BeEmpty())
		first := points(f.srv, "cnpg_extra_probe_value", start)[0]
		Expect(first.value).To(Equal(42.0))
		Expect(first.at.Sub(start)).To(BeNumerically("<=", bound))
		info := points(f.srv, "cnpg_otel_config_info", start)
		Expect(info[len(info)-1].attrs).To(HaveKeyWithValue("generation", "2"))
	})

	It("keeps the metric of a source that becomes invalid and counts a load error", func() {
		writeFile(f.extra, extraQuery+"broken: [\n")
		Eventually(func(g Gomega) {
			errs, ok := last(f.srv, "cnpg_otel_config_loads_total", map[string]string{"source": f.extra, "outcome": "error"})
			g.Expect(ok).To(BeTrue())
			g.Expect(errs).To(BeNumerically(">=", 1))
		}).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		since := time.Now()
		Eventually(func() []point { return points(f.srv, "cnpg_extra_probe_value", since) }).
			WithTimeout(5 * time.Second).ShouldNot(BeEmpty())
		info := points(f.srv, "cnpg_otel_config_info", since)
		Expect(info[len(info)-1].attrs).To(HaveKeyWithValue("generation", "2"))
	})
})

// backends counts the backends that match cond, other than the probe's own.
func backends(ctx context.Context, cond string, args ...any) (n int, err error) {
	cc, err := pg.ConnConfig(server.MonitorConfig(GinkgoT().TempDir()), "postgres")
	if err != nil {
		return 0, err
	}
	conn, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, conn.Close(context.WithoutCancel(ctx))) }()
	err = conn.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE ("+cond+") AND pid <> pg_backend_pid()", args...).Scan(&n)
	return n, err
}

// sessions counts the backends of user in state, other than the probe's own; an empty state counts every one.
func sessions(ctx context.Context, user, state string) (int, error) {
	return backends(ctx, "usename = $1 AND ($2 = '' OR state = $2)", user, state)
}

// holdQuery keeps a connection of the pool busy for 5 s.
const holdQuery = `hold_probe:
  query: SELECT 1 AS value FROM pg_sleep(5)
  metrics:
    - value:
        usage: GAUGE
        description: Holds a connection.
`

var _ = Describe("Run on a change of the postgres user", func() {
	It("closes the old pool's connections at once on a postgres settings change", func(ctx SpecContext) {
		const other, otherPassword = "other_monitor", "other-password"
		Expect(server.Exec(ctx, "CREATE ROLE "+other+" LOGIN PASSWORD '"+otherPassword+"'")).To(Succeed())
		Expect(server.Exec(ctx, "GRANT pg_monitor TO "+other)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) { Expect(server.Exec(ctx, "DROP ROLE "+other)).To(Succeed()) })

		f := newFixture(time.Second)
		writeFile(f.extra, holdQuery)
		writeFile(f.config, f.valid)
		f.run()
		Eventually(expectUp(f.srv, 1)).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		// A run of hold_probe is in flight while other runs leave idle connections in the pool.
		Eventually(func() (int, error) { return sessions(ctx, pgtest.MonitorUser, "active") }).
			WithTimeout(15 * time.Second).WithPolling(50 * time.Millisecond).Should(BeNumerically(">", 0))
		Eventually(func() (int, error) { return sessions(ctx, pgtest.MonitorUser, "idle") }).
			WithTimeout(5 * time.Second).WithPolling(50 * time.Millisecond).Should(BeNumerically(">", 0))

		dir := filepath.Dir(f.config)
		writeFile(filepath.Join(dir, "other.pw"), otherPassword+"\n")
		changed := strings.Replace(f.valid, "user: "+pgtest.MonitorUser+"\n", "user: "+other+"\n", 1)
		changed = strings.Replace(changed, server.MonitorConfig(dir).PasswordFile, filepath.Join(dir, "other.pw"), 1)
		Expect(changed).NotTo(Equal(f.valid))
		writeFile(f.config, changed)

		Eventually(func() (int, error) { return sessions(ctx, other, "") }).
			WithTimeout(10 * time.Second).WithPolling(50 * time.Millisecond).Should(BeNumerically(">", 0))
		Eventually(func() (int, error) { return sessions(ctx, pgtest.MonitorUser, "idle") }).
			WithTimeout(2 * time.Second).WithPolling(50 * time.Millisecond).Should(BeZero())
		// hold_probe's busy connection closes when its run releases it.
		Eventually(func() (int, error) { return sessions(ctx, pgtest.MonitorUser, "") }).
			WithTimeout(8 * time.Second).WithPolling(100 * time.Millisecond).Should(BeZero())
	}, SpecTimeout(time.Minute))
})

// switchable is a Config that has nothing to run while off is set.
type switchable struct {
	engine.Config
	off atomic.Bool
}

func (c *switchable) Settings() (settings.Settings, []byte, error) {
	if c.off.Load() {
		return settings.Settings{}, nil, fmt.Errorf("%w: switched off", engine.ErrNoConfiguration)
	}
	return c.Config.Settings()
}

var _ = Describe("Run on ErrNoConfiguration", func() {
	It("closes the pool's connections and resumes on the next settings", func(ctx SpecContext) {
		f := newFixture(time.Second)
		writeFile(f.config, f.valid)
		cfg := &switchable{Config: engine.FileConfig(f.config)}
		f.runConfig(cfg)
		Eventually(expectUp(f.srv, 1)).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		Eventually(func() (int, error) { return sessions(ctx, pgtest.MonitorUser, "") }).
			WithTimeout(5 * time.Second).Should(BeNumerically(">", 0))

		cfg.off.Store(true)
		writeFile(f.config, f.valid+"# touched\n")
		Eventually(func() (int, error) { return sessions(ctx, pgtest.MonitorUser, "") }).
			WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(BeZero())

		cfg.off.Store(false)
		since := time.Now()
		writeFile(f.config, f.valid)
		Eventually(func() []point { return points(f.srv, "cnpg_backends_total", since) }).
			WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).ShouldNot(BeEmpty())
	}, SpecTimeout(time.Minute))
})

// resourceHas reports whether a request received at or after since has a resource attribute key=value.
func resourceHas(srv *otlptest.Server, since time.Time, key, value string) bool {
	for _, req := range srv.Requests() {
		if req.Received.Before(since) {
			continue
		}
		for _, rm := range req.Metrics.GetResourceMetrics() {
			if attrMap(rm.GetResource().GetAttributes())[key] == value {
				return true
			}
		}
	}
	return false
}

var _ = Describe("Run on a settings change", Ordered, func() {
	var (
		f      *fixture
		second *otlptest.Server
		valid  string // f.valid pointed at second
	)

	BeforeAll(func() {
		f = newFixture(time.Second)
		writeFile(f.config, f.valid)
		f.run()
		Eventually(expectUp(f.srv, 1)).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		second = otlptest.NewHTTP(nil)
		DeferCleanup(second.Close)
		valid = strings.Replace(f.valid, f.srv.URL, second.URL, 1)
	})

	It("sends to a new endpoint and stops sending to the old one", func() {
		writeFile(f.config, valid)
		Eventually(expectUp(second, 1)).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		info := points(second, "cnpg_otel_config_info", time.Time{})
		Expect(info[len(info)-1].attrs).To(HaveKeyWithValue("generation", "2"))

		n := len(f.srv.Requests())
		Consistently(func() int { return len(f.srv.Requests()) }).WithTimeout(3 * time.Second).Should(Equal(n))
	})

	It("connects with changed postgres settings", func() {
		port := fmt.Sprintf("port: %d\n", server.Port)
		Expect(valid).To(ContainSubstring(port))
		writeFile(f.config, strings.Replace(valid, port, "port: 1\n", 1))
		Eventually(expectUp(second, 0)).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		writeFile(f.config, valid)
		Eventually(expectUp(second, 1)).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})

	It("exports with changed resource attributes", func() {
		since := time.Now()
		writeFile(f.config, valid+"resourceAttributes:\n  team: db\n")
		Eventually(func() bool { return resourceHas(second, since, "team", "db") }).
			WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue())
		after := time.Now()
		Eventually(func() []point { return points(second, "cnpg_backends_total", after) }).
			WithTimeout(5 * time.Second).ShouldNot(BeEmpty())
	})
})

var _ = Describe("Run when the exporter files change", func() {
	It("sends the new headers without reloading the configuration", func() {
		f := newFixture(time.Second)
		headers := filepath.Join(filepath.Dir(f.config), "headers")
		writeFile(headers, "x-probe=one")
		writeFile(f.config, f.valid+"  headersFile: headers\n")
		f.run()

		header := func(value string) func() bool {
			return func() bool {
				reqs := f.srv.Requests()
				return len(reqs) > 0 && reqs[len(reqs)-1].Header.Get("X-Probe") == value
			}
		}
		Eventually(header("one")).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue())

		writeFile(headers, "x-probe=two")
		Eventually(header("two")).WithTimeout(engine.Debounce + exportInterval + slack + time.Second).
			WithPolling(50 * time.Millisecond).Should(BeTrue())
		info := points(f.srv, "cnpg_otel_config_info", time.Time{})
		Expect(info[len(info)-1].attrs).To(HaveKeyWithValue("generation", "1"))
	})
})

var _ = Describe("Run on a source change", func() {
	It("keeps the snapshot entries of unchanged queries across a replan", func(SpecContext) {
		f := newFixture(time.Second)
		writeFile(filepath.Join(filepath.Dir(f.config), "default-queries.yaml"), "")
		writeFile(f.extra, alphaBeta)
		// An hour between runs: after the first run of each query only the snapshot entry keeps exporting it.
		valid := strings.Replace(f.valid, fmt.Sprintf("defaultInterval: %s\n", queryInterval), "defaultInterval: 1h\n", 1)
		Expect(valid).NotTo(Equal(f.valid))
		writeFile(f.config, valid)
		f.run()
		Eventually(func(g Gomega) {
			g.Expect(points(f.srv, "cnpg_alpha_value", time.Time{})).NotTo(BeEmpty())
			g.Expect(points(f.srv, "cnpg_beta_value", time.Time{})).NotTo(BeEmpty())
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		writeFile(f.extra, strings.Replace(alphaBeta, "SELECT 2", "SELECT 20", 1))
		var replanned time.Time
		Eventually(func() bool {
			for _, p := range points(f.srv, "cnpg_otel_config_info", time.Time{}) {
				if p.attrs["generation"] == "2" {
					replanned = p.at
					return true
				}
			}
			return false
		}).WithTimeout(10 * time.Second).WithPolling(50 * time.Millisecond).Should(BeTrue())
		Eventually(func() []float64 { return values(points(f.srv, "cnpg_beta_value", replanned.Add(time.Nanosecond))) }).
			WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(ContainElement(20.0))

		after, withAlpha := requestsSince(f.srv, replanned.Add(time.Nanosecond), "cnpg_alpha_value")
		Expect(after).To(BeNumerically(">", 0))
		Expect(withAlpha).To(Equal(after), "alpha's entry stays although alpha does not run again")
		Expect(values(points(f.srv, "cnpg_beta_value", replanned.Add(time.Nanosecond)))).To(HaveEach(20.0), "beta's old entry is dropped")
	}, SpecTimeout(time.Minute))
})

const slowQuery = `slow_probe:
  query: SELECT 7 AS value FROM pg_sleep(1.5)
  metrics:
    - value:
        usage: GAUGE
        description: Slower than its interval.
`

// requestsSince counts the requests received at or after since, and those that carry the metric called name.
func requestsSince(srv *otlptest.Server, since time.Time, name string) (total, with int) {
	for _, req := range srv.Requests() {
		if req.Received.Before(since) {
			continue
		}
		total++
		found := false
		for _, rm := range req.Metrics.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					found = found || m.GetName() == name
				}
			}
		}
		if found {
			with++
		}
	}
	return total, with
}

var _ = Describe("Run with a query slower than its interval", func() {
	It("keeps the fresh entry of each slow run and counts the skips", func() {
		f := newFixture(time.Second)
		writeFile(filepath.Join(filepath.Dir(f.config), "default-queries.yaml"), "")
		writeFile(f.extra, slowQuery)
		writeFile(f.config, f.valid)
		f.run()

		Eventually(func(g Gomega) {
			skips, ok := last(f.srv, "cnpg_otel_query_runs_total", map[string]string{"query": "slow_probe", "outcome": "skipped"})
			g.Expect(ok).To(BeTrue())
			g.Expect(skips).To(BeNumerically(">=", 2))
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		since := time.Now()
		Eventually(func() int {
			total, _ := requestsSince(f.srv, since, "cnpg_slow_probe_value")
			return total
		}).WithTimeout(10 * time.Second).Should(BeNumerically(">=", 3))
		total, with := requestsSince(f.srv, since, "cnpg_slow_probe_value")
		Expect(with).To(Equal(total))
	})
})

var _ = Describe("Run with an invalid configuration at start", func() {
	It("idles until the configuration is fixed", func() {
		f := newFixture(time.Second)
		writeFile(f.config, "cluster: it\nexporter: [\n")
		f.run()

		Consistently(f.srv.Requests).WithTimeout(3 * time.Second).Should(BeEmpty())
		Expect(f.done).NotTo(Receive())

		writeFile(f.config, f.valid)
		Eventually(expectUp(f.srv, 1)).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		errs, ok := last(f.srv, "cnpg_otel_config_loads_total", map[string]string{"source": f.config, "outcome": "error"})
		Expect(ok).To(BeTrue())
		Expect(errs).To(BeNumerically(">=", 1))
	})
})

var _ = Describe("Run while PostgreSQL is not up yet", func() {
	It("retries the facts with backoff and plans once PostgreSQL comes up", func(ctx SpecContext) {
		// Nothing is planned, so no run can ask for an early round, and the planning cycle is far
		// longer than the spec: only the facts backoff can bring cnpg_collector_up to 1.
		f := newFixture(time.Hour)
		writeFile(f.config, f.valid)
		Expect(server.StopServer(ctx)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) { Expect(server.StartServer(ctx)).To(Succeed()) })
		f.run()
		Eventually(expectUp(f.srv, 0)).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		Expect(server.StartServer(ctx)).To(Succeed())
		Eventually(expectUp(f.srv, 1)).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		since := time.Now()
		Eventually(func() []point { return points(f.srv, "cnpg_backends_total", since) }).
			WithTimeout(10 * time.Second).ShouldNot(BeEmpty())
	}, SpecTimeout(2*time.Minute))
})

var _ = Describe("Run when PostgreSQL stops", func() {
	It("gathers facts right after a run loses its connection", func(ctx SpecContext) {
		// The planning cycle is far longer than the spec: after a successful round only an early round, asked
		// for by a run that lost its connection, can bring cnpg_collector_up to 0. The way back to 1 is not
		// asserted: a failed early round leaves the planning timer alone, so it comes with the next planning
		// cycle (see "comes back after an outage") or with a run that loses its connection after the restart.
		f := newFixture(time.Hour)
		writeFile(f.config, f.valid)
		f.run()
		Eventually(expectUp(f.srv, 1)).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		Expect(server.StopServer(ctx)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) { Expect(server.StartServer(ctx)).To(Succeed()) })
		Eventually(expectUp(f.srv, 0)).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	}, SpecTimeout(2*time.Minute))
})

var _ = Describe("Run on a smart shutdown of PostgreSQL", func() {
	It("lets the postmaster exit within 10 s (the shutdown probe) plus a run although jobs keep running", func(ctx SpecContext) {
		f := newFixture(time.Hour)
		writeFile(f.config, f.valid)
		f.run()
		Eventually(expectUp(f.srv, 1)).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		DeferCleanup(server.StartServer)
		Expect(server.Exec(ctx, "CHECKPOINT")).To(Succeed())
		Expect(server.StopServerSmart(ctx)).To(Succeed())
		Eventually(func() bool { return server.Running(ctx) }).
			WithTimeout(15 * time.Second).WithPolling(250 * time.Millisecond).Should(BeFalse())
	}, SpecTimeout(2*time.Minute))
})

var _ = Describe("Run when PostgreSQL comes back after an outage", func() {
	It("reports cnpg_collector_up 1 within a planning cycle although the runs drove the reconnect backoff up", func(ctx SpecContext) {
		// The runs on the default database fail every second during the 17s outage: with a 30s cap their
		// reconnect backoff would hold every connect to that database until about 32s after the stop.
		planInterval := 5 * time.Second
		f := newFixture(planInterval)
		writeFile(f.config, f.valid)
		f.run()
		Eventually(expectUp(f.srv, 1)).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		Expect(server.StopServer(ctx)).To(Succeed())
		stopped := time.Now()
		DeferCleanup(func(ctx SpecContext) { Expect(server.StartServer(ctx)).To(Succeed()) })
		Eventually(expectUp(f.srv, 0)).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		time.Sleep(time.Until(stopped.Add(17 * time.Second)))

		Expect(server.StartServer(ctx)).To(Succeed())
		back := time.Now()
		Eventually(expectUp(f.srv, 1)).WithTimeout(planInterval + exportInterval + 2*slack).
			WithPolling(100 * time.Millisecond).Should(Succeed())
		GinkgoWriter.Printf("cnpg_collector_up was 1 again %s after the restart\n", time.Since(back))
	}, SpecTimeout(2*time.Minute))
})

const failingCounter = `start_probe:
  query: SELECT n FROM start_probe_table
  metrics:
    - n:
        usage: COUNTER
        description: A counter whose table goes away.
`

var _ = Describe("Run when a counter's query fails", func() {
	It("keeps a counter's start time across a failed run of its query", func(ctx SpecContext) {
		Expect(server.Exec(ctx, "CREATE TABLE start_probe_table AS SELECT 10 AS n")).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(server.Exec(ctx, "DROP TABLE IF EXISTS start_probe_table")).To(Succeed())
		})
		Expect(server.Exec(ctx, "GRANT SELECT ON start_probe_table TO "+pgtest.MonitorUser)).To(Succeed())

		f := newFixture(time.Hour)
		writeFile(filepath.Join(filepath.Dir(f.config), "default-queries.yaml"), "")
		writeFile(f.extra, failingCounter)
		writeFile(f.config, f.valid)
		f.run()

		var first []point
		Eventually(func() []point {
			first = points(f.srv, "cnpg_start_probe_n", time.Time{})
			return first
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).ShouldNot(BeEmpty())
		start := first[0].start
		Expect(start).NotTo(BeZero())

		Expect(server.Exec(ctx, "DROP TABLE start_probe_table")).To(Succeed())
		failed := map[string]string{"query": "start_probe", "outcome": "error"}
		Eventually(func(g Gomega) {
			n, ok := last(f.srv, "cnpg_otel_query_runs_total", failed)
			g.Expect(ok).To(BeTrue())
			g.Expect(n).To(BeNumerically(">=", 1))
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		Expect(server.Exec(ctx, "CREATE TABLE start_probe_table AS SELECT 15 AS n")).To(Succeed())
		Expect(server.Exec(ctx, "GRANT SELECT ON start_probe_table TO "+pgtest.MonitorUser)).To(Succeed())
		Eventually(func() []float64 { return values(points(f.srv, "cnpg_start_probe_n", time.Time{})) }).
			WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(ContainElement(15.0))

		for _, p := range points(f.srv, "cnpg_start_probe_n", time.Time{}) {
			Expect(p.start).To(Equal(start), "value %v", p.value)
		}
	}, SpecTimeout(2*time.Minute))
})

// primaryOnlyProbe is a query that runs on a primary only.
const primaryOnlyProbe = `primary_probe:
  primary: true
  query: SELECT 7 AS value
  metrics:
    - value:
        usage: GAUGE
        description: A probe that runs on a primary only.
`

// realFacts reads the facts from PostgreSQL the way the engine does, on a connection of its own.
func realFacts(ctx context.Context, dir string) (planner.Facts, error) {
	cfg := server.MonitorConfig(dir)
	cc, err := pg.ConnConfig(cfg, "postgres")
	if err != nil {
		return planner.Facts{}, fmt.Errorf("connection config: %w", err)
	}
	conn, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		return planner.Facts{}, fmt.Errorf("connect: %w", err)
	}
	facts, err := planner.GatherFacts(ctx, conn, pg.GuardSQL(cfg))
	if cerr := conn.Close(context.WithoutCancel(ctx)); cerr != nil {
		err = errors.Join(err, fmt.Errorf("close: %w", cerr))
	}
	return facts, err
}

var _ = Describe("Run when the server's role changes", func() {
	It("drops a primary-only query at the first facts round that reports recovery, and plans it again after promotion", func(_ SpecContext) {
		var inRecovery atomic.Bool
		dir := GinkgoT().TempDir()
		f := newFixture(time.Second)
		f.gather = func(ctx context.Context, _ settings.Settings) (planner.Facts, error) {
			facts, err := realFacts(ctx, dir)
			facts.InRecovery = inRecovery.Load()
			return facts, err
		}
		writeFile(f.extra, primaryOnlyProbe)
		writeFile(f.config, f.valid)
		f.run()
		Eventually(func() []point { return points(f.srv, "cnpg_primary_probe_value", time.Time{}) }).
			WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).ShouldNot(BeEmpty())

		inRecovery.Store(true)
		var dropped time.Time
		Eventually(func() []point {
			dropped = time.Now()
			return points(f.srv, "cnpg_primary_probe_value", dropped.Add(-2*exportInterval))
		}).WithTimeout(time.Second + exportInterval + slack + 2*exportInterval).WithPolling(100 * time.Millisecond).Should(BeEmpty())
		Consistently(func() []point { return points(f.srv, "cnpg_primary_probe_value", dropped) }).
			WithTimeout(3 * exportInterval).WithPolling(100 * time.Millisecond).Should(BeEmpty())

		inRecovery.Store(false)
		since := time.Now()
		Eventually(func() []point { return points(f.srv, "cnpg_primary_probe_value", since) }).
			WithTimeout(time.Second + queryInterval + exportInterval + 2*slack).WithPolling(100 * time.Millisecond).ShouldNot(BeEmpty())
	}, SpecTimeout(2*time.Minute))
})

var _ = Describe("Run on a superuser session role", func() {
	It("refuses a superuser session, closes its connections, and resumes after NOSUPERUSER", func(ctx SpecContext) {
		const role = "cnpg_super_test"
		planInterval := 5 * time.Second
		DeferCleanup(func(ctx SpecContext) { Expect(server.Exec(ctx, "DROP ROLE IF EXISTS "+role)).To(Succeed()) })
		Expect(server.Exec(ctx, "CREATE ROLE "+role+" LOGIN SUPERUSER PASSWORD '"+pgtest.MonitorPassword+"'")).To(Succeed())

		f := newFixture(planInterval)
		superuser := strings.Replace(f.valid, "user: "+pgtest.MonitorUser+"\n", "user: "+role+"\n", 1)
		Expect(superuser).NotTo(Equal(f.valid))
		writeFile(f.config, superuser)
		f.run()

		check := map[string]string{"check": "superuser"}
		Eventually(func(g Gomega) {
			got, ok := last(f.srv, "cnpg_otel_check", check)
			g.Expect(ok).To(BeTrue())
			g.Expect(got).To(Equal(1.0))
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		Expect(points(f.srv, "cnpg_collector_up", time.Time{})).To(BeEmpty())
		Eventually(func() (int, error) { return backends(ctx, "application_name = 'cnpg_otel_engine'") }).
			WithTimeout(planInterval).WithPolling(250 * time.Millisecond).Should(BeZero())

		Expect(server.Exec(ctx, "ALTER ROLE "+role+" NOSUPERUSER")).To(Succeed())
		Eventually(expectUp(f.srv, 1)).WithTimeout(2*planInterval + slack).WithPolling(100 * time.Millisecond).Should(Succeed())
		Eventually(func(g Gomega) {
			got, ok := last(f.srv, "cnpg_otel_check", check)
			g.Expect(ok).To(BeTrue())
			g.Expect(got).To(Equal(0.0))
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	}, SpecTimeout(2*time.Minute))

	// The timed facts round is a minute away, so only a run's own role check can reach the refusal in time.
	It("refuses at the next run, not at the next planInterval, when the role becomes a superuser", func(ctx SpecContext) {
		role := pgtest.MonitorUser
		planInterval := time.Minute
		DeferCleanup(func(ctx SpecContext) { Expect(server.Exec(ctx, "ALTER ROLE "+role+" NOSUPERUSER")).To(Succeed()) })

		f := newFixture(planInterval)
		writeFile(f.config, f.valid)
		f.run()
		Eventually(expectUp(f.srv, 1)).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		Eventually(func() []point { return points(f.srv, "cnpg_backends_total", time.Time{}) }).
			WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).ShouldNot(BeEmpty())

		Expect(server.Exec(ctx, "ALTER ROLE "+role+" SUPERUSER")).To(Succeed())
		check := map[string]string{"check": "superuser"}
		Eventually(func(g Gomega) {
			got, ok := last(f.srv, "cnpg_otel_check", check)
			g.Expect(ok).To(BeTrue())
			g.Expect(got).To(Equal(1.0))
		}).WithTimeout(planInterval / 3).WithPolling(100 * time.Millisecond).Should(Succeed())
		Eventually(func() (int, error) { return backends(ctx, "application_name = 'cnpg_otel_engine'") }).
			WithTimeout(5 * time.Second).WithPolling(250 * time.Millisecond).Should(BeZero())
		// The series stop well before the next timed round.
		Eventually(func() []point {
			return points(f.srv, "cnpg_backends_total", time.Now().Add(-2*exportInterval))
		}).WithTimeout(planInterval / 3).WithPolling(100 * time.Millisecond).Should(BeEmpty())
	}, SpecTimeout(2*time.Minute))

	It("closes its connections and runs no query once the role becomes a superuser, and resumes after NOSUPERUSER", func(ctx SpecContext) {
		role := pgtest.MonitorUser
		planInterval := 5 * time.Second
		DeferCleanup(func(ctx SpecContext) { Expect(server.Exec(ctx, "ALTER ROLE "+role+" NOSUPERUSER")).To(Succeed()) })

		f := newFixture(planInterval)
		writeFile(f.config, f.valid)
		f.run()
		Eventually(expectUp(f.srv, 1)).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		engineBackends := func() (int, error) { return backends(ctx, "application_name = 'cnpg_otel_engine'") }
		Eventually(engineBackends).WithTimeout(5 * time.Second).WithPolling(100 * time.Millisecond).Should(BeNumerically(">", 0))

		Expect(server.Exec(ctx, "ALTER ROLE "+role+" SUPERUSER")).To(Succeed())
		check := map[string]string{"check": "superuser"}
		Eventually(func(g Gomega) {
			got, ok := last(f.srv, "cnpg_otel_check", check)
			g.Expect(ok).To(BeTrue())
			g.Expect(got).To(Equal(1.0))
		}).WithTimeout(2*planInterval + slack).WithPolling(100 * time.Millisecond).Should(Succeed())
		Eventually(engineBackends).WithTimeout(planInterval).WithPolling(250 * time.Millisecond).Should(BeZero())

		Expect(server.Exec(ctx, "ALTER ROLE "+role+" NOSUPERUSER")).To(Succeed())
		since := time.Now()
		Eventually(func() []point { return points(f.srv, "cnpg_backends_total", since) }).
			WithTimeout(2*planInterval + slack).WithPolling(100 * time.Millisecond).ShouldNot(BeEmpty())
		Eventually(expectUp(f.srv, 1)).WithTimeout(2*planInterval + slack).WithPolling(100 * time.Millisecond).Should(Succeed())
	}, SpecTimeout(2*time.Minute))
})
