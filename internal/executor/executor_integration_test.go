//go:build integration

package executor_test

import (
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr"

	"github.com/Masterminds/semver/v3"
	"github.com/go-logr/logr/funcr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.yaml.in/yaml/v4"
	clocktesting "k8s.io/utils/clock/testing"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gabrosys/plugin-telemetry/internal/executor"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/test/pgtest"
)

var server *pgtest.Postgres

var _ = BeforeSuite(func(ctx SpecContext) {
	image := os.Getenv("POSTGRES_IMAGE")
	Expect(image).NotTo(BeEmpty(), "POSTGRES_IMAGE must name the PostgreSQL image (make test-integration sets it)")
	pg.ClearEnv()

	var err error
	server, err = pgtest.Start(ctx, image)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func(ctx SpecContext) {
		Expect(server.Terminate(ctx)).To(Succeed())
	})
}, NodeTimeout(5*time.Minute))

var serverVersion = semver.MustParse("18.0.7")

func limits() settings.Limits {
	return settings.Limits{
		QueryTimeout:        time.Second,
		LockTimeout:         500 * time.Millisecond,
		MaxRowsPerQuery:     10000,
		MaxSeriesPerQuery:   10000,
		MaxSeriesTotal:      50000,
		MaxDatabases:        50,
		MaxLabelValueLength: 1024,
		MaxLabelsPerSeries:  32,
	}
}

func newPool(l settings.Limits) *pg.Pool { return newPoolOf(l, 4) }

func newPoolOf(l settings.Limits, maxConns int) *pg.Pool {
	GinkgoHelper()
	cfg := server.MonitorConfig(GinkgoT().TempDir())
	cfg.QueryTimeout = l.QueryTimeout
	cfg.LockTimeout = l.LockTimeout
	pool := pg.NewPool(cfg, maxConns, time.Minute, clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
	DeferCleanup(pool.Close)
	return pool
}

func newExecutor(pool *pg.Pool, l settings.Limits) *executor.Executor {
	return executor.New(pool, l, func() *semver.Version { return serverVersion },
		clocktesting.NewFakePassiveClock(time.Now()))
}

// jobFor returns a job of a query with a label column "datname" and a gauge column "value".
func jobFor(sql string, mutate ...func(*queries.Definition)) planner.Job {
	d := &queries.Definition{
		Key: "probe",
		SQL: sql,
		Columns: map[string]queries.Column{
			"datname": {Name: "datname", Role: queries.RoleLabel, Usage: queries.UsageLabel},
			"value": {
				Name: "value", Role: queries.RoleValue, Usage: queries.UsageGauge,
				Metric: "cnpg_probe_value", Kind: queries.KindGauge,
			},
		},
		Labels: []string{"datname"},
	}
	for _, m := range mutate {
		m(d)
	}
	return planner.Job{Query: "probe", Database: "postgres", Def: d}
}

// definedJob returns a job of sql defined as a query with a label column "datname" and a gauge column "value".
func definedJob(sql string) planner.Job {
	def := queries.Define("probe", queries.UserQuery{
		Query: sql,
		Metrics: []queries.Mapping{
			{"datname": {Usage: queries.UsageLabel}},
			{"value": {Usage: queries.UsageGauge}},
		},
	}, queries.Options{})
	return planner.Job{Query: "probe", Database: "postgres", Def: &def}
}

func withPredicate(sql string) func(*queries.Definition) {
	return func(d *queries.Definition) { d.Predicate = sql }
}

var _ = Describe("Executor", func() {
	var (
		l    settings.Limits
		pool *pg.Pool
		exec *executor.Executor
	)

	BeforeEach(func() {
		l = limits()
		pool = newPool(l)
		exec = newExecutor(pool, l)
	})

	It("runs CNPG's default queries as the monitoring role without errors", func(ctx SpecContext) {
		var cm struct {
			Data map[string]string `yaml:"data"`
		}
		raw, err := os.ReadFile("../queries/testdata/default-monitoring-v1.30.1.yaml")
		Expect(err).NotTo(HaveOccurred())
		Expect(yaml.Unmarshal(raw, &cm)).To(Succeed())
		parsed, err := queries.Parse([]byte(cm.Data["queries"]))
		Expect(err).NotTo(HaveOccurred())
		for k, q := range queries.Builtin() {
			parsed[k] = q
		}
		var defs []queries.Definition
		for k, q := range parsed {
			defs = append(defs, queries.Define(k, q, queries.Options{MaxLabelsPerSeries: 32}))
		}

		conn, release, err := pool.AcquireWithin(ctx, "postgres", 0)
		Expect(err).NotTo(HaveOccurred())
		facts, err := planner.GatherFacts(ctx, conn, pool.Guard())
		release(err != nil)
		Expect(err).NotTo(HaveOccurred())

		s := settings.Settings{
			Postgres:        settings.Postgres{Database: "postgres"},
			DefaultInterval: 30 * time.Second,
			Limits:          settings.Limits{MaxDatabases: 50},
		}
		plan := planner.Build(defs, s, facts)
		Expect(plan.Jobs).NotTo(BeEmpty())

		def := limits()
		def.QueryTimeout = 10 * time.Second
		def.LockTimeout = 5 * time.Second
		p := newPool(def)
		e := executor.New(p, def, func() *semver.Version { return &facts.Version },
			clocktesting.NewFakePassiveClock(time.Now()))
		samples := 0
		for _, job := range plan.Jobs {
			res := e.Run(ctx, job)
			Expect(res.Outcome).To(Equal(selfmetrics.OutcomeOK), "%s on %s: %v", job.Query, job.Database, res.Err)
			samples += len(res.Samples)
		}
		Expect(samples).To(BeNumerically(">", 0))
	}, SpecTimeout(2*time.Minute))

	DescribeTable("skips the query when the predicate returns false, NULL or no row",
		func(ctx SpecContext, predicate string) {
			res := exec.Run(ctx, jobFor("SELECT 'a' AS datname, 1 AS value", withPredicate(predicate)))
			Expect(res.Err).NotTo(HaveOccurred())
			Expect(res.Outcome).To(Equal(selfmetrics.OutcomeOK))
			Expect(res.Samples).To(BeEmpty())
		},
		Entry("false", "SELECT false"),
		Entry("NULL", "SELECT NULL::bool"),
		Entry("no row", "SELECT true WHERE false"),
	)

	It("runs the query when the predicate returns true", func(ctx SpecContext) {
		res := exec.Run(ctx, jobFor("SELECT 'a' AS datname, 1 AS value", withPredicate("SELECT true")))
		Expect(res.Err).NotTo(HaveOccurred())
		Expect(res.Samples).To(HaveLen(1))
	})

	It("reports a predicate value that is not a boolean as an error", func(ctx SpecContext) {
		res := exec.Run(ctx, jobFor("SELECT 'a' AS datname, 1 AS value", withPredicate("SELECT 'maybe'")))
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeError))
		Expect(res.Err).To(HaveOccurred())
		Expect(res.Samples).To(BeNil())
	})

	It("runs the predicate and the query in one read-only transaction", func(ctx SpecContext) {
		probe := jobFor(
			"SELECT 'a' AS datname, CASE WHEN current_setting('cnpg.probe', true) = 't' THEN 1 ELSE 0 END AS value",
			withPredicate("SELECT set_config('cnpg.probe', 't', true)::bool"))
		res := exec.Run(ctx, probe)
		Expect(res.Err).NotTo(HaveOccurred())
		Expect(res.Samples).To(HaveLen(1))
		Expect(res.Samples[0].Value).To(Equal(1.0))
	})

	DescribeTable("fails a query that writes with SQLSTATE 25006",
		func(ctx SpecContext, sql string, readsSettings bool) {
			Expect(server.Exec(ctx, "CREATE SEQUENCE IF NOT EXISTS public.ro_seq; GRANT USAGE ON SEQUENCE public.ro_seq TO "+pgtest.MonitorUser)).To(Succeed())
			job := definedJob(sql)
			Expect(job.Def.ReadsSettings).To(Equal(readsSettings))

			res := exec.Run(ctx, job)
			Expect(res.Outcome).To(Equal(selfmetrics.OutcomeError))
			var pgErr *pgconn.PgError
			Expect(errors.As(res.Err, &pgErr)).To(BeTrue(), "error: %v", res.Err)
			Expect(pgErr.Code).To(Equal("25006"))
		},
		Entry("guarded", "SELECT 'a' AS datname, nextval('public.ro_seq') AS value", false),
		Entry("reading pg_settings", "SELECT 'a' AS datname, nextval('public.ro_seq') AS value FROM pg_catalog.pg_settings LIMIT 1", true),
	)

	It("reports the server's values through CNPG's pg_settings query", func(ctx SpecContext) {
		var cm struct {
			Data map[string]string `yaml:"data"`
		}
		raw, err := os.ReadFile("../queries/testdata/default-monitoring-main.yaml")
		Expect(err).NotTo(HaveOccurred())
		Expect(yaml.Unmarshal(raw, &cm)).To(Succeed())
		parsed, err := queries.Parse([]byte(cm.Data["queries"]))
		Expect(err).NotTo(HaveOccurred())
		Expect(parsed).To(HaveKey("pg_settings"))
		def := queries.Define("pg_settings", parsed["pg_settings"], queries.Options{MaxLabelsPerSeries: 32})
		Expect(def.ReadsSettings).To(BeTrue())

		res := exec.Run(ctx, planner.Job{Query: "pg_settings", Database: "postgres", Def: &def})
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeOK), "error: %v", res.Err)
		got := map[string]float64{}
		for _, sample := range res.Samples {
			Expect(sample.Metric).To(Equal("cnpg_pg_settings_setting"))
			Expect(sample.Labels).To(HaveLen(1))
			got[sample.Labels[0].Value] = sample.Value
		}

		plain, err := server.PlainConn(ctx, "postgres")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) { Expect(plain.Close(ctx)).To(Succeed()) })
		for _, name := range []string{
			"statement_timeout", "lock_timeout", "idle_in_transaction_session_timeout", "jit",
			"max_parallel_workers_per_gather", "default_transaction_read_only",
		} {
			var want float64
			Expect(plain.QueryRow(ctx, "SELECT (CASE setting WHEN 'on' THEN '1' WHEN 'off' THEN '0' ELSE setting END)::float8 "+
				"FROM pg_catalog.pg_settings WHERE name = $1", name).Scan(&want)).To(Succeed())
			Expect(got).To(HaveKeyWithValue(name, want), name)
		}
		Expect(got).To(HaveKeyWithValue("transaction_read_only", 1.0))
	})

	It("ends an unguarded statement at the run deadline by a server-side cancel", func(ctx SpecContext) {
		l.QueryTimeout = 4 * time.Second
		slow := newExecutor(newPool(l), l)
		pid := jobFor("SELECT 'a' AS datname, pg_backend_pid() AS value")
		first := slow.Run(ctx, pid)
		Expect(first.Err).NotTo(HaveOccurred())

		job := definedJob("SELECT pg_sleep(5) FROM pg_catalog.pg_settings LIMIT 1")
		Expect(job.Def.ReadsSettings).To(BeTrue())
		deadline, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		start := time.Now()
		res := slow.Run(deadline, job)
		Expect(time.Since(start)).To(BeNumerically("<", 2*time.Second))
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeTimeout), "error: %v", res.Err)
		var pgErr *pgconn.PgError
		Expect(errors.As(res.Err, &pgErr)).To(BeTrue(), "error: %v", res.Err)
		Expect(pgErr.Code).To(Equal("57014"))
		Expect(pgErr.Message).To(ContainSubstring("canceling statement due to user request"))
		Expect(res.ConnectionLost).To(BeFalse())

		again := slow.Run(ctx, pid)
		Expect(again.Err).NotTo(HaveOccurred())
		Expect(again.Samples[0].Value).To(Equal(first.Samples[0].Value), "the connection is kept")
	})

	It("records a run that fails with SQLSTATE 42501 as outcome denied", func(ctx SpecContext) {
		res := exec.Run(ctx, jobFor("SELECT 'a' AS datname, 1 AS value FROM pg_authid LIMIT 1"))
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeDenied), "error: %v", res.Err)
		var pgErr *pgconn.PgError
		Expect(errors.As(res.Err, &pgErr)).To(BeTrue(), "error: %v", res.Err)
		Expect(pgErr.Code).To(Equal("42501"))
		Expect(res.Samples).To(BeNil())
		Expect(res.ConnectionLost).To(BeFalse())
	})

	It("times out a slow query with statement_timeout", func(ctx SpecContext) {
		pid := jobFor("SELECT 'a' AS datname, pg_backend_pid() AS value")
		first := exec.Run(ctx, pid)
		Expect(first.Err).NotTo(HaveOccurred())

		res := exec.Run(ctx, jobFor("SELECT 'a' AS datname, 1 AS value FROM pg_sleep(5)"))
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeTimeout))
		Expect(res.Err).To(HaveOccurred())

		again := exec.Run(ctx, pid)
		Expect(again.Err).NotTo(HaveOccurred())
		Expect(again.Samples[0].Value).To(Equal(first.Samples[0].Value), "the connection is kept")
	})

	It("times out a query that waits on a lock with lock_timeout", func(ctx SpecContext) {
		Expect(server.Exec(ctx, "CREATE TABLE IF NOT EXISTS public.lock_probe (a int); GRANT SELECT ON public.lock_probe TO "+pgtest.MonitorUser)).To(Succeed())

		var wg sync.WaitGroup
		holder := make(chan error, 1)
		wg.Go(func() {
			holder <- server.Exec(context.WithoutCancel(ctx), "LOCK TABLE public.lock_probe IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(4)")
		})
		DeferCleanup(wg.Wait)

		Eventually(func(g Gomega) {
			conn, release, err := pool.AcquireWithin(ctx, "postgres", 0)
			g.Expect(err).NotTo(HaveOccurred())
			defer release(false)
			var n int
			g.Expect(conn.QueryRow(ctx, "SELECT count(*) FROM pg_locks WHERE relation = 'public.lock_probe'::regclass AND mode = 'AccessExclusiveLock'").Scan(&n)).To(Succeed())
			g.Expect(n).To(Equal(1))
		}).WithContext(ctx).WithPolling(100 * time.Millisecond).Should(Succeed())

		start := time.Now()
		res := exec.Run(ctx, jobFor("SELECT 'a' AS datname, count(*) AS value FROM public.lock_probe"))
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeTimeout), "error: %v", res.Err)
		Expect(time.Since(start)).To(BeNumerically("<", l.LockTimeout+time.Second))

		wg.Wait()
		Expect(<-holder).To(Succeed())
	})

	Describe("caps", func() {
		const fiveRows = "SELECT 'x' || g AS datname, 1 AS value FROM generate_series(1, 5) g"

		It("discards a run above MaxRowsPerQuery and reports the series it saw", func(ctx SpecContext) {
			l.MaxRowsPerQuery = 3
			res := newExecutor(pool, l).Run(ctx, jobFor(fiveRows))
			Expect(res.Err).NotTo(HaveOccurred())
			Expect(res.Outcome).To(Equal(selfmetrics.OutcomeCapped))
			Expect(res.WouldBe).To(Equal(3))
			Expect(res.Samples).To(BeNil())
		})

		It("discards a run above MaxSeriesPerQuery and counts the series to the end", func(ctx SpecContext) {
			l.MaxSeriesPerQuery = 3
			res := newExecutor(pool, l).Run(ctx, jobFor(fiveRows))
			Expect(res.Err).NotTo(HaveOccurred())
			Expect(res.Outcome).To(Equal(selfmetrics.OutcomeCapped))
			Expect(res.WouldBe).To(Equal(5))
			Expect(res.Samples).To(BeNil())
		})

		It("stops remembering series past maxSeriesPerQuery + 1 and still counts the later ones", func(ctx SpecContext) {
			l.MaxSeriesPerQuery = 2
			res := newExecutor(pool, l).Run(ctx, jobFor(
				"SELECT x AS datname, 1 AS value FROM unnest(ARRAY['a','b','c','c','c','d','d']) x"))
			Expect(res.Err).NotTo(HaveOccurred())
			Expect(res.Outcome).To(Equal(selfmetrics.OutcomeCapped))
			Expect(res.WouldBe).To(Equal(5))
			Expect(res.Dropped[queries.DropDuplicate]).To(Equal(2))
		})

		It("cancels the statement at the row cap instead of draining it", func(ctx SpecContext) {
			l.MaxRowsPerQuery = 3
			capExec := newExecutor(pool, l)
			pid := jobFor("SELECT 'a' AS datname, pg_backend_pid() AS value")
			first := capExec.Run(ctx, pid)
			Expect(first.Err).NotTo(HaveOccurred())

			start := time.Now()
			res := capExec.Run(ctx, jobFor("SELECT 'x' || generate_series(1, 50000000) AS datname, 1 AS value"))
			Expect(time.Since(start)).To(BeNumerically("<", 2*time.Second))
			Expect(res.Err).NotTo(HaveOccurred())
			Expect(res.Outcome).To(Equal(selfmetrics.OutcomeCapped))
			Expect(res.WouldBe).To(Equal(3))

			again := capExec.Run(ctx, pid)
			Expect(again.Err).NotTo(HaveOccurred())
			Expect(again.Samples).To(HaveLen(1))
			Expect(again.Samples[0].Value).To(Equal(first.Samples[0].Value), "the connection is kept")
		})

		It("logs a failed cancel request and replaces the connection", func(ctx SpecContext) {
			var out []string
			logger := funcr.New(func(_, args string) { out = append(out, args) }, funcr.Options{Verbosity: 1})
			logCtx := ctrllog.IntoContext(ctx, logger)

			l.MaxRowsPerQuery = 3
			capExec := newExecutor(pool, l)
			capExec.SetCancel(func(ctx context.Context, conn *pgx.Conn) error {
				done, cancel := context.WithCancel(ctx)
				cancel()
				return conn.PgConn().CancelRequest(done)
			})
			pid := jobFor("SELECT 'a' AS datname, pg_backend_pid() AS value")
			first := capExec.Run(logCtx, pid)
			Expect(first.Err).NotTo(HaveOccurred())

			res := capExec.Run(logCtx, jobFor("SELECT 'x' || g AS datname, 1 AS value FROM generate_series(1, 10) g"))
			Expect(res.Err).NotTo(HaveOccurred())
			Expect(res.Outcome).To(Equal(selfmetrics.OutcomeCapped))
			Expect(out).To(ContainElement(And(ContainSubstring("cancel"), ContainSubstring(`"query"="probe"`), ContainSubstring(`"database"="postgres"`))))

			again := capExec.Run(logCtx, pid)
			Expect(again.Err).NotTo(HaveOccurred())
			Expect(again.Samples[0].Value).NotTo(Equal(first.Samples[0].Value), "the connection was replaced")
		})

		It("keeps a run at the caps", func(ctx SpecContext) {
			l.MaxRowsPerQuery = 5
			l.MaxSeriesPerQuery = 5
			res := newExecutor(pool, l).Run(ctx, jobFor(fiveRows))
			Expect(res.Outcome).To(Equal(selfmetrics.OutcomeOK))
			Expect(res.Samples).To(HaveLen(5))
		})
	})

	It("keeps the first of two rows with the same labels and counts the rest as duplicate", func(ctx SpecContext) {
		res := exec.Run(ctx, jobFor("SELECT 'a' AS datname, g AS value FROM generate_series(1, 3) g ORDER BY g"))
		Expect(res.Err).NotTo(HaveOccurred())
		Expect(res.Samples).To(HaveLen(1))
		Expect(res.Samples[0].Value).To(Equal(1.0))
		Expect(res.Dropped).To(HaveKeyWithValue(queries.DropDuplicate, 2))
	})

	It("drops a row whose label is not UTF-8 in a LATIN1 database and keeps the others", func(ctx SpecContext) {
		Expect(server.Exec(ctx, "CREATE DATABASE latin1 TEMPLATE template0 ENCODING 'LATIN1' LC_COLLATE 'C' LC_CTYPE 'C'")).To(Succeed())
		DeferCleanup(func(ctx SpecContext) { Expect(server.Exec(ctx, "DROP DATABASE latin1 WITH (FORCE)")).To(Succeed()) })
		for _, stmt := range []string{
			"CREATE TABLE labels (name text, value int)",
			// chr(233) is é, the single byte 0xE9 in LATIN1.
			"INSERT INTO labels VALUES ('plain', 1), ('caf' || chr(233), 2), ('other', 3)",
			"GRANT SELECT ON labels TO " + pgtest.MonitorUser,
		} {
			Expect(server.ExecIn(ctx, "latin1", stmt)).To(Succeed())
		}

		job := jobFor("SELECT name AS datname, value FROM labels ORDER BY value")
		job.Database = "latin1"
		res := exec.Run(ctx, job)
		Expect(res.Err).NotTo(HaveOccurred())
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeOK))
		Expect(res.Dropped).To(Equal(map[queries.DropReason]int{queries.DropLabel: 1}))
		var names []string
		for _, sample := range res.Samples {
			names = append(names, sample.Labels[0].Value)
		}
		Expect(names).To(Equal([]string{"plain", "other"}))
	})

	It("reports a column mismatch as an error", func(ctx SpecContext) {
		res := exec.Run(ctx, jobFor("SELECT 'a' AS datname, 1 AS value, 2 AS extra"))
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeError))
		Expect(res.Err).To(MatchError(queries.ErrColumnMismatch))
		Expect(res.Err).To(MatchError(ContainSubstring("3")))
		Expect(res.Samples).To(BeNil())
	})

	It("recovers a panic in a run, logs its stack and reports error", func(ctx SpecContext) {
		var out []string
		logger := funcr.New(func(_, args string) { out = append(out, args) }, funcr.Options{Verbosity: 1})
		logCtx := ctrllog.IntoContext(ctx, logger)

		res := exec.Run(logCtx, planner.Job{Query: "broken", Database: "postgres"})
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeError))
		Expect(res.Err).To(HaveOccurred())
		Expect(out).To(ContainElement(And(ContainSubstring(`"stack"=`), ContainSubstring(`"query"="broken"`))))
	})

	It("does not cut a slow connect at the interval", func(ctx SpecContext) {
		lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		stalled := make(chan net.Conn, 4)
		accepting := make(chan struct{})
		go func() {
			defer close(accepting)
			for {
				c, err := lis.Accept()
				if err != nil {
					return
				}
				stalled <- c
			}
		}()
		DeferCleanup(func() {
			Expect(lis.Close()).To(Succeed())
			<-accepting
			close(stalled)
			for c := range stalled {
				Expect(c.Close()).To(Succeed())
			}
		})
		addr, ok := lis.Addr().(*net.TCPAddr)
		Expect(ok).To(BeTrue())

		cfg := server.MonitorConfig(GinkgoT().TempDir())
		cfg.Host, cfg.Port = "127.0.0.1", addr.AddrPort().Port()
		cfg.ConnectTimeout = time.Second
		slow := pg.NewPool(cfg, 1, time.Minute, clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
		DeferCleanup(slow.Close)

		job := jobFor("SELECT 'a' AS datname, 1 AS value")
		job.Interval = 200 * time.Millisecond
		start := time.Now()
		res := newExecutor(slow, l).Run(ctx, job)

		Expect(time.Since(start)).To(BeNumerically(">=", 900*time.Millisecond), "the connect runs to its own timeout")
		Expect(res.Outcome).NotTo(Equal(selfmetrics.OutcomeSkipped))
		Expect(res.Err).NotTo(MatchError(pg.ErrNoSlot))
		Expect(res.ConnectionLost).To(BeTrue())
		again := newExecutor(slow, l).Run(ctx, job)
		Expect(again.Err).To(MatchError(pg.ErrBackoff), "the failed connect starts the backoff")
	})

	It("starts a run's deadline once it has a pool slot", func(ctx SpecContext) {
		l.QueryTimeout = 500 * time.Millisecond
		cfg := server.MonitorConfig(GinkgoT().TempDir())
		cfg.QueryTimeout, cfg.LockTimeout = l.QueryTimeout, l.LockTimeout
		one := pg.NewPool(cfg, 1, time.Minute, clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
		DeferCleanup(one.Close)

		_, release, err := one.AcquireWithin(ctx, "postgres", 0)
		Expect(err).NotTo(HaveOccurred())
		held := time.AfterFunc(3*time.Second, func() { release(false) })
		DeferCleanup(held.Stop)

		job := jobFor("SELECT 'a' AS datname, 1 AS value")
		job.Interval = 10 * time.Second
		res := newExecutor(one, l).Run(ctx, job)
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeOK), "error: %v", res.Err)
	})

	It("gives up waiting for a connection after one interval and counts the run as skipped", func(ctx SpecContext) {
		cfg := server.MonitorConfig(GinkgoT().TempDir())
		cfg.QueryTimeout, cfg.LockTimeout = l.QueryTimeout, l.LockTimeout
		one := pg.NewPool(cfg, 1, time.Minute, clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
		DeferCleanup(one.Close)

		holder, release, err := one.AcquireWithin(ctx, "postgres", 0)
		Expect(err).NotTo(HaveOccurred())

		job := jobFor("SELECT 'a' AS datname, 1 AS value")
		job.Interval = 200 * time.Millisecond
		start := time.Now()
		res := newExecutor(one, l).Run(ctx, job)

		Expect(time.Since(start)).To(BeNumerically("<", time.Second))
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeSkipped))
		Expect(res.Err).To(MatchError(pg.ErrNoSlot))
		Expect(res.Err).To(MatchError(ContainSubstring("query probe on database postgres could not get a connection within 200ms")))
		Expect(res.ConnectionLost).To(BeFalse())
		Expect(res.Samples).To(BeNil())

		Expect(holder.IsClosed()).To(BeFalse(), "the slot holder is unaffected")
		release(false)
		again := newExecutor(one, l).Run(ctx, job)
		Expect(again.Outcome).To(Equal(selfmetrics.OutcomeOK), "error: %v", again.Err)
	})

	It("gives up waiting for a connection after QueryTimeout when the job has no interval", func(ctx SpecContext) {
		l.QueryTimeout = 500 * time.Millisecond
		cfg := server.MonitorConfig(GinkgoT().TempDir())
		cfg.QueryTimeout, cfg.LockTimeout = l.QueryTimeout, l.LockTimeout
		one := pg.NewPool(cfg, 1, time.Minute, clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
		DeferCleanup(one.Close)

		_, release, err := one.AcquireWithin(ctx, "postgres", 0)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { release(false) })

		job := jobFor("SELECT 'a' AS datname, 1 AS value")
		Expect(job.Interval).To(BeZero())
		start := time.Now()
		res := newExecutor(one, l).Run(ctx, job)

		Expect(time.Since(start)).To(BeNumerically("<", 3*time.Second))
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeSkipped))
		Expect(res.Err).To(MatchError(pg.ErrNoSlot))
		Expect(res.Err).To(MatchError(ContainSubstring("within 500ms")))
	}, SpecTimeout(30*time.Second))

	Describe("with a session role that is a superuser", func() {
		const role = "cnpg_exec_super"
		var superPool *pg.Pool

		BeforeEach(func(ctx SpecContext) {
			DeferCleanup(func(ctx SpecContext) { Expect(server.Exec(ctx, "DROP ROLE IF EXISTS "+role)).To(Succeed()) })
			Expect(server.Exec(ctx, "CREATE ROLE "+role+" LOGIN SUPERUSER PASSWORD '"+pgtest.MonitorPassword+"'")).To(Succeed())
			cfg := server.MonitorConfig(GinkgoT().TempDir())
			cfg.User = role
			cfg.QueryTimeout, cfg.LockTimeout = l.QueryTimeout, l.LockTimeout
			superPool = pg.NewPool(cfg, 2, time.Minute, clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
			DeferCleanup(superPool.Close)
		})

		// A query that ran would give samples or its own division error; ErrSuperuser proves it never left the sidecar.
		DescribeTable("runs neither the predicate nor the query and gives ErrSuperuser",
			func(ctx SpecContext, mutate func(*queries.Definition)) {
				job := jobFor("SELECT 'a' AS datname, 1/0 AS value", mutate)
				res := newExecutor(superPool, l).Run(ctx, job)

				Expect(res.Err).To(MatchError(planner.ErrSuperuser))
				Expect(res.Outcome).To(Equal(selfmetrics.OutcomeSkipped))
				Expect(res.Samples).To(BeNil())
				Expect(res.ConnectionLost).To(BeFalse())
			},
			Entry("guarded", func(*queries.Definition) {}),
			Entry("guarded with a predicate", withPredicate("SELECT 1/0")),
			Entry("reading the settings", func(d *queries.Definition) { d.ReadsSettings = true }),
		)

		It("runs the job once the role is not a superuser", func(ctx SpecContext) {
			Expect(server.Exec(ctx, "ALTER ROLE "+role+" NOSUPERUSER")).To(Succeed())
			res := newExecutor(superPool, l).Run(ctx, jobFor("SELECT 'a' AS datname, 1 AS value"))
			Expect(res.Outcome).To(Equal(selfmetrics.OutcomeOK), "error: %v", res.Err)
		})
	})

	It("hands a broken connection back so the pool replaces it", func(ctx SpecContext) {
		pid := jobFor("SELECT 'a' AS datname, pg_backend_pid() AS value")
		first := exec.Run(ctx, pid)
		Expect(first.Err).NotTo(HaveOccurred())
		Expect(server.Exec(ctx, "SELECT pg_terminate_backend("+formatPID(first.Samples[0].Value)+")")).To(Succeed())

		dead := exec.Run(ctx, pid)
		Expect(dead.Outcome).To(Equal(selfmetrics.OutcomeError))

		next := exec.Run(ctx, pid)
		Expect(next.Err).NotTo(HaveOccurred())
		Expect(next.Samples[0].Value).NotTo(Equal(first.Samples[0].Value))
	})

	It("reports a database that refuses the connection as denied on every run, also during the backoff", func(ctx SpecContext) {
		job := jobFor("SELECT 'a' AS datname, 1 AS value")
		job.Database = "noconnect"

		res := exec.Run(ctx, job)
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeDenied))
		Expect(res.Err).NotTo(MatchError(pg.ErrBackoff))

		res = exec.Run(ctx, job)
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeDenied))
		Expect(res.Err).To(MatchError(pg.ErrBackoff))
	})
})

var _ = Describe("Executor on a pool of one connection", func() {
	var (
		l    settings.Limits
		exec *executor.Executor
	)

	BeforeEach(func() {
		l = limits()
		exec = newExecutor(newPoolOf(l, 1), l)
	})

	pidJob := func() planner.Job { return jobFor("SELECT 'a' AS datname, pg_backend_pid() AS value") }

	It("keeps a session that turned default_transaction_read_only off from writing", func(ctx SpecContext) {
		res := exec.Run(ctx, jobFor("SELECT 'a' AS datname, 1 AS value",
			withPredicate("SELECT set_config('default_transaction_read_only', 'off', false) IS NOT NULL")))
		Expect(res.Err).NotTo(HaveOccurred())

		res = exec.Run(ctx, jobFor("CREATE TEMP TABLE t AS SELECT 1 AS v"))
		var pgErr *pgconn.PgError
		Expect(errors.As(res.Err, &pgErr)).To(BeTrue(), "error: %v", res.Err)
		Expect(pgErr.Code).To(Equal("25006"))
	})

	It("cancels the statement and replaces the connection on a column mismatch", func(ctx SpecContext) {
		first := exec.Run(ctx, pidJob())
		Expect(first.Err).NotTo(HaveOccurred())

		start := time.Now()
		res := exec.Run(ctx, jobFor("SELECT generate_series(1, 50000000) AS a, 1 AS b", func(d *queries.Definition) {
			d.Columns = map[string]queries.Column{"a": {Name: "a", Role: queries.RoleLabel, Usage: queries.UsageLabel}}
			d.Labels = []string{"a"}
		}))
		Expect(time.Since(start)).To(BeNumerically("<", 2*time.Second))
		Expect(res.Err).To(MatchError(queries.ErrColumnMismatch))

		again := exec.Run(ctx, pidJob())
		Expect(again.Err).NotTo(HaveOccurred())
		Expect(again.Samples[0].Value).NotTo(Equal(first.Samples[0].Value), "the connection was replaced")
	})

	It("gets a connection within 2 s after a panic", func(ctx SpecContext) {
		res := exec.Run(ctx, planner.Job{Query: "broken", Database: "postgres"})
		Expect(res.Err).To(MatchError(ContainSubstring("panic")))

		bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		Expect(exec.Run(bounded, pidJob()).Err).NotTo(HaveOccurred())
	})
})

var _ = Describe("Result.ConnectionLost", func() {
	var (
		l    settings.Limits
		exec *executor.Executor
	)

	BeforeEach(func() {
		l = limits()
		exec = newExecutor(newPool(l), l)
	})

	It("is set when the backend was terminated", func(ctx SpecContext) {
		pid := jobFor("SELECT 'a' AS datname, pg_backend_pid() AS value")
		first := exec.Run(ctx, pid)
		Expect(first.Err).NotTo(HaveOccurred())
		Expect(first.ConnectionLost).To(BeFalse())
		Expect(server.Exec(ctx, "SELECT pg_terminate_backend("+formatPID(first.Samples[0].Value)+")")).To(Succeed())

		dead := exec.Run(ctx, pid)
		Expect(dead.Outcome).To(Equal(selfmetrics.OutcomeError))
		Expect(dead.ConnectionLost).To(BeTrue())
	})

	It("is set when no connection can be made, and not during the backoff that follows", func(ctx SpecContext) {
		cfg := server.MonitorConfig(GinkgoT().TempDir())
		Expect(os.WriteFile(cfg.PasswordFile, []byte("wrong\n"), 0o600)).To(Succeed())
		cfg.QueryTimeout, cfg.LockTimeout = l.QueryTimeout, l.LockTimeout
		pool := pg.NewPool(cfg, 4, time.Minute, clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
		DeferCleanup(pool.Close)
		wrong := newExecutor(pool, l)

		res := wrong.Run(ctx, jobFor("SELECT 'a' AS datname, 1 AS value"))
		Expect(res.Err).To(HaveOccurred())
		Expect(res.Err).NotTo(MatchError(pg.ErrBackoff))
		Expect(res.ConnectionLost).To(BeTrue())

		res = wrong.Run(ctx, jobFor("SELECT 'a' AS datname, 1 AS value"))
		Expect(res.Err).To(MatchError(pg.ErrBackoff))
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeError))
		Expect(res.Err.Error()).NotTo(ContainSubstring("wrong"))
		Expect(res.ConnectionLost).To(BeFalse())
	})

	It("is set for the first refusal of a database that doesn't exist, not for the backoff after it", func(ctx SpecContext) {
		job := jobFor("SELECT 'a' AS datname, 1 AS value")
		job.Database = "nosuchdb"

		res := exec.Run(ctx, job)
		var pgErr *pgconn.PgError
		Expect(errors.As(res.Err, &pgErr)).To(BeTrue(), "error: %v", res.Err)
		Expect(pgErr.Code).To(Equal("3D000"))
		Expect(res.ConnectionLost).To(BeTrue())

		res = exec.Run(ctx, job)
		Expect(res.Err).To(MatchError(pg.ErrBackoff))
		Expect(res.ConnectionLost).To(BeFalse())
	})

	It("is not set when the caller's context is cancelled while the run waits for a pool slot", func(ctx SpecContext) {
		cfg := server.MonitorConfig(GinkgoT().TempDir())
		cfg.QueryTimeout, cfg.LockTimeout = l.QueryTimeout, l.LockTimeout
		one := pg.NewPool(cfg, 1, time.Minute, clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
		DeferCleanup(one.Close)

		_, release, err := one.AcquireWithin(ctx, "postgres", 0)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { release(false) })

		waitCtx, cancel := context.WithCancel(ctx)
		time.AfterFunc(200*time.Millisecond, cancel)
		DeferCleanup(cancel)

		res := newExecutor(one, l).Run(waitCtx, jobFor("SELECT 'a' AS datname, 1 AS value"))
		Expect(res.Err).To(MatchError(context.Canceled))
		// Only a deadline is a timeout; a cancelled wait is an error that lost no connection.
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeError))
		Expect(res.ConnectionLost).To(BeFalse())
	})

	It("is not set for a column mismatch", func(ctx SpecContext) {
		res := exec.Run(ctx, jobFor("SELECT 'a' AS datname, 1 AS value, 2 AS extra"))
		Expect(res.Err).To(MatchError(queries.ErrColumnMismatch))
		Expect(res.ConnectionLost).To(BeFalse())
	})

	It("is not set for a statement timeout", func(ctx SpecContext) {
		res := exec.Run(ctx, jobFor("SELECT 'a' AS datname, 1 AS value FROM pg_sleep(5)"))
		var pgErr *pgconn.PgError
		Expect(errors.As(res.Err, &pgErr)).To(BeTrue(), "%v", res.Err)
		Expect(pgErr.Code).To(Equal("57014"))
		Expect(res.ConnectionLost).To(BeFalse())
	})

	It("is not set for a recovered panic", func(ctx SpecContext) {
		res := exec.Run(ctx, planner.Job{Query: "broken", Database: "postgres"})
		Expect(res.Err).To(MatchError(ContainSubstring("panic")))
		Expect(res.ConnectionLost).To(BeFalse())
	})
})

func formatPID(v float64) string { return strconv.FormatInt(int64(v), 10) }
