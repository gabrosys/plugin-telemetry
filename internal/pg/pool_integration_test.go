//go:build integration

package pg_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr/funcr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/gabrosys/plugin-telemetry/internal/pg"
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

func showSetting(ctx context.Context, conn *pgx.Conn, name string) string {
	GinkgoHelper()
	var value string
	Expect(conn.QueryRow(ctx, "SELECT current_setting($1)", name).Scan(&value)).To(Succeed())
	return value
}

// serverSettings are the settings the engine's sessions leave at the server's values.
var serverSettings = []string{
	"statement_timeout", "lock_timeout", "idle_in_transaction_session_timeout", "jit",
	"max_parallel_workers_per_gather", "default_transaction_read_only",
}

// plainConn opens a session as the monitoring role whose settings are the server's.
func plainConn(ctx context.Context) *pgx.Conn {
	GinkgoHelper()
	conn, err := server.PlainConn(ctx, "app")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(conn.Close, context.WithoutCancel(ctx))
	return conn
}

func backendPID(ctx context.Context, conn *pgx.Conn) uint32 {
	GinkgoHelper()
	var pid uint32
	Expect(conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid)).To(Succeed())
	return pid
}

var _ = Describe("Pool", func() {
	var (
		cfg   pg.Config
		clk   *clocktesting.FakePassiveClock
		pool  *pg.Pool
		start = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	)

	newPool := func(maxConns int, idle time.Duration) {
		pool = pg.NewPool(cfg, maxConns, idle, clk)
		DeferCleanup(pool.Close)
	}

	BeforeEach(func() {
		cfg = server.MonitorConfig(GinkgoT().TempDir())
		cfg.QueryTimeout = 3 * time.Second
		cfg.LockTimeout = time.Second
		clk = clocktesting.NewFakePassiveClock(start)
	})

	It("leaves the server's values in the session", func(ctx SpecContext) {
		newPool(2, time.Minute)
		conn, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)
		plain := plainConn(ctx)

		for _, name := range serverSettings {
			Expect(showSetting(ctx, conn, name)).To(Equal(showSetting(ctx, plain, name)), name)
		}
		Expect(showSetting(ctx, conn, "search_path")).To(Equal("pg_catalog, public, pg_temp"))
		Expect(showSetting(ctx, conn, "application_name")).To(Equal("cnpg_otel_engine"))
		Expect(showSetting(ctx, conn, "standard_conforming_strings")).To(Equal("on"))

		var user string
		Expect(conn.QueryRow(ctx, "SELECT current_user").Scan(&user)).To(Succeed())
		Expect(user).To(Equal("cnpg_metrics_exporter"))
	})

	It("Guard sets the limits for one transaction", func(ctx SpecContext) {
		newPool(2, time.Minute)
		conn, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)
		plain := plainConn(ctx)

		tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		Expect(err).NotTo(HaveOccurred())
		_, err = tx.Exec(ctx, pool.Guard())
		Expect(err).NotTo(HaveOccurred())
		Expect(showSetting(ctx, conn, "statement_timeout")).To(Equal("3s"))
		Expect(showSetting(ctx, conn, "lock_timeout")).To(Equal("1s"))
		Expect(showSetting(ctx, conn, "idle_in_transaction_session_timeout")).To(Equal("6s"))
		Expect(showSetting(ctx, conn, "jit")).To(Equal("off"))
		Expect(showSetting(ctx, conn, "max_parallel_workers_per_gather")).To(Equal("0"))
		Expect(tx.Commit(ctx)).To(Succeed())

		for _, name := range serverSettings {
			Expect(showSetting(ctx, conn, name)).To(Equal(showSetting(ctx, plain, name)), name)
		}
	})

	It("ApplyGuard sets the limits and reports a role that is not a superuser", func(ctx SpecContext) {
		newPool(2, time.Minute)
		conn, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)

		tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		Expect(err).NotTo(HaveOccurred())
		superuser, err := pg.ApplyGuard(ctx, tx, pool.Guard())
		Expect(err).NotTo(HaveOccurred())
		Expect(superuser).To(BeFalse())
		Expect(showSetting(ctx, conn, "statement_timeout")).To(Equal("3s"))
		Expect(showSetting(ctx, conn, "max_parallel_workers_per_gather")).To(Equal("0"))
		Expect(tx.Rollback(ctx)).To(Succeed())
	})

	It("ApplyGuard reports a superuser session role", func(ctx SpecContext) {
		const role = "cnpg_guard_super"
		DeferCleanup(func(ctx SpecContext) { Expect(server.Exec(ctx, "DROP ROLE IF EXISTS "+role)).To(Succeed()) })
		Expect(server.Exec(ctx, "CREATE ROLE "+role+" LOGIN SUPERUSER PASSWORD '"+pgtest.MonitorPassword+"'")).To(Succeed())
		cfg.User = role
		newPool(2, time.Minute)
		conn, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)

		tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		Expect(err).NotTo(HaveOccurred())
		superuser, err := pg.ApplyGuard(ctx, tx, pool.Guard())
		Expect(err).NotTo(HaveOccurred())
		Expect(superuser).To(BeTrue())
		Expect(tx.Rollback(ctx)).To(Succeed())
	})

	It("ends a guarded statement past QueryTimeout with SQLSTATE 57014 and keeps the connection", func(ctx SpecContext) {
		cfg.QueryTimeout = time.Second
		newPool(2, time.Minute)
		conn, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)

		tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		Expect(err).NotTo(HaveOccurred())
		_, err = tx.Exec(ctx, pool.Guard())
		Expect(err).NotTo(HaveOccurred())
		_, err = tx.Exec(ctx, "SELECT pg_sleep(5)")
		var pgErr *pgconn.PgError
		Expect(errors.As(err, &pgErr)).To(BeTrue(), "error: %v", err)
		Expect(pgErr.Code).To(Equal("57014"))
		Expect(tx.Rollback(ctx)).To(Succeed())

		var one int
		Expect(conn.QueryRow(ctx, "SELECT 1").Scan(&one)).To(Succeed())
		Expect(one).To(Equal(1))
	})

	It("reuses an idle connection of the same database", func(ctx SpecContext) {
		newPool(2, time.Minute)
		first, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		pid := backendPID(ctx, first)
		release(false)

		second, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)
		Expect(backendPID(ctx, second)).To(Equal(pid))
	})

	It("closes the least recently used idle connection of another database at the cap", func(ctx SpecContext) {
		newPool(2, time.Minute)
		appConn, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		release(false)
		postgresConn, release, err := pool.Acquire(ctx, "postgres")
		Expect(err).NotTo(HaveOccurred())
		release(false)

		third, release, err := pool.Acquire(ctx, "template1")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)

		Expect(third.Config().Database).To(Equal("template1"))
		Expect(appConn.IsClosed()).To(BeTrue())
		Expect(postgresConn.IsClosed()).To(BeFalse())
	})

	It("waits at the cap while every connection is busy", func(ctx SpecContext) {
		newPool(2, time.Minute)
		_, releaseApp, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		_, releasePostgres, err := pool.Acquire(ctx, "postgres")
		Expect(err).NotTo(HaveOccurred())
		defer releasePostgres(false)

		type result struct {
			conn    *pgx.Conn
			release func(bool)
			err     error
		}
		done := make(chan result, 1)
		go func() {
			defer GinkgoRecover()
			conn, release, acquireErr := pool.Acquire(ctx, "template1")
			done <- result{conn, release, acquireErr}
		}()

		Consistently(done, 500*time.Millisecond).ShouldNot(Receive())
		releaseApp(false)

		var got result
		Eventually(done, 10*time.Second).Should(Receive(&got))
		Expect(got.err).NotTo(HaveOccurred())
		Expect(got.conn.Config().Database).To(Equal("template1"))
		got.release(false)
	})

	It("stops waiting when the context ends", func(ctx SpecContext) {
		newPool(1, time.Minute)
		_, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)

		waitCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		_, _, err = pool.Acquire(waitCtx, "postgres")
		Expect(err).To(MatchError(context.DeadlineExceeded))
	})

	It("gives up after maxWait with ErrNoSlot while every slot is taken, and not when it has a slot", func(ctx SpecContext) {
		newPool(1, time.Minute)
		_, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())

		start := time.Now()
		_, _, err = pool.AcquireWithin(ctx, "postgres", 200*time.Millisecond)
		Expect(err).To(MatchError(pg.ErrNoSlot))
		Expect(time.Since(start)).To(BeNumerically("<", 2*time.Second))

		release(false)
		conn, release2, err := pool.AcquireWithin(ctx, "postgres", 200*time.Millisecond)
		Expect(err).NotTo(HaveOccurred())
		release2(false)
		Expect(conn).NotTo(BeNil())
	})

	It("reports the context's end, not ErrNoSlot, when the context ended as well", func(ctx SpecContext) {
		newPool(1, time.Minute)
		_, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		// With both channels ready select picks at random, so repeat to catch a wrong pick.
		for range 200 {
			_, _, err = pool.AcquireWithin(cancelled, "postgres", time.Nanosecond)
			Expect(err).To(MatchError(context.Canceled))
			Expect(err).NotTo(MatchError(pg.ErrNoSlot))
		}
	})

	It("CloseIdle closes connections idle past idleTimeout", func(ctx SpecContext) {
		newPool(3, time.Minute)
		old, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		release(false)

		clk.SetTime(start.Add(40 * time.Second))
		recent, release, err := pool.Acquire(ctx, "postgres")
		Expect(err).NotTo(HaveOccurred())
		release(false)

		clk.SetTime(start.Add(70 * time.Second))
		pool.CloseIdle()

		Expect(old.IsClosed()).To(BeTrue())
		Expect(recent.IsClosed()).To(BeFalse())
	})

	It("CloseIdle uses the timeout SetIdleTimeout set last", func(ctx SpecContext) {
		newPool(3, time.Hour)
		first, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		pid := backendPID(ctx, first)
		release(false)

		pool.SetIdleTimeout(time.Second)
		clk.SetTime(start.Add(2 * time.Second))
		pool.CloseIdle()

		Expect(first.IsClosed()).To(BeTrue())
		second, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)
		Expect(backendPID(ctx, second)).NotTo(Equal(pid))
	})

	It("replaces a connection whose backend was terminated and handed back broken", func(ctx SpecContext) {
		newPool(2, time.Minute)
		conn, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		pid := backendPID(ctx, conn)

		Expect(server.Exec(ctx, "SELECT pg_terminate_backend("+strconv.FormatUint(uint64(pid), 10)+")")).To(Succeed())
		var one int
		Expect(conn.QueryRow(ctx, "SELECT 1").Scan(&one)).NotTo(Succeed())
		release(true)

		replacement, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)
		Expect(replacement.IsClosed()).To(BeFalse())
		Expect(backendPID(ctx, replacement)).NotTo(Equal(pid))
	})

	It("closes a live connection handed back broken", func(ctx SpecContext) {
		newPool(2, time.Minute)
		conn, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		pid := backendPID(ctx, conn)
		Expect(conn.IsClosed()).To(BeFalse())

		release(true)
		Expect(conn.IsClosed()).To(BeTrue())

		replacement, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)
		Expect(replacement).NotTo(BeIdenticalTo(conn))
		Expect(backendPID(ctx, replacement)).NotTo(Equal(pid))
	})

	DescribeTable("closes a connection handed back inside a transaction instead of reusing it",
		func(ctx SpecContext, sql string, fails bool) {
			newPool(2, time.Minute)
			conn, release, err := pool.Acquire(ctx, "app")
			Expect(err).NotTo(HaveOccurred())
			pid := backendPID(ctx, conn)
			_, err = conn.Exec(ctx, sql)
			Expect(err != nil).To(Equal(fails), "error: %v", err)

			release(false)
			Expect(conn.IsClosed()).To(BeTrue())

			next, release, err := pool.Acquire(ctx, "app")
			Expect(err).NotTo(HaveOccurred())
			defer release(false)
			Expect(backendPID(ctx, next)).NotTo(Equal(pid))
		},
		Entry("in a transaction", "BEGIN", false),
		Entry("in a failed transaction", "BEGIN; SELECT 1/0", true),
	)

	closeWithLogger := func(ctx context.Context, verbosity int) []string {
		var mu sync.Mutex
		var lines []string
		logger := funcr.New(func(prefix, args string) {
			mu.Lock()
			defer mu.Unlock()
			lines = append(lines, prefix+args)
		}, funcr.Options{Verbosity: verbosity})
		pool = pg.NewPool(cfg, 2, 0, clk, pg.WithLogger(logger))
		DeferCleanup(pool.Close)

		conn, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		release(false)
		Expect(conn.PgConn().Conn().Close()).To(Succeed())

		pool.CloseIdle()

		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), lines...)
	}

	It("logs a close error at V(1) with the database name only", func(ctx SpecContext) {
		lines := closeWithLogger(ctx, 1)
		Expect(lines).To(HaveLen(1))
		Expect(lines[0]).To(ContainSubstring(`"database"="app"`))
		Expect(lines[0]).NotTo(ContainSubstring(pgtest.MonitorPassword))
	})

	It("logs nothing at the default verbosity", func(ctx SpecContext) {
		Expect(closeWithLogger(ctx, 0)).To(BeEmpty())
	})

	It("answers ErrBackoff after a failed connect until the delay has passed", func(ctx SpecContext) {
		newPool(2, time.Minute)
		good, err := os.ReadFile(cfg.PasswordFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(cfg.PasswordFile, []byte("wrong\n"), 0o600)).To(Succeed())

		_, _, err = pool.Acquire(ctx, "app")
		Expect(err).To(HaveOccurred())
		Expect(err).NotTo(MatchError(pg.ErrBackoff))
		Expect(errors.As(err, new(*pgconn.ConnectError))).To(BeFalse(), "pgx's ConnectError holds the password")
		Expect(err.Error()).NotTo(ContainSubstring("wrong"))
		var authErr *pgconn.PgError
		Expect(errors.As(err, &authErr)).To(BeTrue())
		Expect(authErr.Code).To(Equal("28P01"))

		Expect(os.WriteFile(cfg.PasswordFile, good, 0o600)).To(Succeed())
		_, _, err = pool.Acquire(ctx, "app")
		Expect(err).To(MatchError(pg.ErrBackoff))

		clk.SetTime(start.Add(999 * time.Millisecond))
		_, _, err = pool.Acquire(ctx, "app")
		Expect(err).To(MatchError(pg.ErrBackoff))

		other, releaseOther, err := pool.Acquire(ctx, "postgres")
		Expect(err).NotTo(HaveOccurred(), "backoff is per database")
		releaseOther(false)
		Expect(other.IsClosed()).To(BeFalse())

		clk.SetTime(start.Add(time.Second))
		conn, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		release(false)
		Expect(conn.IsClosed()).To(BeFalse())
	})

	It("doubles the delay on each failure up to 30s", func(ctx SpecContext) {
		cfg.Port = 1
		newPool(2, time.Minute)

		now := start
		for _, delay := range []time.Duration{
			time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
			16 * time.Second, 30 * time.Second, 30 * time.Second,
		} {
			clk.SetTime(now)
			_, _, err := pool.Acquire(ctx, "app")
			Expect(err).To(HaveOccurred())
			Expect(err).NotTo(MatchError(pg.ErrBackoff))

			clk.SetTime(now.Add(delay - time.Millisecond))
			_, _, err = pool.Acquire(ctx, "app")
			Expect(err).To(MatchError(pg.ErrBackoff), "delay %s", delay)

			now = now.Add(delay)
		}
	})

	It("caps the delay at WithMaxBackoff", func(ctx SpecContext) {
		cfg.Port = 1
		pool = pg.NewPool(cfg, 2, time.Minute, clk, pg.WithMaxBackoff(5*time.Second))
		DeferCleanup(pool.Close)

		now := start
		for _, delay := range []time.Duration{
			time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second,
		} {
			clk.SetTime(now)
			_, _, err := pool.Acquire(ctx, "app")
			Expect(err).To(HaveOccurred())
			Expect(err).NotTo(MatchError(pg.ErrBackoff))

			clk.SetTime(now.Add(delay - time.Millisecond))
			_, _, err = pool.Acquire(ctx, "app")
			Expect(err).To(MatchError(pg.ErrBackoff), "delay %s", delay)

			now = now.Add(delay)
		}
	})

	It("AcquireNow dials during the backoff and a success clears it", func(ctx SpecContext) {
		newPool(2, time.Minute)
		good, err := os.ReadFile(cfg.PasswordFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(cfg.PasswordFile, []byte("wrong\n"), 0o600)).To(Succeed())

		_, _, err = pool.Acquire(ctx, "app")
		Expect(err).To(HaveOccurred())
		Expect(err).NotTo(MatchError(pg.ErrBackoff))
		_, _, err = pool.Acquire(ctx, "app")
		Expect(err).To(MatchError(pg.ErrBackoff))

		_, _, err = pool.AcquireNow(ctx, "app")
		var authErr *pgconn.PgError
		Expect(errors.As(err, &authErr)).To(BeTrue(), "AcquireNow dials while Acquire waits: %v", err)
		Expect(authErr.Code).To(Equal("28P01"))

		Expect(os.WriteFile(cfg.PasswordFile, good, 0o600)).To(Succeed())
		_, _, err = pool.Acquire(ctx, "app")
		Expect(err).To(MatchError(pg.ErrBackoff))
		conn, release, err := pool.AcquireNow(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		Expect(conn.IsClosed()).To(BeFalse())
		release(true)

		conn, release, err = pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred(), "a successful AcquireNow ends the backoff")
		defer release(false)
		Expect(conn.IsClosed()).To(BeFalse())
	})

	It("uses a rotated password file for the next connection", func(ctx SpecContext) {
		newPool(2, time.Minute)
		_, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		release(true)

		DeferCleanup(func(ctx SpecContext) {
			Expect(server.Exec(ctx, "ALTER ROLE cnpg_metrics_exporter PASSWORD '"+pgtest.MonitorPassword+"'")).To(Succeed())
		})
		Expect(server.Exec(ctx, "ALTER ROLE cnpg_metrics_exporter PASSWORD 'rotated'")).To(Succeed())
		Expect(os.WriteFile(cfg.PasswordFile, []byte("rotated\n"), 0o600)).To(Succeed())

		conn, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)
		Expect(conn.IsClosed()).To(BeFalse())
	})

	It("reconnects after the server stops and starts again on the same port", func(ctx SpecContext) {
		newPool(2, time.Minute)
		port := server.Port
		_, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		release(true)

		Expect(server.StopServer(ctx)).To(Succeed())
		_, _, err = pool.Acquire(ctx, "app")
		Expect(err).To(HaveOccurred())

		Expect(server.StartServer(ctx)).To(Succeed())
		Expect(server.Port).To(Equal(port))
		clk.SetTime(start.Add(time.Second))
		conn, release, err := pool.Acquire(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)
		Expect(conn.IsClosed()).To(BeFalse())
	}, SpecTimeout(2*time.Minute))

	It("refuses connections to a database the role cannot connect to", func(ctx SpecContext) {
		newPool(2, time.Minute)
		_, _, err := pool.Acquire(ctx, "noconnect")
		Expect(err).To(HaveOccurred())
		var pgErr *pgconn.PgError
		Expect(errors.As(err, &pgErr)).To(BeTrue())
		Expect(pgErr.Code).To(Equal("42501"))
	})

	It("fails Acquire after Close", func(ctx SpecContext) {
		newPool(2, time.Minute)
		pool.Close()
		_, _, err := pool.Acquire(ctx, "app")
		Expect(err).To(MatchError(pg.ErrPoolClosed))
	})
})
