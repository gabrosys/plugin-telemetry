//go:build integration

package pg_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr"

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

// gatedProxy forwards to the test server. The client's bytes reach the server delay after they arrive.
// On the first connection they also wait for gate, when gate is not nil, so that dial stays in flight
// with its backend already started; started, when not nil, is closed when that connection arrives. When
// one side ends, the proxy closes the other, so a client sees the server close its connection.
func gatedProxy(ctx context.Context, gate <-chan struct{}, started chan<- struct{}, delay time.Duration) uint16 {
	GinkgoHelper()
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	_, portText, err := net.SplitHostPort(ln.Addr().String())
	Expect(err).NotTo(HaveOccurred())
	port, err := strconv.ParseUint(portText, 10, 16)
	Expect(err).NotTo(HaveOccurred())

	var mu sync.Mutex
	var conns []net.Conn
	DeferCleanup(func() {
		Expect(ln.Close()).To(Succeed())
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			if err := c.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				GinkgoWriter.Println("close proxied connection:", err)
			}
		}
	})
	target := net.JoinHostPort(server.Host, strconv.Itoa(int(server.Port)))
	dialCtx := context.WithoutCancel(ctx)
	go func() {
		var d net.Dialer
		for first := true; ; first = false {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			upstream, err := d.DialContext(dialCtx, "tcp", target)
			if err != nil {
				GinkgoWriter.Println("dial the server:", err)
				return
			}
			mu.Lock()
			conns = append(conns, client, upstream)
			mu.Unlock()
			var wait <-chan struct{}
			if first {
				wait = gate
				if started != nil {
					close(started)
				}
			}
			go forward(client, upstream, nil, 0)
			go forward(upstream, client, wait, delay)
		}
	}()
	return uint16(port)
}

// forward copies src to dst, after wait is closed when it is not nil, holding each chunk for delay, and
// closes dst when src ends.
func forward(dst, src net.Conn, wait <-chan struct{}, delay time.Duration) {
	if wait != nil {
		<-wait
	}
	defer func() {
		if err := dst.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			GinkgoWriter.Println("close forwarded connection:", err)
		}
	}()
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			time.Sleep(delay)
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				GinkgoWriter.Println("forward:", err)
			}
			return
		}
	}
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

	newPool := func(maxConns int) {
		pool = pg.NewPool(cfg, maxConns, clk, logr.Logger{}, 0)
		DeferCleanup(pool.Close)
	}

	BeforeEach(func() {
		cfg = server.MonitorConfig(GinkgoT().TempDir())
		cfg.QueryTimeout = 3 * time.Second
		cfg.LockTimeout = time.Second
		clk = clocktesting.NewFakePassiveClock(start)
	})

	It("closes a connection when it is released, so no session stays in the database", func(ctx SpecContext) {
		newPool(2)
		conn, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		pid := backendPID(ctx, conn)
		release()

		Expect(conn.IsClosed()).To(BeTrue())
		var n int
		Expect(plainConn(ctx).QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1", pid).Scan(&n)).To(Succeed())
		Expect(n).To(BeZero())
	})

	It("returns from release only once the backend has left pg_stat_activity", func(ctx SpecContext) {
		newPool(1)
		plain := plainConn(ctx)
		for i := range 100 {
			conn, release, err := pool.AcquireWithin(ctx, "app", 2*time.Second)
			Expect(err).NotTo(HaveOccurred())
			pid := backendPID(ctx, conn)
			release()

			var n int
			Expect(plain.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1", pid).Scan(&n)).To(Succeed())
			Expect(n).To(BeZero(), "cycle %d: backend %d still in pg_stat_activity after release", i, pid)
		}
	})

	It("returns from release once the backend has exited, also when its bytes reach the server late", func(ctx SpecContext) {
		cfg.Host, cfg.Port = "127.0.0.1", gatedProxy(ctx, nil, nil, 100*time.Millisecond)
		newPool(1)
		plain := plainConn(ctx)
		conn, release, err := pool.AcquireWithin(ctx, "app", 10*time.Second)
		Expect(err).NotTo(HaveOccurred())
		pid := backendPID(ctx, conn)
		release()

		var n int
		Expect(plain.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1", pid).Scan(&n)).To(Succeed())
		Expect(n).To(BeZero(), "backend %d still in pg_stat_activity after release", pid)
	})

	It("stays within a role connection limit equal to maxConns", func(ctx SpecContext) {
		const role = "cnpg_limit_one"
		DeferCleanup(func(ctx SpecContext) { Expect(server.Exec(ctx, "DROP ROLE IF EXISTS "+role)).To(Succeed()) })
		Expect(server.Exec(ctx, "CREATE ROLE "+role+" LOGIN CONNECTION LIMIT 1 PASSWORD '"+pgtest.MonitorPassword+"'")).To(Succeed())
		cfg.User = role
		newPool(1)
		for i := range 200 {
			conn, release, err := pool.AcquireWithin(ctx, "app", 2*time.Second)
			Expect(err).NotTo(HaveOccurred(), "cycle %d", i)
			var one int
			Expect(conn.QueryRow(ctx, "SELECT 1").Scan(&one)).To(Succeed())
			release()
		}
	})

	It("leaves the server's values in the session", func(ctx SpecContext) {
		newPool(2)
		conn, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()
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
		newPool(2)
		conn, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()
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
		newPool(2)
		conn, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()

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
		newPool(2)
		conn, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()

		tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		Expect(err).NotTo(HaveOccurred())
		superuser, err := pg.ApplyGuard(ctx, tx, pool.Guard())
		Expect(err).NotTo(HaveOccurred())
		Expect(superuser).To(BeTrue())
		Expect(tx.Rollback(ctx)).To(Succeed())
	})

	It("ends a guarded statement past QueryTimeout with SQLSTATE 57014 and keeps the connection", func(ctx SpecContext) {
		cfg.QueryTimeout = time.Second
		newPool(2)
		conn, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()

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

	It("waits at the cap while every connection is busy", func(ctx SpecContext) {
		newPool(2)
		_, releaseApp, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		_, releasePostgres, err := pool.AcquireWithin(ctx, "postgres", 0)
		Expect(err).NotTo(HaveOccurred())
		defer releasePostgres()

		type result struct {
			conn    *pgx.Conn
			release func()
			err     error
		}
		done := make(chan result, 1)
		go func() {
			defer GinkgoRecover()
			conn, release, acquireErr := pool.AcquireWithin(ctx, "template1", 0)
			done <- result{conn, release, acquireErr}
		}()

		Consistently(done, 500*time.Millisecond).ShouldNot(Receive())
		releaseApp()

		var got result
		Eventually(done, 10*time.Second).Should(Receive(&got))
		Expect(got.err).NotTo(HaveOccurred())
		Expect(got.conn.Config().Database).To(Equal("template1"))
		got.release()
	})

	It("stops waiting when the context ends", func(ctx SpecContext) {
		newPool(1)
		_, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()

		waitCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		_, _, err = pool.AcquireWithin(waitCtx, "postgres", 0)
		Expect(err).To(MatchError(context.DeadlineExceeded))
	})

	It("gives up after maxWait with ErrNoSlot while every slot is taken, and not when it has a slot", func(ctx SpecContext) {
		newPool(1)
		_, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())

		start := time.Now()
		_, _, err = pool.AcquireWithin(ctx, "postgres", 200*time.Millisecond)
		Expect(err).To(MatchError(pg.ErrNoSlot))
		Expect(time.Since(start)).To(BeNumerically("<", 2*time.Second))

		release()
		conn, release2, err := pool.AcquireWithin(ctx, "postgres", 200*time.Millisecond)
		Expect(err).NotTo(HaveOccurred())
		release2()
		Expect(conn).NotTo(BeNil())
	})

	It("reports the context's end, not ErrNoSlot, when the context ended as well", func(ctx SpecContext) {
		newPool(1)
		_, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		// With both channels ready select picks at random, so repeat to catch a wrong pick.
		for range 200 {
			_, _, err = pool.AcquireWithin(cancelled, "postgres", time.Nanosecond)
			Expect(err).To(MatchError(context.Canceled))
			Expect(err).NotTo(MatchError(pg.ErrNoSlot))
		}
	})

	It("frees the slot of a connection whose backend was terminated", func(ctx SpecContext) {
		newPool(1)
		conn, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		pid := backendPID(ctx, conn)

		Expect(server.Exec(ctx, "SELECT pg_terminate_backend("+strconv.FormatUint(uint64(pid), 10)+")")).To(Succeed())
		var one int
		Expect(conn.QueryRow(ctx, "SELECT 1").Scan(&one)).NotTo(Succeed())
		release()

		replacement, release, err := pool.AcquireWithin(ctx, "app", 2*time.Second)
		Expect(err).NotTo(HaveOccurred())
		defer release()
		Expect(replacement.IsClosed()).To(BeFalse())
		Expect(backendPID(ctx, replacement)).NotTo(Equal(pid))
	})

	DescribeTable("closes a connection released inside a transaction",
		func(ctx SpecContext, sql string, fails bool) {
			newPool(2)
			conn, release, err := pool.AcquireWithin(ctx, "app", 0)
			Expect(err).NotTo(HaveOccurred())
			pid := backendPID(ctx, conn)
			_, err = conn.Exec(ctx, sql)
			Expect(err != nil).To(Equal(fails), "error: %v", err)

			release()
			Expect(conn.IsClosed()).To(BeTrue())

			next, release, err := pool.AcquireWithin(ctx, "app", 0)
			Expect(err).NotTo(HaveOccurred())
			defer release()
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
		pool = pg.NewPool(cfg, 2, clk, logger, 0)
		DeferCleanup(pool.Close)

		conn, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(conn.PgConn().Conn().Close()).To(Succeed())

		release()

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
		newPool(2)
		good, err := os.ReadFile(cfg.PasswordFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(cfg.PasswordFile, []byte("wrong\n"), 0o600)).To(Succeed())

		_, _, err = pool.AcquireWithin(ctx, "app", 0)
		Expect(err).To(HaveOccurred())
		Expect(err).NotTo(MatchError(pg.ErrBackoff))
		Expect(errors.As(err, new(*pgconn.ConnectError))).To(BeFalse(), "pgx's ConnectError holds the password")
		Expect(err.Error()).NotTo(ContainSubstring("wrong"))
		var authErr *pgconn.PgError
		Expect(errors.As(err, &authErr)).To(BeTrue())
		Expect(authErr.Code).To(Equal("28P01"))

		Expect(os.WriteFile(cfg.PasswordFile, good, 0o600)).To(Succeed())
		_, _, err = pool.AcquireWithin(ctx, "app", 0)
		Expect(err).To(MatchError(pg.ErrBackoff))

		clk.SetTime(start.Add(999 * time.Millisecond))
		_, _, err = pool.AcquireWithin(ctx, "app", 0)
		Expect(err).To(MatchError(pg.ErrBackoff))

		other, releaseOther, err := pool.AcquireWithin(ctx, "postgres", 0)
		Expect(err).NotTo(HaveOccurred(), "backoff is per database")
		Expect(other.IsClosed()).To(BeFalse())
		releaseOther()

		clk.SetTime(start.Add(time.Second))
		conn, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(conn.IsClosed()).To(BeFalse())
		release()
	})

	It("doubles the delay on each failure up to 30s", func(ctx SpecContext) {
		cfg.Port = 1
		newPool(2)

		now := start
		for _, delay := range []time.Duration{
			time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
			16 * time.Second, 30 * time.Second, 30 * time.Second,
		} {
			clk.SetTime(now)
			_, _, err := pool.AcquireWithin(ctx, "app", 0)
			Expect(err).To(HaveOccurred())
			Expect(err).NotTo(MatchError(pg.ErrBackoff))

			clk.SetTime(now.Add(delay - time.Millisecond))
			_, _, err = pool.AcquireWithin(ctx, "app", 0)
			Expect(err).To(MatchError(pg.ErrBackoff), "delay %s", delay)

			now = now.Add(delay)
		}
	})

	It("caps the delay at the max backoff", func(ctx SpecContext) {
		cfg.Port = 1
		pool = pg.NewPool(cfg, 2, clk, logr.Logger{}, 5*time.Second)
		DeferCleanup(pool.Close)

		now := start
		for _, delay := range []time.Duration{
			time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second,
		} {
			clk.SetTime(now)
			_, _, err := pool.AcquireWithin(ctx, "app", 0)
			Expect(err).To(HaveOccurred())
			Expect(err).NotTo(MatchError(pg.ErrBackoff))

			clk.SetTime(now.Add(delay - time.Millisecond))
			_, _, err = pool.AcquireWithin(ctx, "app", 0)
			Expect(err).To(MatchError(pg.ErrBackoff), "delay %s", delay)

			now = now.Add(delay)
		}
	})

	It("AcquireNow dials during the backoff and a success clears it", func(ctx SpecContext) {
		newPool(2)
		good, err := os.ReadFile(cfg.PasswordFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(cfg.PasswordFile, []byte("wrong\n"), 0o600)).To(Succeed())

		_, _, err = pool.AcquireWithin(ctx, "app", 0)
		Expect(err).To(HaveOccurred())
		Expect(err).NotTo(MatchError(pg.ErrBackoff))
		_, _, err = pool.AcquireWithin(ctx, "app", 0)
		Expect(err).To(MatchError(pg.ErrBackoff))

		_, _, err = pool.AcquireNow(ctx, "app")
		var authErr *pgconn.PgError
		Expect(errors.As(err, &authErr)).To(BeTrue(), "AcquireNow dials while Acquire waits: %v", err)
		Expect(authErr.Code).To(Equal("28P01"))

		Expect(os.WriteFile(cfg.PasswordFile, good, 0o600)).To(Succeed())
		_, _, err = pool.AcquireWithin(ctx, "app", 0)
		Expect(err).To(MatchError(pg.ErrBackoff))
		conn, release, err := pool.AcquireNow(ctx, "app")
		Expect(err).NotTo(HaveOccurred())
		Expect(conn.IsClosed()).To(BeFalse())
		release()

		conn, release, err = pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred(), "a successful AcquireNow ends the backoff")
		defer release()
		Expect(conn.IsClosed()).To(BeFalse())
	})

	It("uses a rotated password file for the next connection", func(ctx SpecContext) {
		newPool(2)
		_, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		release()

		DeferCleanup(func(ctx SpecContext) {
			Expect(server.Exec(ctx, "ALTER ROLE cnpg_metrics_exporter PASSWORD '"+pgtest.MonitorPassword+"'")).To(Succeed())
		})
		Expect(server.Exec(ctx, "ALTER ROLE cnpg_metrics_exporter PASSWORD 'rotated'")).To(Succeed())
		Expect(os.WriteFile(cfg.PasswordFile, []byte("rotated\n"), 0o600)).To(Succeed())

		conn, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()
		Expect(conn.IsClosed()).To(BeFalse())
	})

	It("reconnects after the server stops and starts again on the same port", func(ctx SpecContext) {
		newPool(2)
		port := server.Port
		_, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		release()

		Expect(server.StopServer(ctx)).To(Succeed())
		_, _, err = pool.AcquireWithin(ctx, "app", 0)
		Expect(err).To(HaveOccurred())

		Expect(server.StartServer(ctx)).To(Succeed())
		Expect(server.Port).To(Equal(port))
		clk.SetTime(start.Add(time.Second))
		conn, release, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()
		Expect(conn.IsClosed()).To(BeFalse())
	}, SpecTimeout(2*time.Minute))

	It("refuses connections to a database the role cannot connect to", func(ctx SpecContext) {
		newPool(2)
		_, _, err := pool.AcquireWithin(ctx, "noconnect", 0)
		Expect(err).To(HaveOccurred())
		var pgErr *pgconn.PgError
		Expect(errors.As(err, &pgErr)).To(BeTrue())
		Expect(pgErr.Code).To(Equal("42501"))
	})

	It("closes a connection released after its acquire context ended", func(ctx SpecContext) {
		newPool(2)
		acquire, cancel := context.WithCancel(ctx)
		conn, release, err := pool.AcquireWithin(acquire, "app", 0)
		Expect(err).NotTo(HaveOccurred())
		cancel()

		release()

		Eventually(conn.IsClosed).WithTimeout(time.Second).Should(BeTrue())
	})

	It("closes the connection of a dial that ends after Close, and frees its slot", func(ctx SpecContext) {
		gate := make(chan struct{})
		started := make(chan struct{})
		cfg.Host, cfg.Port = "127.0.0.1", gatedProxy(ctx, gate, started, 0)
		newPool(1)
		plain := plainConn(ctx)

		done := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			_, _, acquireErr := pool.AcquireWithin(ctx, "app", 0)
			done <- acquireErr
		}()
		Eventually(started).Should(BeClosed())
		pool.Close()
		close(gate)
		Eventually(done).WithTimeout(10 * time.Second).Should(Receive(MatchError(pg.ErrPoolClosed)))

		var n int
		Expect(plain.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE usename = $1 AND datname = 'app' "+
			"AND application_name = 'cnpg_otel_engine'", pgtest.MonitorUser).Scan(&n)).To(Succeed())
		Expect(n).To(BeZero(), "the gated dial's backend has left")
		Expect(pool.FreeSlots()).To(Equal(1))
	})

	It("fails AcquireWithin after Close", func(ctx SpecContext) {
		newPool(2)
		pool.Close()
		_, _, err := pool.AcquireWithin(ctx, "app", 0)
		Expect(err).To(MatchError(pg.ErrPoolClosed))
	})
})
