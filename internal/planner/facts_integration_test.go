//go:build integration

package planner_test

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/go-logr/logr"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
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

var _ = Describe("GatherFacts", func() {
	It("reads the version, the recovery state and the connectable databases", func(ctx SpecContext) {
		pool := pg.NewPool(server.MonitorConfig(GinkgoT().TempDir()), 1,
			clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
		DeferCleanup(pool.Close)
		conn, release, err := pool.AcquireWithin(ctx, "postgres", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()

		f, err := planner.GatherFacts(ctx, conn, pool.Guard())
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Version.String()).To(Equal("18.0.7"))
		Expect(f.InRecovery).To(BeFalse())
		Expect(f.Databases).To(Equal([]string{"app", "postgres"}))
		Expect(f.WALSegmentSize).To(Equal(16777216.0))
	})

	gather := func(ctx SpecContext) planner.Facts {
		GinkgoHelper()
		pool := pg.NewPool(server.MonitorConfig(GinkgoT().TempDir()), 1,
			clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
		DeferCleanup(pool.Close)
		conn, release, err := pool.AcquireWithin(ctx, "postgres", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()
		f, err := planner.GatherFacts(ctx, conn, pool.Guard())
		Expect(err).NotTo(HaveOccurred())
		return f
	}

	It("gathers the five checks as the monitoring role", func(ctx SpecContext) {
		Expect(gather(ctx).Checks).To(Equal(map[string]bool{
			"pg_monitor":          true,
			"read_only":           true,
			"track_io_timing":     false,
			"track_wal_io_timing": false,
			"track_functions":     false,
			"superuser":           false,
		}))
	})

	It("checks read_only on the engine's transactions while the server's default_transaction_read_only is off", func(ctx SpecContext) {
		plain, err := server.PlainConn(ctx, "postgres")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) { Expect(plain.Close(ctx)).To(Succeed()) })
		var serverValue string
		Expect(plain.QueryRow(ctx, "SHOW default_transaction_read_only").Scan(&serverValue)).To(Succeed())
		Expect(serverValue).To(Equal("off"))

		Expect(gather(ctx).Checks).To(HaveKeyWithValue("read_only", true))
	})

	It("reports the settings that are on", func(ctx SpecContext) {
		role := pgtest.MonitorUser
		Expect(server.Exec(ctx, "ALTER ROLE "+role+" SET track_io_timing = on")).To(Succeed())
		Expect(server.Exec(ctx, "ALTER ROLE "+role+" SET track_wal_io_timing = on")).To(Succeed())
		Expect(server.Exec(ctx, "ALTER ROLE "+role+" SET track_functions = 'pl'")).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(server.Exec(ctx, "ALTER ROLE "+role+" RESET ALL")).To(Succeed())
		})

		Expect(gather(ctx).Checks).To(Equal(map[string]bool{
			"pg_monitor":          true,
			"read_only":           true,
			"track_io_timing":     true,
			"track_wal_io_timing": true,
			"track_functions":     true,
			"superuser":           false,
		}))
	})

	It("omits only the check whose setting does not exist and still gathers the facts", func(ctx SpecContext) {
		pool := pg.NewPool(server.MonitorConfig(GinkgoT().TempDir()), 1,
			clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
		DeferCleanup(pool.Close)
		conn, release, err := pool.AcquireWithin(ctx, "postgres", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()

		f, err := planner.GatherFactsWith(ctx, conn, pool.Guard(), map[string]string{
			"track_io_timing":     "track_io_timing",
			"track_wal_io_timing": "no_such_setting",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Checks).To(HaveKeyWithValue("track_io_timing", false))
		Expect(f.Checks).NotTo(HaveKey("track_wal_io_timing"))
		Expect(f.Checks).To(HaveKey("pg_monitor"))
		Expect(f.Databases).To(Equal([]string{"app", "postgres"}))
	})

	It("refuses a superuser session with ErrSuperuser and reports the check", func(ctx SpecContext) {
		const role = "cnpg_super_test"
		DeferCleanup(func(ctx SpecContext) { Expect(server.Exec(ctx, "DROP ROLE IF EXISTS "+role)).To(Succeed()) })
		Expect(server.Exec(ctx, "CREATE ROLE "+role+" LOGIN SUPERUSER PASSWORD '"+pgtest.MonitorPassword+"'")).To(Succeed())

		cfg := server.MonitorConfig(GinkgoT().TempDir())
		cfg.User = role
		pool := pg.NewPool(cfg, 1, clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
		DeferCleanup(pool.Close)
		conn, release, err := pool.AcquireWithin(ctx, "postgres", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()

		f, err := planner.GatherFacts(ctx, conn, pool.Guard())
		Expect(errors.Is(err, planner.ErrSuperuser)).To(BeTrue(), "%v", err)
		Expect(f.Checks).To(Equal(map[string]bool{"superuser": true}))
		Expect(f.Databases).To(BeEmpty())
	})

	It("returns a wrapped error when the context is cancelled", func(ctx SpecContext) {
		pool := pg.NewPool(server.MonitorConfig(GinkgoT().TempDir()), 1,
			clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
		DeferCleanup(pool.Close)
		conn, release, err := pool.AcquireWithin(ctx, "postgres", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err = planner.GatherFacts(cancelled, conn, pool.Guard())
		Expect(err).To(MatchError(context.Canceled))
	})

	It("fails the round with the guard's error when the guard fails", func(ctx SpecContext) {
		pool := pg.NewPool(server.MonitorConfig(GinkgoT().TempDir()), 1,
			clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
		DeferCleanup(pool.Close)
		conn, release, err := pool.AcquireWithin(ctx, "postgres", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()

		_, err = planner.GatherFacts(ctx, conn, "SELECT pg_catalog.no_such_guard()")
		Expect(err).To(MatchError(ContainSubstring("set the transaction limits")))
		var pgErr *pgconn.PgError
		Expect(errors.As(err, &pgErr)).To(BeTrue(), "error: %v", err)
		Expect(pgErr.Code).To(Equal("42883"))
	})

	It("bounds the rollback when the server never sees it", func(ctx SpecContext) {
		proxy, err := pgtest.StartProxy(ctx, server.Host, server.Port)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(proxy.Close)
		cfg := server.MonitorConfig(GinkgoT().TempDir())
		cfg.Host, cfg.Port = proxy.Host, proxy.Port
		pool := pg.NewPool(cfg, 1, clocktesting.NewFakePassiveClock(time.Now()), logr.Logger{}, 0)
		DeferCleanup(pool.Close)
		conn, release, err := pool.AcquireWithin(ctx, "postgres", 0)
		Expect(err).NotTo(HaveOccurred())
		defer release()

		long, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		start := time.Now()
		_, err = planner.GatherFacts(long, conn, "SELECT pg_catalog.no_such_guard()")
		// The 2s rollback bound, then pgx's 2s socket deadline after its cancel request, plus a margin.
		Expect(time.Since(start)).To(BeNumerically("<", 6*time.Second))
		Expect(err).To(MatchError(ContainSubstring("set the transaction limits")))
		Expect(err).To(MatchError(ContainSubstring("roll back transaction")))
	}, SpecTimeout(20*time.Second))

	// renameBad renames a database from a SQL_ASCII database, whose sessions pass bytes above 127 unchecked
	// (PostgreSQL 18, "Character Set Support"); the docker exec API cannot carry such a byte in an argument.
	// from and to are SQL expressions.
	renameBad := func(from, to string) string {
		return "SET client_encoding = 'SQL_ASCII'; DO $$ BEGIN EXECUTE format('ALTER DATABASE %I RENAME TO %I', " +
			from + ", " + to + "); END $$"
	}

	It("skips connectable databases whose names are not valid UTF-8", func(ctx SpecContext) {
		const (
			plain   = "'bad' || chr(255) || 'name'"
			control = "'bad' || chr(10) || '\"name' || chr(255)"
		)
		Expect(server.Exec(ctx, "CREATE DATABASE sqlascii TEMPLATE template0 ENCODING 'SQL_ASCII' LOCALE_PROVIDER libc LOCALE 'C'")).To(Succeed())
		Expect(server.Exec(ctx, "CREATE DATABASE badname")).To(Succeed())
		Expect(server.Exec(ctx, "CREATE DATABASE badname2")).To(Succeed())
		renamed := false
		DeferCleanup(func(ctx SpecContext) {
			var errs error
			if renamed {
				errs = errors.Join(
					server.ExecIn(ctx, "sqlascii", renameBad(plain, "'badname'")),
					server.ExecIn(ctx, "sqlascii", renameBad(control, "'badname2'")))
			}
			errs = errors.Join(errs,
				server.Exec(ctx, "DROP DATABASE IF EXISTS badname"),
				server.Exec(ctx, "DROP DATABASE IF EXISTS badname2"),
				server.Exec(ctx, "DROP DATABASE sqlascii"))
			Expect(errs).NotTo(HaveOccurred())
		})
		Expect(server.ExecIn(ctx, "sqlascii", renameBad("'badname'", plain))).To(Succeed())
		Expect(server.ExecIn(ctx, "sqlascii", renameBad("'badname2'", control))).To(Succeed())
		renamed = true

		f := gather(ctx)
		Expect(f.Databases).To(Equal([]string{"app", "postgres", "sqlascii"}))
		Expect(f.SkippedDatabases).To(Equal([]string{`"bad\n\"name\xff"`, `"bad\xffname"`}))
	})
})
