//go:build integration

package planner_test

import (
	"context"
	"os"
	"time"

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
		pool := pg.NewPool(server.MonitorConfig(GinkgoT().TempDir()), 1, time.Minute,
			clocktesting.NewFakePassiveClock(time.Now()))
		DeferCleanup(pool.Close)
		conn, release, err := pool.Acquire(ctx, "postgres")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)

		f, err := planner.GatherFacts(ctx, conn)
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Version.String()).To(Equal("18.0.7"))
		Expect(f.InRecovery).To(BeFalse())
		Expect(f.Databases).To(Equal([]string{"app", "postgres"}))
	})

	gather := func(ctx SpecContext) planner.Facts {
		GinkgoHelper()
		pool := pg.NewPool(server.MonitorConfig(GinkgoT().TempDir()), 1, time.Minute,
			clocktesting.NewFakePassiveClock(time.Now()))
		DeferCleanup(pool.Close)
		conn, release, err := pool.Acquire(ctx, "postgres")
		Expect(err).NotTo(HaveOccurred())
		defer release(false)
		f, err := planner.GatherFacts(ctx, conn)
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
		}))
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
		}))
	})

	It("returns a wrapped error when the context is cancelled", func(ctx SpecContext) {
		pool := pg.NewPool(server.MonitorConfig(GinkgoT().TempDir()), 1, time.Minute,
			clocktesting.NewFakePassiveClock(time.Now()))
		DeferCleanup(pool.Close)
		conn, release, err := pool.Acquire(ctx, "postgres")
		Expect(err).NotTo(HaveOccurred())
		defer release(true)

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err = planner.GatherFacts(cancelled, conn)
		Expect(err).To(MatchError(context.Canceled))
	})
})
