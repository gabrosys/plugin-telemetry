//go:build integration

package preflight_test

import (
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/preflight"
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

var _ = Describe("Run against PostgreSQL", func() {
	run := func(ctx SpecContext) *preflight.PostgresReport {
		GinkgoHelper()
		rep := preflight.Run(ctx, preflight.Config{
			PodTokenDir: GinkgoT().TempDir(),
			Postgres: preflight.PostgresConfig{
				Host: server.Host, Port: server.Port, User: pgtest.MonitorUser,
				Database: "postgres", Password: pgtest.MonitorPassword,
			},
		})
		Expect(rep.Postgres).NotTo(BeNil())
		Expect(rep.Postgres.Error).To(BeEmpty())
		Expect(rep.Postgres.Connected).To(BeTrue())
		return rep.Postgres
	}

	It("reads the WAL directories as the monitoring role", func(ctx SpecContext) {
		rep := run(ctx)
		Expect(rep.CurrentUser).To(Equal(pgtest.MonitorUser))
		Expect(rep.WALFiles).To(BeNumerically(">", 0))
	})

	It("runs its query in a read-only transaction with statement_timeout at QueryTimeout", func(ctx SpecContext) {
		DeferCleanup(preflight.SetQuery(`SELECT pg_catalog.current_setting('transaction_read_only'),
       (SELECT setting::int FROM pg_catalog.pg_settings WHERE name = 'statement_timeout'),
       0`))
		rep := run(ctx)
		Expect(rep.CurrentUser).To(Equal("on"))
		Expect(rep.WALFiles).To(Equal(int(settings.Defaults().Limits.QueryTimeout.Milliseconds())))
	})

	It("fails for a session role that is a superuser", func(ctx SpecContext) {
		const role = "cnpg_pre_super"
		DeferCleanup(func(ctx SpecContext) { Expect(server.Exec(ctx, "DROP ROLE IF EXISTS "+role)).To(Succeed()) })
		Expect(server.Exec(ctx, "CREATE ROLE "+role+" LOGIN SUPERUSER PASSWORD '"+pgtest.MonitorPassword+"'")).To(Succeed())

		rep := preflight.Run(ctx, preflight.Config{
			PodTokenDir: GinkgoT().TempDir(),
			Postgres: preflight.PostgresConfig{
				Host: server.Host, Port: server.Port, User: role,
				Database: "postgres", Password: pgtest.MonitorPassword,
			},
		})
		Expect(rep.Postgres).NotTo(BeNil())
		Expect(rep.Postgres.Connected).To(BeTrue())
		Expect(rep.Postgres.Error).To(ContainSubstring("the session role is a superuser"))
		Expect(rep.Postgres.CurrentUser).To(BeEmpty())
	})
})
