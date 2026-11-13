package pg_test

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/pg"
)

var _ = Describe("ConnConfig", func() {
	var (
		dir string
		cfg pg.Config
	)

	BeforeEach(func() {
		for _, name := range pg.EnvVars {
			GinkgoT().Setenv(name, "")
			Expect(os.Unsetenv(name)).To(Succeed())
		}
		dir = GinkgoT().TempDir()
		cfg = pg.Config{
			Host:           "db.example.com",
			Port:           5433,
			User:           "monitor",
			QueryTimeout:   30 * time.Second,
			LockTimeout:    2 * time.Second,
			ConnectTimeout: 5 * time.Second,
		}
	})

	It("sets every connection key explicitly", func() {
		cc, err := pg.ConnConfig(cfg, "app")
		Expect(err).NotTo(HaveOccurred())

		Expect(cc.Host).To(Equal("db.example.com"))
		Expect(cc.Port).To(BeEquivalentTo(5433))
		Expect(cc.User).To(Equal("monitor"))
		Expect(cc.Database).To(Equal("app"))
		Expect(cc.Password).To(BeEmpty())
		Expect(cc.TLSConfig).To(BeNil())
		Expect(cc.Fallbacks).To(BeEmpty())
		Expect(cc.ConnectTimeout).To(Equal(5 * time.Second))
		Expect(cc.ValidateConnect).To(BeNil())
		Expect(cc.RuntimeParams).To(HaveKeyWithValue("options", ""))
		Expect(cc.RuntimeParams).To(HaveKeyWithValue("application_name", "cnpg_otel_engine"))
		Expect(cc.RuntimeParams).NotTo(HaveKey("timezone"))
	})

	It("puts the session settings in RuntimeParams", func() {
		cc, err := pg.ConnConfig(cfg, "app")
		Expect(err).NotTo(HaveOccurred())

		Expect(cc.RuntimeParams).To(HaveKeyWithValue("default_transaction_read_only", "on"))
		Expect(cc.RuntimeParams).To(HaveKeyWithValue("search_path", "pg_catalog, public, pg_temp"))
		Expect(cc.RuntimeParams).To(HaveKeyWithValue("statement_timeout", "30000"))
		Expect(cc.RuntimeParams).To(HaveKeyWithValue("lock_timeout", "2000"))
		Expect(cc.RuntimeParams).To(HaveKeyWithValue("idle_in_transaction_session_timeout", "60000"))
		Expect(cc.RuntimeParams).To(HaveKeyWithValue("application_name", "cnpg_otel_engine"))
		Expect(cc.RuntimeParams).To(HaveKeyWithValue("jit", "off"))
		Expect(cc.RuntimeParams).To(HaveKeyWithValue("max_parallel_workers_per_gather", "0"))
		Expect(cc.RuntimeParams).To(HaveKeyWithValue("standard_conforming_strings", "on"))
	})

	It("reads the password file at every call, trimming one trailing newline", func() {
		file := filepath.Join(dir, "password")
		cfg.PasswordFile = file

		Expect(os.WriteFile(file, []byte("first\n"), 0o600)).To(Succeed())
		cc, err := pg.ConnConfig(cfg, "app")
		Expect(err).NotTo(HaveOccurred())
		Expect(cc.Password).To(Equal("first"))

		Expect(os.WriteFile(file, []byte("second\n\n"), 0o600)).To(Succeed())
		cc, err = pg.ConnConfig(cfg, "app")
		Expect(err).NotTo(HaveOccurred())
		Expect(cc.Password).To(Equal("second\n"))
	})

	It("fails when the password file cannot be read", func() {
		cfg.PasswordFile = filepath.Join(dir, "missing")
		_, err := pg.ConnConfig(cfg, "app")
		Expect(err).To(MatchError(os.ErrNotExist))
	})

	It("quotes values with spaces, quotes and backslashes", func() {
		file := filepath.Join(dir, "password")
		Expect(os.WriteFile(file, []byte(`pa ss'wo\rd`+"\n"), 0o600)).To(Succeed())
		cfg.PasswordFile = file
		cfg.User = `mon itor'x\`

		cc, err := pg.ConnConfig(cfg, `my db'\ name`)
		Expect(err).NotTo(HaveOccurred())
		Expect(cc.Password).To(Equal(`pa ss'wo\rd`))
		Expect(cc.User).To(Equal(`mon itor'x\`))
		Expect(cc.Database).To(Equal(`my db'\ name`))
		Expect(cc.RuntimeParams).To(HaveKeyWithValue("application_name", "cnpg_otel_engine"))
	})

	It("refuses an empty host or user", func() {
		noHost := cfg
		noHost.Host = ""
		_, err := pg.ConnConfig(noHost, "app")
		Expect(err).To(MatchError(pg.ErrInvalidConfig))

		noUser := cfg
		noUser.User = ""
		_, err = pg.ConnConfig(noUser, "app")
		Expect(err).To(MatchError(pg.ErrInvalidConfig))
	})

	DescribeTable("refuses a timeout below one millisecond, which PostgreSQL reads as disabled",
		func(query, lock time.Duration) {
			cfg.QueryTimeout, cfg.LockTimeout = query, lock
			_, err := pg.ConnConfig(cfg, "app")
			Expect(err).To(MatchError(pg.ErrInvalidConfig))
		},
		Entry("zero query timeout", time.Duration(0), time.Second),
		Entry("sub-millisecond query timeout", 999*time.Microsecond, time.Second),
		Entry("zero lock timeout", time.Second, time.Duration(0)),
		Entry("sub-millisecond lock timeout", time.Second, 999*time.Microsecond),
	)

	DescribeTable("refuses to build a config while a PG* variable is set, naming it",
		func(name string) {
			GinkgoT().Setenv(name, "x")
			_, err := pg.ConnConfig(cfg, "app")
			Expect(err).To(MatchError(pg.ErrPGEnv))
			Expect(err.Error()).To(ContainSubstring(name))
		},
		func(name string) string { return name },
		Entry(nil, "PGHOST"), Entry(nil, "PGPORT"), Entry(nil, "PGDATABASE"), Entry(nil, "PGUSER"),
		Entry(nil, "PGPASSWORD"), Entry(nil, "PGPASSFILE"), Entry(nil, "PGAPPNAME"),
		Entry(nil, "PGCONNECT_TIMEOUT"), Entry(nil, "PGSSLMODE"), Entry(nil, "PGSSLKEY"),
		Entry(nil, "PGSSLCERT"), Entry(nil, "PGSSLCERTMODE"), Entry(nil, "PGSSLSNI"),
		Entry(nil, "PGSSLROOTCERT"), Entry(nil, "PGSSLPASSWORD"), Entry(nil, "PGSSLNEGOTIATION"),
		Entry(nil, "PGTARGETSESSIONATTRS"), Entry(nil, "PGSERVICE"), Entry(nil, "PGSERVICEFILE"),
		Entry(nil, "PGTZ"), Entry(nil, "PGOPTIONS"), Entry(nil, "PGMINPROTOCOLVERSION"),
		Entry(nil, "PGMAXPROTOCOLVERSION"), Entry(nil, "PGCHANNELBINDING"), Entry(nil, "PGREQUIREAUTH"),
	)

	It("lists the 25 variables pgx reads", func() {
		Expect(pg.EnvVars).To(HaveLen(25))
	})

	It("ClearEnv unsets the variables and returns the names it removed", func() {
		GinkgoT().Setenv("PGHOST", "elsewhere")
		GinkgoT().Setenv("PGTZ", "UTC")

		Expect(pg.ClearEnv()).To(ConsistOf("PGHOST", "PGTZ"))
		_, hostSet := os.LookupEnv("PGHOST")
		Expect(hostSet).To(BeFalse())
		Expect(pg.ClearEnv()).To(BeEmpty())
	})

	It("ignores ~/.pgpass, ~/.pg_service.conf and ~/.postgresql/root.crt", func() {
		home := GinkgoT().TempDir()
		GinkgoT().Setenv("HOME", home)
		Expect(os.MkdirAll(filepath.Join(home, ".postgresql"), 0o700)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(home, ".pgpass"), []byte("*:*:*:*:from-pgpass\n"), 0o600)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(home, ".pg_service.conf"),
			[]byte("[monitor]\nhost=elsewhere\nport=1\n"), 0o600)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(home, ".postgresql", "root.crt"), []byte("junk"), 0o600)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(home, ".postgresql", "postgresql.crt"), []byte("junk"), 0o600)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(home, ".postgresql", "postgresql.key"), []byte("junk"), 0o600)).To(Succeed())

		cc, err := pg.ConnConfig(cfg, "app")
		Expect(err).NotTo(HaveOccurred())
		Expect(cc.Password).To(BeEmpty())
		Expect(cc.Host).To(Equal("db.example.com"))
		Expect(cc.TLSConfig).To(BeNil())
	})

	It("returns ErrInvalidConfig without any part of the connection string", func() {
		file := filepath.Join(dir, "password")
		Expect(os.WriteFile(file, []byte("hunter2-secret\n"), 0o600)).To(Succeed())
		cfg.PasswordFile = file
		cfg.Host = "leaky-host\x00"

		_, err := pg.ConnConfig(cfg, "app")
		Expect(err).To(MatchError(pg.ErrInvalidConfig))
		Expect(err.Error()).NotTo(ContainSubstring("hunter2-secret"))
		Expect(err.Error()).NotTo(ContainSubstring("leaky-host"))
		Expect(err.Error()).NotTo(ContainSubstring("monitor"))

		var parseErr *pgconn.ParseConfigError
		Expect(errors.As(err, &parseErr)).To(BeFalse())
	})
})
