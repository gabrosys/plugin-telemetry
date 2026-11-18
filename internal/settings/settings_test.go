package settings_test

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/test/fixture"
)

// loadFile reads the file at path and parses it relative to its directory, as the engine does.
func loadFile(path string) (settings.Settings, error) {
	data, err := os.ReadFile(path) //nolint:gosec // test fixture path
	if err != nil {
		return settings.Settings{}, fmt.Errorf("read settings: %w", err)
	}

	return parseIn(data, filepath.Dir(path))
}

// load reads testdata/name and parses it relative to testdata.
func load(name string) (settings.Settings, error) {
	data, err := fixture.Open(GinkgoTB(), "testdata").ReadFile(name)
	if err != nil {
		return settings.Settings{}, fmt.Errorf("read settings: %w", err)
	}

	return parseIn(data, "testdata")
}

// parseIn parses data with relative paths resolved against dir.
func parseIn(data []byte, dir string) (settings.Settings, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return settings.Settings{}, fmt.Errorf("resolve settings directory: %w", err)
	}

	return settings.Parse(data, abs)
}

func mustLoad(name string) settings.Settings {
	GinkgoHelper()
	s, err := load(name)
	Expect(err).ToNot(HaveOccurred())
	return s
}

func writeConfig(content string) string {
	GinkgoHelper()
	p := filepath.Join(GinkgoT().TempDir(), "engine.yaml")
	Expect(os.WriteFile(p, []byte(content), 0o600)).To(Succeed())
	return p
}

var _ = Describe("Load", func() {
	It("applies defaults to a minimal file", func() {
		s := mustLoad("minimal.yaml")

		Expect(s.Cluster).To(Equal("pg-main"))
		Expect(s.Namespace).To(BeEmpty())
		Expect(s.Postgres).To(Equal(settings.Postgres{
			Host:           "/controller/run",
			Port:           5432,
			User:           "cnpg_metrics_exporter",
			Database:       "postgres",
			MaxConnections: 4,
		}))
		Expect(s.PlanInterval).To(Equal(30 * time.Second))
		Expect(s.DefaultInterval).To(Equal(30 * time.Second))
		Expect(s.Groups).To(BeEmpty())
		Expect(s.Limits).To(Equal(settings.Limits{
			QueryTimeout:        10 * time.Second,
			LockTimeout:         time.Second,
			MaxRowsPerQuery:     10000,
			MaxSeriesPerQuery:   10000,
			MaxSeriesTotal:      50000,
			MaxDatabases:        50,
			MaxLabelValueLength: 1024,
			MaxLabelsPerSeries:  32,
		}))
		Expect(s.Sources).To(BeEmpty())
		Expect(s.Exporter).To(Equal(settings.Exporter{
			Protocol:            "http/protobuf",
			Endpoint:            "http://collector:4318",
			TLS:                 settings.TLS{MinVersion: "1.2"},
			Compression:         "gzip",
			Interval:            30 * time.Second,
			Timeout:             10 * time.Second,
			MaxPointsPerRequest: 2000,
		}))
	})

	It("defaults the exporter timeout to half the interval when that is below 10s", func() {
		p := writeConfig("cluster: c\nexporter:\n  endpoint: http://x\n  interval: 6s\n")

		s, err := loadFile(p)

		Expect(err).ToNot(HaveOccurred())
		Expect(s.Exporter.Timeout).To(Equal(3 * time.Second))
	})

	It("reads every field of a full file and resolves relative paths against the file directory", func() {
		s := mustLoad("full.yaml")
		dir, err := filepath.Abs("testdata")
		Expect(err).ToNot(HaveOccurred())

		Expect(s).To(Equal(settings.Settings{
			Cluster:   "pg-main",
			Namespace: "prod",
			Postgres: settings.Postgres{
				Host:           "db.internal",
				Port:           6432,
				User:           "metrics",
				PasswordFile:   filepath.Join(dir, "secrets/pg-password"),
				Database:       "app",
				MaxConnections: 8,
			},
			PlanInterval:    time.Minute,
			DefaultInterval: 45 * time.Second,
			Groups: []settings.Group{
				{Name: "fast", Interval: 5 * time.Second, Queries: []string{"pg_stat_*", "pg_locks"}},
				{Name: "slow", Interval: 5 * time.Minute, Queries: []string{"pg_database_size"}},
			},
			Limits: settings.Limits{
				QueryTimeout:        20 * time.Second,
				LockTimeout:         2 * time.Second,
				MaxRowsPerQuery:     100,
				MaxSeriesPerQuery:   200,
				MaxSeriesTotal:      300,
				MaxDatabases:        10,
				MaxLabelValueLength: 64,
				MaxLabelsPerSeries:  8,
			},
			Exclude: settings.Exclude{
				Queries:   []string{"pg_slow_*"},
				Metrics:   []string{"cnpg_pg_wal_lsn"},
				Databases: []string{"template*", "postgres"},
			},
			Sources: []string{filepath.Join(dir, "queries/default.yaml"), "/etc/cnpg/extra.yaml"},
			Exporter: settings.Exporter{
				Protocol: "grpc",
				Endpoint: "https://collector:4317",
				TLS: settings.TLS{
					CAFile:     filepath.Join(dir, "certs/ca.pem"),
					CertFile:   filepath.Join(dir, "certs/tls.crt"),
					KeyFile:    filepath.Join(dir, "certs/tls.key"),
					ServerName: "collector.internal",
					MinVersion: "1.3",
				},
				HeadersFile:         filepath.Join(dir, "secrets/headers"),
				Compression:         "none",
				Interval:            20 * time.Second,
				Timeout:             7 * time.Second,
				MaxPointsPerRequest: 500,
			},
			ResourceAttributes: map[string]string{"k8s.cluster.name": "kind", "team": "db"},
		}))
	})

	It("fails on an unknown key", func() {
		_, err := load("unknown-key.yaml")
		Expect(err).To(MatchError(ContainSubstring("retries")))
	})

	It("fails on a malformed duration", func() {
		p := writeConfig("cluster: c\nplanInterval: soon\nexporter:\n  endpoint: http://x\n")
		_, err := loadFile(p)
		Expect(err).To(HaveOccurred())
	})

	It("reports an empty file as a missing cluster", func() {
		_, err := loadFile(writeConfig(""))
		Expect(err).To(MatchError(ContainSubstring("cluster")))
	})

	It("gives a group without interval the default interval", func() {
		p := writeConfig("cluster: c\ndefaultInterval: 45s\nexporter:\n  endpoint: http://x\ngroups:\n  - name: g\n    queries: [q]\n")

		s, err := loadFile(p)

		Expect(err).ToNot(HaveOccurred())
		Expect(s.Groups[0].Interval).To(Equal(45 * time.Second))
	})

	It("rejects an explicit zero group interval", func() {
		p := writeConfig("cluster: c\nexporter:\n  endpoint: http://x\ngroups:\n  - name: g\n    interval: 0s\n    queries: [q]\n")

		_, err := loadFile(p)

		Expect(err).To(MatchError(ContainSubstring("groups[0].interval")))
	})

	DescribeTable("rejects an unknown key at every level",
		func(doc, key string) {
			_, err := loadFile(writeConfig(doc))
			Expect(err).To(MatchError(ContainSubstring(key)))
		},
		Entry("top level", "cluster: c\nbogusTop: 1\nexporter:\n  endpoint: http://x\n", "bogusTop"),
		Entry("postgres", "cluster: c\npostgres:\n  bogusPg: 1\nexporter:\n  endpoint: http://x\n", "bogusPg"),
		Entry("group item", "cluster: c\nexporter:\n  endpoint: http://x\ngroups:\n  - name: g\n    queries: [q]\n    bogusGroup: 1\n", "bogusGroup"),
		Entry("limits", "cluster: c\nlimits:\n  bogusLimit: 1\nexporter:\n  endpoint: http://x\n", "bogusLimit"),
		Entry("exclude", "cluster: c\nexclude:\n  bogusExclude: [a]\nexporter:\n  endpoint: http://x\n", "bogusExclude"),
		Entry("exporter.tls", "cluster: c\nexporter:\n  endpoint: https://x\n  tls:\n    bogusTLS: 1\n", "bogusTLS"),
	)

	It("rejects a zero exporter timeout instead of defaulting it", func() {
		p := writeConfig("cluster: c\nexporter:\n  endpoint: http://x\n  timeout: 0s\n")
		_, err := loadFile(p)
		Expect(err).To(MatchError(ContainSubstring("exporter.timeout")))
	})
})

var _ = Describe("Validate", func() {
	DescribeTable("rejects",
		func(mutate func(*settings.Settings), field string) {
			s := mustLoad("minimal.yaml")
			s.Groups = []settings.Group{{Name: "g", Interval: 10 * time.Second, Queries: []string{"q"}}}
			Expect(s.Validate()).To(Succeed())

			mutate(&s)

			Expect(s.Validate()).To(MatchError(ContainSubstring(field)))
		},
		Entry("empty cluster", func(s *settings.Settings) { s.Cluster = "" }, "cluster"),
		Entry("planInterval below 1s", func(s *settings.Settings) { s.PlanInterval = 999 * time.Millisecond }, "planInterval"),
		Entry("defaultInterval below 1s", func(s *settings.Settings) { s.DefaultInterval = 0 }, "defaultInterval"),
		Entry("group interval below 1s", func(s *settings.Settings) { s.Groups[0].Interval = time.Millisecond }, "groups[0].interval"),
		Entry("exporter interval below 1s", func(s *settings.Settings) { s.Exporter.Interval = 500 * time.Millisecond }, "exporter.interval"),
		Entry("exporter timeout above half the interval", func(s *settings.Settings) {
			s.Exporter.Interval = 20 * time.Second
			s.Exporter.Timeout = 11 * time.Second
		}, "exporter.timeout"),
		Entry("exporter timeout zero", func(s *settings.Settings) { s.Exporter.Timeout = 0 }, "exporter.timeout"),
		Entry("empty endpoint", func(s *settings.Settings) { s.Exporter.Endpoint = "" }, "exporter.endpoint"),
		Entry("non-http endpoint", func(s *settings.Settings) { s.Exporter.Endpoint = "ftp://x" }, "exporter.endpoint"),
		Entry("endpoint without host", func(s *settings.Settings) { s.Exporter.Endpoint = "http://" }, "exporter.endpoint"),
		Entry("endpoint without scheme", func(s *settings.Settings) { s.Exporter.Endpoint = "collector:4318" }, "exporter.endpoint"),
		Entry("endpoint with a user and password", func(s *settings.Settings) {
			s.Exporter.Endpoint = "http://user:pw@x/v1/metrics"
		}, "exporter.endpoint"),
		Entry("endpoint with a user", func(s *settings.Settings) { s.Exporter.Endpoint = "http://token@x/v1/metrics" }, "exporter.endpoint"),
		Entry("endpoint with a query", func(s *settings.Settings) { s.Exporter.Endpoint = "http://x/v1/metrics?key=k" }, "exporter.endpoint"),
		Entry("endpoint with a fragment", func(s *settings.Settings) { s.Exporter.Endpoint = "http://x/v1/metrics#f" }, "exporter.endpoint"),
		Entry("tls with an http endpoint", func(s *settings.Settings) { s.Exporter.TLS.CAFile = "/ca.pem" }, "exporter.tls"),
		Entry("certFile without keyFile", func(s *settings.Settings) {
			s.Exporter.Endpoint = "https://x"
			s.Exporter.TLS.CertFile = "/c.pem"
		}, "exporter.tls.keyFile"),
		Entry("keyFile without certFile", func(s *settings.Settings) {
			s.Exporter.Endpoint = "https://x"
			s.Exporter.TLS.KeyFile = "/k.pem"
		}, "exporter.tls.certFile"),
		Entry("tls minVersion 1.3 with an http endpoint", func(s *settings.Settings) { s.Exporter.TLS.MinVersion = "1.3" }, "exporter.tls"),
		Entry("tls minVersion", func(s *settings.Settings) { s.Exporter.TLS.MinVersion = "1.1" }, "exporter.tls.minVersion"),
		Entry("protocol", func(s *settings.Settings) { s.Exporter.Protocol = "http/json" }, "exporter.protocol"),
		Entry("compression", func(s *settings.Settings) { s.Exporter.Compression = "zstd" }, "exporter.compression"),
		Entry("maxPointsPerRequest", func(s *settings.Settings) { s.Exporter.MaxPointsPerRequest = 0 }, "exporter.maxPointsPerRequest"),
		Entry("group without queries", func(s *settings.Settings) { s.Groups[0].Queries = nil }, "groups[0].queries"),
		Entry("group without name", func(s *settings.Settings) { s.Groups[0].Name = "" }, "groups[0].name"),
		Entry("duplicate group names", func(s *settings.Settings) {
			s.Groups = append(s.Groups, s.Groups[0])
		}, `duplicate group name "g"`),
		Entry("group pattern", func(s *settings.Settings) { s.Groups[0].Queries = []string{"pg_[a"} }, "groups[0].queries[0]"),
		Entry("exclude query pattern", func(s *settings.Settings) { s.Exclude.Queries = []string{"[x"} }, "exclude.queries[0]"),
		Entry("exclude metric pattern", func(s *settings.Settings) { s.Exclude.Metrics = []string{"[x"} }, "exclude.metrics[0]"),
		Entry("exclude database pattern", func(s *settings.Settings) { s.Exclude.Databases = []string{"[x"} }, "exclude.databases[0]"),
		Entry("maxConnections", func(s *settings.Settings) { s.Postgres.MaxConnections = 0 }, "postgres.maxConnections"),
		Entry("port", func(s *settings.Settings) { s.Postgres.Port = 0 }, "postgres.port"),
		Entry("host", func(s *settings.Settings) { s.Postgres.Host = "" }, "postgres.host"),
		Entry("user", func(s *settings.Settings) { s.Postgres.User = "" }, "postgres.user"),
		Entry("database", func(s *settings.Settings) { s.Postgres.Database = "" }, "postgres.database"),
		Entry("queryTimeout below 100ms", func(s *settings.Settings) { s.Limits.QueryTimeout = 99 * time.Millisecond }, "limits.queryTimeout"),
		Entry("lockTimeout", func(s *settings.Settings) { s.Limits.LockTimeout = 0 }, "limits.lockTimeout"),
		Entry("maxRowsPerQuery", func(s *settings.Settings) { s.Limits.MaxRowsPerQuery = 0 }, "limits.maxRowsPerQuery"),
		Entry("maxSeriesPerQuery", func(s *settings.Settings) { s.Limits.MaxSeriesPerQuery = 0 }, "limits.maxSeriesPerQuery"),
		Entry("maxSeriesTotal", func(s *settings.Settings) { s.Limits.MaxSeriesTotal = 0 }, "limits.maxSeriesTotal"),
		Entry("maxDatabases", func(s *settings.Settings) { s.Limits.MaxDatabases = 0 }, "limits.maxDatabases"),
		Entry("maxLabelValueLength", func(s *settings.Settings) { s.Limits.MaxLabelValueLength = 0 }, "limits.maxLabelValueLength"),
		Entry("maxLabelsPerSeries", func(s *settings.Settings) { s.Limits.MaxLabelsPerSeries = 0 }, "limits.maxLabelsPerSeries"),
	)

	DescribeTable("bounds the timeouts by PostgreSQL's integer milliseconds",
		func(mutate func(*settings.Settings), message string) {
			s := mustLoad("minimal.yaml")
			mutate(&s)
			if message == "" {
				Expect(s.Validate()).To(Succeed())
				return
			}
			Expect(s.Validate()).To(MatchError(ContainSubstring(message)))
		},
		Entry("queryTimeout at INT_MAX/2 ms", func(s *settings.Settings) { s.Limits.QueryTimeout = 1073741823 * time.Millisecond }, ""),
		Entry("queryTimeout one past", func(s *settings.Settings) { s.Limits.QueryTimeout = 1073741824 * time.Millisecond },
			"limits.queryTimeout: must be at most 1073741823ms"),
		Entry("lockTimeout at INT_MAX ms", func(s *settings.Settings) { s.Limits.LockTimeout = 2147483647 * time.Millisecond }, ""),
		Entry("lockTimeout one past", func(s *settings.Settings) { s.Limits.LockTimeout = 2147483648 * time.Millisecond },
			"limits.lockTimeout: must be at most 2147483647ms"),
	)

	DescribeTable("never echoes the user info, the query or the fragment of a rejected endpoint",
		func(endpoint string) {
			s := mustLoad("minimal.yaml")
			s.Exporter.Endpoint = endpoint
			err := s.Validate()
			Expect(err).To(MatchError(ContainSubstring("exporter.endpoint")))
			Expect(err.Error()).NotTo(ContainSubstring("secret"))
		},
		Entry("a password", "https://user:secret@x/v1/metrics"),
		Entry("a user", "https://secret@x/v1/metrics"),
		Entry("a query", "https://x/v1/metrics?token=secret"),
		Entry("a fragment", "https://x/v1/metrics#secret"),
		Entry("a password with another scheme", "ftp://user:secret@x"),
		Entry("a password without a host", "http://user:secret@"),
		Entry("a password in a URL that does not parse", "http://user:secret@[::1"),
		Entry("a password without a scheme", "user:secret@collector:4318"),
		Entry("a password after a scheme without slashes", "http:user:secret@collector:4318"),
		Entry("a password after a single slash", "https:/user:secret@collector:4318"),
		Entry("a password after three slashes", "http:///user:secret@collector:4318"),
	)

	It("reports every problem at once", func() {
		s := mustLoad("minimal.yaml")
		s.Cluster = ""
		s.Postgres.MaxConnections = 0

		err := s.Validate()

		Expect(err).To(MatchError(ContainSubstring("cluster")))
		Expect(err).To(MatchError(ContainSubstring("postgres.maxConnections")))
	})

	It("accepts tls settings with an https endpoint and an equal timeout and half interval", func() {
		s := mustLoad("full.yaml")
		s.Exporter.Interval = 14 * time.Second
		s.Exporter.Timeout = 7 * time.Second
		Expect(s.Validate()).To(Succeed())
	})
})

var _ = Describe("groups", func() {
	var s settings.Settings

	BeforeEach(func() {
		s = mustLoad("full.yaml")
	})

	It("GroupFor takes the first matching group", func() {
		s.Groups = append(s.Groups, settings.Group{Name: "all", Interval: time.Minute, Queries: []string{"pg_*"}})

		g, d := s.GroupFor("pg_stat_activity")
		Expect(g).To(Equal("fast"))
		Expect(d).To(Equal(5 * time.Second))

		g, d = s.GroupFor("pg_database_size")
		Expect(g).To(Equal("slow"))
		Expect(d).To(Equal(5 * time.Minute))
	})

	It("GroupFor falls back to the default interval", func() {
		g, d := s.GroupFor("other")
		Expect(g).To(BeEmpty())
		Expect(d).To(Equal(45 * time.Second))
	})

	It("Warnings names a query matched by two groups", func() {
		s.Groups = append(s.Groups, settings.Group{Name: "all", Interval: time.Minute, Queries: []string{"pg_locks"}})

		w := s.Warnings([]string{"pg_locks", "pg_stat_activity", "pg_database_size"})

		Expect(w).To(HaveLen(1))
		Expect(w[0]).To(And(ContainSubstring("pg_locks"), ContainSubstring("fast"), ContainSubstring("all")))
	})

	It("Warnings names a group pattern that matches no query", func() {
		w := s.Warnings([]string{"pg_locks", "pg_database_size", "pg_stat_x"})

		Expect(w).To(BeEmpty())

		w = s.Warnings([]string{"pg_locks", "pg_stat_x"})
		Expect(w).To(ConsistOf(And(ContainSubstring("slow"), ContainSubstring("pg_database_size"))))
	})
})

var _ = Describe("exclusions", func() {
	It("match names and globs", func() {
		s := mustLoad("full.yaml")

		Expect(s.ExcludedQuery("pg_slow_scan")).To(BeTrue())
		Expect(s.ExcludedQuery("pg_locks")).To(BeFalse())
		Expect(s.ExcludedMetric("cnpg_pg_wal_lsn")).To(BeTrue())
		Expect(s.ExcludedMetric("cnpg_pg_wal_lsn_diff")).To(BeFalse())
		Expect(s.ExcludedDatabase("template1")).To(BeTrue())
		Expect(s.ExcludedDatabase("postgres")).To(BeTrue())
		Expect(s.ExcludedDatabase("app")).To(BeFalse())
	})
})
