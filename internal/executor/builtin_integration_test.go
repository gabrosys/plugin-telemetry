//go:build integration

package executor_test

import (
	"encoding/json"
	"math"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/executor"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
)

// cnpgSyncNames is CNPG's synchronousStandbyNamesRegex (pg_collector.go:47 at v1.30.1).
var cnpgSyncNames = regexp.MustCompile(`(?:ANY|FIRST) ([0-9]+) \(.*\)`)

const walSegment = 16777216

// walFileName is CNPG main's regexPGWalFileName (metricserver/wal.go:147 at bc0889a).
var walFileName = regexp.MustCompile("^[0-9A-F]{24}$")

func builtinJob(key string) planner.Job {
	GinkgoHelper()

	q, ok := queries.Builtin()[key]
	Expect(ok).To(BeTrue(), key)
	def := queries.Define(key, q, queries.Options{MaxLabelsPerSeries: 32})
	Expect(def.Warnings).To(BeEmpty())
	Expect(def.Disabled).To(BeEmpty())

	return planner.Job{Query: key, Database: "postgres", Def: &def}
}

// byLabel returns the value of each sample of one metric, keyed by the value of its only label.
func byLabel(samples []queries.Sample, metric string) map[string]float64 {
	GinkgoHelper()

	out := map[string]float64{}
	for _, s := range samples {
		Expect(s.Metric).To(Equal(metric))
		Expect(s.Labels).To(HaveLen(1))
		out[s.Labels[0].Value] = s.Value
	}

	return out
}

func runBuiltin(ctx SpecContext, exec *executor.Executor, key string) []queries.Sample {
	GinkgoHelper()

	res := exec.Run(ctx, builtinJob(key))
	Expect(res.Outcome).To(Equal(selfmetrics.OutcomeOK), "%s: %v", key, res.Err)

	return res.Samples
}

type goldenMetric struct {
	Name   string   `json:"name"`
	Kind   string   `json:"kind"`
	Labels []string `json:"labels"`
}

var _ = Describe("SQL built-ins", func() {
	var exec *executor.Executor

	BeforeEach(func() {
		l := limits()
		l.QueryTimeout = 10 * time.Second
		l.LockTimeout = 5 * time.Second
		exec = newExecutor(newPool(l), l)
	})

	Describe("collector_pg_wal", func() {
		It("reports the WAL directory and the WAL settings as CNPG does", func(ctx SpecContext) {
			samples := runBuiltin(ctx, exec, queries.KeyPGWAL)
			got := byLabel(samples, "cnpg_collector_pg_wal")

			keys := make([]string, 0, len(got))
			for k := range got {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			Expect(keys).To(Equal([]string{"count", "keep", "max", "min", "size", "slots_max"}))

			Expect(got["count"]).To(BeNumerically(">=", 1))
			Expect(got["size"]).To(Equal(got["count"] * walSegment))
			Expect(got["min"]).To(Equal(5.0))
			Expect(got["max"]).To(Equal(64.0))
			Expect(got["keep"]).To(Equal(0.0))
			Expect(math.IsNaN(got["slots_max"])).To(BeTrue(), "slots_max is %v", got["slots_max"])
		})

		It("reports max_slot_wal_keep_size in segments once it is set", func(ctx SpecContext) {
			Expect(server.Exec(ctx, "ALTER SYSTEM SET max_slot_wal_keep_size = '1GB'")).To(Succeed())
			DeferCleanup(func(ctx SpecContext) {
				Expect(server.Exec(ctx, "ALTER SYSTEM RESET max_slot_wal_keep_size")).To(Succeed())
				Expect(server.Exec(ctx, "SELECT pg_reload_conf()")).To(Succeed())
				Eventually(func(g Gomega) {
					g.Expect(math.IsNaN(byLabel(runBuiltin(ctx, exec, queries.KeyPGWAL), "cnpg_collector_pg_wal")["slots_max"])).To(BeTrue())
				}).WithContext(ctx).WithPolling(200 * time.Millisecond).Should(Succeed())
			}, NodeTimeout(time.Minute))
			Expect(server.Exec(ctx, "SELECT pg_reload_conf()")).To(Succeed())

			Eventually(func(g Gomega) {
				g.Expect(byLabel(runBuiltin(ctx, exec, queries.KeyPGWAL), "cnpg_collector_pg_wal")).
					To(HaveKeyWithValue("slots_max", 64.0))
			}).WithContext(ctx).WithPolling(200 * time.Millisecond).Should(Succeed())
		})

		It("counts the names under pg_wal that the regex matches", func(ctx SpecContext) {
			conn, err := server.PlainConn(ctx, "postgres")
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func(ctx SpecContext) { Expect(conn.Close(ctx)).To(Succeed()) })

			Eventually(func(g Gomega) {
				names, err := pgx.CollectRows(mustQuery(ctx, conn, "SELECT name FROM pg_catalog.pg_ls_waldir()"), pgx.RowTo[string])
				g.Expect(err).NotTo(HaveOccurred())
				want := 0
				for _, name := range names {
					if walFileName.MatchString(name) {
						want++
					}
				}
				g.Expect(want).To(BeNumerically(">=", 1))

				got := byLabel(runBuiltin(ctx, exec, queries.KeyPGWAL), "cnpg_collector_pg_wal")
				g.Expect(got["count"]).To(Equal(float64(want)))
			}).WithContext(ctx).WithPolling(200 * time.Millisecond).Should(Succeed())
		})

		It("uses the anchored regex of CNPG main", func() {
			Expect(queries.Builtin()[queries.KeyPGWAL].Query).To(ContainSubstring("name ~ '^[0-9A-F]{24}$'"))
		})

		// The entries of CNPG main metricserver/wal_test.go:34-50 at bc0889a.
		DescribeTable("matches only valid WAL segment names in SQL as CNPG main does in Go",
			func(ctx SpecContext, name string, want bool) {
				conn, err := server.PlainConn(ctx, "postgres")
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func(ctx SpecContext) { Expect(conn.Close(ctx)).To(Succeed()) })

				var got bool
				Expect(conn.QueryRow(ctx, "SELECT $1::text ~ '^[0-9A-F]{24}$'", name).Scan(&got)).To(Succeed())
				Expect(got).To(Equal(want))
				Expect(walFileName.MatchString(name)).To(Equal(want))
			},
			Entry("valid WAL segment", "000000010000000000000001", true),
			Entry("partial WAL file", "000000010000000000000001.partial", false),
			Entry("backup label file", "000000010000000000000001.00000028.backup", false),
			Entry("timeline history file", "00000002.history", false),
			Entry("archive_status directory", "archive_status", false),
			Entry("lowercase hex (invalid)", "00000001000000000000000a", false),
			Entry("too short", "0000000100000000000000", false),
			Entry("too long (25 hex chars)", "0000000100000000000000011", false),
			Entry("empty string", "", false),
		)
	})

	Describe("collector_pg_wal_archive_status", func() {
		It("counts the ready and done files of the archive status directory", func(ctx SpecContext) {
			// The cleanups run in reverse: the server is started, then archive_mode is reset with a restart.
			DeferCleanup(func(ctx SpecContext) {
				Expect(server.Exec(ctx, "ALTER SYSTEM RESET archive_mode")).To(Succeed())
				Expect(server.Exec(ctx, "ALTER SYSTEM RESET archive_command")).To(Succeed())
				Expect(server.StopServer(ctx)).To(Succeed())
				Expect(server.StartServer(ctx)).To(Succeed())
			}, NodeTimeout(2*time.Minute))
			Expect(server.Exec(ctx, "ALTER SYSTEM SET archive_mode = 'on'")).To(Succeed())
			Expect(server.Exec(ctx, "ALTER SYSTEM SET archive_command = '/bin/true'")).To(Succeed())
			DeferCleanup(func(ctx SpecContext) { Expect(server.StartServer(ctx)).To(Succeed()) }, NodeTimeout(2*time.Minute))
			Expect(server.StopServer(ctx)).To(Succeed())
			Expect(server.StartServer(ctx)).To(Succeed())

			Expect(server.Exec(ctx, "CREATE TABLE IF NOT EXISTS public.archive_probe (a int); "+
				"INSERT INTO public.archive_probe SELECT generate_series(1, 1000); SELECT pg_switch_wal()")).To(Succeed())

			Eventually(func(g Gomega) {
				got := byLabel(runBuiltin(ctx, exec, queries.KeyArchiveStatus), "cnpg_collector_pg_wal_archive_status")
				g.Expect(got).To(HaveLen(2))
				g.Expect(got["done"]).To(BeNumerically(">=", 1))
				g.Expect(got["ready"]).To(Equal(0.0))
			}).WithContext(ctx).WithPolling(500 * time.Millisecond).Should(Succeed())
		}, SpecTimeout(3*time.Minute))
	})

	Describe("collector_sync_replicas", func() {
		It("reports 0 while synchronous_standby_names is empty", func(ctx SpecContext) {
			got := byLabel(runBuiltin(ctx, exec, queries.KeySyncObserved), "cnpg_collector_sync_replicas")
			Expect(got).To(Equal(map[string]float64{"observed": 0}))
		})
	})

	Describe("StatsResetSQL", func() {
		DescribeTable("renders stats_reset as CNPG's label",
			func(ctx SpecContext, zone string, ts any, want string) {
				conn, err := server.PlainConn(ctx, "postgres")
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func(ctx SpecContext) { Expect(conn.Close(ctx)).To(Succeed()) })
				_, err = conn.Exec(ctx, "SET TIME ZONE '"+zone+"'")
				Expect(err).NotTo(HaveOccurred())

				var got string
				Expect(conn.QueryRow(ctx, "SELECT "+queries.StatsResetSQL+" AS stats_reset FROM (SELECT $1::timestamptz AS stats_reset) t", ts).
					Scan(&got)).To(Succeed())
				Expect(got).To(Equal(want))
			},
			Entry("no reset yet", "UTC", nil, "-infinity"),
			Entry("two fraction digits", "UTC", "2026-11-15 10:01:02.12+00", "2026-11-15T10:01:02.12Z"),
			Entry("whole seconds", "UTC", "2026-11-15 10:01:00+00", "2026-11-15T10:01:00Z"),
			Entry("six fraction digits", "UTC", "2026-11-15 10:01:02.123456+00", "2026-11-15T10:01:02.123456Z"),
			Entry("a session zone other than UTC", "Europe/Rome", "2026-11-15 11:01:02.12+01", "2026-11-15T10:01:02.12Z"),
			Entry("trailing zeros in the seconds", "UTC", "2026-11-15 10:01:10.5+00", "2026-11-15T10:01:10.5Z"),
		)
	})

	Describe("SyncObservedSQL", func() {
		DescribeTable("reads the requested number from synchronous_standby_names",
			func(ctx SpecContext, names string, want float64) {
				conn, err := server.PlainConn(ctx, "postgres")
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func(ctx SpecContext) { Expect(conn.Close(ctx)).To(Succeed()) })

				var got float64
				Expect(conn.QueryRow(ctx, "SELECT "+queries.SyncObservedSQL+" FROM (SELECT $1::text) AS t(s)", names).
					Scan(&got)).To(Succeed())
				Expect(got).To(Equal(want))
				if want < 0 {
					Expect(cnpgSyncNames.MatchString(names)).To(BeFalse())
				}
			},
			Entry("ANY", `ANY 2 ("a", "b")`, 2.0),
			Entry("FIRST", `FIRST 1 ("a")`, 1.0),
			Entry("a plain list", "a, b", -1.0),
			Entry("empty", "", 0.0),
			// Go's . excludes \n, so CNPG's regex (pg_collector.go:47) does not match and CNPG reports -1.
			Entry("a newline in a quoted name", "ANY 1 (\"a\nb\")", -1.0),
		)
	})

	Describe("PostgreSQL 18 golden", func() {
		// CNPG v1.30.1 probes.go:639-657, the PostgreSQL 18 branch of TryGetPgStatWAL.
		const cnpgPG18 = `SELECT wal_records, wal_fpi, wal_bytes, wal_buffers_full, COALESCE(stats_reset, '-infinity')
			FROM pg_catalog.pg_stat_wal`

		It("the wal_* metrics equal CNPG's values in one transaction", func(ctx SpecContext) {
			Expect(server.Exec(ctx, "SELECT pg_stat_reset_shared('wal')")).To(Succeed())
			Expect(server.Exec(ctx, "CREATE TABLE IF NOT EXISTS public.wal_probe (a int); "+
				"INSERT INTO public.wal_probe SELECT generate_series(1, 5000)")).To(Succeed())

			l := limits()
			pool := newPool(l)
			conn, release, err := pool.Acquire(ctx, "postgres")
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { release(false) })

			tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func(ctx SpecContext) { Expect(tx.Rollback(ctx)).To(Succeed()) })

			job := builtinJob(queries.KeyStatWAL)
			rows, err := tx.Query(ctx, job.Def.SQL, pg.ResultFormats)
			Expect(err).NotTo(HaveOccurred())
			var samples []queries.Sample
			for rows.Next() {
				fields := rows.FieldDescriptions()
				columns := make([]string, len(fields))
				for i, f := range fields {
					columns[i] = f.Name
				}
				values, err := pg.Values(conn.TypeMap(), fields, rows.RawValues())
				Expect(err).NotTo(HaveOccurred())
				got, dropped := job.Def.RowSamples(columns, values, serverVersion, 0)
				Expect(dropped).To(BeEmpty())
				samples = append(samples, got...)
			}
			rows.Close()
			Expect(rows.Err()).NotTo(HaveOccurred())

			var records, fpi, bytes, full int64
			var reset time.Time
			Expect(tx.QueryRow(ctx, cnpgPG18).Scan(&records, &fpi, &bytes, &full, &reset)).To(Succeed())
			want := map[string]float64{
				"cnpg_collector_wal_records":      float64(records),
				"cnpg_collector_wal_fpi":          float64(fpi),
				"cnpg_collector_wal_bytes":        float64(bytes),
				"cnpg_collector_wal_buffers_full": float64(full),
			}
			Expect(records).To(BeNumerically(">", 0))

			got := map[string]float64{}
			var golden []goldenMetric
			for _, s := range samples {
				got[s.Metric] = s.Value
				Expect(s.Labels).To(HaveLen(1))
				Expect(s.Labels[0].Key).To(Equal("stats_reset"))
				Expect(s.Labels[0].Value).To(Equal(reset.UTC().Format(time.RFC3339Nano)))
				golden = append(golden, goldenMetric{Name: s.Metric, Kind: s.Kind.String(), Labels: []string{s.Labels[0].Key}})
			}
			Expect(got).To(Equal(want))

			sort.Slice(golden, func(i, j int) bool { return golden[i].Name < golden[j].Name })
			encoded, err := json.MarshalIndent(golden, "", "  ")
			Expect(err).NotTo(HaveOccurred())
			encoded = append(encoded, '\n')
			if os.Getenv("UPDATE_GOLDEN") != "" {
				Expect(os.WriteFile("testdata/builtin-pg18.golden.json", encoded, 0o600)).To(Succeed())
			}
			want18, err := os.ReadFile("testdata/builtin-pg18.golden.json")
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(string(encoded))).To(Equal(strings.TrimSpace(string(want18))))
		}, SpecTimeout(time.Minute))

		It("does not run the PostgreSQL 14 to 17 query on 18", func() {
			def := builtinJob(queries.KeyStatWAL17).Def
			Expect(def.RunOnServer.Check(serverVersion)).To(BeFalse())
		})
	})
})

func mustQuery(ctx SpecContext, conn *pgx.Conn, sql string) pgx.Rows {
	GinkgoHelper()

	rows, err := conn.Query(ctx, sql)
	Expect(err).NotTo(HaveOccurred())

	return rows
}
