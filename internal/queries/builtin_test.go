package queries_test

import (
	"bytes"
	"encoding/json"
	"sort"

	"github.com/Masterminds/semver/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/test/fixture"
)

// builtinHelp is the Help text of each metric in CNPG v1.30.1 pg_collector.go:148-153,160-166,173-180,217-272.
var builtinHelp = map[string]string{
	"cnpg_collector_pg_wal": "Total size in bytes of WAL segments in the '/var/lib/postgresql/data/pgdata/pg_wal' directory " +
		" computed as (wal_segment_size * count)",
	"cnpg_collector_pg_wal_archive_status": "Number of WAL segments in the " +
		"'/var/lib/postgresql/data/pgdata/pg_wal/archive_status' directory (ready, done)",
	"cnpg_collector_wal_records":      "Total number of WAL records generated. Only available on PG 14+",
	"cnpg_collector_wal_fpi":          "Total number of WAL full page images generated. Only available on PG 14+",
	"cnpg_collector_wal_bytes":        "Total amount of WAL generated in bytes. Only available on PG 14+",
	"cnpg_collector_wal_buffers_full": "Number of times WAL data was written to disk because WAL buffers became full. Only available on PG 14+",
	"cnpg_collector_wal_write": "Number of times WAL buffers were written out to disk via XLogWrite request." +
		" Only available on PG 14 to 17.",
	"cnpg_collector_wal_sync": "Number of times WAL files were synced to disk via issue_xlog_fsync request " +
		"(if fsync is on and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero)." +
		" Only available on PG 14 to 17.",
	"cnpg_collector_wal_write_time": "Total amount of time spent writing WAL buffers to disk via XLogWrite request, in milliseconds " +
		"(if track_wal_io_timing is enabled, otherwise zero). This includes the sync time when wal_sync_method " +
		"is either open_datasync or open_sync." +
		" Only available on PG 14 to 17.",
	"cnpg_collector_wal_sync_time": "Total amount of time spent syncing WAL files to disk via issue_xlog_fsync request, in milliseconds " +
		"(if track_wal_io_timing is enabled, fsync is on, and wal_sync_method is either fdatasync, fsync or " +
		"fsync_writethrough, otherwise zero). Only available on PG 14 to 17.",
	"cnpg_collector_sync_replicas": "Number of requested synchronous replicas (synchronous_standby_names)",
}

func defineBuiltin() map[string]queries.Definition {
	GinkgoHelper()

	defs := map[string]queries.Definition{}
	for key, q := range queries.Builtin() {
		defs[key] = queries.Define(key, q, queries.Options{MaxLabelsPerSeries: 32})
	}

	return defs
}

var _ = Describe("Builtin", func() {
	It("holds the six built-in queries, each named collector", func() {
		got := queries.Builtin()
		Expect(got).To(HaveLen(6))
		for _, key := range []string{
			queries.KeyLOPages, queries.KeyPGWAL, queries.KeyArchiveStatus,
			queries.KeyStatWAL, queries.KeyStatWAL17, queries.KeySyncObserved,
		} {
			Expect(got).To(HaveKey(key))
			Expect(got[key].Name).To(Equal("collector"), key)
		}
	})

	It("defines without warnings and equals the golden file", func() {
		defs := defineBuiltin()
		for key, def := range defs {
			Expect(def.Warnings).To(BeEmpty(), key)
			Expect(def.Disabled).To(BeEmpty(), key)
		}

		got := marshalSummary(defs)
		Expect(string(got)).To(Equal(string(fixture.Golden(GinkgoTB(), "testdata", "builtin.golden.json", got))))
	})

	It("names every metric cnpg_collector_<column> and carries CNPG's help", func() {
		var names []string
		for key, def := range defineBuiltin() {
			Expect(def.Namespace).To(Equal("cnpg_collector"), key)
			for column, c := range def.Columns {
				if c.Role != queries.RoleValue {
					continue
				}
				Expect(c.Metric).To(Equal("cnpg_collector_"+column), key)
				Expect(c.Kind).To(Equal(queries.KindGauge), key)
				names = append(names, c.Metric)
				if want, ok := builtinHelp[c.Metric]; ok {
					Expect(c.Description).To(Equal(want), c.Metric)
				}
			}
		}
		sort.Strings(names)
		Expect(names).To(Equal([]string{
			"cnpg_collector_lo_pages", "cnpg_collector_pg_wal", "cnpg_collector_pg_wal_archive_status",
			"cnpg_collector_sync_replicas", "cnpg_collector_wal_buffers_full", "cnpg_collector_wal_bytes",
			"cnpg_collector_wal_fpi", "cnpg_collector_wal_records", "cnpg_collector_wal_sync",
			"cnpg_collector_wal_sync_time", "cnpg_collector_wal_write", "cnpg_collector_wal_write_time",
		}))
		Expect(names).To(HaveLen(12))
		Expect(builtinHelp).To(HaveLen(11))
	})

	It("pins each built-in's SQL and descriptions in a golden file", func() {
		type pinned struct {
			SQL          string            `json:"sql"`
			Descriptions map[string]string `json:"descriptions"`
		}
		got := map[string]pinned{}
		for key, def := range defineBuiltin() {
			p := pinned{SQL: def.SQL, Descriptions: map[string]string{}}
			for name, c := range def.Columns {
				p.Descriptions[name] = c.Description
			}
			got[key] = p
		}

		var out bytes.Buffer
		enc := json.NewEncoder(&out)
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		Expect(enc.Encode(got)).To(Succeed())
		Expect(out.String()).To(Equal(string(fixture.Golden(GinkgoTB(), "testdata", "builtin-sql.golden.json", out.Bytes()))))
	})

	It("makes only collector_sync_replicas primary-only", func() {
		for key, def := range defineBuiltin() {
			Expect(def.PrimaryOnly).To(Equal(key == queries.KeySyncObserved), key)
		}
	})

	It("reads pg_settings only in collector_pg_wal", func() {
		for key, def := range defineBuiltin() {
			Expect(def.ReadsSettings).To(Equal(key == queries.KeyPGWAL), key)
		}
	})

	It("selects the pg_stat_wal query by server version", func() {
		defs := defineBuiltin()
		Expect(defs[queries.KeyStatWAL].RunOnServer).NotTo(BeNil())
		Expect(defs[queries.KeyStatWAL17].RunOnServer).NotTo(BeNil())

		for version, want := range map[string][2]bool{
			"13.0.9": {false, false}, "14.0.0": {true, true}, "17.0.5": {true, true}, "18.0.7": {true, false},
		} {
			v := semver.MustParse(version)
			Expect(defs[queries.KeyStatWAL].RunOnServer.Check(v)).To(Equal(want[0]), version)
			Expect(defs[queries.KeyStatWAL17].RunOnServer.Check(v)).To(Equal(want[1]), version)
		}
	})

	It("reads the default database, except the large object query", func() {
		for key, def := range defineBuiltin() {
			if key == queries.KeyLOPages {
				Expect(def.TargetDatabases).To(Equal([]string{"*"}))
				continue
			}
			Expect(def.TargetDatabases).To(BeEmpty(), key)
		}
	})
})
