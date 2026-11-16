package queries

// Keys of the built-in queries. Each one is named "collector", so its metrics are cnpg_collector_<column>.
const (
	KeyLOPages       = "collector"
	KeyPGWAL         = "collector_pg_wal"
	KeyArchiveStatus = "collector_pg_wal_archive_status"
	KeyStatWAL       = "collector_pg_stat_wal"
	KeyStatWAL17     = "collector_pg_stat_wal_17"
	KeySyncObserved  = "collector_sync_replicas"
)

// PGWALHelp is CNPG's help for pg_wal (pg_collector.go:196-201 at v1.30.1), shared with the Cluster-derived rows.
const PGWALHelp = "Total size in bytes of WAL segments in the '/var/lib/postgresql/data/pgdata/pg_wal' directory " +
	" computed as (wal_segment_size * count)"

// StatsResetSQL renders stats_reset as CNPG's label does: '-infinity' when NULL, else UTC RFC 3339
// with the trailing zeros of the fraction removed (Go's time.RFC3339Nano).
const StatsResetSQL = `CASE WHEN stats_reset IS NULL THEN '-infinity' ELSE ` +
	`pg_catalog.rtrim(pg_catalog.rtrim(pg_catalog.to_char(stats_reset AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US'), '0'), '.') || 'Z' END`

// SyncObservedSQL is the number in synchronous_standby_names (column s): 0 when empty, -1 when it
// does not match CNPG's regex (pg_collector.go:47, 604-618 at v1.30.1).
// [^\n] stands for Go's . (PostgreSQL's . matches a newline). substring(s, pattern) is the function form of substring(s FROM pattern), which cannot be schema-qualified.
const SyncObservedSQL = `CASE WHEN s = '' THEN 0 WHEN s ~ '(?:ANY|FIRST) ([0-9]+) \([^\n]*\)' ` +
	`THEN pg_catalog.substring(s, '(?:ANY|FIRST) ([0-9]+) \([^\n]*\)')::float8 ELSE -1 END`

const (
	pgWALSQL = `WITH s AS (SELECT max(setting::float8) FILTER (WHERE name = 'wal_segment_size') AS seg,
  max(setting::float8) FILTER (WHERE name = 'min_wal_size') AS min_mb, max(setting::float8) FILTER (WHERE name = 'max_wal_size') AS max_mb,
  max(setting::float8) FILTER (WHERE name = 'wal_keep_size') AS keep_mb, max(setting::float8) FILTER (WHERE name = 'max_slot_wal_keep_size') AS slots_mb
  FROM pg_catalog.pg_settings WHERE name IN ('wal_segment_size','min_wal_size','max_wal_size','wal_keep_size','max_slot_wal_keep_size')),
w AS (SELECT count(*)::float8 AS n FROM pg_catalog.pg_ls_waldir() WHERE name ~ '^[0-9A-F]{24}$')
SELECT v.value, v.pg_wal FROM s, w, LATERAL (VALUES ('count', w.n), ('size', w.n * s.seg),
  ('min', s.min_mb * 1048576 / s.seg), ('max', s.max_mb * 1048576 / s.seg), ('keep', s.keep_mb * 1048576 / s.seg),
  ('slots_max', CASE WHEN s.slots_mb = -1 THEN 'NaN'::float8 ELSE s.slots_mb * 1048576 / s.seg END)) AS v(value, pg_wal)`

	archiveStatusSQL = `SELECT v.value, v.pg_wal_archive_status FROM (SELECT count(*) FILTER (WHERE name LIKE '%.ready')::float8 AS ready,
  count(*) FILTER (WHERE name LIKE '%.done')::float8 AS done FROM pg_catalog.pg_ls_archive_statusdir()) c,
  LATERAL (VALUES ('ready', c.ready), ('done', c.done)) AS v(value, pg_wal_archive_status)`

	statWALSQL = `SELECT wal_records, wal_fpi, wal_bytes, wal_buffers_full, ` + StatsResetSQL + ` AS stats_reset FROM pg_catalog.pg_stat_wal`

	statWAL17SQL = `SELECT wal_write, wal_sync, wal_write_time, wal_sync_time, ` + StatsResetSQL + ` AS stats_reset FROM pg_catalog.pg_stat_wal`

	syncObservedQuery = `SELECT 'observed' AS value, ` + SyncObservedSQL + ` AS sync_replicas FROM pg_catalog.current_setting('synchronous_standby_names') AS s`
)

func gauge(description string) ColumnMapping {
	return ColumnMapping{Usage: UsageGauge, Description: description}
}

func label(description string) ColumnMapping {
	return ColumnMapping{Usage: UsageLabel, Description: description}
}

func statWALQuery(sql, runOnServer string, columns ...Mapping) UserQuery {
	return UserQuery{
		Name:        "collector",
		Query:       sql,
		RunOnServer: runOnServer,
		Metrics:     append(columns, Mapping{"stats_reset": label("Time at which these statistics were last reset")}),
	}
}

// Builtin returns the queries behind CNPG's hard-coded metrics that PostgreSQL computes. The names and
// descriptions are CNPG's (pg_collector.go:160-180, 217-272 at v1.30.1); the pg_wal count follows main (wal.go:147).
func Builtin() UserQueries {
	return UserQueries{
		KeyLOPages: UserQuery{
			Name: "collector",
			Query: "SELECT pg_catalog.current_database() as datname, relpages as lo_pages " +
				"FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace) " +
				"WHERE n.nspname = 'pg_catalog' AND c.relname = 'pg_largeobject';",
			TargetDatabases: []string{"*"},
			Metrics: []Mapping{
				{"datname": label("Name of the database")},
				{"lo_pages": gauge("Estimated number of pages in the pg_largeobject table")},
			},
		},
		KeyPGWAL: {
			Name:  "collector",
			Query: pgWALSQL,
			Metrics: []Mapping{
				{"value": label("Which WAL figure the row holds")},
				{"pg_wal": gauge(PGWALHelp)},
			},
		},
		KeyArchiveStatus: {
			Name:  "collector",
			Query: archiveStatusSQL,
			Metrics: []Mapping{
				{"value": label("ready or done")},
				{"pg_wal_archive_status": gauge("Number of WAL segments in the " +
					"'/var/lib/postgresql/data/pgdata/pg_wal/archive_status' directory (ready, done)")},
			},
		},
		KeyStatWAL: statWALQuery(statWALSQL, ">=14.0.0",
			Mapping{"wal_records": gauge("Total number of WAL records generated. Only available on PG 14+")},
			Mapping{"wal_fpi": gauge("Total number of WAL full page images generated. Only available on PG 14+")},
			Mapping{"wal_bytes": gauge("Total amount of WAL generated in bytes. Only available on PG 14+")},
			Mapping{"wal_buffers_full": gauge("Number of times WAL data was written to disk because WAL buffers became full. " +
				"Only available on PG 14+")},
		),
		KeyStatWAL17: statWALQuery(statWAL17SQL, ">=14.0.0 <18.0.0",
			Mapping{"wal_write": gauge("Number of times WAL buffers were written out to disk via XLogWrite request." +
				" Only available on PG 14 to 17.")},
			Mapping{"wal_sync": gauge("Number of times WAL files were synced to disk via issue_xlog_fsync request " +
				"(if fsync is on and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero)." +
				" Only available on PG 14 to 17.")},
			Mapping{"wal_write_time": gauge("Total amount of time spent writing WAL buffers to disk via XLogWrite request, in milliseconds " +
				"(if track_wal_io_timing is enabled, otherwise zero). This includes the sync time when wal_sync_method " +
				"is either open_datasync or open_sync. Only available on PG 14 to 17.")},
			Mapping{"wal_sync_time": gauge("Total amount of time spent syncing WAL files to disk via issue_xlog_fsync request, in milliseconds " +
				"(if track_wal_io_timing is enabled, fsync is on, and wal_sync_method is either fdatasync, fsync or " +
				"fsync_writethrough, otherwise zero). Only available on PG 14 to 17.")},
		),
		KeySyncObserved: {
			Name:    "collector",
			Query:   syncObservedQuery,
			Primary: true,
			Metrics: []Mapping{
				{"value": label("observed")},
				{"sync_replicas": gauge("Number of requested synchronous replicas (synchronous_standby_names)")},
			},
		},
	}
}
