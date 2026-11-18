package queries_test

import (
	"testing"

	"go.yaml.in/yaml/v4"

	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/test/fixture"
)

// benchDefault defines one query of CNPG's v1.30.1 default monitoring ConfigMap.
func benchDefault(b *testing.B, key string) queries.Definition {
	b.Helper()

	data := fixture.Read(b, "testdata", "default-monitoring-v1.30.1.yaml")
	var cm struct {
		Data map[string]string `yaml:"data"`
	}
	if err := yaml.Unmarshal(data, &cm); err != nil {
		b.Fatalf("parse the ConfigMap: %v", err)
	}

	return benchDefine(b, key, cm.Data["queries"])
}

func benchDefine(b *testing.B, key, document string) queries.Definition {
	b.Helper()

	parsed, err := queries.Parse([]byte(document))
	if err != nil {
		b.Fatalf("parse the queries: %v", err)
	}
	q, ok := parsed[key]
	if !ok {
		b.Fatalf("query %q not found", key)
	}

	return queries.Define(key, q, queries.Options{MaxLabelsPerSeries: 32})
}

func BenchmarkRowSamples(b *testing.B) {
	synthetic := "synthetic:\n  query: SELECT 1\n  metrics:\n" +
		"    - schemaname: {usage: LABEL}\n    - relname: {usage: LABEL}\n" +
		"    - seq_scan: {usage: COUNTER}\n    - n_live_tup: {usage: GAUGE}\n"
	histogram := "process_idle_seconds:\n  query: SELECT 1\n  metrics:\n" +
		"    - application_name: {usage: LABEL}\n    - idle: {usage: HISTOGRAM}\n"

	cases := []struct {
		name    string
		def     queries.Definition
		columns []string
		values  []any
	}{
		{
			name:    "pg_settings",
			def:     benchDefault(b, "pg_settings"),
			columns: []string{"name", "setting"},
			values:  []any{"max_connections", "100"},
		},
		{
			name: "pg_stat_database",
			def:  benchDefault(b, "pg_stat_database"),
			columns: []string{
				"datname", "xact_commit", "xact_rollback", "blks_read", "blks_hit", "tup_returned", "tup_fetched",
				"tup_inserted", "tup_updated", "tup_deleted", "conflicts", "temp_files", "temp_bytes", "deadlocks",
				"blk_read_time", "blk_write_time",
			},
			values: []any{
				"app", int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7), int64(8), int64(9),
				int64(10), int64(11), int64(12), int64(13), 14.5, 15.5,
			},
		},
		{
			name:    "synthetic",
			def:     benchDefine(b, "synthetic", synthetic),
			columns: []string{"schemaname", "relname", "seq_scan", "n_live_tup"},
			values:  []any{"schema_7", "relation_00000042", 42.0, 84.0},
		},
		{
			name:    "histogram",
			def:     benchDefine(b, "process_idle_seconds", histogram),
			columns: []string{"application_name", "idle", "idle_bucket", "idle_sum", "idle_count"},
			values: []any{
				"psql", "{0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2.5,5,10}",
				"{1,2,3,4,5,6,7,8,9,10,11}", 12.5, int64(11),
			},
		},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()

			var samples int
			for b.Loop() {
				got, _ := c.def.RowSamples(c.columns, c.values, nil, 0)
				samples = len(got)
			}
			b.ReportMetric(float64(samples), "samples/op")
		})
	}
}
