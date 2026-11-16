# Self-metrics

## Engine

Metrics the engine reports about itself.

| Name | Instrument | Unit | Attributes | Description |
|---|---|---|---|---|
| `cnpg_otel_query_runs_total` | counter | `{run}` | `query`, `database`, `outcome` | Query runs by outcome (ok, error, timeout, skipped, capped, denied). |
| `cnpg_otel_query_last_duration_seconds` | gauge | `s` | `query`, `database` | Duration of the last run of a query. |
| `cnpg_otel_query_last_success_timestamp_seconds` | gauge | `s` | `query`, `database` | Unix time of the last run of a query that returned rows (outcome ok). |
| `cnpg_otel_query_interval_seconds` | gauge | `s` | `query` | Effective run interval of a query. |
| `cnpg_otel_query_capped_series` | gauge | `{series}` | `query`, `database` | Series a capped query would have produced in its last run. |
| `cnpg_otel_query_dropped_series_total` | counter | `{series}` | `query`, `database`, `reason` | Series dropped from a query, by reason. |
| `cnpg_otel_series` | gauge | `{series}` |  | Series currently held for export. |
| `cnpg_otel_config_loads_total` | counter | `{load}` | `source`, `outcome` | Configuration loads by source and outcome (ok, error, denied). |
| `cnpg_otel_config_duplicate_queries_total` | counter | `{query}` | `source` | Duplicate query names found while loading a source. |
| `cnpg_otel_config_info` | gauge |  | `generation`, `hash` | Always 1; the active configuration generation and hash are attributes. |
| `cnpg_otel_check` | gauge |  | `check` | 1 if a live check holds, 0 otherwise, from the last facts round (pg_monitor, read_only, track_io_timing, track_wal_io_timing, track_functions, superuser). read_only checks the engine's transactions, so it is 1 while they run read-only (transaction_read_only=on). superuser is 1 while the session role is a superuser; the sidecar then runs no query. |
| `cnpg_otel_exports_total` | counter | `{export}` | `outcome` | Export operations by outcome (ok, error, skipped). |
| `cnpg_otel_last_export_success_timestamp_seconds` | gauge | `s` |  | Unix time of the last successful export. |
| `otel.sdk.exporter.metric_data_point.exported` | counter | `{data_point}` | `error.type`, `otel.component.type`, `otel.component.name`, `server.address`, `server.port` | Data points an export operation sent; rejected points count as failed with error.type=rejected. error.type is set only on failure. |
| `otel.sdk.exporter.operation.duration` | histogram | `s` | `error.type`, `http.response.status_code`, `otel.component.type`, `otel.component.name`, `server.address`, `server.port` | Duration of an export operation after the last retry, in a single bucket. error.type is set only on failure, http.response.status_code only for HTTP. |
| `cnpg_collector_collections_total` | counter |  |  | Total number of times PostgreSQL was accessed for metrics. |
| `cnpg_collector_collection_errors_total` | counter |  | `collector` | Total errors occurred accessing PostgreSQL for metrics. |
| `cnpg_collector_last_collection_error` | gauge |  |  | 1 if the last collection ended with error, 0 otherwise. |
| `cnpg_collector_collection_duration_seconds` | gauge |  | `collector` | Collection time duration in seconds |

## Plugin

Metrics the plugin process reports about itself. They are exported only when `operator` gets `--metrics-endpoint`.

| Name | Instrument | Unit | Attributes | Description |
|---|---|---|---|---|
| `cnpg_otel_plugin_hook_calls_total` | counter | `{call}` | `service`, `operation`, `outcome` | Hook calls by service (lifecycle, operator), operation (create, evaluate, set_status) and outcome (patched, unchanged, skipped, error, noop, status). |
| `cnpg_otel_plugin_hook_duration_seconds` | histogram | `s` | `service`, `operation` | Duration of hook calls, in a single bucket. |
| `cnpg_otel_plugin_identity_reconciles_total` | counter | `{reconcile}` | `outcome` | Identity controller reconciles by outcome (ok, error). |
