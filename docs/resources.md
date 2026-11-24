# Resource use

Measured on kind (Kubernetes 1.37, PostgreSQL 18.7) with the commands below; each writes its numbers to `bin/measure/<name>-<cnpg>.json`. The current results ran on the code this document ships with, except the per-row figures, which ran on v0.5.0; the conversion code they measure has changed only in comments since. The runs kept as history ran on earlier code; the Environment table lists them.

## Verdicts

| Quantity | Target | Measured | Verdict |
|---|---|---|---|
| Sidecar, CNPG's default queries, one database, every 30 s (CNPG v1.30.1): working set | max <= 40 MiB (41943040 B) | max 19214336 B (18.3 MiB) | met |
| The same: CPU | mean <= 10m | mean 725512 nanocores (0.7m) | met |
| Sidecar at `maxSeriesTotal` 50000: working set, restarts | max < the sidecar's memory limit, no OOM kill, no restart, no export error | At the 96Mi default (100663296 B): 48415 series, max reading 77017088 B (73.4 MiB, 22.6 MiB under the limit), 0 restarts, no OOM kill, 0 export errors. At 64Mi the sidecar was OOM-killed in both runs at 48417 series (max readings 60993536 B and 54104064 B before the kill) | met at 96Mi |
| Plugin replica with 1000 Clusters cached: settled working set | <= 64 MiB (67108864 B) | max 61435904 B (58.6 MiB) over both replicas | met |
| A replacement replica with 1000 Clusters | no OOM kill under its 128Mi limit, settles <= 64 MiB | Ready in 2.0 s, 0 restarts, no termination reason, max 60272640 B (57.5 MiB) | met |
| Open watches added by 10 sidecars | 60 +/- 5 (6 per sidecar: its 5 informers and the kubelet's watch on the token Secret volume) | 60 | met |
| Per-row conversion cost | measured, no target | 296 to 4756 B and 0.4 to 5.3 us per row; the histogram row was 5464 B and 5.6 us without the pooled `pgtype.Map` | see "Per-row conversion and batching" |
| Pipelined batches | needed only if `skippedRuns` > 0 | `skippedRuns` 0 | not needed |

## How the numbers are taken

`make measure CNPG_VERSION=<v> [E2E_FOCUS=<regex>]` runs the specs in `test/measure` against the kind environment. A spec reads the container's usage from the kubelet's stats summary of the Pod's node, through the API server (`/api/v1/nodes/<node>/proxy/stats/summary`), every 15 s.

- Working set (`workingSetBytes`) is what the kernel's OOM kill and the kubelet's evictions use, so the targets are judged on it.
- `rssBytes` is the kubelet's anonymous memory figure, not the process's VmRSS; it is reported beside the working set.
- CPU is `usageNanoCores`, the kubelet's rate over its last sampling window; the mean is over the readings.

A measurement writes its JSON before it asserts, so a missed target is on record with its numbers.

The Go runtime's garbage collector makes the sidecar's working set move between readings; the min, median and max cover it.

## Sidecar with CNPG's default queries

`make measure CNPG_VERSION=v1.30.1 E2E_FOCUS='default queries'` writes `bin/measure/sidecar-v1.30.1.json`.

Setup: one `TelemetryExporter` with `spec.metrics.interval: 30s` and nothing else; one peer Cluster with one instance and the default sidecar resources, so CNPG's default monitoring ConfigMap applies. The spec waits 5 minutes after the sidecar's first successful export, then reads usage every 15 s for 10 minutes (41 readings).

| Quantity | Min | Median | Max | Target | Met |
|---|---|---|---|---|---|
| Working set | 16216064 B (15.5 MiB) | 16900096 B (16.1 MiB) | 19214336 B (18.3 MiB) | <= 41943040 B | yes |
| `rssBytes` | 15581184 B | 16289792 B | 18571264 B | | |
| Mean CPU | | 725512 nanocores (0.7m) | | <= 10000000 | yes |

The sidecar exported 415 series, skipped no query run (`cnpg_otel_query_runs_total{outcome="skipped"}` delta 0) and did not restart. Min to max span 3.0 MB.

## Sidecar at maxSeriesTotal 50000

`make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=maxSeriesTotal` writes `bin/measure/series-v1.30.1.json`.

Setup: a `MetricQuerySet` `synthetic` of five queries, each returning 4800 rows with two labels, a counter and a gauge (9600 series each), loaded through `spec.metrics.querySets` on a `TelemetryExporter` with `spec.metrics.interval: 30s`; one peer Cluster with one instance and the default sidecar resources. With CNPG's default queries the sidecar held 48415 series, under the default `maxSeriesTotal` of 50000. The spec waits until `cnpg_otel_series` reaches 48000, 2 more minutes, then reads usage every 15 s for 10 minutes. The spec reads the limit from the default (`podpatch.ParseParams(nil)`), so it follows the default.

### Result at 64Mi: missed, twice

With a 64Mi limit, the kernel's OOM killer ended the sidecar (`lastState.terminated.reason: OOMKilled`) in both runs; the default is 96Mi. Both runs are kept as history: `bin/measure/series-v1.30.1-64Mi-first.json` (first run, without the pooled `pgtype.Map`, see [Allocation](#allocation)) and `bin/measure/series-v1.30.1-64Mi-rerun.json` (rerun, with it).

| Quantity | First run | Rerun |
|---|---|---|
| Working set, min / median / max | 43745280 / 49983488 / 60993536 B (33 readings before the kill) | 20074496 / 50008064 / 56897536 B (41 readings across the restart; 14 before the kill, max 54104064 B) |
| `rssBytes` min / median / max | 43036672 / 49201152 / 60026880 B | 19460096 / 49258496 / 55873536 B |
| Mean CPU | 31169253 nanocores (31m) | 30844199 nanocores (31m) |
| Series at the start and the end | 48417 and 48417 | 48417 and 48417 |
| Restarts, last termination reason | 0 before, 1 after, `OOMKilled` | 0 before, 1 after, `OOMKilled` |
| Export errors | 0 | 0 |

The kubelet reads every 15 s and the peak of a collection falls between readings, so the container was killed at 64Mi while no reading passed 60993536 B or 54104064 B. The pooled map does not touch this workload (the five queries have no histogram column), so the difference between the two runs is run-to-run noise.

The sidecar sets `GOMEMLIMIT` to 80% of its limit (`internal/memlimit/memlimit.go`): 53687091 B (53.7 MB) at 64Mi. The 64Mi readings therefore sit in a regime where the garbage collector is clamped to that soft limit, and their medians (about 50 MB) are held down by it. Estimates drawn from them (about 785 to 871 B per series over a base of 19 MB, a peak at least 1.10 and 1.24 times the highest reading) are loose, and the 96Mi run below replaces them with a measurement.

### Result at 96Mi: met

The default `sidecarMemory` is 96Mi (100663296 B), sized so that the default `maxSeriesTotal` of 50000 fits without being lowered. `GOMEMLIMIT` is then 80530636 B (80.5 MB). Run: `bin/measure/series-v1.30.1.json`, 787 s. Two earlier 96Mi runs read a highest 77631488 B (74.0 MiB) and 78082048 B (74.5 MiB).

| Quantity | Measured | Target |
|---|---|---|
| Working set, min / median / max (41 readings) | 58343424 B (55.6 MiB) / 73588736 B (70.2 MiB) / 77017088 B (73.4 MiB, 23646208 B or 22.6 MiB under the limit) | max < 100663296 B |
| `rssBytes` min / median / max | 57552896 / 72609792 / 76206080 B | |
| Mean CPU | 9536933 nanocores (9.5m) | |
| Series at the start and the end | 48415 and 48415 | 48000 to 50000 |
| Restarts, last termination reason | 0 before, 0 after, none | no restart, no `OOMKilled` |
| Export errors | 0 | 0 |

At 96Mi the working set is higher than at 64Mi (median 73.6 MB against 50.0 MB) because the collector runs later under the 80.5 MB soft limit; CPU is lower (9.5m against 31m) for the same reason. The highest reading is 73.4 MiB, so the 50000 default stays inside 96Mi with 22.6 MiB above the highest reading.

## Plugin replica with 1000 Clusters

`make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=plugin` writes `bin/measure/plugin-v1.30.1.json`.

Method: one real peer Cluster `measure-ref` with 2 instances, brought to Ready. Then 100 namespaces, each with a `TelemetryExporter/default` and 10 Clusters `pg-00` to `pg-09`. Each carries `measure-ref`'s spec, the annotation `cnpg.io/reconciliationLoop: disabled` (so CloudNativePG creates no Pods for it) and `measure-ref`'s status with the name replaced, written through the status subresource. The test client runs at QPS 50, burst 100, 16 creates at a time. Everything is deleted afterwards.

The measured replica is the deployed one, with the same binary, manifest (request 64Mi, limit 128Mi) and manager options. Its cache decodes the 1000 Clusters into the pinned CloudNativePG types, and the identity controller gives each its ServiceAccount, Role, RoleBinding and token Secret, so the cache holds 1000 Clusters and 4000 identity objects as in production. The 1000 Clusters make no hook calls, so there is no per-call allocation from them; hook traffic comes from real Clusters, whose allocations are per call and short-lived.

`exportersToClusters` lists a namespace's Clusters once, with `client.UnsafeDisableDeepCopy`.

Objects: 100 namespaces, 1000 unreconciled Clusters (plus `measure-ref`), 1000 token Secrets with a token, no Pod in the 100 namespaces. The mean JSON size of one Cluster is 9495 B (`clusterBytes`). Creating the objects took 38 s; the 1000 token Secrets were ready 159 s after the first create (149 s and 201 s in earlier runs; the 201 s run shared the host with the watches measurement).

Idle replicas, 5 minutes after the identities were ready, then 41 readings over 10 minutes:

| Replica | Working set min | median | max | `rssBytes` max | Target | Met |
|---|---|---|---|---|---|---|
| first | 54177792 B (51.7 MiB) | 55791616 B (53.2 MiB) | 61435904 B (58.6 MiB) | 60321792 B | <= 67108864 B | yes |
| second | 55271424 B (52.7 MiB) | 55885824 B (53.3 MiB) | 58286080 B (55.6 MiB) | 57176064 B | <= 67108864 B | yes |

Replacement replica: one replica deleted with the 1000 Clusters cached. The new Pod was Ready 2.0 s after the delete, with 0 restarts and no last termination reason (no OOM kill under the 128Mi limit). Readings from 2 minutes after Ready, every 15 s for 5 minutes (21 readings):

| Quantity | Min | Median | Max | Target | Met |
|---|---|---|---|---|---|
| Working set | 46829568 B (44.7 MiB) | 47177728 B (45.0 MiB) | 60272640 B (57.5 MiB) | <= 67108864 B | yes |
| `rssBytes` | 45723648 B | 45809664 B | 59166720 B | | |

The headroom to 64 MiB is 5.4 MiB (5672960 B) on the highest idle reading.

## API load of the sidecars' watches

`make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=watches` writes `bin/measure/watches-v1.30.1.json`. One CNPG version: the sidecar's watches don't depend on it.

Method: a Secret `measure-queries` (one `SELECT 1` query), a `MetricQuerySet` `measure-set` and a `TelemetryExporter` with `querySets: [measure-set]`; 10 Clusters `w-00` to `w-09` with one instance and `customQueriesSecret` `measure-queries`, first without the plugin. The spec reads the API server's `/metrics` at the start and the end of a 30 minute window (baseline), adds the plugin to every Cluster, waits until each sidecar exports, and reads a second 30 minute window. Per window it sums `apiserver_longrunning_requests{verb="WATCH"}` and takes the growth of `apiserver_request_total` for `verb="LIST"` and `verb="WATCH"`. The sidecar load is the plugin window minus the baseline window.

Each sidecar adds 6 watches: its five informers, each a watch on one object by name (the Cluster, the exporter, `cnpg-default-monitoring`, the Secret and the set), and the kubelet's watch on the Pod's token Secret volume (below). A client-go reflector's watch timeout is random between 5 and 10 minutes, so 60 watches renew between 60 x 30 / 10 = 180 and 60 x 30 / 5 = 360 times in 30 minutes, 240 at the 7.5 minute midpoint. An initial sync may be a WATCH instead of a LIST (`WatchListClient`).

Result: the open-watch increase was 60 (6 per sidecar), as expected (60 +/- 5).

| Quantity | Baseline window | Plugin window | Sidecar load |
|---|---|---|---|
| Open WATCH (start of window) | 285 | 345 | +60 (expected 60 +/- 5) |
| LIST requests | 10 | 10 | 0 |
| WATCH requests | 1070 | 1259 | +189 (expected renewals 180 to 360) |

Per sidecar per hour: about 38 WATCH requests (189 x 2 / 10) and no measurable LIST requests; the initial syncs are WATCH requests. The measured 189 sits inside the expected 180 to 360 renewals. These figures, and the extrapolation below, cover all 60 added watches, including the kubelet's token-Secret watch: up to 1 in 6 of the requests is the kubelet's.

Linear extrapolation (every watch is its own informer):

| Instances | WATCH per hour | LIST per hour |
|---|---|---|
| 10 | 378 | none measurable |
| 100 | 3780 | none measurable |
| 1000 | 37800 | none measurable |

That is about 10.5 WATCH requests per second at 1000 instances.

The sixth watch per sidecar is the kubelet's. A single-Cluster check (the same setup, one Cluster, the plugin removed and added again) read `apiserver_longrunning_requests{verb="WATCH"}` per resource: `clusters` +1, `telemetryexporters` +1, `metricquerysets` +1, `configmaps` +1 (`cnpg-default-monitoring`) and `secrets` +2. The sidecar opens one Secret watch (the queries Secret; `internal/kubesource/store.go` `resources`). The other is the kubelet's watch on the Pod's token Secret volume (`internal/podpatch/podpatch.go` `Apply`). It is not an informer of the sidecar, but it is part of the measured load and goes away with the Pod.

## Per-row conversion and batching

`go test -run '^$' -bench '^BenchmarkRowSamples$' -benchmem -count 5 ./internal/queries/` (24 s) writes `bin/measure/bench.txt`. One operation converts one row with `(*Definition).RowSamples`; cases `pg_settings` and `pg_stat_database` use CNPG v1.30.1's default queries, `synthetic` has the shape of the 50000-series run (two labels, a counter, a gauge) and `histogram` that of `process_idle_seconds` (one label, 11 buckets). Median of 5 runs without the pooled map, kept as history:

| Case | ns/op | B/op | allocs/op | samples/op | B per sample |
|---|---|---|---|---|---|
| `pg_settings` | 407.8 | 296 | 9 | 1 | 296 |
| `pg_stat_database` | 2591 | 4528 | 19 | 15 | 302 |
| `synthetic` | 698.5 | 664 | 14 | 2 | 332 |
| `histogram` | 5629 | 5464 | 118 | 1 | 5464 |

### Allocation

The sidecar missed its memory target at 64Mi, so the case with the largest B/op per sample picks the site. That is `histogram` (5464 B), where every array scan built a `pgtype.Map` (`pgtype.NewMap()`). `scanArray` takes the map from a `sync.Pool` (`internal/queries/histogram.go`). With the pooled map, and no other change, the histogram case was 5124 ns/op, 4540 B/op and 102 allocs/op (`bin/measure/bench-after.txt`).

At v0.5.0 (median of 5 runs):

| Case | ns/op | B/op | allocs/op | samples/op | B per sample |
|---|---|---|---|---|---|
| `pg_settings` | 399.6 | 296 | 9 | 1 | 296 |
| `pg_stat_database` | 2599 | 4528 | 19 | 15 | 302 |
| `synthetic` | 697.5 | 664 | 14 | 2 | 332 |
| `histogram` | 5349 | 4756 | 109 | 1 | 4756 |

A histogram row now costs 216 B and 7 allocations more than with the pooled map alone; there is no target on it.

The other cases allocate about 300 B per sample, which is the sample and its labels. The pooled map cannot lower the 50000-series working set (see above); it lowers the garbage of histogram queries only.

### Batching

Pipelined batches are not needed. `skippedRuns` is 0 in `sidecar-v1.30.1.json` (`cnpg_otel_query_runs_total{outcome="skipped"}` delta over the 10 minute window), and the sidecar's mean CPU is 0.7m against the 10m target. At 0.7 us per synthetic row, converting the 24200 rows of a 48415-series collection takes about 17 ms.

## Pod shutdown

The sidecar is a native sidecar, so it stops only after `postgres` has stopped. PostgreSQL's smart shutdown waits for every open session, and CloudNativePG falls back to a fast shutdown only after `smartShutdownTimeout`, 180 s by default (CloudNativePG v1.30.1 `api/v1/cluster_funcs.go` `GetSmartShutdownTimeout`). A sidecar that keeps idle sessions open between runs holds the smart shutdown to the full 180 s (182.5 s measured from the smart request to the fast one), against 0.18 s for the same Cluster without the plugin (measured on kind with CloudNativePG v1.30.1 and PostgreSQL 18.7, one instance). Idle sessions also make PostgreSQL refuse `DROP DATABASE` without `FORCE`, `CREATE DATABASE ... TEMPLATE`, `ALTER DATABASE ... RENAME` and `ALTER DATABASE ... SET TABLESPACE` on the database they are in, with SQLSTATE 55006. CloudNativePG's `Database` resource drops without `FORCE` (CloudNativePG v1.30.1 `internal/management/controller/database_controller_sql.go` `dropDatabase`).

The sidecar holds no session between runs. Each query run and each facts round opens its own connection and closes it when it ends (`internal/pg/pool.go` `Pool`), as CloudNativePG's exporter does with `SetMaxIdleConns(0)` (CloudNativePG v1.30.1 `pkg/management/postgres/pool/pool.go`). A smart shutdown therefore waits only for the query runs and the facts round in flight. A query run has a deadline of the query timeout plus 2 s from when it has its connection (`internal/executor/executor.go` `RunTimeout`), 12 s with the default 10 s query timeout. A facts round has a deadline of the 5 s connect timeout plus two query runs (`internal/engine/engine.go` `FactsTimeout`), 29 s with the defaults. Once the shutdown has started, PostgreSQL refuses new sessions, so no new run or round joins the wait.

Cost of a connection per run:

- One connection per query run per database, and one per facts round. `spec.metrics.limits.maxConnections` caps how many are open at once.
- A new session took about 1.7 ms, measured on bare PostgreSQL 18.7 over a local socket with `trust` authentication (AMD Ryzen 7 5800X). On the same setup, a new session that runs 11 monitoring queries took 5.7 ms, against 1.5 ms on a session that was already open. Password mode adds a SCRAM exchange to each connection.
- Each release waits for the backend to exit, one round trip plus the backend's exit, and at most 2 s (`internal/pg/pool.go` `closeTimeout`) on a server that does not answer.
- With `log_connections` and `log_disconnections` on, each connection logs up to four lines on PostgreSQL 18 (connection received, authenticated and authorized, then the disconnection). The authenticated line appears only for authentication methods that log it, and PostgreSQL 17 and earlier log fewer lines per connection. Both settings are off by default and CloudNativePG v1.30.1 does not set them.

The spec `test/e2e/resilience/shutdown_test.go` deletes the Pod of a one-instance Cluster with the plugin and asserts that the `postgres` container stops within 60 s of the delete. In the current runs, on CloudNativePG v1.30.1 and main, the container stopped within 1 s of the delete (its finish time is recorded to the second).

## Environment

| Item | Value |
|---|---|
| CPU | AMD Ryzen 7 5800X 8-Core Processor, 16 threads |
| RAM | 31711536 kB (about 30.2 GiB) |
| Kernel (`uname -r`) | 6.14.0-37-generic |
| Docker server (`docker version --format '{{.Server.Version}}'`) | 29.2.1 |
| kind node image | `kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5` (Kubernetes 1.37.0), kind v0.33.0 |
| CloudNativePG, PostgreSQL | v1.30.1, 18.7 |
| Code | The code this document ships with, for every current result except the per-row figures (v0.5.0, see the top). Runs kept as history, on earlier code: the two 64Mi runs (without and with the pooled map), the two earlier 96Mi runs, the per-row figures without the pooled map and with it alone, the 149 s and 201 s token Secret waits of earlier plugin runs, and the 182.5 s shutdown with idle sessions |

## Reproducing

Each command needs the kind cluster of its CNPG version (`KIND_CLUSTER=plugin-telemetry-v1-30-1`). Durations are from the runs above and count the specs alone. The current measurements ran as a single `make measure CNPG_VERSION=v1.30.1`, spec after spec on one kind cluster, with no other test on the host.

| Measurement | Command | Duration |
|---|---|---|
| Sidecar with defaults | `make measure CNPG_VERSION=v1.30.1 E2E_FOCUS='default queries'` | 973 s |
| Sidecar at 50000 series | `make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=maxSeriesTotal` | 787 s (96Mi); earlier: 643 s and 775 s (64Mi), 778 s and 829 s (the earlier 96Mi runs) |
| Plugin with 1000 Clusters | `make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=plugin` | 1592 s |
| Watches | `make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=watches` | 3738 s |
| Per-row cost | `go test -run '^$' -bench '^BenchmarkRowSamples$' -benchmem -count 5 ./internal/queries/` | 24 s |
| Pod shutdown | `make e2e-resilience CNPG_VERSION=v1.30.1 E2E_FOCUS='smart shutdown'` | 34 s (26 s on main) |
