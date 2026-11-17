# Resource use

Measured on kind (Kubernetes 1.37, PostgreSQL 18.7) with the commands below; each writes its numbers to `bin/measure/<name>-<cnpg>.json`.

## Verdicts

Each row gives the measured value; the sections below hold the method and the full numbers.

| Quantity | Target | Measured | Verdict |
|---|---|---|---|
| Sidecar, CNPG's default queries, one database, every 30 s (CNPG v1.30.1): working set | max <= 40 MiB (41943040 B) | max 19202048 B (18.3 MiB) | met |
| The same: CPU | mean <= 10m | mean 626924 nanocores (0.6m) | met |
| Sidecar at `maxSeriesTotal` 50000: working set, restarts | max < the sidecar's memory limit, no OOM kill, no restart, no export error | At the 96Mi default (100663296 B): 48417 series, max reading 77631488 B (74.0 MiB, 22.0 MiB under the limit), 0 restarts, no OOM kill, 0 export errors. The default was raised from 64Mi to 96Mi after two OOM kills at 64Mi (both with 48417 series; max readings 60993536 B and 54104064 B before the kill) | met at 96Mi |
| Plugin replica with 1000 Clusters cached: settled working set | <= 64 MiB (67108864 B) | max 60252160 B (57.5 MiB) over both replicas | met |
| A replacement replica with 1000 Clusters | no OOM kill under its 128Mi limit, settles <= 64 MiB | Ready in 12.1 s, 0 restarts, no termination reason, max 58773504 B (56.1 MiB) | met |
| Open watches added by 10 sidecars | 60 +/- 5 (6 per sidecar: its 5 informers and the kubelet's watch on the token Secret volume) | 60 | met |
| Per-row conversion cost | measured, no target | 296 to 4540 B and 0.4 to 5.1 us per row; the histogram row was 5464 B and 5.6 us before the change | see "Per-row conversion and batching" |
| Pipelined batches | needed only if `skippedRuns` > 0 | `skippedRuns` 0 | not needed |

## How the numbers are taken

`make measure CNPG_VERSION=<v> [E2E_FOCUS=<regex>]` runs the specs in `test/measure` against the kind environment. A spec reads the container's usage from the kubelet's stats summary of the Pod's node, through the API server (`/api/v1/nodes/<node>/proxy/stats/summary`), every 15 s.

- Working set (`workingSetBytes`) is what the kernel's OOM kill and the kubelet's evictions use, so the targets are judged on it.
- `rssBytes` is the kubelet's anonymous memory figure, not the process's VmRSS; it is reported beside the working set.
- CPU is `usageNanoCores`, the kubelet's rate over its last sampling window; the mean is over the readings.

A measurement writes its JSON before it asserts, so a missed target is on record with its numbers.

The Go runtime's garbage collector makes the sidecar's working set move between readings; the min, median and max cover it.

## Sidecar with CNPG's default queries

`make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=sidecar` writes `bin/measure/sidecar-v1.30.1.json`.

Setup: one `TelemetryExporter` with `spec.metrics.interval: 30s` and nothing else; one peer Cluster with one instance and the default sidecar resources, so CNPG's default monitoring ConfigMap applies. The spec waits 5 minutes after the sidecar's first successful export, then reads usage every 15 s for 10 minutes (41 readings).

| Quantity | Min | Median | Max | Target | Met |
|---|---|---|---|---|---|
| Working set | 17510400 B (16.7 MiB) | 18190336 B (17.3 MiB) | 19202048 B (18.3 MiB) | <= 41943040 B | yes |
| `rssBytes` | 16859136 B | 17539072 B | 18182144 B | | |
| Mean CPU | | 626924 nanocores (0.6m) | | <= 10000000 | yes |

The sidecar exported 417 series, skipped no query run (`cnpg_otel_query_runs_total{outcome="skipped"}` delta 0) and did not restart. Min to max span 1.7 MB.

## Sidecar at maxSeriesTotal 50000

`make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=maxSeriesTotal` writes `bin/measure/series-v1.30.1.json`.

Setup: a `MetricQuerySet` `synthetic` of five queries, each returning 4800 rows with two labels, a counter and a gauge (9600 series each), loaded through `spec.metrics.querySets` on a `TelemetryExporter` with `spec.metrics.interval: 30s`; one peer Cluster with one instance and the default sidecar resources. With CNPG's default queries the sidecar held 48417 series, under the default `maxSeriesTotal` of 50000. The spec waits until `cnpg_otel_series` reaches 48000, 2 more minutes, then reads usage every 15 s for 10 minutes. The spec reads the limit from the default (`podpatch.ParseParams(nil)`), so it follows the default.

### Result at 64Mi: missed, twice

The default sidecar memory was 64Mi. The kubelet killed the sidecar for exceeding it (`lastState.terminated.reason: OOMKilled`) in both runs. The numbers are kept as the history: `bin/measure/series-v1.30.1-64Mi-first.json` (first run, before the per-row change) and `bin/measure/series-v1.30.1-64Mi-rerun.json` (rerun, with it).

| Quantity | First run | Rerun |
|---|---|---|
| Working set, min / median / max | 43745280 / 49983488 / 60993536 B (33 readings before the kill) | 20074496 / 50008064 / 56897536 B (41 readings across the restart; 14 before the kill, max 54104064 B) |
| `rssBytes` min / median / max | 43036672 / 49201152 / 60026880 B | 19460096 / 49258496 / 55873536 B |
| Mean CPU | 31169253 nanocores (31m) | 30844199 nanocores (31m) |
| Series at the start and the end | 48417 and 48417 | 48417 and 48417 |
| Restarts, last termination reason | 0 before, 1 after, `OOMKilled` | 0 before, 1 after, `OOMKilled` |
| Export errors | 0 | 0 |

The kubelet reads every 15 s and the peak of a collection falls between readings, so the container was killed at 64Mi while no reading passed 60993536 B or 54104064 B. The per-row change does not touch this workload (the five queries have no histogram column), so the difference between the two runs is run-to-run noise.

The sidecar sets `GOMEMLIMIT` to 80% of its limit (`internal/memlimit/memlimit.go`): 53687091 B (53.7 MB) at 64Mi. The 64Mi readings therefore sit in a regime where the garbage collector is clamped to that soft limit, and their medians (about 50 MB) are held down by it. Estimates drawn from them (about 785 to 871 B per series over a base of 19 MB, a peak at least 1.10 and 1.24 times the highest reading) are loose, and the 96Mi run below replaces them with a measurement.

### Result at 96Mi: met

After the two kills the default `sidecarMemory` was raised from 64Mi to 96Mi (100663296 B) instead of lowering the default `maxSeriesTotal`. `GOMEMLIMIT` is then 80530636 B (80.5 MB). Run: `bin/measure/series-v1.30.1.json`, 778 s, with the plugin image carrying the new default (the sidecar's init container showed a 96Mi limit).

| Quantity | Measured | Target |
|---|---|---|
| Working set, min / median / max (41 readings) | 59637760 B (56.9 MiB) / 70942720 B (67.7 MiB) / 77631488 B (74.0 MiB) | max < 100663296 B (22.0 MiB, 23031808 B, under the limit) |
| `rssBytes` min / median / max | 58871808 / 70144000 / 76861440 B | |
| Mean CPU | 11554838 nanocores (11.6m) | |
| Series at the start and the end | 48417 and 48417 | 48000 to 50000 |
| Restarts, last termination reason | 0 before, 0 after, none | no restart, no `OOMKilled` |
| Export errors | 0 | 0 |

At 96Mi the working set is higher than at 64Mi (median 70.9 MB against 50.0 MB) because the collector runs later under the 80.5 MB soft limit; CPU is lower (11.6m against 31m) for the same reason. The highest reading is 74.0 MiB, so the 50000 default stays inside 96Mi with 22.0 MiB above the highest reading.

## Plugin replica with 1000 Clusters

`make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=plugin` writes `bin/measure/plugin-v1.30.1.json`.

Method: one real peer Cluster `measure-ref` with 2 instances, brought to Ready. Then 100 namespaces, each with a `TelemetryExporter/default` and 10 Clusters `pg-00` to `pg-09`. Each carries `measure-ref`'s spec, the annotation `cnpg.io/reconciliationLoop: disabled` (so CloudNativePG creates no Pods for it) and `measure-ref`'s status with the name replaced, written through the status subresource. The test client runs at QPS 50, burst 100, 16 creates at a time. Everything is deleted afterwards.

Why it stands for a real replica: the replica is the deployed one, with the same binary, manifest (request 64Mi, limit 128Mi) and manager options. Its cache decodes the 1000 Clusters into the pinned CloudNativePG types, and the identity controller gives each its ServiceAccount, Role, RoleBinding and token Secret, so the cache holds 1000 Clusters and 4000 identity objects as in production. The 1000 Clusters make no hook calls, so there is no per-call allocation from them; hook traffic comes from real Clusters, whose allocations are per call and short-lived.

The measurement was taken with the single Cluster List of `exportersToClusters` using `client.UnsafeDisableDeepCopy`.

Objects: 100 namespaces, 1000 unreconciled Clusters (plus `measure-ref`), 1000 token Secrets with a token, no Pod in the 100 namespaces. The mean JSON size of one Cluster is 9504 B (`clusterBytes`). Creating the objects took 38 s; the 1000 token Secrets were ready 149 s after the first create.

Idle replicas, 5 minutes after the identities were ready, then 41 readings over 10 minutes:

| Replica | Working set min | median | max | `rssBytes` max | Target | Met |
|---|---|---|---|---|---|---|
| first | 51855360 B (49.5 MiB) | 56115200 B (53.5 MiB) | 58347520 B (55.6 MiB) | 57192448 B | <= 67108864 B | yes |
| second | 53018624 B (50.6 MiB) | 56360960 B (53.7 MiB) | 60252160 B (57.5 MiB) | 59113472 B | <= 67108864 B | yes |

Replacement replica: one replica deleted with the 1000 Clusters cached. The new Pod was Ready 12.1 s after the delete, with 0 restarts and no last termination reason (no OOM kill under the 128Mi limit). Readings from 2 minutes after Ready, every 15 s for 5 minutes (21 readings):

| Quantity | Min | Median | Max | Target | Met |
|---|---|---|---|---|---|
| Working set | 44109824 B (42.1 MiB) | 47292416 B (45.1 MiB) | 58773504 B (56.1 MiB) | <= 67108864 B | yes |
| `rssBytes` | 43020288 B | 46202880 B | 57683968 B | | |

Both targets are met with the change in place. The headroom to 64 MiB is 6.5 MiB (6856704 B) on the highest idle reading.

## API load of the sidecars' watches

`make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=watches` writes `bin/measure/watches-v1.30.1.json`. One CNPG version: the sidecar's watches don't depend on it.

Method: a Secret `measure-queries` (one `SELECT 1` query), a `MetricQuerySet` `measure-set` and a `TelemetryExporter` with `querySets: [measure-set]`; 10 Clusters `w-00` to `w-09` with one instance and `customQueriesSecret` `measure-queries`, first without the plugin. The spec reads the API server's `/metrics` at the start and the end of a 30 minute window (baseline), adds the plugin to every Cluster, waits until each sidecar exports, and reads a second 30 minute window. Per window it sums `apiserver_longrunning_requests{verb="WATCH"}` and takes the growth of `apiserver_request_total` for `verb="LIST"` and `verb="WATCH"`. The sidecar load is the plugin window minus the baseline window.

Each sidecar adds 6 watches: its five informers, each a watch on one object by name (the Cluster, the exporter, `cnpg-default-monitoring`, the Secret and the set), and the kubelet's watch on the Pod's token Secret volume (below). A client-go reflector's watch timeout is random between 5 and 10 minutes, so 60 watches renew between 60 x 30 / 10 = 180 and 60 x 30 / 5 = 360 times in 30 minutes, 240 at the 7.5 minute midpoint. An initial sync may be a WATCH instead of a LIST (`WatchListClient`).

Result: the open-watch increase was 60 (6 per sidecar), as expected (60 +/- 5).

| Quantity | Baseline window | Plugin window | Sidecar load |
|---|---|---|---|
| Open WATCH (start of window) | 287 | 347 | +60 (expected 60 +/- 5) |
| LIST requests | 11 | 10 | -1 (noise) |
| WATCH requests | 1075 | 1264 | +189 (expected renewals 180 to 360) |

Per sidecar per hour: about 38 WATCH requests (189 x 2 / 10) and no measurable LIST requests; the initial syncs are WATCH requests. The measured 189 sits inside the expected 180 to 360 renewals. These figures, and the extrapolation below, cover all 60 added watches, including the kubelet's token-Secret watch: up to 1 in 6 of the requests is the kubelet's.

Linear extrapolation (every watch is its own informer):

| Instances | WATCH per hour | LIST per hour |
|---|---|---|
| 10 | 378 | none measurable |
| 100 | 3780 | none measurable |
| 1000 | 37800 | none measurable |

That is about 10.5 WATCH requests per second at 1000 instances.

The sixth watch per sidecar is the kubelet's. A single-Cluster check (the same setup, one Cluster, the plugin removed and added again) read `apiserver_longrunning_requests{verb="WATCH"}` per resource: `clusters` +1, `telemetryexporters` +1, `metricquerysets` +1, `configmaps` +1 (`cnpg-default-monitoring`) and `secrets` +2. The sidecar opens one Secret watch (the queries Secret; `internal/kubesource/store.go:31`). The other is the kubelet's watch on the Pod's token Secret volume (`internal/podpatch/podpatch.go:23`): the kubelet's `configMapAndSecretChangeDetectionStrategy` defaults to `Watch` (Kubernetes v1.37.0 `pkg/kubelet/apis/config/v1beta1/defaults.go:268-269`). It is not an informer of the sidecar, but it is part of the measured load and goes away with the Pod.

## Per-row conversion and batching

`go test -run '^$' -bench '^BenchmarkRowSamples$' -benchmem -count 5 ./internal/queries/` (24 s) writes `bin/measure/bench.txt`. One operation converts one row with `(*Definition).RowSamples`; cases `pg_settings` and `pg_stat_database` use CNPG v1.30.1's default queries, `synthetic` has the shape of the 50000-series run (two labels, a counter, a gauge) and `histogram` that of `process_idle_seconds` (one label, 11 buckets). Median of 5 runs, before the change:

| Case | ns/op | B/op | allocs/op | samples/op | B per sample |
|---|---|---|---|---|---|
| `pg_settings` | 407.8 | 296 | 9 | 1 | 296 |
| `pg_stat_database` | 2591 | 4528 | 19 | 15 | 302 |
| `synthetic` | 698.5 | 664 | 14 | 2 | 332 |
| `histogram` | 5629 | 5464 | 118 | 1 | 5464 |

Decision 1, allocation: the sidecar's memory target at 64Mi was missed, so the case with the largest B/op per sample picks the site. That is `histogram` (5464 B), where every array scan built a `pgtype.Map` (`pgtype.NewMap()`). `scanArray` now takes the map from a `sync.Pool` (`internal/queries/histogram.go`). After the change the histogram case is 5124 ns/op, 4540 B/op and 102 allocs/op (`bin/measure/bench-after.txt`). Nothing else changed. The other cases allocate about 300 B per sample, which is the sample and its labels; `dropped` and the sorted column names were not touched. The change cannot lower the 50000-series working set (see above); it lowers the garbage of histogram queries only.

Decision 2, batching: pipelined batches are not needed. `skippedRuns` is 0 in `sidecar-v1.30.1.json` (`cnpg_otel_query_runs_total{outcome="skipped"}` delta over the 10 minute window), and the sidecar's mean CPU is 0.6m against the 10m target. At 0.7 us per synthetic row, converting the 24200 rows of a 48417-series collection takes about 17 ms.

## Pod shutdown

The sidecar is a native sidecar, so it stops only after `postgres` has stopped. PostgreSQL's smart shutdown waits for every open session, and CloudNativePG falls back to a fast shutdown only after `smartShutdownTimeout`, 180 s by default (CloudNativePG v1.30.1 `api/v1/cluster_funcs.go` `GetSmartShutdownTimeout`). A sidecar that kept its idle sessions open held that wait to the full timeout: 182.5 s from the smart request to the fast one, against 0.18 s for the same Cluster without the plugin (measured on kind with CloudNativePG v1.30.1 and PostgreSQL 18.7, one instance).

The sidecar keeps its sessions between rounds and probes PostgreSQL with one short connection every 10 s (`internal/engine/engine.go` `shutdownProbe`, `internal/pg/pool.go` `Pool.Probe`):

- On SQLSTATE 57P03 (`cannot_connect_now`), the pool closes its idle sessions, and closes each other session when its run ends. PostgreSQL answers 57P03 while it shuts down, starts up or recovers.
- A successful dial that began after the refusal ends the drain.
- Any other refusal leaves the sessions alone. Monitoring continues while PostgreSQL is at `max_connections`.

The hold is therefore about 10 s plus the query in flight, whatever `planInterval` and the query intervals are.

Cost of the probe:

- One short connection every 10 s per instance, outside `spec.postgres.maxConnections`. The monitoring role's connection limit must be at least `maxConnections` + 1.
- The probe is 8,640 connections a day (86,400 s / 10 s). With `log_connections` and `log_disconnections` on, each one logs up to four lines on PostgreSQL 18 (connection received, authenticated and authorized, then the disconnection), so up to 34,560 lines a day per instance. The authenticated line appears only for authentication methods that log it, and PostgreSQL 17 and earlier log fewer lines per connection. Both settings are off by default and CloudNativePG v1.30.1 does not set them.
- While the role's connection limit is reached, PostgreSQL logs one FATAL line every 10 s.

The spec `test/e2e/resilience/shutdown_test.go` deletes the Pod of a one-instance Cluster with the plugin and asserts that the `postgres` container stops within 60 s of the delete. Measured with the probe at ca518db, five runs: 9.54 s, 12.81 s and 3.02 s on CNPG v1.30.1, 11.60 s and 10.03 s on `main`. In every run the PostgreSQL log shows the smart shutdown request, then only refused new sessions, then the postmaster exit, with no "terminating connection due to administrator command" line, so the fast shutdown never had to end a sidecar session. The earlier design, which closed connections by age, measured 24.84 s and 25.15 s at the M4.6 gate.

## Environment

| Item | Value |
|---|---|
| CPU | AMD Ryzen 7 5800X 8-Core Processor, 16 threads |
| RAM | 31711536 kB (about 30.2 GiB) |
| Kernel (`uname -r`) | 6.14.0-37-generic |
| Docker server (`docker version --format '{{.Server.Version}}'`) | 29.2.1 |
| kind node image | `kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5` (Kubernetes 1.37.0), kind v0.33.0 |
| CloudNativePG, PostgreSQL | v1.30.1, 18.7 |
| Commit | a1ee3f7 for the 96Mi run. The earlier runs used earlier commits of this branch; the product changes since the first run are fb3579b (the plugin lists Clusters without deep copies, in place for the plugin run), 9add9b1 (the per-row change, in place for the 64Mi rerun) and a1ee3f7 (the 96Mi default). The watches JSON's expectation fields (50 open watches, 200 renewals) predate the correction to 60 and 240; its measured values are unaffected |

## Reproducing

Each command needs the kind cluster of its CNPG version (`KIND_CLUSTER=plugin-telemetry-v1-30-1`). Durations are from the runs above.

| Measurement | Command | Duration |
|---|---|---|
| Sidecar with defaults | `make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=sidecar` | 925 s |
| Sidecar at 50000 series | `make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=maxSeriesTotal` | 643 s (64Mi first run), 775 s (64Mi rerun), 778 s (96Mi) |
| Plugin with 1000 Clusters | `make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=plugin` | 1734 s |
| Watches | `make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=watches` | 4150 s |
| Per-row cost | `go test -run '^$' -bench '^BenchmarkRowSamples$' -benchmem -count 5 ./internal/queries/` | 24 s |
| Pod shutdown | `make e2e-resilience CNPG_VERSION=v1.30.1 E2E_FOCUS='smart shutdown'` | 67 to 111 s |
