# Resource use

Measured on kind (Kubernetes 1.37, PostgreSQL 18.7) with the commands below; each writes its numbers to `bin/measure/<name>-<cnpg>.json`.

## Verdicts

Each row names the measured value and the section that holds the method. Commit 360b671 is the tree of the sidecar, plugin and watch runs; the series rerun was taken with the per-row change (see "Per-row conversion and batching").

| Quantity | Target | Measured | Verdict |
|---|---|---|---|
| Sidecar, CNPG's default queries, one database, every 30 s (CNPG v1.30.1): working set | max <= 40 MiB (41943040 B) | max 19202048 B (18.3 MiB) | met |
| The same: CPU | mean <= 10m | mean 626924 nanocores (0.6m) | met |
| Sidecar at `maxSeriesTotal` 50000: working set, restarts | max < 64 MiB (67108864 B), no OOM kill, no restart, no export error | 48417 series; max reading 56897536 B (54.3 MiB), `OOMKilled` once, restart count 0 to 1, 0 export errors | missed, pending the user's decision (proposal in its section) |
| Plugin replica with 1000 Clusters cached: settled working set | <= 64 MiB (67108864 B) | max 60252160 B (57.5 MiB) over both replicas | met |
| A replacement replica with 1000 Clusters | no OOM kill under its 128Mi limit, settles <= 64 MiB | Ready in 12.1 s, 0 restarts, no termination reason, max 58773504 B (56.1 MiB) | met |
| Open watches added by 10 sidecars | 60 +/- 5 (6 per sidecar: its 5 informers and the kubelet's watch on the token Secret volume) | 60 | met |
| Per-row conversion cost (ENG-07) | measured, no target | 296 to 5464 B and 0.4 to 5.6 us per row | see "Per-row conversion and batching" |
| Pipelined batches (design 6.3) | needed only if `skippedRuns` > 0 | `skippedRuns` 0 | not needed |

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

Setup: a `MetricQuerySet` `synthetic` of five queries, each returning 4800 rows with two labels, a counter and a gauge (9600 series each), loaded through `spec.metrics.querySets` on a `TelemetryExporter` with `spec.metrics.interval: 30s`; one peer Cluster with one instance and the default sidecar resources (64Mi limit). With CNPG's default queries the sidecar held 48417 series, under the default `maxSeriesTotal` of 50000. The spec waits until `cnpg_otel_series` reaches 48000, 2 more minutes, then reads usage every 15 s for 10 minutes.

Result: **target missed, again, with the per-row change in place.** The kubelet killed the sidecar for exceeding its 64Mi limit (`lastState.terminated.reason: OOMKilled`) once during the window; the readings continue across the restart (41 readings, the 15th is the restarted container at 20074496 B). No export error was recorded, and the series count held at 48417.

| Quantity | First run (before the change) | Rerun (after the change) | Target |
|---|---|---|---|
| Working set, min / median / max | 43745280 / 49983488 / 60993536 B (33 readings before the kill) | 20074496 / 50008064 / 56897536 B (41 readings; 14 before the kill, max 54104064 B) | max < 67108864 B |
| `rssBytes` min / median / max | 43036672 / 49201152 / 60026880 B | 19460096 / 49258496 / 55873536 B | |
| Mean CPU | 31169253 nanocores (31m) | 30844199 nanocores (31m) | |
| Series at the start and the end | 48417 and 48417 | 48417 and 48417 | 48000 to 50000 |
| Restarts, last termination reason | 0 before, 1 after, `OOMKilled` | 0 before, 1 after, `OOMKilled` | none |
| Export errors | 0 | 0 | 0 |

The kubelet reads every 15 s and the peak of a collection falls between readings, so the container was killed at 64Mi while no reading passed 60993536 B (first run) or 54104064 B (rerun); the peak is at least 1.10 and 1.24 times the highest reading. The change to the histogram path does not touch this workload (the five queries have no histogram column), so the rerun's difference from the first run is run-to-run noise, not an effect of the change. The working set is made of the series' own state (the 48417 series held by the executor and the exporter), not of per-row garbage.

Per-series memory, from the two runs' highest working-set readings: 417 series at 19202048 B and 48417 series at 56897536 B (rerun) or 60993536 B (first run) give 785 to 871 B per series over a base of about 19 MB. This is an estimate from two points.

Options for the user:

1. Raise the default `sidecarMemory` to 96Mi (100663296 B). At 50000 series the estimate is a highest reading of about 62.8 MB, and with the 1.24 peak factor about 78 MB (74 MiB), which is 22 MiB under 96Mi. Not measured.
2. Lower the default `maxSeriesTotal`. Holding `(19.2 MB + N x 871 B) x 1.24 < 67108864 B` gives N below about 40000. The peak factor is a lower bound, so the real bound is lower: 36000 leaves margin. Estimate only: a run at 36000 and 40000 series would confirm it.

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

Decision 1, allocation: the sidecar's memory target (4.5.13) was missed, so the case with the largest B/op per sample picks the site. That is `histogram` (5464 B), where every array scan built a `pgtype.Map` (`pgtype.NewMap()`). `scanArray` now takes the map from a `sync.Pool` (`internal/queries/histogram.go`). After the change the histogram case is 5124 ns/op, 4540 B/op and 102 allocs/op (`bin/measure/bench-after.txt`). Nothing else changed. The other cases allocate about 300 B per sample, which is the sample and its labels; `dropped` and the sorted column names were not touched. The change cannot lower the 50000-series working set (see above); it lowers the garbage of histogram queries only.

Decision 2, batching: pipelined batches are not needed. `skippedRuns` is 0 in `sidecar-v1.30.1.json` (`cnpg_otel_query_runs_total{outcome="skipped"}` delta over the 10 minute window), and the sidecar's mean CPU is 0.6m against the 10m target. At 0.7 us per synthetic row, converting the 24200 rows of a 48417-series collection takes about 17 ms.

## Environment

| Item | Value |
|---|---|
| CPU | AMD Ryzen 7 5800X 8-Core Processor, 16 threads |
| RAM | 31711536 kB (about 30.2 GiB) |
| Kernel (`uname -r`) | 6.14.0-37-generic |
| Docker server (`docker version --format '{{.Server.Version}}'`) | 29.2.1 |
| kind node image | `kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5` (Kubernetes 1.37.0), kind v0.33.0 |
| CloudNativePG, PostgreSQL | v1.30.1, 18.7 |
| Commit | 360b671 for the sidecar, plugin and watch runs; the series rerun and the benchmark with the per-row change on top of it |

## Reproducing

Each command needs the kind cluster of its CNPG version (`KIND_CLUSTER=plugin-telemetry-v1-30-1`). Durations are from the runs above.

| Measurement | Command | Duration |
|---|---|---|
| Sidecar with defaults | `make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=sidecar` | 925 s |
| Sidecar at 50000 series | `make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=maxSeriesTotal` | 643 s (first run), 775 s (rerun) |
| Plugin with 1000 Clusters | `make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=plugin` | 1734 s |
| Watches | `make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=watches` | 4150 s |
| Per-row cost | `go test -run '^$' -bench '^BenchmarkRowSamples$' -benchmem -count 5 ./internal/queries/` | 24 s |
