# Resource use

Measured on kind (Kubernetes 1.37, PostgreSQL 18.7) with the commands below; each writes its numbers to `bin/measure/<name>-<cnpg>.json`.

## Targets

| Quantity | Target |
|---|---|
| Sidecar, CNPG's default queries, one database, every 30 s (CNPG v1.30.1) | max working set <= 40 MiB (41943040 B), mean CPU <= 10m |
| Sidecar at `maxSeriesTotal` 50000 | max working set < 64 MiB, no OOM kill, no restart, no export error |
| Plugin replica with 1000 Clusters cached | settled working set <= 64 MiB (67108864 B); a replacement replica starts with no OOM kill under its 128Mi limit |

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

Result: **target missed.** The kubelet killed the sidecar for exceeding its 64Mi limit (`lastState.terminated.reason: OOMKilled`, exit code 137) during the window, which ended the readings after 33 of 41. The first run's sidecar was OOM killed once; the second run's had been killed twice when its Pod was deleted. No export error was recorded.

| Quantity | Min | Median | Max | Target | Met |
|---|---|---|---|---|---|
| Working set (33 readings before the kill) | 43745280 B (41.7 MiB) | 49983488 B (47.7 MiB) | 60993536 B (58.2 MiB) | < 67108864 B | no (OOM killed) |
| `rssBytes` | 43036672 B | 49201152 B | 60026880 B | | |
| Mean CPU | | 31169253 nanocores (31m) | | | |
| Series | 48417 at the start | | 48417 at the end | 48000 to 50000 | yes |
| Restarts | 0 before | | 1 after | none | no |
| Last termination reason | | | `OOMKilled` | not `OOMKilled` | no |
| Export errors (`cnpg_otel_exports_total{outcome="error"}` delta) | | | 0 | 0 | yes |

The highest working set read, 58.2 MiB, was under the limit while the container was killed: the kubelet reads every 15 s and the memory peak of a collection falls between readings. The working set moved over 17 MB between its min and max.

Options for the user (decision in Task 4.5.16):

1. Raise the default `sidecarMemory` to 96Mi.
2. Lower the default `maxSeriesTotal` to the largest count measured within 64Mi. No count below 48417 was measured; this needs a further run at a lower count (for example 24000 and 36000 series).

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

Each sidecar runs five informers, each a watch on one object by name: the Cluster, the exporter, `cnpg-default-monitoring`, the Secret and the set. A client-go reflector's watch timeout is random between 5 and 10 minutes, so 50 watches renew about 50 x 30 / 7.5 = 200 times in 30 minutes. An initial sync may be a WATCH instead of a LIST (`WatchListClient`).

Result: the open-watch increase was 60 (6 per sidecar). The brief's expected 50 (5 per sidecar) missed it; the sixth watch is the kubelet's (below), so the corrected expectation is 60 +/- 5, which the measurement meets.

| Quantity | Baseline window | Plugin window | Sidecar load |
|---|---|---|---|
| Open WATCH (start of window) | 287 | 347 | +60 (expected 60 +/- 5) |
| LIST requests | 11 | 10 | -1 (noise) |
| WATCH requests | 1075 | 1264 | +189 (expected renewals about 200) |

Per sidecar per hour: about 38 WATCH requests (189 x 2 / 10) and no measurable LIST requests; the initial syncs are WATCH requests. The measured renewals (189) match the 200 expected.

Linear extrapolation (every watch is its own informer):

| Instances | WATCH per hour | LIST per hour |
|---|---|---|
| 10 | 378 | none measurable |
| 100 | 3780 | none measurable |
| 1000 | 37800 | none measurable |

That is about 10.5 WATCH requests per second at 1000 instances.

The sixth watch per sidecar is the kubelet's. A single-Cluster check (the same setup, one Cluster, the plugin removed and added again) read `apiserver_longrunning_requests{verb="WATCH"}` per resource: `clusters` +1, `telemetryexporters` +1, `metricquerysets` +1, `configmaps` +1 (`cnpg-default-monitoring`) and `secrets` +2. The sidecar opens one Secret watch (the queries Secret; `internal/kubesource/store.go:31`). The other is the kubelet's watch on the Pod's token Secret volume (`internal/podpatch/podpatch.go:23`): the kubelet's `configMapAndSecretChangeDetectionStrategy` defaults to `Watch` (Kubernetes v1.37.0 `pkg/kubelet/apis/config/v1beta1/defaults.go:268-269`). It is not the sidecar's load, and it goes away with the Pod.
