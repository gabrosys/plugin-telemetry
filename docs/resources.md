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

The Go runtime's garbage collector makes the sidecar's working set move in a sawtooth of 1 to 2 MB around its level; the min, median and max cover it.

## Sidecar with CNPG's default queries

`make measure CNPG_VERSION=v1.30.1 E2E_FOCUS=sidecar` writes `bin/measure/sidecar-v1.30.1.json`.

Setup: one `TelemetryExporter` with `spec.metrics.interval: 30s` and nothing else; one peer Cluster with one instance and the default sidecar resources, so CNPG's default monitoring ConfigMap applies. The spec waits 5 minutes after the sidecar's first successful export, then reads usage every 15 s for 10 minutes (41 readings).

| Quantity | Min | Median | Max | Target | Met |
|---|---|---|---|---|---|
| Working set | 16355328 B (15.6 MiB) | 18006016 B (17.2 MiB) | 19169280 B (18.3 MiB) | <= 41943040 B | yes |
| `rssBytes` | 15917056 B | 17367040 B | 18444288 B | | |
| Mean CPU | | 838092 nanocores (0.8m) | | <= 10000000 | yes |

The sidecar exported 417 series, skipped no query run (`cnpg_otel_query_runs_total{outcome="skipped"}` delta 0) and did not restart.
