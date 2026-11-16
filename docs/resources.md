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
