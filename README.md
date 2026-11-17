# plugin-telemetry

> [!WARNING]
> This plugin was built with AI assistance. Its quality and production readiness are still under review.

A CloudNativePG (CNPG) plugin that pushes a Cluster's PostgreSQL metrics to an OpenTelemetry Collector over OTLP. CNPG exposes the same metrics on a Prometheus endpoint in each instance pod. This plugin delivers them by push, so a cluster can move to an OpenTelemetry pipeline. It reads the custom-query configuration you already keep for CNPG's exporter, so your metric definitions keep working. It also reproduces CNPG's built-in `cnpg_collector_*` metrics, which dashboards depend on.

## How it works

```
CNPG operator --> plugin --> sidecar in each instance pod --> OTLP --> your collector
```

- Queries use CNPG's custom-query format, from the Cluster's monitoring ConfigMaps and Secrets or from a `MetricQuerySet`.
- The `cnpg_collector_*` built-in metrics are reproduced.
- Each query or group of queries has its own collection interval, and exports run on a separate cadence.
- Configuration reloads while the pod runs. PostgreSQL does not restart.
- The sidecar connects to PostgreSQL with a read-only role and reads the Kubernetes API with its own narrow token.

## Compatibility

- CNPG v1.30.1 and `main`.
- Kubernetes 1.34 to 1.37. Tested on 1.37 in the e2e workflow, and on a 1.34 node for the `admission` suite and the `peer` container of the `plugin` suite.
- PostgreSQL 18.
- Connection modes `peer` and `password`.
- OpenShift through `password` mode. Untested.

## Opt in

A `TelemetryExporter` in the Cluster's namespace holds the collector's address. The Cluster names the plugin and maps the sidecar's OS user to the monitoring role in `pg_ident`. This example uses `peer` mode, the default. `password` mode needs different Cluster lines; see section 4 of the [runbook](docs/runbook.md#4-send-a-clusters-metrics).

```yaml
apiVersion: telemetry.gabrosys.github.io/v1alpha1
kind: TelemetryExporter
metadata:
  name: default
  namespace: app
spec:
  exporter:
    endpoint: http://collector.otel.svc:4318
---
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: pg
  namespace: app
spec:
  instances: 1
  storage:
    size: 1Gi
  postgresql:
    pg_ident:
      - cnpg_metrics_exporter nobody cnpg_metrics_exporter
  plugins:
    - name: telemetry.gabrosys.github.io
```

Peer mode needs the user `nobody` (UID 65534) in the operand image's `/etc/passwd`. The runbook lists the images checked for it.

## Documents

- [Runbook](docs/runbook.md): install, opt in, check, monitor, restrict the network, upgrade, remove.
- [CRD reference](docs/crd-reference.md)
- [Self-metrics](docs/self-metrics.md)
- [Security](docs/security.md)
- [Resources](docs/resources.md)

## Development

Run `make help` to list the targets.

- `build`: build `bin/plugin-telemetry` and compile every package.
- `test`: run unit tests with the race detector.
- `test-integration`: run the tests that need Docker (PostgreSQL in a container).
- `lint`: run linters and the vulnerability scan.
- `generate`: regenerate `docs/self-metrics.md`, `docs/crd-reference.md`, the deepcopy code and the CRDs.
- `dev-up`: create the kind environment (`CNPG_VERSION=v1.30.1|main`).
- `e2e-<suite>`: run an e2e suite on the kind environment (`admission`, `plugin`, `collector-metrics`, `resilience`). `E2E_FOCUS=<regex>` runs matching specs only.

## Releases

release-please opens a release PR with the next semantic version, derived from Conventional Commits. Merging it tags the release and pushes the image to a private ghcr.io package.

## License

Apache 2.0. See [LICENSE](LICENSE).
