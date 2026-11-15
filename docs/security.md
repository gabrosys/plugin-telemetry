# Security

What the plugin writes in your cluster, which rights it holds, what the admission policies enforce, and what stays open.

## What the plugin writes

For each Cluster that enables the plugin, the plugin writes four objects in the Cluster's namespace:

| Object | Name |
|---|---|
| ServiceAccount | `<cluster>.telemetry` |
| Role | `<cluster>.telemetry` |
| RoleBinding | `<cluster>.telemetry` |
| Secret, type `kubernetes.io/service-account-token` | `<cluster>.telemetry-token` |

Each carries the labels `app.kubernetes.io/managed-by: plugin-telemetry` and `telemetry.gabrosys.github.io/cluster: <cluster>`, and an owner reference to the Cluster (`internal/identity/identity.go:55-56`, `internal/metadata/metadata.go:32-41`). Deleting the Cluster deletes them. The plugin also deletes them when the Cluster no longer enables it.

The Role grants `get`, `list` and `watch` on named objects only: the Cluster, the TelemetryExporter, the MetricQuerySets, and the ConfigMaps and Secrets the exporter and the Cluster refer to (`internal/identity/identity.go:82-105`, `internal/clusterconfig/refs.go:31-77`). A kind with no names gets no rule, so the Role never grants a whole kind.

How the plugin writes them:

- One controller writes them, in `internal/operator/controller.go`. The lifecycle hook and the status call write nothing to the API server.
- The controller reads through a cache limited to objects that carry the `managed-by` label (`internal/operator/manager.go:69-72`).
- An object that the cache lacks is created with a plain create. If the API server says it already exists, the object is not the plugin's and is left alone (`internal/identity/ensure.go:59-67`).
- An object that the cache holds and that differs from the wanted state is updated with a server-side apply pinned to the cached uid and resourceVersion. If the stored copy changed since the cache read, the apply fails with a conflict and nothing is overwritten (`internal/identity/ensure.go:72-80`, `internal/identity/ensure.go:201-258`).
- A token Secret of any type other than `kubernetes.io/service-account-token` is never written or deleted (`internal/identity/ensure.go:68-70`, `internal/identity/ensure.go:115-117`).

An object that is "in the way" (already there but not in the cache, a wrong-typed token, or a changed stored copy) is not written. The controller logs `identity controller` with `outcome` set to `in the way` once, retries every 5 s, and logs `in the way cleared` once when a pass succeeds (`internal/operator/controller.go:47-73`). The Cluster status shows `identity: pending` with one line in `problems` for each reason (`internal/operator/status.go:41-43`, `internal/identity/ensure.go:131-149`):

- `<Kind>/<name> is not in the plugin's cache: not created yet, or not the plugin's`
- `Secret/<name> has type <type>, not kubernetes.io/service-account-token: delete it`
- `Secret/<name> has no data.token yet`

Other facts:

- The plugin never writes the Cluster. It returns its status to CloudNativePG, which stores it (`internal/operator/status.go:70-123`). The ClusterRole grants only read on `clusters`.
- The sidecar's token volume is optional, so the Pod starts before the token exists. The sidecar waits for the token file, polls every 5 s and logs `waiting for the sidecar token` once (`internal/podpatch/podpatch.go:20-26`, `internal/kubesource/source.go:187-205`).
- The sidecar sees only its own token. An empty volume hides the Pod's default token from it (`internal/podpatch/podpatch.go:28-31`, `internal/podpatch/podpatch.go:50-51`).
- Release 0.4.0 named these objects `<cluster>-telemetry` (`git show v0.4.0:internal/metadata/metadata.go`, line 28). The plugin does not remove them. They have an owner reference to their Cluster, so they go when the Cluster goes. Their rights stay until then.

## The plugin's rights

The plugin runs as ServiceAccount `plugin-telemetry` in `cnpg-system`, bound to one ClusterRole (`config/rbac.yaml`):

| API group | Resources | Verbs |
|---|---|---|
| `postgresql.cnpg.io` | `clusters` | get, list, watch |
| `telemetry.gabrosys.github.io` | `telemetryexporters`, `metricquerysets` | get, list, watch |
| core | `configmaps` | get, list, watch |
| core | `secrets` | get, list, watch, create, patch, delete |
| core | `serviceaccounts` | get, list, watch, create, patch, delete |
| `rbac.authorization.k8s.io` | `roles`, `rolebindings` | get, list, watch, create, patch, delete |

The ClusterRole has no `serviceaccounts/token`, no `escalate`, no `bind` and no `update`. There is no Role or RoleBinding in `cnpg-system`. Write rights on Secrets, ServiceAccounts, Roles and RoleBindings reach every namespace, so the admission policies below narrow them.

Each sidecar runs with its own ServiceAccount. Its only rights are the read rules of its Role above.

## The admission policies

`config/admission-policy.yaml` defines three ValidatingAdmissionPolicies, each with a binding that denies. All three have `failurePolicy: Fail` and apply to every namespace.

Two of them match a request only when the user is the plugin (`system:serviceaccount:cnpg-system:plugin-telemetry`) or any ServiceAccount user that ends in `.telemetry` (`system:serviceaccount:*:*.telemetry`, match condition `plugin-or-identity-account`). So they bind the plugin and the sidecar accounts, and no other user.

`plugin-telemetry-identities` covers Secrets and ServiceAccounts (create, update, delete) and TokenRequest (create on `serviceaccounts/token`):

- A ServiceAccount must have a name that ends in `.telemetry`.
- A Secret must have type `kubernetes.io/service-account-token`, an annotation `kubernetes.io/service-account.name` that ends in `.telemetry`, and the name `<that account>-token`. On update, the old and the new annotation must be equal.
- A TokenRequest is always denied.
- Messages:
  - `only ServiceAccounts named *.telemetry, and kubernetes.io/service-account-token Secrets named <account>-token for them, may be written`
  - `ServiceAccount tokens may not be requested (TokenRequest)`

`plugin-telemetry-roles` covers Roles and RoleBindings (create, update, delete):

- The name must end in `.telemetry`, on delete too.
- A Role may grant only `get`, `list` and `watch`. A Role without rules passes.
- A RoleBinding must refer to the Role of its own name, and bind exactly one subject: the ServiceAccount of its own name in its own namespace.
- Messages:
  - `only Roles and RoleBindings named *.telemetry may be written or deleted`
  - `a Role may grant only get, list and watch`
  - `a RoleBinding may refer only to the Role of its own name`
  - `a RoleBinding may bind only the ServiceAccount of its own name, in its own namespace`

`plugin-telemetry-reserved-names` matches every user, and only the CREATE of a Pooler. It denies a name that ends in `.telemetry`, so a Pooler cannot be created with an identity name. Message: `Pooler names ending in .telemetry are reserved for the telemetry plugin's identity objects`.

A Cluster name is a DNS-1035 label (`internal/webhook/v1/cluster_webhook.go:1839-1846` in CloudNativePG v1.30.1) and `spec.serviceAccountName` matches `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$` (`api/v1/cluster_types.go:342` in v1.30.1). Neither can hold a dot, so a Cluster cannot be named, or run as an account named, like an identity object.

## Who may write a TelemetryExporter

The plugin grants each sidecar read access to every Secret and ConfigMap that the exporter names: the headers Secret, the TLS CA, the client certificate, and the `envFrom` ConfigMap and Secret (`internal/clusterconfig/refs.go:50-76`). The sidecar sends the headers to the endpoint that the exporter sets. Whoever may create or change a TelemetryExporter can therefore make the sidecar read those Secrets in the exporter's namespace and send the headers to an endpoint they choose. Grant that right only to those who may read those Secrets.

## Values that other outputs show

Where a value of `spec.exporter`, `envFrom` or the password Secret appears:

- The endpoint is shown in `print-config`, in the Cluster status message and in errors, with user info, query and fragment replaced by `xxxxx` (`internal/settings/settings.go:281-303`, `internal/settings/settings.go:243`, `internal/cli/instance_tools.go:396`, `internal/export/exporter.go:99`). An endpoint with user info, a query or a fragment is rejected (`internal/settings/settings.go:270-278`, `internal/clusterconfig/otlpenv.go:150-168`), so put credentials in the headers.
- The endpoint's host and port are attributes `server.address` and `server.port` on the sidecar's export self-metrics, which go to the same endpoint (`internal/export/exporter.go:104`, `internal/export/loop.go:139-140`, `internal/selfmetrics/stats.go:253-254`, `docs/self-metrics.md`).
- `OTEL_RESOURCE_ATTRIBUTES` from the `envFrom` ConfigMap or Secret, `spec.resourceAttributes` and the allowed Cluster labels are resource attributes on every export (`internal/clusterconfig/clusterconfig.go:232-264`, `internal/export/resource.go:19-21`). They also show in `print-config`.
- The `username` key of the password Secret is `postgres.user` in `print-config` (`internal/clusterconfig/clusterconfig.go:121`).
- Protocol, compression and timeout from `envFrom` show in `print-config` as `exporter.*` settings (`internal/clusterconfig/otlpenv.go:95-140`).

Never shown: header values (`print-config` lists the header keys with `<redacted>`, `internal/cli/instance_tools.go:404-412`), the password (only the path of its file shows), and key material (only the file paths show). A header in the `envFrom` ConfigMap is an error, because headers are credentials and belong in the Secret (`internal/clusterconfig/otlpenv.go:66-70`). So put credentials in the headers Secret or the password Secret, not in the endpoint, a ConfigMap or the resource attributes.

## What stays open

- A compromised plugin process can read every ConfigMap and Secret in the cluster, because its ClusterRole has `get`, `list` and `watch` on both without a name limit (`config/rbac.yaml`). The policies limit what it may write, not what it may read.
- The policies bind the plugin and `*.telemetry` accounts only. Any other user who may create ServiceAccounts or Roles can create a ServiceAccount named `<something>.telemetry`, and can give it other rights. The `*.telemetry` policies then limit that account on Secrets, ServiceAccounts, Roles and RoleBindings only. Such an account also stays in the way of a Cluster of that name.
- The CloudNativePG instance manager serves an API on `localhost:8010` without authentication (`pkg/management/url/url.go:31` in v1.30.1). The Pod's containers share one network namespace, so the sidecar can call it. This is tracked as SEC-12 in `openpoints.md`.
