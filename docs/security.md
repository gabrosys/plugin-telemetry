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

Each carries the labels `app.kubernetes.io/managed-by: plugin-telemetry` and `telemetry.gabrosys.github.io/cluster: <cluster>`, and an owner reference to the Cluster (`internal/identity/identity.go` `Desired`, `internal/metadata/metadata.go` `ServiceAccountName`, `TokenSecretName`). Deleting the Cluster deletes them. The plugin also deletes them when the Cluster no longer enables it.

The Role grants `get`, `list` and `watch` on named objects only: the Cluster, the TelemetryExporter, the MetricQuerySets, and the ConfigMaps and Secrets the exporter and the Cluster refer to (`internal/identity/identity.go` `rules`, `internal/clusterconfig/refs.go` `References`). A kind with no names gets no rule, so the Role never grants a whole kind.

How the plugin writes them:

- One controller writes them, in `internal/operator/controller.go`. The lifecycle hook and the status call write nothing to the API server.
- For the four identity kinds (ServiceAccount, Role, RoleBinding, Secret), the controller reads through a cache limited to objects that carry the `managed-by` label (`internal/operator/manager.go` `managerOptions`). Other Secrets are not cached, so a Secret without the label is invisible to it.
- An object that the cache lacks is created with a plain create. If the API server says it already exists, the object is not the plugin's and is left alone (`internal/identity/ensure.go` `Ensurer.Ensure`).
- An object that the cache holds and that differs from the wanted state is updated with a server-side apply pinned to the cached uid and resourceVersion. If the stored copy changed since the cache read, the apply fails with a conflict and nothing is overwritten (`Ensurer.Ensure`, `steps`).
- A token Secret of any type other than `kubernetes.io/service-account-token` (an empty type too) is never written or deleted (`Ensurer.Ensure`, `Ensurer.Remove`, `wrongType`). A cached token Secret that names another account, or a cached RoleBinding that refers to another role, is never written either (`inTheWay`).

An object that is "in the way" (already there but not in the cache, a wrong-typed token, a token for another account, a RoleBinding that refers to another role, or a changed stored copy) is not written. The controller logs `identity controller` with `outcome` set to `in the way` once, retries every 5 s, and logs `in the way cleared` once when a pass succeeds (`internal/operator/controller.go` `IdentityController.Reconcile`, `requeueInTheWay`). The Cluster status shows `identity: pending` with one line in `problems` for each reason (`internal/operator/status.go` `ClusterStatus`, `internal/identity/ensure.go` `Ensurer.Pending`):

- `<Kind>/<name> is not in the plugin's cache: not created yet, or not the plugin's`
- `Secret/<name> has type <type>, not kubernetes.io/service-account-token: delete it`
- `Secret/<name> names ServiceAccount "<account>", not "<cluster>.telemetry": delete it`
- `RoleBinding/<name> refers to <kind>/<name>, not Role/<cluster>.telemetry: delete it`
- `Secret/<name> has no data.token yet`

Other facts:

- The plugin never writes the Cluster. It returns its status to CloudNativePG, which stores it (`internal/operator/status.go` `Status.SetStatusInCluster`). The ClusterRole grants only read on `clusters`.
- The sidecar's token volume is optional, so the Pod starts before the token exists. The sidecar waits for the token file, polls every 5 s and logs `waiting for the sidecar token` once (`internal/podpatch/podpatch.go` `Apply`, `internal/kubesource/source.go` `Source.waitForToken`).
- The sidecar sees only its own token. An empty volume hides the Pod's default token from it (`internal/podpatch/podpatch.go` `Apply`, `sidecar`).
- Release 0.4.0 named these objects `<cluster>-telemetry` (`internal/metadata/metadata.go` `identitySuffix` at tag v0.4.0). The plugin does not remove them. They have an owner reference to their Cluster, so they go when the Cluster goes. Their rights stay until then. To remove them earlier, run `kubectl --context <context> --namespace <namespace> delete serviceaccount/<cluster>-telemetry role/<cluster>-telemetry rolebinding/<cluster>-telemetry secret/<cluster>-telemetry-token`.

## The plugin's rights

The plugin runs as ServiceAccount `plugin-telemetry` in the namespace of the CloudNativePG operator, `cnpg-system` by default: CloudNativePG finds plugins only there (CloudNativePG v1.30.1 `internal/controller/plugin_predicates.go` `isPluginService`). It is bound to one ClusterRole (`config/rbac.yaml`):

| API group | Resources | Verbs |
|---|---|---|
| `postgresql.cnpg.io` | `clusters` | get, list, watch |
| `telemetry.gabrosys.github.io` | `telemetryexporters`, `metricquerysets` | get, list, watch |
| core | `configmaps` | get, list, watch |
| core | `secrets` | get, list, watch, create, patch, delete |
| core | `serviceaccounts` | get, list, watch, create, patch, delete |
| `rbac.authorization.k8s.io` | `roles`, `rolebindings` | get, list, watch, create, patch, delete |

The ClusterRole has no `serviceaccounts/token`, no `escalate`, no `bind` and no `update`. There is no Role or RoleBinding in `cnpg-system`. Write rights on Secrets, ServiceAccounts, Roles and RoleBindings reach every namespace, so the admission policies below narrow them.

Each sidecar runs with its own ServiceAccount. Its own rights are the read rules of its Role above. Its token also carries the groups `system:serviceaccounts`, `system:serviceaccounts:<namespace>` and `system:authenticated`, so every binding to those groups applies to the sidecar too.

## The admission policies

`config/admission-policy.yaml` defines three ValidatingAdmissionPolicies (ValidatingAdmissionPolicy v1), each with a binding that denies. All three have `failurePolicy: Fail` and bind every namespace. They ship in the default manifest (`config/kustomization.yaml`) and need Kubernetes 1.34 or later, the project's floor. Where they are not installed or not enforced, the ClusterRole table above is the only limit on the plugin.

On a first install, the default render lists the policies and their bindings after the Deployment, so the API server may not enforce them yet when the plugin Pods start. The Pods wait for the cert-manager Secrets of `config/certificates.yaml`, which the render lists last, and the plugin then syncs its caches before it writes, so the gap is seconds. To close it, apply the policies first: `kubectl --context <context> apply -f config/admission-policy.yaml`, then the rest.

Two of them match a request only when the user is a ServiceAccount whose name is `plugin-telemetry` or ends in `.telemetry`, in any namespace (`system:serviceaccount:*:plugin-telemetry` and `system:serviceaccount:*:*.telemetry`, match condition `plugin-or-identity-account`). No namespace is named, so the policies follow the plugin when CloudNativePG runs outside `cnpg-system`. They bind the plugin, the sidecar accounts and any other ServiceAccount with one of these names, and no other user.

`plugin-telemetry-identities` covers Secrets and ServiceAccounts (create, update, delete) and TokenRequest (create on `serviceaccounts/token`):

- A ServiceAccount must have a name that ends in `.telemetry`.
- A Secret must have type `kubernetes.io/service-account-token`, an annotation `kubernetes.io/service-account.name` that ends in `.telemetry`, and the name `<that account>-token`. On update, the old and the new annotation must be equal.
- A TokenRequest is always denied.
- Messages:
  - `only ServiceAccounts named *.telemetry, and kubernetes.io/service-account-token Secrets named <account>-token for them, may be written`
  - `ServiceAccount tokens may not be requested (TokenRequest)`

`plugin-telemetry-roles` covers Roles and RoleBindings (create, update, delete):

- The name must end in `.telemetry`, on delete too.
- A Role may grant only `get`, `list` and `watch`, and every rule must carry `resourceNames`, so a Role never grants a whole kind. A rule may not name a subresource (`pods/exec`) or `*` as a resource. A Role without rules passes.
- A RoleBinding must refer to the Role of its own name, and bind exactly one subject: the ServiceAccount of its own name in its own namespace.
- Messages:
  - `only Roles and RoleBindings named *.telemetry may be written or deleted`
  - `a Role may grant only get, list and watch`
  - `every rule of a Role must name the objects it covers (resourceNames)`
  - `a Role may not name subresources, or * for every resource`
  - `a RoleBinding may refer only to the Role of its own name`
  - `a RoleBinding may bind only the ServiceAccount of its own name, in its own namespace`

`plugin-telemetry-reserved-names` matches every user, and only the CREATE of a Pooler. It denies a name that ends in `.telemetry`, so a Pooler cannot be created with an identity name. Message: `Pooler names ending in .telemetry are reserved for the telemetry plugin's identity objects`.

A Cluster name is a DNS-1035 label (CloudNativePG v1.30.1 `internal/webhook/v1/cluster_webhook.go` `validateName`) and `spec.serviceAccountName` matches `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$` (v1.30.1 `api/v1/cluster_types.go`, `ClusterSpec.ServiceAccountName`). Neither can hold a dot, so a Cluster cannot be named, or run as an account named, like an identity object.

## Who chooses the Secrets the sidecar reads

The plugin grants each sidecar read access to every Secret and ConfigMap that the exporter names: the headers Secret, the TLS CA, the client certificate, and the `envFrom` ConfigMap and Secret (`internal/clusterconfig/refs.go` `References`). The sidecar sends the headers to the endpoint that the exporter sets. Whoever may create or change a TelemetryExporter can therefore make the sidecar read those Secrets in the exporter's namespace and send the headers to an endpoint they choose. Grant that right only to those who may read those Secrets.

Whoever may write a Cluster chooses Secrets as well. The plugin parameter `passwordSecret` and `spec.monitoring.customQueriesSecret` also go into the Role, so the sidecar may read them (`internal/clusterconfig/refs.go` `References`). The sidecar reads the first as the Postgres password and the second as query definitions. Grant the right to write Clusters only to those who may read those Secrets.

## Values that other outputs show

Where a value of `spec.exporter`, `envFrom` or the password Secret appears:

- `print-config` shows the endpoint as written (`internal/cli/instance_tools.go` `describe`); that is safe because `Validate` refuses user info, a query and a fragment (`internal/settings/settings.go` `CheckEndpoint`, `internal/clusterconfig/otlpenv.go` `otlpEnv.endpoint`), and `print-config` prints nothing when it fails. `RedactEndpoint` still replaces user info, query and fragment with `xxxxx` in the error messages that quote a rejected endpoint: `Settings.Validate` (the `invalid settings` error of `internal/clusterconfig/clusterconfig.go`, which reaches the Cluster status message) and `internal/export/exporter.go` `NewExporter`. Credentials belong in the headers or the password, never in the endpoint or its path.
- The endpoint's host and port are attributes `server.address` and `server.port` on the sidecar's export self-metrics, which go to the same endpoint (`internal/export/exporter.go` `NewExporter`, `internal/selfmetrics/stats.go` `exportAttrs`, `docs/self-metrics.md`).
- `OTEL_RESOURCE_ATTRIBUTES` from the `envFrom` ConfigMap or Secret, `spec.resourceAttributes` and the allowed Cluster labels are resource attributes on every export (`internal/clusterconfig/clusterconfig.go` `resourceAttributes`, `internal/export/resource.go` `Resource`). They also show in `print-config`.
- The `username` key of the password Secret is `postgres.user` in `print-config` (`internal/clusterconfig/clusterconfig.go` `applyAuth`).
- Protocol, compression and timeout from `envFrom` show in `print-config` as `exporter.*` settings (`internal/clusterconfig/otlpenv.go` `otlpEnv.apply`).

Never shown: header values, the password and key material. `print-config` prints only the `exporter.headersFile` setting, `secret/<name>/<key>` for the headers Secret and for `OTEL_EXPORTER_OTLP_HEADERS` from the `envFrom` Secret alike (`internal/clusterconfig/clusterconfig.go`, `internal/clusterconfig/otlpenv.go` `otlpEnv.headersFile`). It never reads that file, so it lists neither header values nor header keys (`internal/cli/instance_tools.go` `describe` marshals the settings and the source statuses only). The password and key material show only as file paths. A header in the `envFrom` ConfigMap is an error, because headers are credentials and belong in the Secret (`internal/clusterconfig/otlpenv.go` `otlpEnv.unsupported`). So put credentials in the headers Secret or the password Secret, not in the endpoint, a ConfigMap or the resource attributes.

## What stays open

- A compromised plugin process can read every ConfigMap and Secret in the cluster, because its ClusterRole has `get`, `list` and `watch` on both without a name limit (`config/rbac.yaml`). The policies limit what it may write, not what it may read.
- The policies bind the plugin and `*.telemetry` accounts only. Any other user who may create ServiceAccounts or Roles can create a ServiceAccount named `<something>.telemetry`, and can give it other rights. The `*.telemetry` policies then limit that account on Secrets, ServiceAccounts, Roles and RoleBindings only. Such an account also stays in the way of a Cluster of that name.
- A token Secret of type `kubernetes.io/service-account-token` does not expire, and it stays after the plugin that wrote it is gone. Tokens written before a fix, or by a compromised plugin, persist until someone deletes them.
- A `*.telemetry` account inherits every right that is bound to the groups `system:serviceaccounts` or `system:serviceaccounts:<namespace>`. The roles policy limits the Roles the plugin writes, not these group bindings. A compromised plugin can create such an account in a namespace where the group holds rights and read its token.
- A compromised plugin can write a token Secret for any existing `*.telemetry` account, including one that someone else bound to broader rights, and read the token. The identities policy checks the name of the account, not who owns it.
- CloudNativePG applies the plugin's Pod patch to every Pod of an opted-in Cluster (`internal/operator/lifecycle.go` `Lifecycle.LifecycleHook`). A compromised plugin controls that patch, so it can run its own code in the PostgreSQL Pods. The policies limit its API writes, not what it puts into the patch.
- The CloudNativePG instance manager serves an API on `localhost:8010` without authentication (CloudNativePG v1.30.1 `pkg/management/url/url.go`). The Pod's containers share one network namespace, so the sidecar can call it. This is a CloudNativePG property the plugin does not change.
