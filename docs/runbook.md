# Runbook

How to install the plugin, send a Cluster's metrics to your collector, check that it works, notice when the sidecar goes quiet, restrict the plugin's network, upgrade it and remove it.

Run every command from the repository root. Set these two variables once in your shell. Every later command uses them.

```bash
NS=app        # namespace of the Cluster and its TelemetryExporter
CLUSTER=pg    # the Cluster you opt in; set it again for the next Cluster
```

The commands use your current kubectl context and never pass `--context`. Check it before you start.

```bash
kubectl config current-context
```

The plugin runs in `cnpg-system`, and the example collector in `otel`. Those two names are fixed by `config/` and `docs/examples/collector.yaml`.

## 1. What you need

- Kubernetes 1.34 or later. The plugin is tested on 1.37, and the 1.34 floor is tested on a 1.34 node.
- CloudNativePG v1.30.1 or later. CI also runs CloudNativePG `main`.
- cert-manager. It is tested with v1.21.3. The plugin's gRPC server uses mutual TLS with certificates from it.
- Docker, to build the plugin image, and `make`.
- `kubectl`. `make dev-up` also downloads one to `bin/kubectl-v1.37.2`.
- An OTLP collector that the instance Pods can reach, over HTTP (4318) or gRPC (4317).

For a local try, `make dev-up` creates a kind cluster with cert-manager and CloudNativePG. The cluster is named `plugin-telemetry-v1-30-1`, and `make dev-up KIND_CLUSTER=<name>` changes the name. Point kubectl at it afterwards.

```bash
make dev-up
kubectl config use-context kind-plugin-telemetry-v1-30-1
```

If you have no collector, apply the example one. It receives OTLP and prints a summary of what it gets.

```bash
kubectl apply -f docs/examples/collector.yaml
kubectl rollout status deployment/collector -n otel --timeout=180s
```

Its endpoints are `http://collector.otel.svc:4318` and `http://collector.otel.svc:4317`.

## 2. Build the image

The image package on ghcr is private, so you build the image and put it where your nodes can pull it. The plugin and the sidecar are one image. The plugin pins it by digest, and a tag is refused. Both steps below end with the digest reference in `bin/image-ref`. The next section reads it from there.

On kind, `make kind-load` builds the image, loads it into the cluster and writes the reference. Pass the same `KIND_CLUSTER` you gave `make dev-up`.

```bash
make kind-load
cat bin/image-ref
```

On any other cluster, build for your platform, push to a registry your nodes can pull from, and record the digest the registry reports.

```bash
IMG=registry.example.com/team/plugin-telemetry:v0.5.0
make image IMG=$IMG
docker push $IMG
mkdir -p bin
echo "${IMG%:*}@$(docker buildx imagetools inspect $IMG | awk '/^Digest:/ {print $2}')" > bin/image-ref
cat bin/image-ref
```

The nodes' architecture must match the platform you built for. The instance Pods pull the image too, in the Cluster's namespace. If your registry needs credentials, give them to both the plugin's Pods and your Cluster's Pods.

## 3. Install the plugin

Apply the CRDs first and wait until the API server serves them.

```bash
kubectl apply --server-side -k config/crd
kubectl wait --for=condition=Established \
  crd/metricquerysets.telemetry.gabrosys.github.io \
  crd/telemetryexporters.telemetry.gabrosys.github.io --timeout=60s
```

Then write a kustomization that points at `config/`, pins the plugin image, and sets the image that the plugin injects as the sidecar. The `bin/` directory is not tracked by git.

```bash
mkdir -p bin/install
IMAGE_REF=$(cat bin/image-ref)
cat > bin/install/kustomization.yaml <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../config
images:
  - name: ghcr.io/gabrosys/plugin-telemetry
    newName: ${IMAGE_REF%@*}
    digest: ${IMAGE_REF#*@}
configMapGenerator:
  - name: telemetry-sidecar-image
    namespace: cnpg-system
    behavior: replace
    literals:
      - image=$IMAGE_REF
generatorOptions:
  disableNameSuffixHash: true
EOF
kubectl apply --server-side -k bin/install
```

Wait for the certificates and the rollout. The plugin serves only after it holds a valid sidecar image and its caches have synced, so a Pod that is ready can reach the API server.

```bash
kubectl wait --for=condition=Ready certificate --all -n cnpg-system --timeout=120s
kubectl rollout status deployment/plugin-telemetry -n cnpg-system --timeout=180s
```

Then annotate the plugin's Service. CloudNativePG reads a plugin's capabilities only when it builds its connection to it. An event on the Service makes it build one again. Without this, CloudNativePG can keep the old capabilities for up to about 10 hours.

```bash
kubectl annotate service -n cnpg-system plugin-telemetry \
  "telemetry.gabrosys.github.io/image=$IMAGE_REF" --overwrite
```

### The plugin's own metrics (optional)

The plugin exports its own metrics over OTLP/HTTP when it gets `--metrics-endpoint`, a base URL like `exporter.endpoint`, every 30 s by default (`--metrics-interval`). The base manifests leave it unset. To set it, add a patch to the kustomization and apply it again.

```bash
cat >> bin/install/kustomization.yaml <<'EOF'
patches:
  - target:
      kind: Deployment
      name: plugin-telemetry
    patch: |-
      - op: add
        path: /spec/template/spec/containers/0/args/-
        value: --metrics-endpoint=http://collector.otel.svc:4318
EOF
kubectl apply --server-side -k bin/install
kubectl rollout status deployment/plugin-telemetry -n cnpg-system --timeout=180s
```

Use your collector's address instead of `collector.otel.svc:4318`. The metrics are the "Plugin" table in [self-metrics.md](self-metrics.md). They are hook and identity counters, so they reach the collector only after the plugin has served CloudNativePG calls. With no opted-in Cluster nothing arrives, and that is not an error.

## 4. Send a Cluster's metrics

The Cluster and the TelemetryExporter live in the same namespace. Create the namespace and the TelemetryExporter first. `default` is the name a Cluster uses when it sets no `exporter` parameter. One TelemetryExporter serves every Cluster in the namespace.

```bash
kubectl create namespace $NS --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f - <<EOF
apiVersion: telemetry.gabrosys.github.io/v1alpha1
kind: TelemetryExporter
metadata:
  name: default
  namespace: $NS
spec:
  exporter:
    endpoint: http://collector.otel.svc:4318
EOF
```

`endpoint` is the collector's base URL. For `http/protobuf`, the default protocol, the sidecar appends `/v1/metrics`. An `http://` endpoint is plaintext. Every other field is in [crd-reference.md](crd-reference.md). The sidecar exports every 30 s unless `spec.metrics.interval` says otherwise.

A Cluster opts in with an entry in `spec.plugins`. The sidecar logs in to PostgreSQL as a read-only monitoring role, in one of two modes that you choose per Cluster with the `auth` parameter. The plugin cannot change a Cluster's PostgreSQL settings, so you add the lines each mode needs yourself. When the Cluster lacks them, its plugin status says so ([section 5](#5-check-that-it-works)).

Adding the plugin to a Cluster that already runs rolls its instances once, because the sidecar joins the Pod. Changing `pg_ident` or `pg_hba` reloads PostgreSQL and rolls nothing. A new Cluster starts with the sidecar. The sidecar uses 50m CPU and 96Mi memory unless the `sidecarCPU` and `sidecarMemory` parameters say otherwise.

### Peer mode

Use it on vanilla Kubernetes. It needs no secret. The sidecar connects over the Unix socket as the role `cnpg_metrics_exporter`, which CloudNativePG creates with `pg_monitor` only. The one line you add, in `spec.postgresql.pg_ident`, maps the sidecar's OS user `nobody` to that role. The operand image must have `nobody` (UID 65534) in `/etc/passwd`. The `postgresql:18.7` minimal, standard and system images of CloudNativePG do.

```bash
kubectl apply -f - <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: $CLUSTER
  namespace: $NS
spec:
  instances: 1
  imageName: ghcr.io/cloudnative-pg/postgresql:18.7-system-trixie@sha256:13f5dc1b42c5d96233724be976740df031136ca013f98a56a2877a62b362b453
  storage:
    size: 1Gi
  postgresql:
    pg_ident:
      - cnpg_metrics_exporter nobody cnpg_metrics_exporter
  plugins:
    - name: telemetry.gabrosys.github.io
EOF
```

### Password mode

Use it on any platform, and on OpenShift, where Pods run with random UIDs and peer authentication fails. You declare a monitoring role with `pg_monitor`, a Secret with its password, and `pg_hba` lines that let the role in only from the loopback addresses and reject it everywhere else. The Secret must be of type `kubernetes.io/basic-auth` with the role's name as `username`, and CloudNativePG wants it labelled `cnpg.io/reload`. The role's `connectionLimit` must be at least `maxConnections` + 1. The default `maxConnections` is 4, so the example uses 5. The sidecar opens one more short connection every 10 s outside that cap, to see whether PostgreSQL is shutting down.

```bash
ROLE=otel_monitor
SECRET=$CLUSTER-otel-monitor
kubectl create secret generic $SECRET -n $NS --type=kubernetes.io/basic-auth \
  --from-literal=username=$ROLE --from-literal=password="$(head -c 24 /dev/urandom | base64)"
kubectl label secret $SECRET -n $NS cnpg.io/reload=true
kubectl apply -f - <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: $CLUSTER
  namespace: $NS
spec:
  instances: 1
  imageName: ghcr.io/cloudnative-pg/postgresql:18.7-system-trixie@sha256:13f5dc1b42c5d96233724be976740df031136ca013f98a56a2877a62b362b453
  storage:
    size: 1Gi
  managed:
    roles:
      - name: $ROLE
        ensure: present
        login: true
        connectionLimit: 5
        inRoles:
          - pg_monitor
        passwordSecret:
          name: $SECRET
  postgresql:
    pg_hba:
      - host all $ROLE 127.0.0.1/32 scram-sha-256
      - host all $ROLE ::1/128 scram-sha-256
      - host all $ROLE all reject
  plugins:
    - name: telemetry.gabrosys.github.io
      parameters:
        auth: password
        passwordSecret: $SECRET
EOF
```

Wait until the Cluster is ready.

```bash
kubectl wait --for=condition=Ready cluster.postgresql.cnpg.io/$CLUSTER -n $NS --timeout=600s
```

To opt in an existing Cluster, add the same `plugins` entry and the lines of its mode to the Cluster's manifest. Do not replace a `pg_hba` or `pg_ident` list that has lines of its own. After you edit an existing Cluster, `kubectl get pods -n $NS` shows the instances restart, one at a time.

## 5. Check that it works

The plugin reports in the Cluster's status. CloudNativePG asks for it at the end of a reconcile, so it can take a few seconds to appear.

```bash
kubectl get cluster.postgresql.cnpg.io $CLUSTER -n $NS \
  -o jsonpath='{.status.pluginStatus[?(@.name=="telemetry.gabrosys.github.io")].status}'; echo
```

A healthy Cluster shows this and nothing else:

```json
{"exporter":"default","exporterState":"ok","identity":"ready"}
```

`exporterState` is `ok`, `missing` or `invalid`. `identity` is `ready` or `pending`. `message` (the first line of the exporter's error) and `problems` (a sorted list) appear only when there is something to say. [Section 7](#7-troubleshooting) covers each one.

The plugin creates four objects for the Cluster in its namespace: a ServiceAccount, a Role and a RoleBinding named `<cluster>.telemetry`, and a token Secret named `<cluster>.telemetry-token`.

```bash
kubectl get serviceaccount,role,rolebinding,secret -n $NS -l app.kubernetes.io/managed-by=plugin-telemetry
```

The sidecar is the container `otel-metrics`, a native sidecar that runs as an init container. Until kubelet delivers the sidecar's token, the first minute can show `waiting for the sidecar token` in its log.

```bash
kubectl get pod $CLUSTER-1 -n $NS \
  -o jsonpath='{.status.initContainerStatuses[?(@.name=="otel-metrics")].started}'; echo
kubectl logs $CLUSTER-1 -n $NS -c otel-metrics | grep -E 'configuration applied|plan updated'
```

The first command prints `true`. The second prints one line each, for example `"msg":"configuration applied"` with the `generation`, `hash` and `queries` count, and `"msg":"plan updated"` with `jobs` and `skipped`.

Check the configuration without connecting, then run every planned query once. Both commands run inside the sidecar and read the same Kubernetes objects it does.

```bash
kubectl exec $CLUSTER-1 -n $NS -c otel-metrics -- /plugin-telemetry instance validate; echo "exit $?"
kubectl exec $CLUSTER-1 -n $NS -c otel-metrics -- /plugin-telemetry instance test > bin/instance-test.out
grep '^cnpg_collector_pg_wal' bin/instance-test.out
sed -n '/^# errors/,$p' bin/instance-test.out
```

`validate` prints one line per finding, `error` or `warning`, and nothing when there is none. It exits with 1 only when a line starts with `error`. `test` prints a series per line, then `# skipped` with one line per query it did not run, then `# errors`. Healthy output has `cnpg_collector_pg_wal` series and nothing after `# errors`.

Last, check what the collector received. The example collector's `debug` exporter logs every metric it gets. Wait one export interval first.

```bash
sleep 45
kubectl logs deployment/collector -n otel | grep -m1 'Name: cnpg_collector_up'
kubectl logs deployment/collector -n otel | grep 'service.name' | sort -u
```

The first `grep` prints `-> Name: cnpg_collector_up`. The second lists `service.name: Str(<cluster>)` for each Cluster that exports. With a collector of your own, look for the same metric name and resource attribute in your backend.

## 6. Notice a quiet sidecar

A push pipeline has no scrape `up`: when a sidecar stops, its series stop arriving and no sample says so. Alerting belongs to your backend, so this runbook gives no rules. The signals below are metrics of the sidecar (the "Engine" table in [self-metrics.md](self-metrics.md)), not of the plugin Pod.

- The heartbeat is `cnpg_otel_last_export_success_timestamp_seconds`, the Unix time of the last successful export. It moves only when a request had at least one accepted point. Alert when the newest point of an instance is older than two export intervals, which is 60 s with the default 30 s. Alert too when an instance has no point at all. Compare the instances that report with the instances that exist (`k8s.pod.name` and `service.instance.id` are resource attributes on every point). Your backend's staleness window may be longer than two intervals, so measure the age of the newest point yourself.
- `cnpg_otel_exports_total` by `outcome` (`ok`, `error`, `skipped`). A rising `error` count means the sidecar runs but its exports fail.
- `cnpg_otel_query_runs_total` with an `outcome` other than `ok`. The outcomes are `ok`, `error`, `timeout`, `skipped`, `capped` and `denied`.
- `cnpg_otel_check` by `check`. The checks are `pg_monitor`, `read_only`, `track_io_timing`, `track_wal_io_timing`, `track_functions` and `superuser`. It is 1 when the check holds. `superuser` is the reverse: 1 means the sidecar's role is a superuser and no query runs.

The last three arrive only while exports work, so they do not replace the heartbeat. A series that disappears is not marked stale. Backends see it age out. The catalogue of every self-metric, with attributes, is in [self-metrics.md](self-metrics.md).

## 7. Troubleshooting

The plugin's status is in the Cluster ([section 5](#5-check-that-it-works)). The sidecar's logs are `kubectl logs <instance Pod> -n $NS -c otel-metrics`, as JSON lines. The plugin's logs are `kubectl logs -n cnpg-system -l app.kubernetes.io/name=plugin-telemetry`. To make CloudNativePG reconcile a Cluster and refresh its plugin status, touch it:

```bash
kubectl annotate cluster.postgresql.cnpg.io $CLUSTER -n $NS runbook.telemetry.gabrosys.github.io/touch="$(date +%s)" --overwrite
```

| Symptom | What the plugin shows | Fix |
|---|---|---|
| The TelemetryExporter does not exist, or the `exporter` parameter names the wrong one | Status `"exporterState":"missing"`, with `exporter` set to the name it looked for. The sidecar logs `no configuration, waiting for one` with `reason` `no configuration: telemetryexporter "default" does not exist`, and exports nothing, its own metrics included. | Create the TelemetryExporter in the Cluster's namespace under that name, or correct the parameter. The sidecar starts on its own and no Pod restarts. |
| The TelemetryExporter is invalid | Status `"exporterState":"invalid"` and a `message` that starts with `check exporter:` and names the field. The sidecar logs `invalid configuration, waiting for a valid one`, or `invalid configuration, keeping the one in force` when an earlier configuration worked. | Fix the field the message names. The sidecar reloads at once. The plugin does not read Secrets, so a bad key inside the headers, CA, client certificate or `envFrom` Secret still shows `ok`. The sidecar logs it. |
| The Cluster was opted in before its TelemetryExporter, and the status still says `missing` after you created it | Exports and the identity resume at once, but the status changes only when CloudNativePG reconciles the Cluster, and creating the exporter does not trigger that. | Touch the Cluster (above). |
| `"identity":"pending"` with a line in `problems` that names an object | `Secret/<c>.telemetry-token has type <type>, not kubernetes.io/service-account-token: delete it`, `Secret/<c>.telemetry-token names ServiceAccount "<a>", not "<c>.telemetry": delete it`, `RoleBinding/<c>.telemetry refers to <kind>/<name>, not Role/<c>.telemetry: delete it`, or `<Kind>/<name> is not in the plugin's cache: not created yet, or not the plugin's`. The plugin logs `identity controller` with `"outcome":"in the way"` once. | The plugin never adopts an object it did not create. Delete the object the line names, for example `kubectl delete secret <c>.telemetry-token -n $NS`. The plugin retries every 5 s. `has no data.token yet` clears when Kubernetes fills the token. |
| No `otel-metrics` container in the instance Pods | `problems` has `auth "<value>" is invalid: no sidecar is injected`. An invalid `auth` also removes the Cluster's identity objects until it is fixed. | Set `auth` to `peer` or `password`, or leave it out for `peer`. The instances roll once to gain the sidecar. |
| Peer mode: `peer mode needs the pg_ident line "cnpg_metrics_exporter nobody cnpg_metrics_exporter"` | The sidecar logs `gather facts` with an error that says `Peer authentication failed`, and `cnpg_collector_up` is 0. | Add the line to `spec.postgresql.pg_ident`. PostgreSQL reloads, no Pod restarts, and the sidecar logs `gathered facts again`. |
| Password mode: `password mode needs the passwordSecret parameter`, `no managed role uses passwordSecret "<name>" with login and pg_monitor`, `pg_hba has no line for role "<role>" from 127.0.0.1/32` (or `::1/128`), or `pg_hba has no reject line for role "<role>"` | The status is the only place these appear. They list what the Cluster spec lacks. | Add the parameter, the managed role (with `login: true` and `pg_monitor` in `inRoles`) or the `pg_hba` lines from [section 4](#password-mode). |
| `cnpg_otel_check{check="superuser"}` is 1 | `cnpg_collector_up` is not reported, and no query runs. The sidecar logs `query failed` with `"query":"facts"`, `"outcome":"denied"` and the error `the session role is a superuser: <role>; no query runs until the role is not a superuser`. | The sidecar must log in as a role that is not a superuser. In password mode, check the `username` in the Secret that `passwordSecret` names, and the managed role ([section 4](#password-mode)). |
| `cnpg_otel_query_runs_total` has `outcome="denied"` | `cnpg_otel_check{check="pg_monitor"}` is 0 when the role is not in `pg_monitor`. The sidecar logs `query failed` with `"outcome":"denied"`. PostgreSQL reports it as SQLSTATE 42501. | Put the role in `pg_monitor`. In password mode that is `inRoles` of the managed role. If `pg_monitor` is held, the query reads something `pg_monitor` cannot, so change the query. |
| Exports fail | `cnpg_otel_exports_total{outcome="error"}` rises. The sidecar logs `export metrics` with an `error.type`: `timeout`, `_OTHER`, `request_too_large`, an HTTP status such as `401` or a gRPC code such as `Unavailable`. A partly accepted export logs `export partially rejected`. The log says `export recovered` once exports work again. | Check from the Cluster's namespace that the endpoint resolves and answers, that no NetworkPolicy blocks it, and that `exporter.endpoint`, protocol and credentials match the collector. |
| `ImagePullBackOff` after an image change | `kubectl get pods -n $NS` shows `Init:ImagePullBackOff` or `Init:ErrImagePull` on instance Pods. The sidecar is an init container, so the Pod never starts. | The sidecar digest in the `telemetry-sidecar-image` ConfigMap cannot be pulled by that node. Make it pullable, or put the previous digest back ([section 10](#10-upgrade-the-plugin)). Either change rolls the instances again. |
| The plugin's Pods are not ready | The plugin logs stop at `waiting for the sidecar image` or `waiting for the caches to sync`. `ignoring sidecar image configmap` means the `image` key is not `<repository>@sha256:<64 hex digits>`. | Fix the ConfigMap. A tag is refused. If the log stops at `waiting for the caches to sync`, the plugin cannot reach the API server: look for a NetworkPolicy ([section 9](#9-restrict-the-plugins-network)). |
| Deleting, draining or restarting an instance takes about 10 s longer | None. The sidecar holds PostgreSQL's smart shutdown open until its next probe, which runs every 10 s. | Nothing. It was 0.18 s without the plugin against 182.5 s before the probe existed. The measurements are in [resources.md](resources.md#pod-shutdown). |

## 8. Known log lines

On every clean stop of a plugin Pod, cnpg-i-machinery v0.4.2 logs an error-level line with the message `While terminating server` (`pkg/pluginhelper/http/server.go:222`). It logs it whenever the gRPC server's `Serve` returns something other than a closed-listener error, and a clean stop returns nil. It is harmless. Ignore it in alerts on error-level logs from the plugin.

## 9. Restrict the plugin's network

`docs/examples/networkpolicy.yaml` limits the plugin's Pods to what they need: ingress on TCP 9090 from the CloudNativePG operator, and egress to DNS, to the API server and, if you enable it, to the collector for the plugin's own metrics. A replica that cannot sync its caches never serves CloudNativePG. If no replica serves, every Cluster that opted in stops reconciling, failover included. So read this section before you apply the policy.

It needs a CNI that enforces NetworkPolicy. kind's default CNI, kindnet, does.

The policy matches the API server by IP address, and what a CNI matches depends on the CNI. On kindnet in kind v0.33.0 the policy matches the endpoint's IP and port after the Service address is translated: `172.18.0.3:6443` on the test cluster. A rule for the Service address and port 443 blocked the traffic there. This was measured on a single-node kind cluster. Other CNIs may match the Service IP and 443 instead, because the Kubernetes NetworkPolicy documentation leaves undefined whether `ipBlock` applies before or after the Service address is rewritten ("Behavior of ipBlock selectors"). Test on your CNI with the checks below. If the plugin cannot reach the API server, also allow the Service IP and port 443 (`kubectl get service kubernetes -n default`).

Find the API server's address and port. The `kubernetes` Service in `default` has an EndpointSlice for it.

```bash
kubectl get endpointslices -n default -l kubernetes.io/service-name=kubernetes
```

Write them into a copy of the example and apply it. The example holds the placeholder `192.0.2.10/32`, which blocks the API server until you replace it. An HA or managed control plane has several addresses: copy the file and add one `ipBlock` entry for each, or use the provider's API server CIDR. Replace `/32` with `/128` for an IPv6 address.

```bash
API_IP=$(kubectl get endpointslices -n default -l kubernetes.io/service-name=kubernetes \
  -o jsonpath='{.items[0].endpoints[0].addresses[0]}')
API_PORT=$(kubectl get endpointslices -n default -l kubernetes.io/service-name=kubernetes \
  -o jsonpath='{.items[0].ports[0].port}')
sed -e "s|192.0.2.10/32|$API_IP/32|" -e "s|port: 6443|port: $API_PORT|" \
  docs/examples/networkpolicy.yaml | kubectl apply -f -
```

If you set `--metrics-endpoint`, the plugin's metrics export also needs a rule. Without it, the export times out and the plugin logs `export metrics` with `"error.type":"timeout"`. With the rule, the metrics arrive only after the plugin has served CloudNativePG calls, so with no opted-in Cluster none arrive. That is not an error. The example has the rule commented out, for a collector in `otel` with the label `app.kubernetes.io/name: collector` on port 4318. Change those to match yours and uncomment it. For the example collector, apply the policy like this instead:

```bash
sed -e "s|192.0.2.10/32|$API_IP/32|" -e "s|port: 6443|port: $API_PORT|" \
  -e '/^    # - to:/,$ s|^    # |    |' \
  docs/examples/networkpolicy.yaml | kubectl apply -f -
```

DNS is needed only to resolve the `--metrics-endpoint` host, because the plugin reaches the API server by IP. The example's `kube-dns` selector does not cover NodeLocal DNSCache or other DNS setups, so adjust it to yours. The plugin runs in the CloudNativePG operator's namespace. If that is not `cnpg-system`, change the policy's `metadata.namespace` and the ingress `namespaceSelector` together.

The readiness probe is a TCP check on port 9090 from the kubelet. Kubernetes allows node traffic to Pods, but some CNIs differ. The restart check below covers it: a replica that never becomes ready may be blocked by the policy.

The policy covers the plugin's Pods only. A policy on the instance Pods would also govern PostgreSQL's own traffic, so none is given. The sidecar's export path, from the Cluster's namespace to the collector, is not covered. If you restrict instance Pods yourself, the sidecar needs the API server and the collector.

### Confirm the plugin still works

Restart one replica and wait until the plugin is ready. The readiness probe is a TCP check on the gRPC port, and the plugin listens only after its caches have synced, so a ready replica reaches the API server.

```bash
POD=$(kubectl get pods -n cnpg-system -l app.kubernetes.io/name=plugin-telemetry -o jsonpath='{.items[0].metadata.name}')
kubectl delete pod $POD -n cnpg-system
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=plugin-telemetry -n cnpg-system --timeout=180s
kubectl logs -n cnpg-system -l app.kubernetes.io/name=plugin-telemetry --since=5m --tail=-1 | grep -E 'waiting for the caches to sync|sidecar image loaded, serving'
```

Then make CloudNativePG reconcile your Cluster, give it 30 s, and read its phase and the operator's log.

```bash
kubectl annotate cluster.postgresql.cnpg.io $CLUSTER -n $NS runbook.telemetry.gabrosys.github.io/touch="$(date +%s)" --overwrite
sleep 30
kubectl get cluster.postgresql.cnpg.io $CLUSTER -n $NS -o jsonpath='{.status.phase}'; echo
kubectl logs -n cnpg-system deployment/cnpg-controller-manager --since=2m | grep -c '"level":"error"'
```

The phase is `Cluster in healthy state`, and the count is 0. When the plugin cannot answer, the phase is `Cluster cannot proceed to reconciliation due to an error while interacting with plugins`.

Finally, check that the policy is enforced, using a Pod that is not the operator. From `default`, the connection to the plugin times out. Run the same command after you delete the policy to see the difference.

```bash
kubectl run curl-np -n default --rm -i --restart=Never \
  --image=curlimages/curl:8.23.0@sha256:3fb7e13e5d179a845c97d78204f3f1064d1907037f26a3ebf91710571151fd93 \
  -- curl -sv -m 5 -k -o /dev/null https://plugin-telemetry.cnpg-system:9090
```

With the policy the output ends with `Connection timed out after 5000 milliseconds` and the exit code is 28. Without it, curl completes the TLS handshake with `SSL connection using TLSv1.3`.

### Back out

Delete the policy. The plugin recovers on its own once the traffic flows.

```bash
kubectl delete networkpolicy plugin-telemetry -n cnpg-system
```

## 10. Upgrade the plugin

Run the steps in this order. Build the new image as in [section 2](#2-build-the-image). Keep the old reference first, because you may need it.

```bash
cp bin/image-ref bin/image-ref.previous
```

Then build, which overwrites `bin/image-ref`, and read the new reference.

```bash
NEW_REF=$(cat bin/image-ref)
```

1. Roll the plugin Deployment. Change only the plugin image in the kustomization. The sidecar image in the ConfigMap stays as it is. No Pod rolls, and no hook call fails, because a stopping replica keeps serving until the endpoint removal has propagated.

   ```bash
   sed -e "s|^    newName: .*|    newName: ${NEW_REF%@*}|" -e "s|^    digest: .*|    digest: ${NEW_REF#*@}|" \
     bin/install/kustomization.yaml > bin/install/kustomization.yaml.new && mv bin/install/kustomization.yaml.new bin/install/kustomization.yaml
   kubectl apply --server-side -k bin/install
   kubectl rollout status deployment/plugin-telemetry -n cnpg-system --timeout=180s
   ```

2. Once the Deployment has rolled out, change the plugin Service's annotation. CloudNativePG reads the plugin's capabilities only when it rebuilds its connection, and it rebuilds on an event of the Service. Without this step it can keep calling with the old capability set for up to about 10 hours.

   ```bash
   kubectl annotate service -n cnpg-system plugin-telemetry "telemetry.gabrosys.github.io/image=$NEW_REF" --overwrite
   ```

3. Check that the new digest is pullable on every node, then change the sidecar image in the ConfigMap. Every opted-in Cluster rolls once. A well-formed digest that cannot be pulled puts every opted-in instance into `Init:ImagePullBackOff`, so do not skip the check. This starts a short-lived Pod on each node in `$NS`, where the instance Pods pull the image.

   ```bash
   for node in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
     kubectl run "pull-check-$node" -n $NS --labels=app=pull-check --image="$NEW_REF" --restart=Never \
       --overrides="{\"spec\":{\"nodeName\":\"$node\"}}" -- version
   done
   kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod -l app=pull-check -n $NS --timeout=180s
   kubectl delete pod -l app=pull-check -n $NS
   ```

   When the wait times out, `kubectl get pods -l app=pull-check -n $NS` shows which node cannot pull. Delete those Pods and fix the pull before you go on. Then change the ConfigMap and watch the instances roll.

   ```bash
   sed "s|^      - image=.*|      - image=$NEW_REF|" bin/install/kustomization.yaml > bin/install/kustomization.yaml.new \
     && mv bin/install/kustomization.yaml.new bin/install/kustomization.yaml
   kubectl apply --server-side -k bin/install
   kubectl get pods -n $NS -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.initContainers[?(@.name=="otel-metrics")].image}{"\n"}{end}'
   ```

   To go back, run the same two `sed` commands of steps 1 and 3 with `$(cat bin/image-ref.previous)`. This rolls the instances again.

4. A release that changes the Pod patch rolls every opted-in instance on upgrade. Read its release notes before you start.

## 11. Remove the plugin

### From a Cluster

Remove the `plugins` entry from the Cluster. This is also the emergency exit when the plugin is down, since an outage stops the reconciles of every Cluster that names it. It costs one rolling restart, because the sidecar leaves the Pod. If the plugin is the Cluster's only entry:

```bash
kubectl patch cluster.postgresql.cnpg.io $CLUSTER -n $NS --type=json -p '[{"op":"remove","path":"/spec/plugins"}]'
```

With other plugins, remove only the entry whose name is `telemetry.gabrosys.github.io`, by its index in `spec.plugins`. Then wait until no instance has the sidecar, and until the Cluster is ready.

```bash
until [ -z "$(kubectl get pods -n $NS -l cnpg.io/cluster=$CLUSTER -o jsonpath='{.items[*].spec.initContainers[?(@.name=="otel-metrics")].name}')" ]; do sleep 5; done
kubectl wait --for=condition=Ready cluster.postgresql.cnpg.io/$CLUSTER -n $NS --timeout=600s
```

The identity controller deletes the four identity objects of a Cluster that no longer has the plugin, with the plugin running. Check that they are gone.

```bash
kubectl get serviceaccount,role,rolebinding,secret -n $NS -l app.kubernetes.io/managed-by=plugin-telemetry
```

The list holds no object named `<cluster>.telemetry` or `<cluster>.telemetry-token`. The plugin never removes a token Secret that is in the way (wrong type or another account); delete it yourself. The `pg_ident` line, the managed role, the `pg_hba` lines and the password Secret are your own Cluster settings, which the plugin never wrote, so it leaves them. Remove them if you no longer want them. The TelemetryExporter stays until you delete it.

### Uninstall

Remove the plugin from every Cluster first. The identity objects are deleted only while the plugin runs, and CloudNativePG cannot reconcile a Cluster that still names a plugin that is gone. Then delete what you applied. Deleting the CRDs deletes every TelemetryExporter and MetricQuerySet.

```bash
kubectl delete -k config
```

cert-manager does not delete the Secrets of a deleted Certificate. Delete the three that remain.

```bash
kubectl delete secret -n cnpg-system plugin-telemetry-ca-tls plugin-telemetry-server-tls plugin-telemetry-client-tls
```

Last, delete the example collector, if you applied it, and the files you wrote.

```bash
kubectl delete -f docs/examples/collector.yaml
rm -rf bin/install
```
