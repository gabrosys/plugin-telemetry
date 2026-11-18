#!/usr/bin/env bash
# Usage: deploy.sh IMAGE_REF
# Environment: KUBECTL, KIND_CLUSTER, BIN, COLLECTOR_IMAGE, POSTGRES_IMAGE.
# Applies the CRDs, the admission policies, then config/ with the image pinned to IMAGE_REF (REPO@DIGEST), the sidecar
# image set to IMAGE_REF and the plugin exporting its self-metrics to the e2e collector (hack/collector.yaml, whose reader
# container runs POSTGRES_IMAGE), waits for both rollouts and annotates the plugin Service so CloudNativePG rebuilds its
# connection and reloads the capabilities.
# The overlay sets --log-level=debug on the plugin: framework.HookCalls reads the debug-level "lifecycle hook" line.
set -euo pipefail

: "${KUBECTL:?}" "${KIND_CLUSTER:?}" "${BIN:?}" "${COLLECTOR_IMAGE:?}" "${POSTGRES_IMAGE:?}"

metrics_endpoint=http://collector.telemetry-e2e.svc:4318

kctl() { "$KUBECTL" --context "kind-${KIND_CLUSTER}" "$@"; }

if [[ $# -ne 1 || ! $1 =~ ^[^@]+@sha256:[0-9a-f]{64}$ ]]; then
	echo "usage: $0 REPO@sha256:DIGEST" >&2
	exit 2
fi

ref=$1
digest=${ref#*@}

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
dir=$BIN/deploy
mkdir -p "$dir"
manifest=$(<"$root/hack/collector.yaml")
manifest=${manifest//__COLLECTOR_IMAGE__/$COLLECTOR_IMAGE}
manifest=${manifest//__POSTGRES_IMAGE__/$POSTGRES_IMAGE}
printf '%s\n' "$manifest" >"$dir/collector.yaml"
cat >"$dir/kustomization.yaml" <<YAML
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - $(realpath --relative-to="$dir" "$root/config")
  - collector.yaml
patches:
  - target:
      kind: Deployment
      name: plugin-telemetry
    patch: |-
      - op: add
        path: /spec/template/spec/containers/0/args/-
        value: --metrics-endpoint=$metrics_endpoint
      - op: add
        path: /spec/template/spec/containers/0/args/-
        value: --log-level=debug
images:
  - name: ghcr.io/gabrosys/plugin-telemetry
    newName: ${ref%@*}
    digest: $digest
configMapGenerator:
  - name: telemetry-sidecar-image
    namespace: cnpg-system
    behavior: replace
    literals:
      - image=$ref
generatorOptions:
  disableNameSuffixHash: true
YAML

# Every apply uses --force-conflicts to take back fields a hand edit took over, such as the sidecar image ConfigMap.
kctl apply --server-side --force-conflicts -k "$root/config/crd"
kctl wait --for=condition=Established \
	crd/metricquerysets.telemetry.gabrosys.github.io crd/telemetryexporters.telemetry.gabrosys.github.io --timeout=60s
# The admission policies are in place before the plugin starts.
kctl apply --server-side --force-conflicts -f "$root/config/admission-policy.yaml"
kctl apply --server-side --force-conflicts -k "$dir"
kctl wait --for=condition=Ready certificate --all -n cnpg-system --timeout=120s
kctl rollout status deployment/plugin-telemetry -n cnpg-system --timeout=180s
kctl rollout status deployment/collector -n telemetry-e2e --timeout=180s
kctl annotate service -n cnpg-system plugin-telemetry "telemetry.gabrosys.github.io/image=$ref" --overwrite
