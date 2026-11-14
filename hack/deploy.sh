#!/usr/bin/env bash
# Usage: deploy.sh IMAGE_REF
# Environment: KUBECTL, KIND_CLUSTER, BIN.
# Applies the CRDs, then config/ with the image pinned to IMAGE_REF (REPO@DIGEST) and the sidecar image set to IMAGE_REF,
# waits for the rollout and annotates the plugin Service so CloudNativePG rebuilds its connection and reloads the capabilities.
set -euo pipefail

: "${KUBECTL:?}" "${KIND_CLUSTER:?}" "${BIN:?}"

kctl() { "$KUBECTL" --context "kind-${KIND_CLUSTER}" "$@"; }

if [[ $# -ne 1 || $1 != *@sha256:* ]]; then
	echo "usage: $0 REPO@sha256:DIGEST" >&2
	exit 2
fi

ref=$1
digest=${ref#*@}

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
dir=$BIN/deploy
mkdir -p "$dir"
cat >"$dir/kustomization.yaml" <<YAML
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../config
images:
  - name: ghcr.io/gabrosys/plugin-telemetry
    digest: $digest
configMapGenerator:
  - name: telemetry-sidecar-image
    behavior: replace
    literals:
      - image=$ref
generatorOptions:
  disableNameSuffixHash: true
YAML

kctl apply --server-side --force-conflicts -k "$root/config/crd"
kctl wait --for=condition=Established \
	crd/metricquerysets.telemetry.gabrosys.github.io crd/telemetryexporters.telemetry.gabrosys.github.io --timeout=60s
kctl apply --server-side --force-conflicts -k "$dir"
kctl wait --for=condition=Ready certificate --all -n cnpg-system --timeout=120s
kctl rollout status deployment/plugin-telemetry -n cnpg-system --timeout=180s
kctl annotate service -n cnpg-system plugin-telemetry "telemetry.gabrosys.github.io/image=$ref" --overwrite
