#!/usr/bin/env bash
# Usage: deploy.sh IMAGE_REF
# Environment: KUBECTL, KIND_CLUSTER, BIN.
# Applies config/ with the image pinned to IMAGE_REF (REPO@DIGEST) and the sidecar image set to IMAGE_REF.
set -euo pipefail

: "${KUBECTL:?}" "${KIND_CLUSTER:?}" "${BIN:?}"

kctl() { "$KUBECTL" --context "kind-${KIND_CLUSTER}" "$@"; }

if [[ $# -ne 1 || $1 != *@sha256:* ]]; then
	echo "usage: $0 REPO@sha256:DIGEST" >&2
	exit 2
fi

ref=$1
digest=${ref#*@}

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

kctl apply --server-side --force-conflicts -k "$dir"
kctl wait --for=condition=Ready certificate --all -n cnpg-system --timeout=120s
kctl rollout status deployment/plugin-telemetry -n cnpg-system --timeout=180s
