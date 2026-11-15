#!/usr/bin/env bash
# Usage: dev-env.sh up|down
# Environment: KIND, KUBECTL, CMCTL, KIND_CLUSTER, KIND_NODE_IMAGE, CNPG_VERSION, CERT_MANAGER_VERSION.
set -euo pipefail

: "${KIND:?}" "${KUBECTL:?}" "${KIND_CLUSTER:?}"

kctl() { "$KUBECTL" --context "kind-${KIND_CLUSTER}" "$@"; }

usage() {
	echo "dev-env: $*" >&2
	exit 2
}

fail() {
	echo "dev-env: $*" >&2
	exit 1
}

cnpg_manifest() {
	local version=${1#v}
	case $1 in
	main) echo "https://raw.githubusercontent.com/cloudnative-pg/artifacts/main/manifests/operator-manifest.yaml" ;;
	*) echo "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-${version%.*}/releases/cnpg-${version}.yaml" ;;
	esac
}

summary() {
	local node=${KIND_NODE_IMAGE%%@*} image
	image=$(kctl get deployment cnpg-controller-manager -n cnpg-system \
		-o jsonpath='{.spec.template.spec.containers[?(@.name=="manager")].image}')
	echo "$1 kind=${KIND_CLUSTER} node=${node##*:} cert-manager=${CERT_MANAGER_VERSION} cnpg=${CNPG_VERSION} operator-image=${image}"
}

wait_reachable() {
	local deadline=$((SECONDS + 30))
	"$KIND" export kubeconfig --name "$KIND_CLUSTER" >/dev/null 2>&1 &&
		until kctl get --raw=/readyz >/dev/null 2>&1; do
			((SECONDS < deadline)) || break
			sleep 2
		done
	kctl wait --for=condition=Ready node --all --timeout=120s >/dev/null 2>&1 ||
		fail "cluster $KIND_CLUSTER is not reachable; run 'make dev-down' first"
}

up() {
	: "${CMCTL:?}" "${KIND_NODE_IMAGE:?}" "${CNPG_VERSION:?}" "${CERT_MANAGER_VERSION:?}"
	[[ $CNPG_VERSION =~ ^(main|v[0-9]+\.[0-9]+\.[0-9]+)$ ]] ||
		usage "CNPG_VERSION must be main or vX.Y.Z, got '$CNPG_VERSION'"

	if ! "$KIND" get clusters | grep -qx "$KIND_CLUSTER"; then
		"$KIND" create cluster --name "$KIND_CLUSTER" --image "$KIND_NODE_IMAGE" --wait 120s
	fi
	wait_reachable

	kctl apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
	"$CMCTL" check api --context "kind-${KIND_CLUSTER}" --wait=2m
	kctl apply --server-side -f "$(cnpg_manifest "$CNPG_VERSION")"
	kctl rollout status deployment/cnpg-controller-manager -n cnpg-system --timeout=300s
	summary "dev-env ready:"
}

down() {
	"$KIND" delete cluster --name "$KIND_CLUSTER"
}

case ${1:-} in
up) up ;;
down) down ;;
*) usage "usage: $0 up|down" ;;
esac
