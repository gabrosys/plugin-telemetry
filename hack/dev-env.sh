#!/usr/bin/env bash
# Usage: dev-env.sh up|down
# Environment: KIND, KUBECTL, KIND_CLUSTER, KIND_NODE_IMAGE, CNPG_VERSION, CERT_MANAGER_VERSION.
set -euo pipefail

: "${KIND:?}" "${KUBECTL:?}" "${KIND_CLUSTER:?}"

marker=plugin-telemetry-dev-env
marker_keys=(cnpgVersion certManagerVersion kindNodeImage)

kctl() { "$KUBECTL" --context "kind-${KIND_CLUSTER}" "$@"; }

usage() {
	echo "dev-env: $*" >&2
	exit 2
}

cnpg_manifest() {
	case $1 in
	main) echo "https://raw.githubusercontent.com/cloudnative-pg/artifacts/main/manifests/operator-manifest.yaml" ;;
	*) echo "https://github.com/cloudnative-pg/cloudnative-pg/releases/download/$1/cnpg-${1#v}.yaml" ;;
	esac
}

summary() {
	local node=${KIND_NODE_IMAGE%%@*} image
	image=$(kctl get deployment cnpg-controller-manager -n cnpg-system \
		-o jsonpath='{.spec.template.spec.containers[?(@.name=="manager")].image}')
	echo "$1 kind=${KIND_CLUSTER} node=${node##*:} cert-manager=${CERT_MANAGER_VERSION} cnpg=${CNPG_VERSION} operator-image=${image}"
}

wait_cert_manager_webhook() {
	local deadline=$((SECONDS + 120))
	until kctl apply --dry-run=server -f - >/dev/null 2>&1 <<-EOF; do
		apiVersion: cert-manager.io/v1
		kind: Issuer
		metadata:
		  name: dev-env-probe
		  namespace: default
		spec:
		  selfSigned: {}
	EOF
		((SECONDS < deadline)) || {
			echo "dev-env: cert-manager webhook not serving after 120s" >&2
			exit 1
		}
		sleep 5
	done
}

up() {
	: "${KIND_NODE_IMAGE:?}" "${CNPG_VERSION:?}" "${CERT_MANAGER_VERSION:?}"
	[[ $CNPG_VERSION =~ ^(main|v[0-9]+\.[0-9]+\.[0-9]+)$ ]] ||
		usage "CNPG_VERSION must be main or vX.Y.Z, got '$CNPG_VERSION'"

	if ! "$KIND" get clusters | grep -qx "$KIND_CLUSTER"; then
		"$KIND" create cluster --name "$KIND_CLUSTER" --image "$KIND_NODE_IMAGE" --wait 120s
	fi

	local -A want=(
		[cnpgVersion]=$CNPG_VERSION
		[certManagerVersion]=$CERT_MANAGER_VERSION
		[kindNodeImage]=$KIND_NODE_IMAGE
	)
	if kctl get configmap -n default "$marker" >/dev/null 2>&1; then
		local key have
		for key in "${marker_keys[@]}"; do
			have=$(kctl get configmap -n default "$marker" -o "jsonpath={.data.$key}")
			[[ $have == "${want[$key]}" ]] ||
				usage "environment has $key=$have, requested ${want[$key]}; run 'make dev-down' first"
		done
		kctl rollout status deployment/cnpg-controller-manager -n cnpg-system --timeout=300s >/dev/null
		summary "dev-env up to date:"
		return
	fi

	kctl apply --server-side -f \
		"https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
	kctl wait --for=condition=Available deployment --all -n cert-manager --timeout=180s
	wait_cert_manager_webhook

	kctl apply --server-side -f "$(cnpg_manifest "$CNPG_VERSION")"
	kctl rollout status deployment/cnpg-controller-manager -n cnpg-system --timeout=300s

	kctl create configmap -n default "$marker" \
		--from-literal=cnpgVersion="$CNPG_VERSION" \
		--from-literal=certManagerVersion="$CERT_MANAGER_VERSION" \
		--from-literal=kindNodeImage="$KIND_NODE_IMAGE" \
		--dry-run=client -o yaml | kctl apply -f -
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
