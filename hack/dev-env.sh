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

fail() {
	echo "dev-env: $*" >&2
	exit 1
}

wait_reachable() {
	"$KIND" export kubeconfig --name "$KIND_CLUSTER" >/dev/null 2>&1 ||
		fail "cluster $KIND_CLUSTER exists but is not reachable; run 'make dev-down' first"
	local deadline=$((SECONDS + 30))
	until kctl get --raw=/readyz >/dev/null 2>&1; do
		((SECONDS < deadline)) || fail "cluster $KIND_CLUSTER exists but is not reachable; run 'make dev-down' first"
		sleep 2
	done
	kctl wait --for=condition=Ready node --all --timeout=120s >/dev/null 2>&1 ||
		fail "cluster $KIND_CLUSTER node is not ready; run 'make dev-down' first"
}

write_marker() {
	kctl create configmap -n default "$marker" \
		--from-literal=cnpgVersion="$CNPG_VERSION" \
		--from-literal=certManagerVersion="$CERT_MANAGER_VERSION" \
		--from-literal=kindNodeImage="$KIND_NODE_IMAGE" \
		--from-literal=complete="$1" \
		--dry-run=client -o yaml | kctl apply -f - >/dev/null
}

up() {
	: "${KIND_NODE_IMAGE:?}" "${CNPG_VERSION:?}" "${CERT_MANAGER_VERSION:?}"
	[[ $CNPG_VERSION =~ ^(main|v[0-9]+\.[0-9]+\.[0-9]+)$ ]] ||
		usage "CNPG_VERSION must be main or vX.Y.Z, got '$CNPG_VERSION'"

	local clusters
	clusters=$("$KIND" get clusters)
	if ! grep -qx "$KIND_CLUSTER" <<<"$clusters"; then
		"$KIND" create cluster --name "$KIND_CLUSTER" --image "$KIND_NODE_IMAGE" --wait 120s
	fi
	wait_reachable

	local -A want=(
		[cnpgVersion]=$CNPG_VERSION
		[certManagerVersion]=$CERT_MANAGER_VERSION
		[kindNodeImage]=$KIND_NODE_IMAGE
	)
	local -A have=()
	local recorded line key
	# shellcheck disable=SC2016 # Go template, not a shell expansion
	recorded=$(kctl get configmap -n default "$marker" --ignore-not-found \
		-o go-template='{{range $k, $v := .data}}{{$k}}={{$v}}{{"\n"}}{{end}}')
	while IFS= read -r line; do
		if [[ -n $line ]]; then have[${line%%=*}]=${line#*=}; fi
	done <<<"$recorded"

	if [[ -n $recorded ]]; then
		for key in "${marker_keys[@]}"; do
			[[ ${have[$key]:-} == "${want[$key]}" ]] ||
				usage "environment has $key=${have[$key]:-}, requested ${want[$key]}; run 'make dev-down' first"
		done
		if [[ ${have[complete]:-} == true ]]; then
			kctl rollout status deployment/cnpg-controller-manager -n cnpg-system --timeout=300s >/dev/null
			summary "dev-env up to date:"
			return
		fi
	else
		write_marker false
	fi

	kctl apply --server-side -f \
		"https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
	kctl wait --for=condition=Available deployment --all -n cert-manager --timeout=180s
	wait_cert_manager_webhook

	kctl apply --server-side -f "$(cnpg_manifest "$CNPG_VERSION")"
	kctl rollout status deployment/cnpg-controller-manager -n cnpg-system --timeout=300s

	write_marker true
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
