#!/usr/bin/env bash
# Usage: kind-image-ref.sh CLUSTER IMAGE
# Environment: KIND.
# Tags IMAGE as REPO@DIGEST on every node of CLUSTER and prints REPO@DIGEST.
set -euo pipefail

: "${KIND:?}"

if [[ $# -ne 2 ]]; then
	echo "usage: $0 CLUSTER IMAGE" >&2
	exit 2
fi

cluster=$1
image=$2
if [[ ${image##*/} == *:* ]]; then
	repo=${image%:*}
else
	repo=$image
	image=$image:latest
fi

nodes=$("$KIND" get nodes --name "$cluster")
[[ -n $nodes ]] || {
	echo "kind-image-ref: no nodes for cluster $cluster" >&2
	exit 1
}

digest=
while IFS= read -r node; do
	node_digest=$(docker exec "$node" ctr -n k8s.io images ls "name==$image" | awk -v ref="$image" '$1 == ref {print $3}')
	[[ $node_digest =~ ^sha256:[0-9a-f]{64}$ ]] || {
		echo "kind-image-ref: $image not found on node $node" >&2
		exit 1
	}
	if [[ -n $digest && $node_digest != "$digest" ]]; then
		echo "kind-image-ref: nodes disagree on the digest of $image ($digest, $node_digest)" >&2
		exit 1
	fi
	digest=$node_digest
	docker exec "$node" ctr -n k8s.io images tag --force "$image" "$repo@$digest" >/dev/null
done <<<"$nodes"

echo "$repo@$digest"
