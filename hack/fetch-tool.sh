#!/usr/bin/env bash
# Usage: fetch-tool.sh URL CHECKSUM_URL DEST [TAR_MEMBER]
# Installs URL at DEST after checking its SHA-256 against CHECKSUM_URL.
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
	echo "usage: $0 URL CHECKSUM_URL DEST [TAR_MEMBER]" >&2
	exit 2
fi

url=$1
checksum_url=$2
dest=$3
member=${4:-}

asset=$(basename "$url")
tmp=$(mktemp -d)
staged="$dest.tmp.$$"
trap 'rm -rf "$tmp" "$staged"' EXIT

curl --proto '=https' -fsSL --retry 3 -o "$tmp/$asset" "$url"
curl --proto '=https' -fsSL --retry 3 -o "$tmp/checksums" "$checksum_url"

if [[ $(grep -c . "$tmp/checksums") -le 1 ]]; then
	want=$(awk '{print $1; exit}' "$tmp/checksums")
else
	want=$(awk -v f="$asset" '$2 == f || $2 == "*" f {print $1; exit}' "$tmp/checksums")
fi
if [[ ! $want =~ ^[0-9a-f]{64}$ ]]; then
	echo "fetch-tool: no SHA-256 for $asset in $checksum_url" >&2
	exit 1
fi
if ! echo "$want  $tmp/$asset" | sha256sum --check --quiet --strict - >&2; then
	echo "fetch-tool: checksum mismatch for $asset" >&2
	exit 1
fi

src="$tmp/$asset"
if [[ -n $member ]]; then
	tar -xzf "$tmp/$asset" -C "$tmp" "$member"
	src="$tmp/$member"
fi

mkdir -p "$(dirname "$dest")"
cp "$src" "$staged"
chmod 0755 "$staged"
mv -f "$staged" "$dest"
echo "fetch-tool: installed $dest"
