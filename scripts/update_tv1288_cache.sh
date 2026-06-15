#!/usr/bin/env bash
set -euo pipefail
cd /home/ubuntu/iptv-epg
CACHE=/home/ubuntu/iptv-epg/tv1288_cache.txt
TMP=$(mktemp /home/ubuntu/iptv-epg/tv1288_cache.XXXXXX)
cleanup() { rm -f "$TMP"; }
trap cleanup EXIT
curl -fsSL --max-time 30 -A 'Mozilla/5.0' 'https://itv.tv1288.xyz' -o "$TMP"
if ! grep -q '#genre#' "$TMP"; then
  echo "tv1288 cache update failed: invalid content" >&2
  exit 1
fi
if [ "$(wc -c < "$TMP")" -lt 1000 ]; then
  echo "tv1288 cache update failed: content too small" >&2
  exit 1
fi
install -m 644 "$TMP" "$CACHE"
echo "tv1288 cache updated: $(wc -c < "$CACHE") bytes"
