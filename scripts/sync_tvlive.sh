#!/usr/bin/env bash
# Incrementally sync myself.m3u from GitHub.
# Compares SHA — only writes to disk if content changed.
set -euo pipefail

# Load GitHub token from .env
ENV_FILE="$HOME/.hermes/.env"
[ -f "$ENV_FILE" ] && set -a && source "$ENV_FILE" && set +a
export GITHUB_TOKEN

DEST="/srv/tvlive/myself.m3u"
SHA_FILE="${DEST}.sha"
LOG="/home/ubuntu/iptv-epg/tvlive_sync.log"

echo "=== $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG"

# Get file metadata + content from GitHub (one API call)
RESP=$(gh api repos/Shincyann128/iptv/contents/myself.m3u 2>>"$LOG")
REMOTE_SHA=$(echo "$RESP" | jq -r '.sha')

# Compare with cached SHA — skip if unchanged
if [ -f "$SHA_FILE" ] && [ "$(cat "$SHA_FILE")" = "$REMOTE_SHA" ]; then
    echo "SKIP: unchanged" >> "$LOG"
    exit 0
fi

# SHA differs — decode and write
echo "$RESP" | jq -r '.content' | base64 -d > "${DEST}.tmp"
mv "${DEST}.tmp" "$DEST"
echo "$REMOTE_SHA" > "$SHA_FILE"
echo "OK: updated ($(wc -c < "$DEST") bytes)" >> "$LOG"
