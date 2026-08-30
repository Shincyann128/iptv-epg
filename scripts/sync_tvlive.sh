#!/usr/bin/env bash
# Sync all split .m3u files from GitHub to /srv/tvlive/ (excludes myself.m3u).
# Each file is served as lowercase:  Beijing.m3u -> /srv/tvlive/beijing.m3u
# Compares SHA per file — only writes to disk if content changed.
set -euo pipefail

ENV_FILE="/home/ubuntu/.hermes/.env"
[ -f "$ENV_FILE" ] && set -a && source "$ENV_FILE" && set +a
export GITHUB_TOKEN

DEST_DIR="/srv/tvlive"
LOG="/home/ubuntu/iptv-epg/tvlive_sync.log"
REPO="Shincyann128/iptv"
EPG_REPO="Shincyann128/iptv-epg"

echo "=== $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG"

# --- Sync M3U playlists ---
FILES=$(gh api "repos/$REPO/contents" --jq '.[].name' 2>>"$LOG" | grep '\.m3u$' | grep -v '^myself\.m3u$')

UPDATED=0
SKIPPED=0

while IFS= read -r remote_name; do
    [ -z "$remote_name" ] && continue

    # Lowercase + replace spaces with dashes for clean URLs
    local_name=$(echo "$remote_name" | tr '[:upper:]' '[:lower:]' | tr ' ' '-')
    dest="$DEST_DIR/$local_name"
    sha_file="${dest}.sha"

    # Get remote SHA
    remote_sha=$(gh api "repos/$REPO/contents/$remote_name" --jq '.sha' 2>>"$LOG" || echo "")

    if [ -z "$remote_sha" ]; then
        echo "WARN $remote_name: failed to get SHA" >> "$LOG"
        continue
    fi

    # Skip if unchanged
    if [ -f "$sha_file" ] && [ "$(cat "$sha_file")" = "$remote_sha" ]; then
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Fetch and write
    gh api "repos/$REPO/contents/$remote_name" --jq '.content' 2>>"$LOG" | base64 -d > "${dest}.tmp"
    mv "${dest}.tmp" "$dest"
    chmod 664 "$dest"
    echo "$remote_sha" > "$sha_file"
    UPDATED=$((UPDATED + 1))
    echo "OK: $remote_name -> $local_name ($(wc -c < "$dest") bytes)" >> "$LOG"

done <<< "$FILES"

echo "M3U: $UPDATED updated, $SKIPPED skipped" >> "$LOG"

# --- Sync EPG from iptv-epg repo via raw URL ---
EPG_DEST="$DEST_DIR/epg.xml.gz"
EPG_SHA_FILE="${EPG_DEST}.sha"
EPG_RAW_URL="https://raw.githubusercontent.com/$EPG_REPO/main/epg/epg.xml.gz"

# Get the latest SHA via API (for change detection)
EPG_SHA=$(curl -fsSL --max-time 20 \
  "https://api.github.com/repos/$EPG_REPO/commits?path=epg/epg.xml.gz&per_page=1" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[0]['sha'] if d else '')" 2>/dev/null || echo "")

if [ -n "$EPG_SHA" ]; then
    if [ -f "$EPG_SHA_FILE" ] && [ "$(cat "$EPG_SHA_FILE")" = "$EPG_SHA" ]; then
        echo "EPG: skipped (unchanged)" >> "$LOG"
    else
        curl -fsSL --max-time 60 "$EPG_RAW_URL" -o "${EPG_DEST}.tmp" 2>>"$LOG"
        if [ -s "${EPG_DEST}.tmp" ]; then
            mv "${EPG_DEST}.tmp" "$EPG_DEST"
            chmod 664 "$EPG_DEST"
            echo "$EPG_SHA" > "$EPG_SHA_FILE"
            echo "EPG: updated ($(wc -c < "$EPG_DEST") bytes)" >> "$LOG"
        else
            echo "WARN: EPG download produced empty file" >> "$LOG"
            rm -f "${EPG_DEST}.tmp"
        fi
    fi
else
    echo "WARN: failed to get EPG commit SHA" >> "$LOG"
fi

echo "Done: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
