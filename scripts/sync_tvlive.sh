#!/usr/bin/env bash
# Publish the split M3U files from a single GitHub revision and refresh XMLTV.
set -euo pipefail

ENV_FILE="/home/ubuntu/.hermes/.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    source "$ENV_FILE"
    set +a
fi
export GITHUB_TOKEN

DEST_DIR="/srv/tvlive"
LOG="/home/ubuntu/iptv-epg/tvlive_sync.log"
REPO="Shincyann128/iptv"
EPG_REPO="Shincyann128/iptv-epg"

# The scheduled run, livecam updates, and sports refresh share this publisher.
exec 9>"$LOG.lock"
flock -w 60 9
exec >>"$LOG" 2>&1

stage=$(mktemp -d "$DEST_DIR/.tvlive-sync.XXXXXX")
trap 'rm -rf -- "$stage"' EXIT

marker() {
    local name=$1 sha=$2
    if [[ ! -f "$DEST_DIR/$name.sha" ]] || [[ $(<"$DEST_DIR/$name.sha") != "$sha" ]]; then
        printf '%s\n' "$sha" >"$stage/$name.sha"
        chmod 664 "$stage/$name.sha"
        mv -fT -- "$stage/$name.sha" "$DEST_DIR/$name.sha"
    fi
}

same_blob() {
    local path=$1 sha=$2
    [[ -f "$path" ]] && [[ $(git hash-object -- "$path") == "$sha" ]]
}

echo "=== $(date '+%Y-%m-%d %H:%M:%S') ==="
revision=$(gh api "repos/$REPO/git/ref/heads/main" --jq '.object.sha')
[[ "$revision" =~ ^[0-9a-f]{40}$ ]]
listing=$(gh api "repos/$REPO/contents?ref=$revision" \
    --jq '.[] | select(.type=="file" and (.name | endswith(".m3u")) and .name!="myself.m3u") | [.name,.sha] | @tsv')
[[ -n "$listing" ]]
echo "M3U revision $revision"

declare -A blobs
names=()
updates=()
skipped=0
while IFS=$'\t' read -r remote_name blob; do
    [[ "$blob" =~ ^[0-9a-f]{40}$ ]]
    name=$(printf '%s' "$remote_name" | tr '[:upper:] ' '[:lower:]-')
    if [[ -z "$name" || "$name" == .* || "$name" == */* || -n "${blobs[$name]+set}" ]]; then
        echo "ERROR: invalid or duplicate publication name $remote_name"
        exit 1
    fi
    names+=("$name")
    blobs[$name]=$blob

    if same_blob "$DEST_DIR/$name" "$blob"; then
        skipped=$((skipped + 1))
        continue
    fi

    gh api "repos/$REPO/git/blobs/$blob" --jq '.content' | base64 -d >"$stage/$name"
    if ! same_blob "$stage/$name" "$blob" || ! grep -aq '^#EXTINF:' "$stage/$name"; then
        echo "ERROR: failed verification for $remote_name"
        exit 1
    fi
    chmod 664 "$stage/$name"
    updates+=("$name")
done <<<"$listing"

for name in "${updates[@]}"; do
    mv -fT -- "$stage/$name" "$DEST_DIR/$name"
    echo "OK: $name ($(wc -c <"$DEST_DIR/$name") bytes)"
done
for name in "${names[@]}"; do
    marker "$name" "${blobs[$name]}"
done
echo "M3U: ${#updates[@]} updated, $skipped unchanged"

# Verify EPG independently so an unavailable guide never replaces the last good one.
epg_name=epg.xml.gz
if ! epg_revision=$(gh api "repos/$EPG_REPO/git/ref/heads/main" --jq '.object.sha') ||
    [[ ! "$epg_revision" =~ ^[0-9a-f]{40}$ ]] ||
    ! epg_blob=$(gh api "repos/$EPG_REPO/contents/epg/$epg_name?ref=$epg_revision" --jq '.sha') ||
    [[ ! "$epg_blob" =~ ^[0-9a-f]{40}$ ]]; then
    echo "ERROR: could not identify the published EPG"
    exit 1
fi
if same_blob "$DEST_DIR/$epg_name" "$epg_blob"; then
    echo "EPG: unchanged"
else
    if ! curl -fsSL --max-time 60 \
        "https://raw.githubusercontent.com/$EPG_REPO/$epg_revision/epg/$epg_name" \
        -o "$stage/$epg_name" ||
        ! same_blob "$stage/$epg_name" "$epg_blob" ||
        ! gzip -t "$stage/$epg_name"; then
        echo "ERROR: EPG download or verification failed"
        exit 1
    fi
    chmod 664 "$stage/$epg_name"
    mv -fT -- "$stage/$epg_name" "$DEST_DIR/$epg_name"
    echo "EPG: updated ($(wc -c <"$DEST_DIR/$epg_name") bytes)"
fi
marker "$epg_name" "$epg_blob"
echo "Done: $(date '+%Y-%m-%d %H:%M:%S')"
