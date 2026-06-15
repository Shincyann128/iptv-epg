#!/usr/bin/env bash
# Sync myself.m3u: prefer local git repo, fall back to GitHub API.
# Runs once daily via crontab.
set -euo pipefail

CACHE_FILE="/home/ubuntu/iptv-epg/myself.m3u"
LOG_FILE="/home/ubuntu/iptv-epg/myself_sync.log"
LOCAL_REPO="/home/ubuntu/iptv/myself.m3u"

echo "=== $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"

# Try local git repo first (always up-to-date after push)
if [ -f "$LOCAL_REPO" ]; then
    cp "$LOCAL_REPO" "$CACHE_FILE"
    echo "myself.m3u copied from local repo ($(wc -c < "$CACHE_FILE") bytes)" >> "$LOG_FILE"
else
    # Fallback: GitHub API (requires gh auth)
    if gh api repos/Shincyann128/iptv/contents/myself.m3u --jq '.content' 2>>"$LOG_FILE" | base64 -d > "${CACHE_FILE}.tmp" 2>>"$LOG_FILE"; then
        mv "${CACHE_FILE}.tmp" "$CACHE_FILE"
        echo "myself.m3u synced from GitHub ($(wc -c < "$CACHE_FILE") bytes)" >> "$LOG_FILE"
    else
        echo "ERROR: both local copy and GitHub fetch failed, keeping existing cache" >> "$LOG_FILE"
        rm -f "${CACHE_FILE}.tmp"
    fi
fi