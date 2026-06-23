#!/usr/bin/env bash
set -euo pipefail
cd /home/ubuntu/iptv-epg
echo "=== $(date '+%Y-%m-%d %H:%M:%S') ===" >> /home/ubuntu/iptv-epg/merge_live_m3u.log
/usr/bin/env python3 scripts/merge_live_m3u.py >> /home/ubuntu/iptv-epg/merge_live_m3u.log 2>&1
install -d /srv/iptv
install -m 644 /home/ubuntu/iptv-epg/live_merged.m3u /srv/iptv/live_merged.m3u
