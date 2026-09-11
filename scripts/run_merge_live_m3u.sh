#!/usr/bin/env bash
set -euo pipefail
cd /home/ubuntu/iptv-epg
echo "=== $(date '+%Y-%m-%d %H:%M:%S') ===" >> /home/ubuntu/iptv-epg/merge_live_m3u.log
/usr/bin/env python3 scripts/merge_live_m3u.py >> /home/ubuntu/iptv-epg/merge_live_m3u.log 2>&1
install -d /srv/iptv
install -m 644 /home/ubuntu/iptv-epg/live_merged.m3u /srv/iptv/live_merged.m3u
# 2026-09-11 用户要求：live_merged.m3u 只保留 sports 域名一份（/srv/iptv）。
# tvlive 域名下不再放副本，旧链接 https://tvlive.shincyann.com/live_merged.m3u
# 由 nginx 301 跳到 sports.shincyann.com（见 /etc/nginx/conf.d/tvlive.conf）。
rm -f /srv/tvlive/live_merged.m3u
