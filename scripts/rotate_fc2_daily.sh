#!/usr/bin/env bash
# rotate_fc2_daily.sh — pick 3 random FC2 entries daily into xxx.m3u (marked block).
# Runs once daily via crontab. Idempotent per day: skips if today's block already written.
set -euo pipefail

# 防止与 sync_tvlive / merge_live_m3u 并发冲突
exec 9>/tmp/fc2-rotate.lock
flock -n 9 || { echo "$(date '+%F %T') WARN: another fc2 rotate running, skip" >> /home/ubuntu/iptv-epg/fc2_rotate/rotate.log; exit 0; }

REPO_DIR="/home/ubuntu/iptv"
FC2_SRC="$REPO_DIR/fc2.m3u"
XXX="$REPO_DIR/xxx.m3u"
STATE_DIR="/home/ubuntu/iptv-epg/fc2_rotate"
TODAY=$(date +%F)
SEED="seed-$TODAY"
MARK_S="# ---fc2-daily start---"
MARK_E="# ---fc2-daily end---"
PICKS=3
MAX_TRIES=40

LOG="$STATE_DIR/rotate.log"
mkdir -p "$STATE_DIR"

log(){ echo "$(date '+%F %T') $*" >> "$LOG"; }

log "=== start ($(date '+%F %T')) ==="

# Already rotated today? -> exit silently
if [ -f "$STATE_DIR/last_date" ] && [ "$(cat "$STATE_DIR/last_date")" = "$TODAY" ]; then
    log "already rotated today, skip"
    exit 0
fi

# Fresh pool from local repo copy (kept in sync by git push flow)
[ -f "$FC2_SRC" ] || { log "ERROR: fc2.m3u missing, keep yesterday"; exit 1; }

cd "$REPO_DIR"
timeout 30 git pull --rebase -q origin main 2>>"$LOG" || log "WARN: git pull failed/timeout, using local state"

# Build clean pool (strict EXTINF+URL pairs), exclude recently used URLs
python3 - "$FC2_SRC" > "$STATE_DIR/pool.txt" <<'PYEOF'
import sys
lines = open(sys.argv[1], encoding='utf-8').read().splitlines()[1:]
recent = set()
try:
    recent = set(open('/home/ubuntu/iptv-epg/fc2_rotate/recent_urls.txt', encoding='utf-8').read().split())
except FileNotFoundError:
    pass
for k in range(0, len(lines) - 1, 2):
    ext, url = lines[k].strip(), lines[k+1].strip()
    if ext.startswith('#EXTINF') and url.startswith('http') and url not in recent:
        print(ext + '\t' + url)
PYEOF

POOL_N=$(wc -l < "$STATE_DIR/pool.txt")
[ "$POOL_N" -ge "$PICKS" ] || { log "ERROR: pool too small ($POOL_N), keep yesterday"; exit 1; }

# Pick N alive entries (probe each candidate; skip dead ones)
: > "$STATE_DIR/picked.txt"
TRY=0
while [ "$(wc -l < "$STATE_DIR/picked.txt")" -lt "$PICKS" ] && [ "$TRY" -lt "$MAX_TRIES" ]; do
    TRY=$((TRY+1))
    LINE=$(shuf -n 1 "$STATE_DIR/pool.txt")
    URL=$(printf '%s' "$LINE" | cut -f2)
    grep -qxF "$URL" <(cut -f2 "$STATE_DIR/picked.txt") 2>/dev/null && continue
    # probe: m3u8 must respond 200
    CODE=$(curl -sL --max-time 12 -o /dev/null -w '%{http_code}' "$URL" || echo 000)
    if [ "$CODE" = "200" ]; then
        printf '%s\n' "$LINE" >> "$STATE_DIR/picked.txt"
        log "pick ok ($CODE): $(printf '%s' "$LINE" | cut -c1-70)"
    else
        log "dead ($CODE): $(printf '%s' "$LINE" | cut -c1-70)"
    fi
done

if [ "$(wc -l < "$STATE_DIR/picked.txt")" -lt "$PICKS" ]; then
    log "ERROR: only found $(wc -l < "$STATE_DIR/picked.txt")/$PICKS alive after $MAX_TRIES tries, keep yesterday"
    exit 1
fi

# Rebuild xxx.m3u replacing the marked block (create block if absent)
python3 - "$XXX" "$STATE_DIR/picked.txt" <<'PYEOF'
import sys
xxx_path, picked_path = sys.argv[1], sys.argv[2]
picked = []
for ln in open(picked_path, encoding='utf-8'):
    ext, url = ln.rstrip('\n').split('\t', 1)
    picked.append((ext, url))
body_lines = []
for i, (ext, url) in enumerate(picked):
    # force group-title to xxx, keep logo/name
    import re
    ext = re.sub(r'group-title="[^"]*"', 'group-title="xxx"', ext)
    body_lines.append(ext.replace('#EXTINF:-1,', '#EXTINF:-1 ', 1) if ' tvg-' in ext else ext)
    body_lines.append(url)
S, E = '# ---fc2-daily start---', '# ---fc2-daily end---'
lines = open(xxx_path, encoding='utf-8').read().splitlines()
out, inside, replaced = [], False, False
for ln in lines:
    if ln.strip() == S:
        inside = True; replaced = True
        out.append(ln); out.extend(body_lines); continue
    if ln.strip() == E:
        inside = False; out.append(ln); continue
    if not inside:
        out.append(ln)
if not replaced:
    out.append(S); out.extend(body_lines); out.append(E)
open(xxx_path, 'w', encoding='utf-8').write('\n'.join(out).rstrip() + '\n')
print(f'block written: {len(picked)} entries')
PYEOF

# Record used urls (keep last 60 days worth)
cut -f2 "$STATE_DIR/picked.txt" >> "$STATE_DIR/recent_urls.txt"
tail -180 "$STATE_DIR/recent_urls.txt" > "$STATE_DIR/recent_urls.tmp" && mv "$STATE_DIR/recent_urls.tmp" "$STATE_DIR/recent_urls.txt"

chmod 664 "$XXX"
git add "$XXX"
git commit -q -m "fc2 daily rotation $TODAY" || { log "no changes to commit"; exit 0; }
if ! git push -q origin main 2>>"$LOG"; then
    log "ERROR: git push failed, xxx.m3u unchanged on GitHub"
    exit 1
fi

echo "$TODAY" > "$STATE_DIR/last_date"
log "OK: pushed 3 picks for $TODAY, triggering sync_tvlive"

# Trigger immediate sync under lock to avoid racing the 3h cron
flock /tmp/tvlive-sync.lock bash /home/ubuntu/iptv-epg/scripts/sync_tvlive.sh >/dev/null 2>&1 || true
chmod 664 /srv/tvlive/*.m3u 2>/dev/null || true
log "DONE"
