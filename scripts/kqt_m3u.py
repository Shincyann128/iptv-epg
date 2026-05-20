#!/usr/bin/env python3
"""看球通(kqt29.com)直播源 M3U 生成器

从看球通 API 拉取所有在线直播间，生成 M3U 播放列表。
流地址含 auth_key，有时效性，需定时刷新。

API:
  房间列表: GET https://aapi2.xbncs.com/api/room/page?roomType={1|2|3}&pageNum=1&pageSize=30
  房间详情: GET https://aapi2.xbncs.com/api/room/info?roomId={id}
    → pullUrl (m3u8) / pushUrl (flv)

roomType: 1=足球, 2=篮球, 3=其他
"""

import json
import sys
import urllib.request
import ssl

API_BASE = "https://aapi2.xbncs.com"
ROOM_TYPES = {1: "足球", 2: "篮球", 3: "其他"}
PAGE_SIZE = 30
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
OUTPUT = "看球通.m3u"

# Allow self-signed certs if needed
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def api_get(path: str) -> dict:
    url = f"{API_BASE}{path}"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
        return json.loads(resp.read())


def fetch_hot_rooms() -> list:
    """Fetch hot/featured rooms from hotAppRoom endpoint — these may not appear in room/page."""
    try:
        data = api_get("/api/room/hotAppRoom?size=50&channelId=3&platform=1")
        if data.get("code") == 200 and data.get("data", {}).get("list"):
            return data["data"]["list"]
    except Exception as e:
        print(f"  WARN: hotAppRoom error: {e}", file=sys.stderr)
    return []


def fetch_all_rooms() -> list:
    """Fetch all live rooms across all types + hot rooms, deduplicated by roomId."""
    all_rooms = []
    seen = set()
    for rt, label in ROOM_TYPES.items():
        page = 1
        while True:
            try:
                data = api_get(f"/api/room/page?roomType={rt}&pageNum={page}&pageSize={PAGE_SIZE}")
            except Exception as e:
                print(f"  WARN: roomType={rt} page={page} error: {e}", file=sys.stderr)
                break
            if data.get("code") != 200 or not data.get("data"):
                break
            rooms = data["data"].get("list", [])
            for r in rooms:
                rid = r.get("roomId")
                if rid and rid not in seen:
                    seen.add(rid)
                    all_rooms.append(r)
            total = data["data"].get("total", 0)
            if len(rooms) < PAGE_SIZE or len(all_rooms) >= total:
                break
            page += 1
        print(f"  roomType={rt}({label}): {len(rooms)} rooms", file=sys.stderr)
    
    # Also fetch hot rooms (may include rooms not in page listing)
    hot_rooms = fetch_hot_rooms()
    for r in hot_rooms:
        rid = r.get("roomId")
        if rid and rid not in seen:
            seen.add(rid)
            all_rooms.append(r)
    print(f"  hotAppRoom: {len(hot_rooms)} rooms, new={len([r for r in hot_rooms if r.get('roomId') not in seen or True])}", file=sys.stderr)
    print(f"  total after merge: {len(all_rooms)}", file=sys.stderr)
    return all_rooms


def fetch_room_info(room_id: int) -> dict:
    """Fetch room info to get stream URLs."""
    try:
        data = api_get(f"/api/room/info?roomId={room_id}")
        if data.get("code") == 200 and data.get("data"):
            return data["data"]
    except Exception as e:
        print(f"  WARN: room/info roomId={room_id} error: {e}", file=sys.stderr)
    return {}


def generate_m3u(rooms: list) -> str:
    """Generate M3U content from room list."""
    lines = ["#EXTM3U"]
    for r in rooms:
        rid = r.get("roomId")
        if not rid:
            continue

        info = fetch_room_info(rid)
        pull_url = info.get("pullUrl", "")
        push_url = info.get("pushUrl", "")
        stream_url = pull_url or push_url

        if not stream_url:
            continue

        title = r.get("title", "Unknown")
        nick = info.get("nickName", "") or r.get("nickName", "")
        league = r.get("leagueName", "")
        nav = r.get("navName", "") or ROOM_TYPES.get(r.get("matchType", 0), "看球通")

        label = title
        if nick:
            label += f" - {nick}"
        if league:
            label += f" [{league}]"

        lines.append(f'#EXTINF:-1 group-title="看球通-{nav}",{label}')
        lines.append(stream_url)

    return "\n".join(lines) + "\n"


def main():
    import os

    # Script is in the repo root or scripts/ dir; output goes to repo root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.join(script_dir, "..") if os.path.basename(script_dir) == "scripts" else script_dir
    output_path = os.path.join(repo_root, OUTPUT)

    print(f"Fetching live rooms from kqt29 API...", file=sys.stderr)
    rooms = fetch_all_rooms()
    print(f"Total rooms: {len(rooms)}", file=sys.stderr)

    print(f"Fetching stream URLs...", file=sys.stderr)
    m3u = generate_m3u(rooms)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(m3u)

    channel_count = m3u.count("#EXTINF")
    print(f"Written {channel_count} channels to {output_path}", file=sys.stderr)
    return channel_count


if __name__ == "__main__":
    count = main()
    if count == 0:
        print("WARNING: No channels found (might be off-peak hours)", file=sys.stderr)
