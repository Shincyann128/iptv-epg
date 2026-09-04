#!/usr/bin/env python3
"""看球通(kqt29.com)直播源 M3U 生成器

从看球通 API 拉取所有在线直播间，生成 M3U 播放列表。
流地址含 auth_key，有时效性，需定时刷新。

API (与网站 /live/all 页面调用一致):
  房间列表: GET /api/room/page?roomType=&page=1&pageSize=30&channelId=3&platform=1
  热门推荐: GET /api/room/hotAppRoom?size=50&channelId=3&platform=1
  房间详情: GET /api/room/info?roomId={id}
    → pullUrl (m3u8) / pushUrl (flv)
"""

import json
import sys
import urllib.request
import ssl

API_BASE = "https://aapi20.xbncs.com"
PAGE_SIZE = 30
CHANNEL_PARAMS = "channelId=3&platform=1"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
OUTPUT = "看球通.m3u"

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# 广播分类映射
CATEGORY_MAP = {1: "足球", 2: "篮球", 3: "其他", 4: "回放", 5: "原声", 6: "电竞"}


def api_get(path: str) -> dict:
    url = f"{API_BASE}{path}"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
        return json.loads(resp.read())


def fetch_all_rooms() -> list:
    """Fetch all live rooms — unfiltered roomType (same as website '全部' tab)."""
    all_rooms = []
    seen = set()
    page = 1
    while True:
        try:
            data = api_get(
                f"/api/room/page?roomType=&navId=&roomId=&word=&page={page}"
                f"&pageSize={PAGE_SIZE}&{CHANNEL_PARAMS}"
            )
        except Exception as e:
            print(f"  WARN: page={page} error: {e}", file=sys.stderr)
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
        print(f"  page={page}: {len(rooms)} rooms (total={total}, unique={len(all_rooms)})", file=sys.stderr)
        if len(rooms) < PAGE_SIZE or len(rooms) >= total:
            break
        page += 1

    # 热门推荐（通常与 page 接口重合，兜底）
    try:
        data = api_get(f"/api/room/hotAppRoom?size=50&{CHANNEL_PARAMS}")
        if data.get("code") == 200 and data.get("data", {}).get("list"):
            hot_list = data["data"]["list"]
            new = 0
            for r in hot_list:
                rid = r.get("roomId")
                if rid and rid not in seen:
                    seen.add(rid)
                    all_rooms.append(r)
                    new += 1
            print(f"  hotAppRoom: {len(hot_list)} rooms, {new} new", file=sys.stderr)
    except Exception as e:
        print(f"  WARN: hotAppRoom error: {e}", file=sys.stderr)

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
    import re
    import time

    # 死链过滤：解析 pullUrl 中 auth_key 的 Unix 时间戳，超过 24h 视为已结束
    # （API 不清理历史房间，state 字段新旧都是 1，不可靠）
    AUTH_MAX_AGE_HOURS = 24
    now_ts = int(time.time())

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

        # 过滤过期死链（仅对有 auth_key 的 URL）
        auth_match = re.search(r"auth_key=(\d{10})-", stream_url)
        if auth_match:
            auth_ts = int(auth_match.group(1))
            age_hours = (now_ts - auth_ts) / 3600
            if age_hours > AUTH_MAX_AGE_HOURS:
                continue  # 跳过死链

        title = r.get("title", "Unknown")
        nick = info.get("nickName", "") or r.get("nickName", "")
        league = r.get("leagueName", "")
        match_type = r.get("matchType", 0)
        nav = r.get("navName", "") or CATEGORY_MAP.get(match_type, "看球通")

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

    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.join(script_dir, "..") if os.path.basename(script_dir) == "scripts" else script_dir
    output_path = os.path.join(repo_root, OUTPUT)

    print("Fetching live rooms from kqt29 API...", file=sys.stderr)
    rooms = fetch_all_rooms()
    print(f"Total rooms: {len(rooms)}", file=sys.stderr)

    if not rooms:
        print("WARNING: No live rooms found", file=sys.stderr)
        return 0

    print("Fetching stream URLs...", file=sys.stderr)
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
