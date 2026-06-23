#!/usr/bin/env python3
"""Fetch live streams from 857直播 and render as M3U.

857 uses JSONP endpoints. Stream URLs carry short-lived auth_key values, so this
script must fetch room details on every run.
"""
from __future__ import annotations

import json
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import urljoin, urlparse, parse_qs

BASE = "https://json.yyzb456.top"
LIST_URL = BASE + "/all_live_rooms.json"
DETAIL_URL = BASE + "/room/{room_num}/detail.json"
TIMEOUT = 10
MAX_WORKERS = 8
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36"
STREAM_FIELDS = ("hdM3u8", "m3u8", "hdFlv", "flv")

TYPE_PARENT_GROUPS = {
    1: "足球",
    2: "篮球",
    3: "电竞",
}

TYPE_GROUPS = {
    1: "足球",
    2: "篮球",
    3: "电竞",
    13: "足球",
    18: "足球",
}


def fetch_text(url: str) -> str:
    sep = "&" if "?" in url else "?"
    req = urllib.request.Request(
        f"{url}{sep}v={int(time.time() * 1000)}",
        headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        return resp.read().decode("utf-8", "ignore")


def parse_jsonp(text: str) -> dict[str, Any]:
    text = text.strip()
    match = re.match(r"^[\w$]+\((.*)\)\s*;?$", text, re.S)
    if match:
        text = match.group(1)
    return json.loads(text)


def flatten_rooms(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        rooms: list[dict[str, Any]] = []
        for value in data.values():
            rooms.extend(flatten_rooms(value))
        return rooms
    return []


def fetch_rooms() -> list[dict[str, Any]]:
    payload = parse_jsonp(fetch_text(LIST_URL))
    if payload.get("code") != 200:
        raise RuntimeError(f"list api returned code={payload.get('code')} msg={payload.get('msg')}")
    rooms = flatten_rooms(payload.get("data", {}))
    seen = set()
    result = []
    for room in rooms:
        room_num = str(room.get("roomNum") or "").strip()
        if not room_num or room_num in seen:
            continue
        seen.add(room_num)
        if room.get("liveStatus") not in (None, 1, "1"):
            continue
        result.append(room)
    return result


def fetch_detail(room_num: str) -> dict[str, Any] | None:
    try:
        payload = parse_jsonp(fetch_text(DETAIL_URL.format(room_num=room_num)))
        if payload.get("code") != 200:
            return None
        return payload.get("data") or None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        return None


def choose_stream(stream: dict[str, Any]) -> str:
    for key in STREAM_FIELDS:
        value = stream.get(key)
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            return value.replace("\\u003d", "=")
    return ""


def is_stale_auth_key(url: str, max_age_hours: int = 24) -> bool:
    parsed = urlparse(url)
    auth_key = parse_qs(parsed.query).get("auth_key", [""])[0]
    match = re.match(r"(\d{10})-", auth_key)
    if not match:
        return False
    return (time.time() - int(match.group(1))) > max_age_hours * 3600


def resolve_m3u8_url(url: str) -> str:
    """Resolve 857 master playlist to the direct sub-playlist URL.

    857's CDN returns a master playlist (with EXT-X-STREAM-INF) pointing to
    a livehwc4.com sub-playlist. Some IPTV players don't follow master
    playlists, so we resolve it server-side and put the sub-playlist URL in
    the M3U directly.
    """
    parsed = urlparse(url)
    if not parsed.path.endswith(('.m3u8', '.M3U8')):
        return url  # FLV streams don't need resolution
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            text = resp.read().decode("utf-8", "ignore")
        expect_variant = False
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("#EXT-X-STREAM-INF"):
                expect_variant = True
                continue
            if expect_variant and not line.startswith("#"):
                return urljoin(url, line)
            if line.startswith(("http://", "https://")) and "livehwc" in line:
                return line
    except Exception:
        pass
    return url  # fall back to original if resolution fails


def group_for(room: dict[str, Any]) -> str:
    parent = room.get("liveTypeParent")
    live_type = room.get("liveType")
    if parent in TYPE_PARENT_GROUPS:
        return TYPE_PARENT_GROUPS[parent]
    if live_type in TYPE_GROUPS:
        return TYPE_GROUPS[live_type]
    title = str(room.get("title") or "")
    if any(kw in title for kw in ("NBA", "CBA", "篮球", "WNBA")):
        return "篮球"
    if any(kw in title for kw in ("电竞", "LOL", "DOTA", "CS2")):
        return "电竞"
    if any(kw in title for kw in ("足球", "中超", "英超", "西甲", "德甲", "意甲", "法甲", "欧冠", "瑞典超", "vs", "VS")):
        return "足球"
    return "综合"


def build_entry(room: dict[str, Any], detail: dict[str, Any]) -> dict[str, str] | None:
    detail_room = detail.get("room") or room
    stream = detail.get("stream") or {}
    url = choose_stream(stream)
    if not url:
        return None
    if is_stale_auth_key(url):
        return None
    name = str(detail_room.get("title") or room.get("title") or room.get("roomNum") or "857直播").strip()
    if not name:
        return None
    return {
        "name": name,
        "group": group_for(detail_room),
        "url": resolve_m3u8_url(url),
        "logo": str(detail_room.get("customCoverUrl") or detail_room.get("cover") or ""),
    }


def build_entries(rooms: list[dict[str, Any]]) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(fetch_detail, str(room.get("roomNum"))): room
            for room in rooms
            if room.get("roomNum")
        }
        for future in as_completed(future_map):
            room = future_map[future]
            detail = future.result()
            if not detail:
                continue
            entry = build_entry(room, detail)
            if entry:
                entries.append(entry)
    entries.sort(key=lambda e: (e["group"], e["name"]))
    return entries


def escape_attr(value: str) -> str:
    return value.replace('"', "'").replace("\n", " ").strip()


def render(entries: list[dict[str, str]]) -> str:
    lines = ["#EXTM3U"]
    for entry in entries:
        name = escape_attr(entry["name"])
        group = escape_attr(entry.get("group") or "综合")
        logo = escape_attr(entry.get("logo") or "")
        logo_attr = f' tvg-logo="{logo}"' if logo else ""
        lines.append(f'#EXTINF:-1 group-title="{group}" tvg-name="{name}"{logo_attr},{name}')
        lines.append(entry["url"])
    return "\n".join(lines) + "\n"


def main() -> int:
    rooms = fetch_rooms()
    entries = build_entries(rooms)
    sys.stderr.write(f"857直播 raw={len(rooms)} built={len(entries)}\n")
    print(render(entries), end="")
    return len(entries)


if __name__ == "__main__":
    main()
