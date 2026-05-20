#!/usr/bin/env python3
import json
import urllib.request
from pathlib import Path
from urllib.parse import urljoin

API_URL = "https://www.kafeizhibo.com/api/v1/archor"
BASE_URL = "https://www.kafeizhibo.com"
OUTPUT_FILE = Path(__file__).resolve().parents[1] / "kafeizhibo_live.m3u"
USER_AGENT = "Mozilla/5.0 (Hermes Agent)"


def fetch_archors():
    req = urllib.request.Request(
        API_URL,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.kafeizhibo.com/live/living",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    data = payload.get("data") or []
    if not isinstance(data, list):
        raise RuntimeError("Unexpected API payload: data is not a list")
    return data


def abs_url(value: str | None) -> str:
    if not value:
        return ""
    return urljoin(BASE_URL, value)


def normalize_group(item: dict) -> str:
    league = (item.get("league_name") or "").strip()
    if league:
        return league
    return "咖啡直播"


def normalize_name(item: dict) -> str:
    title = (item.get("title") or "").strip()
    name = (item.get("name") or "").strip()
    if title and title != name:
        return title
    if name:
        return name
    room_id = item.get("room_id") or item.get("archor_id") or "unknown"
    return f"咖啡直播-{room_id}"


def build_entries(items: list[dict]) -> list[dict]:
    entries = []
    seen = set()
    for item in items:
        status = (item.get("status") or "").lower()
        stream_url = (item.get("stream_url") or "").strip()
        if status not in {"live", "online"}:
            continue
        if not stream_url:
            continue
        key = (item.get("archor_id"), stream_url)
        if key in seen:
            continue
        seen.add(key)
        entries.append(
            {
                "name": normalize_name(item),
                "anchor_name": (item.get("name") or "").strip(),
                "room_id": str(item.get("room_id") or item.get("archor_id") or ""),
                "group": normalize_group(item),
                "logo": abs_url(item.get("avatar") or item.get("screenshot")),
                "url": stream_url,
                "is_top": bool(item.get("is_top")),
                "category": int(item.get("category") or 0),
                "heat": int(item.get("heat") or 0),
                "status": status,
            }
        )

    name_counts = {}
    for entry in entries:
        name_counts[entry["name"]] = name_counts.get(entry["name"], 0) + 1
    for entry in entries:
        if name_counts[entry["name"]] > 1:
            suffix = entry["anchor_name"] or entry["room_id"]
            if suffix and suffix not in entry["name"]:
                entry["name"] = f'{entry["name"]}｜{suffix}'

    entries.sort(
        key=lambda x: (
            0 if x["status"] == "live" else 1,
            0 if x["is_top"] else 1,
            x["category"],
            -x["heat"],
            x["group"],
            x["name"],
        )
    )
    return entries


def render(entries: list[dict]) -> str:
    lines = [
        "#EXTM3U",
        "# Generated from https://www.kafeizhibo.com/live/living",
        "# Auto-updated by GitHub Actions every 2 hours",
        "",
    ]
    for entry in entries:
        attrs = [
            f'tvg-name="{entry["name"]}"',
            f'group-title="{entry["group"]}"',
        ]
        if entry["logo"]:
            attrs.append(f'tvg-logo="{entry["logo"]}"')
        lines.append(f'#EXTINF:-1 {" ".join(attrs)},{entry["name"]}')
        lines.append(entry["url"])
    lines.append("")
    return "\n".join(lines)


def main():
    items = fetch_archors()
    entries = build_entries(items)
    if not entries:
        raise RuntimeError("No live entries found from kafeizhibo API")
    OUTPUT_FILE.write_text(render(entries), encoding="utf-8")
    print(f"Wrote {len(entries)} entries to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
