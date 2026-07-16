#!/usr/bin/env python3
"""Keep epg/channel_map.json structurally in sync with split M3U playlists.

This script is intentionally conservative:
- New playlist channels are added as disabled placeholders.
- Stale mappings are removed.
- It does not try to fuzzy-match EPG display names.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

IPTV_REPO = Path(os.environ.get("IPTV_REPO", "/home/ubuntu/iptv"))
EPG_REPO = Path(os.environ.get("EPG_REPO", "/home/ubuntu/iptv-epg"))
MAP_PATH = EPG_REPO / "epg" / "channel_map.json"
CHECK_SCRIPT = EPG_REPO / "scripts" / "check_playlist_sync.py"

EXCLUDE_FILES = {
    "myself.m3u",
    "Game.m3u",
    "cameralive.m3u",
    "livecam.m3u",
    "migu.m3u",
}

BJ_TZ = ZoneInfo("Asia/Shanghai")


def run(cmd: list[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed in {cwd}: {' '.join(cmd)}\n{proc.stdout}")
    return proc


def git_pull(repo: Path) -> None:
    run(["git", "stash", "push", "--quiet", "--include-untracked",
         "-m", "auto-sync-stash"], repo, check=False)
    run(["git", "pull", "--rebase", "origin", "main"], repo)
    run(["git", "stash", "pop", "--quiet"], repo, check=False)


def extract_playlist_channels(path: Path) -> list[str]:
    channels: list[str] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if line.startswith("#EXTINF"):
            channels.append(line.rsplit(",", 1)[-1].strip())
    return channels


def collect_playlist_channels() -> dict[str, set[str]]:
    by_file: dict[str, set[str]] = {}
    for f in sorted(IPTV_REPO.iterdir()):
        if f.suffix.lower() != ".m3u":
            continue
        if f.name in EXCLUDE_FILES:
            continue
        by_file[f.name] = set(extract_playlist_channels(f))
    return by_file


def source_hint(filename: str) -> str | None:
    # Only a hint for human follow-up; entries stay disabled.
    if filename == "News.m3u":
        return "US"
    if filename == "Sports.m3u":
        return "US"
    if filename == "WorldCup.m3u":
        return "DE"
    if filename in {"Japan.m3u", "Japan-relay.m3u"}:
        return "JP"
    if filename == "International4K.m3u":
        return "GB"
    if filename in {"Beijing.m3u"}:
        return "BJ"
    if filename in {"Shandong Unicom.m3u", "China4K.m3u"}:
        return "CN"
    return None


def load_map() -> dict:
    return json.loads(MAP_PATH.read_text(encoding="utf-8"))


def save_map(data: dict) -> None:
    MAP_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sync_map() -> tuple[list[str], list[str]]:
    by_file = collect_playlist_channels()
    playlist_channels: set[str] = set().union(*by_file.values()) if by_file else set()
    channel_to_files: dict[str, list[str]] = {}
    for filename, channels in by_file.items():
        for ch in channels:
            channel_to_files.setdefault(ch, []).append(filename)

    data = load_map()
    channels: dict = data["channels"]
    mapped_channels = set(channels)

    missing = sorted(playlist_channels - mapped_channels)
    extra = sorted(mapped_channels - playlist_channels)

    today = datetime.now(BJ_TZ).strftime("%Y-%m-%d")

    # Remove stale mappings first.
    for ch in extra:
        channels.pop(ch, None)

    # Add new channels as disabled placeholders.
    for ch in missing:
        files = sorted(channel_to_files.get(ch, []))
        hints = sorted({h for f in files if (h := source_hint(f))})
        entry = {
            "enabled": False,
            "reason": f"auto-added {today}, needs EPG config",
        }
        if files:
            entry["playlist_files"] = files
        if hints:
            entry["source_hint"] = hints[0] if len(hints) == 1 else hints
        channels[ch] = entry

    if missing or extra:
        # Keep deterministic order for clean diffs.
        data["channels"] = dict(sorted(channels.items(), key=lambda kv: kv[0]))
        save_map(data)

    return missing, extra


def verify_exact() -> None:
    exclude = ",".join(sorted(EXCLUDE_FILES))
    run([
        sys.executable,
        str(CHECK_SCRIPT),
        "--dir",
        str(IPTV_REPO) + "/",
        "--exclude",
        exclude,
        "--require-exact",
    ], EPG_REPO)


def commit_and_push(missing: list[str], extra: list[str]) -> None:
    run(["git", "add", "epg/channel_map.json", "scripts/auto_sync_channel_map.py"], EPG_REPO)
    diff = run(["git", "diff", "--cached", "--quiet"], EPG_REPO, check=False)
    if diff.returncode == 0:
        print("channel_map already in sync; nothing to commit")
        return

    msg = f"chore: auto-sync channel map ({len(missing)} add, {len(extra)} remove)"
    run(["git", "commit", "-m", msg], EPG_REPO)
    run(["git", "push", "origin", "main"], EPG_REPO)
    print(msg)


def main() -> int:
    git_pull(IPTV_REPO)
    git_pull(EPG_REPO)
    missing, extra = sync_map()
    verify_exact()
    commit_and_push(missing, extra)
    if missing:
        print("added disabled placeholders:")
        for ch in missing:
            print(f"  + {ch}")
    if extra:
        print("removed stale mappings:")
        for ch in extra:
            print(f"  - {ch}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
