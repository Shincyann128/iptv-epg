#!/usr/bin/env python3
"""Refresh verified Sky and TNT stream URLs in the official GitHub playlist."""

import argparse
import base64
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from iptv_spares_media import ProbeError, probe

BJ = timezone(timedelta(hours=8))
SOURCE_M3U = "https://raw.githubusercontent.com/BuddyChewChew/My-Streams/main/tv.m3u"
FILE_API = "repos/Shincyann128/iptv/contents/Sports.m3u"
PUBLISHED = Path("/srv/tvlive/sports.m3u")
SYNC = "/home/ubuntu/iptv-epg/scripts/sync_tvlive.sh"
STATE = Path.home() / ".hermes/data/sky_token_state.json"
BACKUP_DIR = Path.home() / ".hermes/profiles/code/backups/iptv/sky-refresh"
ALLOWED_HOSTS = {"cdnlivetv.tv", "stream.bottledesk.net"}
CHANNELS = {
    "Sky Sports Main Event": "Sky Sports Main Event UK",
    "Sky Sports Premier League": "Sky Sports Premier League UK",
    "Sky Sports Football": "Sky Sports Football UK",
    "Sky Sports Cricket": "Sky Sports Cricket UK",
    "Sky Sports F1": "Sky Sports F1 UK",
    "Sky Sports Tennis": "Sky Sports Tennis UK",
    "Sky Sports Action": "Sky Sports Action UK",
    "Sky Sports Racing": "Sky Sports Racing UK",
    "Sky Sports Mix": "Sky Sports Mix UK",
    "Sky Sports Golf": "Sky Sports Golf UK",
    "TNT Sports 1": "TNT Sports 1 UK",
    "TNT Sports 2": "TNT Sports 2 UK",
}


def blob(data):
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def gh(args, *, payload=None):
    p = subprocess.run(
        ["gh", "api", *args], input=json.dumps(payload) if payload is not None else None,
        text=True, capture_output=True, timeout=25,
    )
    if p.returncode:
        raise RuntimeError("GitHub API request failed or the base file changed")
    return json.loads(p.stdout)


def url_rows(lines):
    rows = defaultdict(list)
    name = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("#EXTINF:"):
            name = stripped.rsplit(",", 1)[-1].strip()
        elif name and stripped.startswith(("http://", "https://")):
            rows[name].append((i, stripped))
            name = None
    return rows


def candidates():
    req = Request(SOURCE_M3U, headers={"User-Agent": "TVLive playlist refresh"})
    with urlopen(req, timeout=12) as response:
        body = response.read(1_200_001)
    if len(body) > 1_200_000:
        raise ValueError("source playlist exceeded its byte limit")
    rows = url_rows(body.decode("utf-8-sig").splitlines())
    out = {}
    for alias, name in CHANNELS.items():
        for _, url in rows.get(alias, ()):
            try:
                link = urlsplit(url)
                allowed = (link.scheme == "https" and link.hostname in ALLOWED_HOSTS
                           and link.port in (None, 443) and not link.username and not link.password)
            except ValueError:
                continue
            if allowed:
                out[name] = url
                break
    return out


def write_state(state):
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", prefix=".sports-refresh-", dir=STATE.parent, delete=False,
    ) as tmp:
        try:
            json.dump(state, tmp, ensure_ascii=False, indent=2)
            tmp.write("\n")
            tmp.flush()
            os.replace(tmp.name, STATE)
        finally:
            Path(tmp.name).unlink(missing_ok=True)


def refresh(args):
    base = gh([FILE_API + "?ref=main"])
    data = base64.b64decode(base["content"])
    if blob(data) != base["sha"]:
        raise ValueError("GitHub base playlist did not match its object ID")
    lines = data.decode("utf-8").splitlines(keepends=True)
    rows = url_rows(lines)
    upstream = candidates()
    selected = args.channel or CHANNELS.values()
    changes, rejected = {}, {}
    deadline = time.monotonic() + 75
    for name in selected:
        if len(rows.get(name, ())) != 1 or name not in upstream:
            rejected[name] = "missing_or_ambiguous"
            continue
        i, previous = rows[name][0]
        candidate = upstream[name]
        if candidate == previous:
            continue
        if time.monotonic() >= deadline:
            rejected[name] = "verification_budget"
            continue
        try:
            result = probe(candidate, 576, min(deadline, time.monotonic() + 12))
        except (ProbeError, OSError, subprocess.SubprocessError, ValueError) as exc:
            rejected[name] = str(exc) if isinstance(exc, ProbeError) else type(exc).__name__
            continue
        preserved_ending = lines[i][len(lines[i].rstrip("\r\n")):]
        lines[i] = candidate + preserved_ending
        changes[name] = asdict(result)

    state = {
        "last_run": datetime.now(BJ).isoformat(),
        "base_blob": base["sha"], "changed": sorted(changes), "rejected": rejected,
        "media": {name: {k: v for k, v in proof.items() if k != "sequence"}
                  for name, proof in changes.items()},
        "ok": True,
    }
    if args.dry_run:
        print(json.dumps(state, ensure_ascii=False), flush=True)
        return

    final_blob = base["sha"]
    if changes:
        updated = "".join(lines).encode("utf-8")
        final_blob = blob(updated)
        BACKUP_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
        backup = BACKUP_DIR / ("Sports.m3u.base-" + base["sha"])
        backup.write_bytes(data)
        revision = gh(["--method", "PUT", FILE_API, "--input", "-"], payload={
            "message": "Refresh verified sports streams: " + ", ".join(sorted(changes)),
            "branch": "main", "sha": base["sha"],
            "content": base64.b64encode(updated).decode("ascii"),
        })
        if revision["content"]["sha"] != final_blob:
            raise ValueError("GitHub returned a different playlist object ID")
        state["commit"] = revision["commit"]["sha"]

    if not PUBLISHED.is_file() or blob(PUBLISHED.read_bytes()) != final_blob:
        subprocess.run(["bash", SYNC], check=True, timeout=100, capture_output=True)
        if blob(PUBLISHED.read_bytes()) != final_blob:
            raise ValueError("Published playlist did not match the GitHub update")

    write_state(state)
    print(json.dumps(state, ensure_ascii=False), flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--channel", choices=sorted(CHANNELS.values()), action="append")
    args = p.parse_args()
    if args.dry_run:
        refresh(args)
        return
    with STATE.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        refresh(args)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        message = str(exc) if isinstance(exc, (RuntimeError, ValueError)) else type(exc).__name__
        print(json.dumps({"ok": False, "error": message}, ensure_ascii=False), flush=True)
        raise SystemExit(1)
