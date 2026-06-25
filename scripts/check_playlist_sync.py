#!/usr/bin/env python3
"""Check whether channel_map.json matches playlist files (single or directory)."""

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MAP_PATH = REPO_ROOT / 'epg' / 'channel_map.json'


def extract_playlist_channels(path: Path) -> list[str]:
    channels = []
    for line in path.read_text(encoding='utf-8', errors='ignore').splitlines():
        if line.startswith('#EXTINF'):
            channels.append(line.rsplit(',', 1)[-1].strip())
    return sorted(set(channels))


def collect_dir_channels(dir_path: Path, exclude: set[str]) -> list[str]:
    all_channels: set[str] = set()
    for f in sorted(dir_path.iterdir()):
        if f.suffix.lower() != '.m3u':
            continue
        if f.name in exclude:
            continue
        all_channels.update(extract_playlist_channels(f))
    return sorted(all_channels)


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Check whether channel_map.json matches playlist files.'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--playlist', help='Path to a single playlist .m3u file')
    group.add_argument('--dir', help='Path to directory of .m3u files')
    parser.add_argument('--exclude', default='', help='Comma-separated filenames to exclude (--dir mode only)')
    parser.add_argument('--map', dest='map_path', default=str(DEFAULT_MAP_PATH), help='Path to channel_map.json')
    parser.add_argument('--require-exact', action='store_true', help='Exit non-zero if missing/extra mappings exist')
    args = parser.parse_args()

    map_path = Path(args.map_path)
    channel_map = json.loads(map_path.read_text(encoding='utf-8'))
    mapped_channels = set(channel_map['channels'])

    if args.playlist:
        playlist_path = Path(args.playlist)
        playlist_channels = set(extract_playlist_channels(playlist_path))
        label = str(playlist_path)
    else:
        dir_path = Path(args.dir)
        exclude = {f.strip() for f in args.exclude.split(',') if f.strip()}
        playlist_channels = set(collect_dir_channels(dir_path, exclude))
        label = f'{args.dir} (-{exclude})'

    missing = sorted(playlist_channels - mapped_channels)
    extra = sorted(mapped_channels - playlist_channels)

    result = {
        'source': label,
        'playlist_channels': len(playlist_channels),
        'mapped_channels': len(mapped_channels),
        'missing_in_map': missing,
        'extra_in_map': extra,
        'exact_match': not missing and not extra,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))

    if args.require_exact and (missing or extra):
        if missing:
            print(f'\nMissing from channel_map ({len(missing)}):', file=sys.stderr)
            for m in missing:
                print(f'  {m}', file=sys.stderr)
        if extra:
            print(f'\nExtra in channel_map (not in playlist) ({len(extra)}):', file=sys.stderr)
            for e in extra:
                print(f'  {e}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
