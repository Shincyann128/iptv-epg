#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MAP_PATH = REPO_ROOT / 'epg' / 'channel_map.json'


def extract_playlist_channels(path: Path) -> list[str]:
    channels = []
    for line in path.read_text(encoding='utf-8', errors='ignore').splitlines():
        if line.startswith('#EXTINF'):
            channels.append(line.rsplit(',', 1)[-1].strip())
    return sorted(set(channels))


def main() -> int:
    parser = argparse.ArgumentParser(description='Check whether channel_map.json matches a playlist file.')
    parser.add_argument('--playlist', required=True, help='Path to playlist .m3u file')
    parser.add_argument('--map', dest='map_path', default=str(DEFAULT_MAP_PATH), help='Path to channel_map.json')
    parser.add_argument('--require-exact', action='store_true', help='Exit non-zero if missing/extra mappings exist')
    args = parser.parse_args()

    playlist_path = Path(args.playlist)
    map_path = Path(args.map_path)

    playlist_channels = set(extract_playlist_channels(playlist_path))
    channel_map = json.loads(map_path.read_text(encoding='utf-8'))['channels']
    mapped_channels = set(channel_map)

    missing = sorted(playlist_channels - mapped_channels)
    extra = sorted(mapped_channels - playlist_channels)

    result = {
        'playlist': str(playlist_path),
        'channel_map': str(map_path),
        'playlist_channels': len(playlist_channels),
        'mapped_channels': len(mapped_channels),
        'missing_in_map': missing,
        'extra_in_map': extra,
        'exact_match': not missing and not extra,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))

    if args.require_exact and (missing or extra):
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
