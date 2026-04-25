#!/usr/bin/env python3
import copy
import gzip
import io
import json
import time
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parent.parent
MAP_PATH = REPO_ROOT / 'epg' / 'channel_map.json'
OUT_XML = REPO_ROOT / 'epg' / 'epg.xml'
OUT_GZ = REPO_ROOT / 'epg' / 'epg.xml.gz'
BJ_TZ = ZoneInfo('Asia/Shanghai')
PUBLIC_REPO_URL = 'https://github.com/Shincyann128/iptv-epg'
WOWOW_CHANNELS = {'WOWOW Prime', 'WOWOW Live', 'WOWOW Cinema'}
SOURCE_MODE = {
    'CN': 'epgpw_local',
    'BJ': 'keep_offset',
    'HK': 'epgpw_local',
    'TW': 'epgpw_local',
    'JP': 'keep_offset',
    'JPT': 'keep_offset',
    'GB': 'epgpw_local',
    'US': 'epgpw_local',
}
SOURCE_TZ = {
    'CN': ZoneInfo('Asia/Shanghai'),
    'HK': ZoneInfo('Asia/Hong_Kong'),
    'TW': ZoneInfo('Asia/Taipei'),
    'GB': ZoneInfo('Europe/London'),
    'US': ZoneInfo('America/New_York'),
}


def fetch_bytes(url: str, retries: int = 3, backoff_seconds: float = 1.0) -> bytes:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 Hermes EPG Builder"})
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = resp.read()
            if url.endswith('.gz'):
                return gzip.decompress(data)
            return data
        except Exception as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(backoff_seconds * (2 ** (attempt - 1)))
    raise RuntimeError(f'failed to fetch {url}: {last_error}') from last_error


def convert_xmltv_time(dt_str: str, source_key: str) -> str:
    if not dt_str:
        return dt_str
    parts = dt_str.strip().split()
    digits = parts[0]
    if len(digits) < 14:
        return dt_str
    mode = SOURCE_MODE[source_key]
    if mode == 'keep_offset':
        tz_part = parts[1] if len(parts) > 1 else '+0000'
        aware = datetime.strptime(f'{digits[:14]} {tz_part}', '%Y%m%d%H%M%S %z')
        bj_dt = aware.astimezone(BJ_TZ)
        return bj_dt.strftime('%Y%m%d%H%M%S +0800')

    naive = datetime.strptime(digits[:14], '%Y%m%d%H%M%S')
    local_dt = naive.replace(tzinfo=SOURCE_TZ[source_key])
    bj_dt = local_dt.astimezone(BJ_TZ)
    return bj_dt.strftime('%Y%m%d%H%M%S +0800')


def parse_bj_time(dt_str: str) -> datetime | None:
    if not dt_str:
        return None
    return datetime.strptime(dt_str, '%Y%m%d%H%M%S +0800')


def programme_duration_seconds(start: str, stop: str) -> float:
    start_dt = parse_bj_time(start)
    stop_dt = parse_bj_time(stop)
    if not start_dt or not stop_dt:
        return 0
    return max((stop_dt - start_dt).total_seconds(), 0)


def should_skip_programme(start_raw: str, stop_raw: str, source_key: str) -> bool:
    if source_key not in {'JP', 'JPT'}:
        return False
    s = (start_raw or '').strip().split()[0]
    e = (stop_raw or '').strip().split()[0]
    if len(s) < 14 or len(e) < 14:
        return False
    if not e.endswith('000000'):
        return False
    ds = datetime.strptime(s[:14], '%Y%m%d%H%M%S')
    de = datetime.strptime(e[:14], '%Y%m%d%H%M%S')
    duration_hours = (de - ds).total_seconds() / 3600
    return duration_hours > 8


def should_cleanup_overlaps(target_name: str, source_key: str) -> bool:
    return source_key in {'JP', 'JPT'} and target_name in WOWOW_CHANNELS


def should_replace_existing(existing: dict, candidate_start: str, candidate_stop: str) -> bool:
    existing_duration = programme_duration_seconds(existing['start'], existing['stop'])
    candidate_duration = programme_duration_seconds(candidate_start, candidate_stop)
    if candidate_duration > existing_duration:
        return True
    if candidate_duration == existing_duration:
        return candidate_stop > existing['stop']
    return False


def append_programme(root_out: ET.Element, programme_seen: set, last_programme_by_target: dict,
                     target_name: str, elem: ET.Element, start: str, stop: str, title: str,
                     source_key: str) -> bool:
    key = (target_name, start, stop, title)
    if key in programme_seen:
        return False

    candidate_start_dt = parse_bj_time(start)
    last = last_programme_by_target.get(target_name)
    if (
        last
        and should_cleanup_overlaps(target_name, source_key)
        and candidate_start_dt
        and candidate_start_dt < last['stop_dt']
    ):
        if should_replace_existing(last, start, stop):
            try:
                root_out.remove(last['element'])
            except ValueError:
                pass
            programme_seen.discard(last['key'])
        else:
            return False

    elem.attrib['channel'] = target_name
    if start:
        elem.attrib['start'] = start
    if stop:
        elem.attrib['stop'] = stop
    root_out.append(elem)
    programme_seen.add(key)

    stop_dt = parse_bj_time(stop)
    if stop_dt:
        last_programme_by_target[target_name] = {
            'element': elem,
            'key': key,
            'start': start,
            'stop': stop,
            'stop_dt': stop_dt,
        }
    return True


def resolve_channel_config(channel_name: str, channels: dict, stack: set | None = None) -> dict:
    if stack is None:
        stack = set()
    if channel_name in stack:
        raise ValueError(f'alias cycle detected: {channel_name}')

    cfg = dict(channels[channel_name])
    alias_of = cfg.get('alias_of')
    if not alias_of:
        return cfg

    # Some mappings use alias_of only as a human-readable canonical label
    # while still providing explicit source/epg_name in the same row.
    if alias_of not in channels:
        if 'source' in cfg and 'epg_name' in cfg:
            return cfg
        raise KeyError(f'alias target not found: {alias_of}')

    stack.add(channel_name)
    base_cfg = resolve_channel_config(alias_of, channels, stack)
    stack.remove(channel_name)

    merged = dict(base_cfg)
    merged.update(cfg)
    merged['alias_of'] = alias_of
    return merged


def build_targets_by_source(channels: dict):
    targets_by_source = defaultdict(lambda: defaultdict(list))
    for m3u_name in channels:
        cfg = resolve_channel_config(m3u_name, channels)
        if not cfg.get('enabled'):
            continue
        targets_by_source[cfg['source']][cfg['epg_name']].append(m3u_name)
    return targets_by_source


def parse_source(source_key: str, url: str, targets_by_epg_name: dict, root_out: ET.Element):
    if not targets_by_epg_name:
        return {"channels": 0, "programmes": 0}

    try:
        raw = fetch_bytes(url)
    except Exception as exc:
        return {"channels": 0, "programmes": 0, "error": str(exc)}

    source_to_targets = defaultdict(list)
    channels_added = 0
    programme_count = 0
    channel_written = set()
    programme_seen = set()
    claimed_epg_names = {}
    last_programme_by_target = {}

    context = ET.iterparse(io.BytesIO(raw), events=("start", "end"))
    _, root_in = next(context)

    for event, elem in context:
        if event != 'end':
            continue

        if elem.tag == 'channel':
            names = []
            for dn in elem.findall('display-name'):
                if dn.text:
                    names.append(dn.text.strip())
            matched_targets = []
            matched_epg_names = []
            for name in names:
                targets = targets_by_epg_name.get(name, [])
                if targets:
                    matched_epg_names.append(name)
                    matched_targets.extend(targets)
            if matched_targets:
                source_id = elem.attrib.get('id')
                active_epg_names = []
                for epg_name in matched_epg_names:
                    if epg_name not in claimed_epg_names:
                        claimed_epg_names[epg_name] = source_id
                        active_epg_names.append(epg_name)
                if active_epg_names:
                    active_targets = []
                    for epg_name in active_epg_names:
                        active_targets.extend(targets_by_epg_name[epg_name])
                    source_to_targets[source_id].extend(active_targets)
                    icon_elem = elem.find('icon')
                    icon_src = icon_elem.attrib.get('src') if icon_elem is not None else None
                    for target_name in active_targets:
                        if target_name in channel_written:
                            continue
                        ch = ET.SubElement(root_out, 'channel', {'id': target_name})
                        dn = ET.SubElement(ch, 'display-name', {'lang': 'zh'})
                        dn.text = target_name
                        if icon_src:
                            ET.SubElement(ch, 'icon', {'src': icon_src})
                        channel_written.add(target_name)
                        channels_added += 1
            elem.clear()

        elif elem.tag == 'programme':
            source_id = elem.attrib.get('channel')
            targets = source_to_targets.get(source_id)
            if targets:
                start_raw = elem.attrib.get('start', '')
                stop_raw = elem.attrib.get('stop', '')
                if should_skip_programme(start_raw, stop_raw, source_key):
                    elem.clear()
                    root_in.clear()
                    continue
                title_elem = elem.find('title')
                title = title_elem.text.strip() if title_elem is not None and title_elem.text else ''
                start = convert_xmltv_time(start_raw, source_key)
                stop = convert_xmltv_time(stop_raw, source_key)
                for target_name in targets:
                    new_prog = copy.deepcopy(elem)
                    if append_programme(root_out, programme_seen, last_programme_by_target,
                                        target_name, new_prog, start, stop, title, source_key):
                        programme_count += 1
            elem.clear()
            root_in.clear()

    return {"channels": channels_added, "programmes": programme_count}


def main():
    data = json.loads(MAP_PATH.read_text(encoding='utf-8'))
    source_urls = data['sources']
    channels = data['channels']

    targets_by_source = build_targets_by_source(channels)

    root = ET.Element('tv', {
        'generator-info-name': 'Hermes custom EPG builder',
        'generator-info-url': PUBLIC_REPO_URL
    })

    stats = {}
    for source_key, epg_map in targets_by_source.items():
        stats[source_key] = parse_source(source_key, source_urls[source_key], epg_map, root)

    tree = ET.ElementTree(root)
    ET.indent(tree, space='  ')
    OUT_XML.parent.mkdir(parents=True, exist_ok=True)
    tree.write(OUT_XML, encoding='utf-8', xml_declaration=True)
    with gzip.open(OUT_GZ, 'wb') as f:
        f.write(OUT_XML.read_bytes())

    print(json.dumps({
        'sources_used': {k: {'wanted_epg_names': len(v), **stats.get(k, {})} for k, v in targets_by_source.items()},
        'output_xml': str(OUT_XML),
        'output_gz': str(OUT_GZ),
        'size_xml': OUT_XML.stat().st_size,
        'size_gz': OUT_GZ.stat().st_size,
    }, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
