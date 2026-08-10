#!/usr/bin/env python3
import copy
import gzip
import io
import json
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timedelta
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
    'SD': 'keep_offset',
    'BJ': 'keep_offset',
    'ERW': 'keep_offset',
    'HK': 'epgpw_local',
    'TW': 'epgpw_local',
    'JP': 'keep_offset',  # japanterebi: +0000 是真 UTC（おはよう日本 20:00UTC=JST次日5:00）
    'JPT': 'epgpw_local',  # epg.pw JP: +0000 是 JST 墙钟（おはよう日本 04:00 JST）
    'GB': 'epgpw_local',
    'US': 'epgpw_local',
    'CA': 'epgpw_local',
    'DE': 'epgpw_local',
}
SOURCE_TZ = {
    'CN': ZoneInfo('Asia/Shanghai'),
    'HK': ZoneInfo('Asia/Hong_Kong'),
    'TW': ZoneInfo('Asia/Taipei'),
    'JP': ZoneInfo('Asia/Tokyo'),
    'JPT': ZoneInfo('Asia/Tokyo'),
    'GB': ZoneInfo('Etc/GMT+8'),  # epg.pw GB 时间戳 = 真实 UTC-8（ITV News at Ten 22:00英国→源13:00，差9h）
    'US': ZoneInfo('America/New_York'),
    'CA': ZoneInfo('America/Toronto'),
    'DE': ZoneInfo('Europe/Berlin'),
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
    # Normalize seconds to :00 — japanterebi (JP) timestamps carry a stray 22-second
    # offset on ~67% of rows (e.g. 13:00:22), which otherwise creates duplicate
    # programme rows next to the clean :00 versions of the same slot.
    digits = digits[:12] + '00'
    mode = SOURCE_MODE[source_key]
    if mode == 'keep_offset':
        tz_part = parts[1] if len(parts) > 1 else '+0000'
        aware = datetime.strptime(f'{digits} {tz_part}', '%Y%m%d%H%M%S %z')
        bj_dt = aware.astimezone(BJ_TZ)
        return bj_dt.strftime('%Y%m%d%H%M%S +0800')

    naive = datetime.strptime(digits, '%Y%m%d%H%M%S')
    local_dt = naive.replace(tzinfo=SOURCE_TZ[source_key])
    bj_dt = local_dt.astimezone(BJ_TZ)
    return bj_dt.strftime('%Y%m%d%H%M%S +0800')


def fix_stop_after_start(start: str, stop: str) -> str:
    """japanterebi (JP) emits ~3% of rows with stop dated one day BEFORE start
    (e.g. start 08-06 13:30 -> stop 08-05 15:30). Those are cross-midnight rows
    whose stop date was written wrongly. Push stop forward one day so ordering
    is restored; the title/timing otherwise matches the real slot."""
    if len(start) >= 14 and len(stop) >= 14 and stop[:14] <= start[:14]:
        dt = datetime.strptime(stop[:14], '%Y%m%d%H%M%S') + timedelta(days=1)
        return dt.strftime('%Y%m%d%H%M%S') + stop[14:]
    return stop


def should_filter_promo(title: str, target_name: str, duration_min: float) -> bool:
    """Filter promo/INFO fragments that upstream feeds embed next to real
    programmes (they overlap the actual show and produce bogus overlap pairs):
    - NHK World: 'INFO' interstitial rows (2min)
    - CCTV4K: '宣传片/节目预告/标识演绎' promo rows — upstream emits BOTH a
      1-2min short version AND a 24h all-day version of the same promo; the
      24h one overlaps every real show, so filter by keyword, not duration."""
    t = (title or '').strip()
    if target_name in ('NHK World Japan', 'NHK World-Japan') and t.upper().startswith('INFO'):
        return True
    if target_name in ('CCTV-4K超高清', 'CCTV4K超高清', 'cctv4k') and (
        '宣传片' in t or '频道宣传' in t or '节目预告' in t or '标识演绎' in t
    ):
        return True
    return False


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
    s_tokens = (start_raw or '').strip().split()
    e_tokens = (stop_raw or '').strip().split()
    if not s_tokens or not e_tokens:
        return True
    s, e = s_tokens[0], e_tokens[0]
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
    # Dedup by (target, start, stop) — NOT title: japanterebi emits duplicate
    # rows for the same slot with slightly different titles (clean :00 version
    # plus 22-second-offset version); only one should survive.
    key = (target_name, start, stop)
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


def count_programmes_per_channel(raw: bytes) -> dict:
    """First pass: count total programmes per source channel ID."""
    root = ET.fromstring(raw)
    counts = defaultdict(int)
    for el in root.findall('programme'):
        sid = el.attrib.get('channel', '')
        if sid:
            counts[sid] += 1
    return dict(counts)


def empty_title_ratio(raw: bytes) -> float:
    """Ratio of programme rows with empty <title>. Upstream feeds sometimes
    ship thousands of rows whose titles are all blank (epg.pw DE 2026-08-06);
    treating those as data would silently wipe good EPG data on the next build.

    NOTE: this must NOT use ET.iterparse with el.clear() — element objects get
    reused after clear, so elem.find('title') returns stale/blank nodes (the
    2026-08-06 build falsely read 100% blank titles from healthy sources).
    """
    root = ET.fromstring(raw)
    progs = root.findall('programme')
    total = len(progs)
    if not total:
        return 0.0
    empty = 0
    for el in progs:
        t = el.find('title')
        if not (t is not None and t.text and t.text.strip()):
            empty += 1
    return empty / total


EMPTY_TITLE_ABORT_RATIO = 0.5  # >50% blank titles → source is broken, skip it


def parse_source(source_key: str, url: str, targets_by_epg_name: dict, root_out: ET.Element):
    if not targets_by_epg_name:
        return {"channels": 0, "programmes": 0}

    try:
        raw = fetch_bytes(url)
    except Exception as exc:
        return {"channels": 0, "programmes": 0, "error": str(exc)}

    # Data-integrity gate: a source whose programmes are mostly blank-titled is
    # broken upstream (not just sparse) — abort it so we never publish garbage.
    er = empty_title_ratio(raw)
    if er > EMPTY_TITLE_ABORT_RATIO:
        return {
            "channels": 0,
            "programmes": 0,
            "error": f"源数据损坏: {er*100:.0f}% 节目空标题（>50% 阈值）",
        }

    # Full in-memory parse: iterparse+clear is unreliable for child-text reads
    # (element reuse), so we parse once and walk the tree directly.
    try:
        src_root = ET.fromstring(raw)
    except ET.ParseError as exc:
        return {"channels": 0, "programmes": 0, "error": f"XML解析失败: {exc}"}

    # First pass: count programmes per channel, to prefer channels that have data
    prog_counts = defaultdict(int)
    for el in src_root.findall('programme'):
        sid = el.attrib.get('channel', '')
        if sid:
            prog_counts[sid] += 1
    prog_counts = dict(prog_counts)

    source_to_targets = defaultdict(list)
    channels_added = 0
    programme_count = 0
    channel_written = set()
    programme_seen = set()
    claimed_epg_names = {}  # epg_name -> (source_id, prog_count)
    last_programme_by_target = {}

    # First pass: collect all channel mappings
    for elem in src_root.findall('channel'):
        names = [dn.text.strip() for dn in elem.findall('display-name') if dn.text]
        matched_targets = []
        matched_epg_names = []
        for name in names:
            targets = targets_by_epg_name.get(name, [])
            if targets:
                matched_epg_names.append(name)
                matched_targets.extend(targets)
        if not matched_targets:
            continue
        source_id = elem.attrib.get('id')
        this_count = prog_counts.get(source_id, 0)
        active_epg_names = []
        for epg_name in matched_epg_names:
            prev = claimed_epg_names.get(epg_name)
            if prev is None:
                claimed_epg_names[epg_name] = (source_id, this_count)
                active_epg_names.append(epg_name)
            elif this_count > prev[1]:
                old_src_id = prev[0]
                claimed_epg_names[epg_name] = (source_id, this_count)
                if old_src_id in source_to_targets:
                    del source_to_targets[old_src_id]
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

    # Second pass: process all programme elements
    for elem in src_root.findall('programme'):
        source_id = elem.attrib.get('channel')
        targets = source_to_targets.get(source_id)
        if not targets:
            continue
        start_raw = elem.attrib.get('start', '')
        stop_raw = elem.attrib.get('stop', '')
        if should_skip_programme(start_raw, stop_raw, source_key):
            continue
        title_elem = elem.find('title')
        title = title_elem.text.strip() if title_elem is not None and title_elem.text else ''
        start = convert_xmltv_time(start_raw, source_key)
        stop = convert_xmltv_time(stop_raw, source_key)
        stop = fix_stop_after_start(start, stop)
        duration_min = programme_duration_seconds(start, stop) / 60.0
        for target_name in targets:
            if should_filter_promo(title, target_name, duration_min):
                continue
            new_prog = copy.deepcopy(elem)
            if append_programme(root_out, programme_seen, last_programme_by_target,
                                target_name, new_prog, start, stop, title, source_key):
                programme_count += 1

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
    failures = []
    for source_key, epg_map in targets_by_source.items():
        st = parse_source(source_key, source_urls[source_key], epg_map, root)
        stats[source_key] = st
        wanted = len(epg_map)
        if wanted > 0 and st.get('programmes', 0) == 0:
            err = st.get('error', '')
            hint = f"fetch error: {err[:120]}" if err else "no fetch error — likely all epg_name mismatches or empty source"
            failures.append(f"{source_key}: wanted {wanted} epg_names but 0 programmes ({hint})")

    tree = ET.ElementTree(root)
    ET.indent(tree, space='  ')
    OUT_XML.parent.mkdir(parents=True, exist_ok=True)

    # --- 当天数据完整性检查（2026-08-10 修复 Actions 缺当天数据）---
    # epg.pw 等上游在北京时间上午才生成当天节目；若 build 太早抓到旧数据，
    # "北京时间今天有节目的频道"会明显不足。此时不覆盖上次好 EPG，并报错提示。
    today_bj = datetime.now(BJ_TZ).strftime('%Y%m%d')
    today_channels = set()
    for prog in root.findall('programme'):
        if prog.get('start', '').startswith(today_bj):
            today_channels.add(prog.get('channel'))
    total_channels = len(root.findall('channel'))
    today_ratio = len(today_channels) / total_channels if total_channels else 0
    MIN_TODAY_RATIO = 0.6
    if today_ratio < MIN_TODAY_RATIO:
        failures.append(
            f"today-coverage: 北京时间今天({today_bj})只有 {len(today_channels)}/{total_channels} "
            f"频道有节目 ({today_ratio:.0%} < {MIN_TODAY_RATIO:.0%})，上游数据可能未更新，保留上次好 EPG"
        )

    if failures:
        # Never overwrite the last good build with partial/broken data: a
        # broken upstream source (e.g. epg.pw DE blank titles) would otherwise
        # silently wipe the previous valid epg.xml/epg.xml.gz.
        result = {
            'sources_used': {k: {'wanted_epg_names': len(v), **stats.get(k, {})} for k, v in targets_by_source.items()},
            'failures': failures,
            'preserved_previous_build': True,
            'output_xml': str(OUT_XML),
            'output_gz': str(OUT_GZ),
            'size_xml': OUT_XML.stat().st_size if OUT_XML.exists() else 0,
            'size_gz': OUT_GZ.stat().st_size if OUT_GZ.exists() else 0,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print(f"BUILD FAILED: {len(failures)} source(s) produced 0 programmes: {', '.join(f.split(':')[0] for f in failures)}")
        sys.exit(1)

    tree.write(OUT_XML, encoding='utf-8', xml_declaration=True)
    with gzip.open(OUT_GZ, 'wb') as f:
        f.write(OUT_XML.read_bytes())

    result = {
        'sources_used': {k: {'wanted_epg_names': len(v), **stats.get(k, {})} for k, v in targets_by_source.items()},
        'output_xml': str(OUT_XML),
        'output_gz': str(OUT_GZ),
        'size_xml': OUT_XML.stat().st_size,
        'size_gz': OUT_GZ.stat().st_size,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
