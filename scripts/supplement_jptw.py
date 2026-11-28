"""Overlay verified Japan/Taiwan schedules after the standard XMLTV build."""
import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import copy
from datetime import datetime, timedelta
from functools import lru_cache
import gzip
from pathlib import Path
import re
import time
from urllib.parse import urlencode
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

import build_epg
import osaka_guide as osaka
import taiwan_guide as tw

BJ = ZoneInfo('Asia/Shanghai')
FAST_XML = 'https://richardallensmith.github.io/japan-epg/epg.xml'
FAST_M3U = 'https://skinred78.github.io/jp-iptv-epg/jp-playlist.m3u'
FAST_IDS = set('rch_115 rch_120 rch_54 rch_56 rch_101 rch_97 rch_34 rch_73 rch_113 rch_114 rch_74 rch_104 rch_105 rch_106 rch_107 fastch_v2_entns fastch_v2_snyon fastch_v2_comdy fastch_v2_jpthp fastch_v2_asdrm fastch_v2_camng fastch_v2_pett1 fastch_v2_news1 fastch_v2_hokkaido fastch_v2_travl fastch_v2_tokai fastch_v2_recip'.split())
TOKYO = {'NHK東京　総合':['NHK G','NHK G AVC 1080p'], 'NHK東京　教育':['NHK E'], '日本テレビ':['NTV'], 'テレビ朝日':['TV Asahi'], 'TBS':['TBS'], 'テレビ東京':['TV Tokyo'], 'フジテレビ':['Fuji TV']}


def playlist(text):
    pending = None
    for line in text.splitlines():
        if line.startswith('#EXTINF:') and ',' in line:
            match = re.search(r'\btvg-id="([^"]*)"', line)
            pending = line.rsplit(',', 1)[1].strip(), match[1] if match else ''
        elif pending and line.startswith(('https://','http://')):
            yield (*pending, line.strip())
            pending = None


def retry(fn):
    for i in range(3):
        try:
            return fn()
        except (OSError, ValueError, ET.ParseError) as exc:
            if i == 2:
                raise RuntimeError(f'{fn.__name__} failed after 3 attempts ({type(exc).__name__})') from None
            time.sleep(2 ** i)


def base_extras():
    root = ET.Element('tv')
    build_epg.parse_source('JPT','https://epg.pw/xmltv/epg_JP.xml.gz',TOKYO,root)
    build_epg.parse_source('TW',tw.TW,{'TVBS 新聞台':['TVBS新聞']},root)
    return root


def tw_extras():
    now = datetime.now(BJ)
    today = now.date()
    end = today+timedelta(days=8)
    query = urlencode(dict(chid=52, start=today-timedelta(days=1), end=end, callback='ctiEPG'))
    rows = {'中天新聞台':tw.parse_cti(tw.fetch(tw.CTI+'?'+query)),
            '龍祥時代':tw.parse_ls(tw.fetch(tw.LS)),
            'cna':tw.parse_cna(tw.fetch(tw.TW))}
    guide = ET.fromstring(tw.format_guide(rows,today,end,optional_channels={'龍祥時代'}))
    ls_programmes = guide.findall('programme[@channel="龍祥時代"]')
    if ls_programmes and not fresh(ls_programmes, now):
        print('WARNING: LS TIME guide is too short; retaining the base schedule')
        for programme in ls_programmes:
            guide.remove(programme)
    return guide


def osaka_extras():
    return osaka.build(osaka.fetch_days())


def fast_extras():
    listing = list(playlist(tw.fetch(FAST_M3U).decode('utf-8-sig')))
    return ET.fromstring(tw.fetch(FAST_XML)), listing


@lru_cache(maxsize=32768)
def clock(s):
    return datetime.strptime(s,'%Y%m%d%H%M%S %z').astimezone(BJ)


def row_index(root):
    rows = defaultdict(list)
    for p in root.findall('programme'):
        rows[p.get('channel')].append(p)
    return rows


def fresh(rows, now):
    return any(p.get('stop') and clock(p.get('stop')) > now+timedelta(hours=18)
               and clock(p.get('start')) < now+timedelta(hours=24) for p in rows)


def overlay(root, feeds, jp, taiwan, now):
    chars = {c.get('id'): c for c in root.findall('channel')}
    if len(chars) != len(root.findall('channel')):
        raise ValueError('duplicate base XMLTV channel ids')
    programmes = row_index(root)
    applied = set()
    remove = set()
    add = []
    new_channels = []
    missing = []
    for incoming, targets in feeds:
        source = row_index(incoming)
        for cid, name in targets.items():
            if name in applied:
                raise ValueError(f'duplicate supplement target: {name}')
            good = []
            prev_end = None
            seen = set()
            for p in sorted(source[cid], key=lambda p:p.get('start','')):
                start, stop = p.get('start'), p.get('stop')
                # The last LS TIME row has no announced stop; omit it rather than guess.
                if not stop or not p.findtext('title') or clock(start) >= clock(stop):
                    continue
                s, e = clock(start), clock(stop)
                if (s,e) in seen:
                    continue
                if prev_end and s < prev_end:
                    raise ValueError(f'overlap in supplement: {name} at {s.isoformat()}')
                seen.add((s,e)); prev_end = e
                q = copy.deepcopy(p); q.attrib.update(channel=name, start=s.strftime('%Y%m%d%H%M%S %z'), stop=e.strftime('%Y%m%d%H%M%S %z'))
                good.append(q)
            if not fresh(good,now):
                if fresh(programmes[name],now):
                    print(f'Using fresh existing base schedule for {name}')
                    continue
                if not good:
                    # Upstream permanently dropped this exact id (e.g. Rakuten
                    # re-issued the channel under a new id). There is nothing to
                    # overlay, so publish with whatever the base build has (or
                    # without a guide) instead of failing the whole 645-channel
                    # refresh over one channel. A stale-but-nonempty source feed
                    # still fails hard below.
                    missing.append(name)
                    print(f'WARNING: no schedule source for {name}; keeping base data as-is')
                    continue
                raise ValueError(f'no fresh schedule for {name}; will not publish a partial build')
            remove.update(programmes[name]); programmes[name] = good; add.extend(good); applied.add(name)
            if name not in chars:
                c = ET.Element('channel',id=name); ET.SubElement(c,'display-name').text=name
                new_channels.append(c); chars[name]=c

    ids = Counter(i for _,i,_ in jp+taiwan if i)
    aliases = 0
    for name, i, _ in jp:
        if not i or i == name or i in chars or ids[i] != 1 or not programmes[name]:
            continue
        c = ET.Element('channel',id=i)
        # Keep name-based player lookups unambiguous; canonical channel remains by name.
        ET.SubElement(c,'display-name').text=i
        chars[i]=c; new_channels.append(c); aliases += 1
        for p in programmes[name]:
            q=copy.deepcopy(p); q.set('channel',i); add.append(q)
    root[:] = [c for c in root if c.tag=='channel'] + new_channels + [p for p in root if p.tag!='channel' and p not in remove] + add
    return len(applied), aliases, missing


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--playlist-dir', type=Path, required=True)
    cli.add_argument('--epg-dir', type=Path, default=Path('epg'))
    args=cli.parse_args()
    jp,taiwan = [list(playlist((args.playlist_dir/(k+'.m3u')).read_text(encoding='utf-8-sig'))) for k in ('japan','taiwan')]
    if not jp or not taiwan:
        raise ValueError('empty target playlist')
    names={n for n,_,_ in jp+taiwan}
    now=datetime.now(BJ)
    with ThreadPoolExecutor(max_workers=4) as pool:
        base, osaka_xml, tw_xml, fast = [f.result() for f in [pool.submit(retry,fn) for fn in (base_extras, osaka_extras, tw_extras, fast_extras)]]
    fast_xml, external = fast
    official = {i:u for _,i,u in external if i in FAST_IDS}
    fast_targets = {}
    for name,i,u in jp:
        if i in FAST_IDS:
            if official.get(i)!=u:
                raise ValueError(f'FAST feed changed for {name}; verify before remapping')
            fast_targets[i]=name
    fixed = lambda channel_names: {n:n for n in channel_names if n in names}
    guides = [(base,fixed([n for rows in TOKYO.values() for n in rows]+['TVBS新聞'])), (osaka_xml,fixed(osaka.STATIONS.values())), (tw_xml,fixed(['中天新聞台','龍祥時代','cna'])), (fast_xml,fast_targets)]
    xml = args.epg_dir/'epg.xml'
    tree=ET.parse(xml)
    applied,aliases,missing=overlay(tree.getroot(),guides,jp,taiwan,now)
    ET.indent(tree,space='  ')
    data=ET.tostring(tree.getroot(), encoding='utf-8', xml_declaration=True)
    xml.write_bytes(data)
    (args.epg_dir/'epg.xml.gz').write_bytes(gzip.compress(data,mtime=0))
    print(f'Japan/Taiwan schedules supplemented: {applied}; Japan tvg-id aliases: {aliases}')
    if missing:
        print(f'Channels published without a guide (upstream id gone): {", ".join(missing)}')


if __name__ == '__main__':
    main()
