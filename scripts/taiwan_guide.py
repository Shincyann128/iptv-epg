#!/usr/bin/env python3
"""Build a scratch XMLTV supplement from two official guides and epg.pw TW."""
import argparse
import gzip
from collections import Counter
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

TAIPEI = timezone(timedelta(hours=8))
LS = 'https://www.lstime.com.tw/schedule'
CTI_PAGE = 'https://www.ctitv.com.tw/program/program.php?chid=52'
CTI = 'https://asia-east1-ctitv-237901.cloudfunctions.net/ProgramList-Api2'
TW = 'https://epg.pw/xmltv/epg_TW.xml.gz'
CHANNELS = [('中天新聞台', '中天新聞台', CTI_PAGE, 'zh'),
            ('龍祥時代', '龍祥時代', LS, 'zh'),
            ('cna', 'cna', 'https://www.channelnewsasia.com/watch/tv-schedule', 'en')]


def fetch(url):
    with urlopen(Request(url, headers={'User-Agent': 'Mozilla/5.0'}), timeout=20) as r:
        return r.read()


class LSPage(HTMLParser):
    def __init__(self, payload):
        super().__init__(convert_charrefs=True)
        self.month = self.day = self.field = self.cell = None
        self.depth = 0
        self.rows = []
        self.feed(payload.decode('utf-8-sig'))
        self.close()

    def handle_starttag(self, tag, attributes):
        a = dict(attributes)
        classes = a.get('class', '').split()
        if tag == 'div':
            if 'time-list' in classes:
                self.depth, self.day = 1, None
            elif self.depth:
                self.depth += 1
        if self.depth and tag == 'li':
            self.cell = {}
        if tag == 'span' and a.get('id') == 'navMonthTitle':
            self.field = 'month'
        elif self.depth and tag == 'span' and 'viewDate' in classes:
            self.field = 'day'
        elif self.cell is not None and tag in ('time', 'h2'):
            self.field = tag
        else:
            return
        self.chunks = []

    def handle_data(self, text):
        if self.field:
            self.chunks.append(text)

    def handle_endtag(self, tag):
        if self.field and (tag == self.field or (self.field in ('month', 'day') and tag == 'span')):
            value = ' '.join(''.join(self.chunks).split())
            if self.field in ('month', 'day'):
                setattr(self, self.field, value)
            else:
                self.cell[self.field] = value
            self.field = None
        if tag == 'li' and self.cell is not None:
            self.rows.append((self.day, self.cell['time'], self.cell['h2']))
            self.cell = None
        if tag == 'div' and self.depth:
            self.depth -= 1


def parse_ls(payload):
    page = LSPage(payload)
    if not page.rows or not re.fullmatch(r'\d{4}-\d{2}', page.month or ''):
        raise ValueError('No official LS TIME rows/month found')
    year, month = map(int, page.month.split('-'))
    by_date = defaultdict(list)
    for day, clock, title in page.rows:
        m, d = map(int, day.split('/'))
        y = year + (m == 1 and month == 12) - (m == 12 and month == 1)
        h, n = map(int, clock.split(':'))
        when = datetime(y, m, d, h, n, tzinfo=TAIPEI)
        by_date[when.date()].append((when, title))

    points = []
    for day, entries in by_date.items():
        # The site can publish a whole day's film titles before it has their
        # start times. Every row in that case is displayed as 00:00.
        if len(entries) > 1 and all(not start.hour and not start.minute for start, _ in entries):
            print(f'WARNING: LS TIME has no confirmed start times for {day}')
            continue
        for when, title in entries:
            if not title or (points and when <= points[-1][0]):
                raise ValueError('LS TIME row is blank or out of order')
            points.append((when, title))

    # Stop at the next announced start if there is no intervening day without
    # a schedule. Keep the stop unknown across a gap and on the final row.
    programmes = []
    for i, (when, title) in enumerate(points):
        next_start = points[i+1][0] if i+1 < len(points) else None
        if next_start is not None and (next_start.date() - when.date()).days > 1:
            next_start = None
        programmes.append((when, next_start, title))
    return programmes


def parse_cti(payload):
    text = payload.decode('utf-8-sig').strip()
    wrapped = re.fullmatch(r'ctiEPG\s*\((.*)\)\s*;?', text, re.S)
    rows = json.loads(wrapped[1] if wrapped else text)
    if not isinstance(rows, list) or not rows:
        raise ValueError('Empty CTI programme list')
    def local(s):
        # Verified against the official calendar: the literal T06:00:00Z
        # 06中天晨報 is drawn at 06:00 in TW, not 14:00.
        if not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ', s):
            raise ValueError('Unexpected CTI timestamp format')
        return datetime.fromisoformat(s[:-1]).replace(tzinfo=TAIPEI)
    if any(str(row.get('channel_id')) != '52' for row in rows):
        raise ValueError('Unexpected CTI channel')
    return [(local(row['start']), local(row['end']), row['title'].strip()) for row in rows]


def parse_cna(payload):
    root = ET.fromstring(gzip.decompress(payload))
    channel = root.find('channel[@id="456649"]')
    if channel is None or channel.findtext('display-name') != 'Channel NewsAsia':
        raise ValueError('CNA source ID changed')
    def local(s):
        # TW feed encodes SGT/Taipei wall time with a misleading +0000.
        if not re.fullmatch(r'\d{14} \+0000', s):
            raise ValueError('Unexpected CNA timestamp format')
        return datetime.strptime(s[:14], '%Y%m%d%H%M%S').replace(tzinfo=TAIPEI)
    rows = sorted((local(p.attrib['start']), local(p.attrib['stop']), p.findtext('title', '').strip())
                  for p in root.findall('programme[@channel="456649"]'))
    # This feed sometimes ends with a single midnight-to-midnight row.
    # It is not a full day of listings; the official guide has multiple slots.
    if rows and rows[-1][0].hour == rows[-1][0].minute == rows[-1][0].second == 0 and rows[-1][1] - rows[-1][0] == timedelta(days=1):
        rows.pop()
    return rows


def format_guide(rows_by_channel, first, end, *, optional_channels=()):
    root = ET.Element('tv', {'generator-info-name': 'Taiwan EPG supplement'})
    stamp = lambda v: v.strftime('%Y%m%d%H%M%S %z')
    lo = datetime.combine(first, datetime.min.time(), TAIPEI)
    hi = datetime.combine(end, datetime.min.time(), TAIPEI)
    for cid, name, url, lang in CHANNELS:
        c = ET.SubElement(root, 'channel', id=cid)
        ET.SubElement(c, 'display-name', lang=lang).text = name
        ET.SubElement(c, 'url').text = url
    for cid, _, _, lang in CHANNELS:
        source_rows = rows_by_channel.get(cid, ()) if cid in optional_channels else rows_by_channel[cid]
        rows = sorted({(s, e, t) for s, e, t in source_rows
                       if s < hi and (e > lo if e else s >= lo)}, key=lambda x: x[0])
        if not rows or not any(s.date() == first for s, _, _ in rows):
            if cid in optional_channels:
                print(f'WARNING: {cid} has no complete guide for {first}; retaining the base schedule')
                continue
            raise ValueError(f'{cid}: no programmes starting on {first}')
        prev_start = prev_end = None
        for s, e, t in rows:
            if not t or (e and e <= s) or (prev_start and s <= prev_start) or (prev_end and s < prev_end):
                raise ValueError(f'{cid}: malformed or overlapping programme at {s}')
            prev_start, prev_end = s, e
            attrs = {'start': stamp(s), 'channel': cid}
            if e:
                attrs['stop'] = stamp(e)
            ET.SubElement(ET.SubElement(root, 'programme', attrs), 'title', lang=lang).text = t
        print(cid, len(rows), stamp(rows[0][0]), '→', stamp(rows[-1][1] or rows[-1][0]),
              'last stop unknown' if rows[-1][1] is None else '')
    ET.indent(root)
    return ET.tostring(root, encoding='utf-8', xml_declaration=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--start', type=date.fromisoformat, default=datetime.now(TAIPEI).date())
    p.add_argument('--days', type=int, default=8, choices=range(1, 15), metavar='1–14')
    p.add_argument('--fixtures', type=Path, help='read ls, cti-events and tw from this directory')
    p.add_argument('--basename', default='taiwan-supplement')
    a = p.parse_args()
    if not re.fullmatch(r'[a-zA-Z0-9_-]+', a.basename):
        p.error('basename must be a simple filename; output stays next to script')
    end = a.start + timedelta(days=a.days)
    cti_url = CTI + '?' + urlencode(dict(chid=52, start=a.start-timedelta(days=1), end=end, callback='ctiEPG'))
    def get(name, url):
        return (a.fixtures / name).read_bytes() if a.fixtures else fetch(url)
    rows = {'中天新聞台': parse_cti(get('cti-events', cti_url)),
            '龍祥時代': parse_ls(get('ls', LS)), 'cna': parse_cna(get('tw', TW))}
    xml = format_guide(rows, a.start, end)
    base = Path(__file__).resolve().parent / a.basename
    base.with_suffix('.xml').write_bytes(xml)
    base.with_suffix('.xml.gz').write_bytes(gzip.compress(xml, mtime=0))


if __name__ == '__main__':
    main()
