import copy
import gzip
import importlib.util
import json
import sys
from pathlib import Path

import pytest
import xml.etree.ElementTree as ET


MODULE_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'build_epg.py'
spec = importlib.util.spec_from_file_location('build_epg', MODULE_PATH)
build_epg = importlib.util.module_from_spec(spec)
sys.modules['build_epg'] = build_epg
spec.loader.exec_module(build_epg)


def test_alias_of_inherits_source_and_epg_name_when_missing():
    channels = {
        'Parent': {'enabled': True, 'source': 'JP', 'epg_name': '親チャンネル'},
        'Child': {'enabled': True, 'alias_of': 'Parent'},
    }

    targets = build_epg.build_targets_by_source(channels)

    assert targets['JP']['親チャンネル'] == ['Parent', 'Child']


def test_alias_of_missing_target_does_not_break_explicit_mapping():
    channels = {
        '4K': {'enabled': True, 'source': 'CN', 'epg_name': 'CCTV4K', 'alias_of': 'CCTV4K'},
    }

    targets = build_epg.build_targets_by_source(channels)

    assert targets['CN']['CCTV4K'] == ['4K']


def test_fetch_bytes_retries_then_succeeds(monkeypatch):
    attempts = {'count': 0}

    class DummyResp:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b'ok'

    def fake_urlopen(req, timeout=120):
        attempts['count'] += 1
        if attempts['count'] < 3:
            raise TimeoutError('boom')
        return DummyResp()

    monkeypatch.setattr(build_epg.urllib.request, 'urlopen', fake_urlopen)
    monkeypatch.setattr(build_epg.time, 'sleep', lambda _: None)

    assert build_epg.fetch_bytes('https://example.com/feed.xml') == b'ok'
    assert attempts['count'] == 3


def test_parse_source_skips_failed_source_instead_of_crashing(monkeypatch):
    root = ET.Element('tv')

    def fake_fetch(url):
        raise OSError('source down')

    monkeypatch.setattr(build_epg, 'fetch_bytes', fake_fetch)

    stats = build_epg.parse_source('JP', 'https://bad.example/xml', {'親チャンネル': ['Parent']}, root)

    assert stats['channels'] == 0
    assert stats['programmes'] == 0
    assert 'error' in stats
    assert list(root) == []


def test_generator_info_url_points_to_public_repo(tmp_path, monkeypatch):
    channel_map = {
        'sources': {},
        'channels': {},
    }
    map_path = tmp_path / 'channel_map.json'
    out_xml = tmp_path / 'epg.xml'
    out_gz = tmp_path / 'epg.xml.gz'
    map_path.write_text(json.dumps(channel_map), encoding='utf-8')

    monkeypatch.setattr(build_epg, 'MAP_PATH', map_path)
    monkeypatch.setattr(build_epg, 'OUT_XML', out_xml)
    monkeypatch.setattr(build_epg, 'OUT_GZ', out_gz)

    build_epg.main()

    text = out_xml.read_text(encoding='utf-8')
    assert 'generator-info-url="https://github.com/Shincyann128/iptv-epg"' in text


def test_overlap_cleanup_prefers_longer_programme_for_wowow_channel():
    root = ET.Element('tv')
    seen = set()
    last = {}

    long_prog = ET.fromstring('<programme channel="src" start="20260425010000 +0900" stop="20260425030000 +0900"><title>长节目</title></programme>')
    short_prog = ET.fromstring('<programme channel="src" start="20260425013000 +0900" stop="20260425014000 +0900"><title>短插播</title></programme>')

    build_epg.append_programme(root, seen, last, 'WOWOW Live', copy.deepcopy(long_prog), '20260425000000 +0800', '20260425020000 +0800', '长节目', 'JP')
    build_epg.append_programme(root, seen, last, 'WOWOW Live', copy.deepcopy(short_prog), '20260425003000 +0800', '20260425004000 +0800', '短插播', 'JP')

    titles = [node.findtext('title') for node in root.findall('programme')]
    assert titles == ['长节目']
