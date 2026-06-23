import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock


MODULE_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'update_857_live.py'
spec = importlib.util.spec_from_file_location('update_857_live', MODULE_PATH)
update_857_live = importlib.util.module_from_spec(spec)
sys.modules['update_857_live'] = update_857_live
spec.loader.exec_module(update_857_live)


def test_parse_jsonp_supports_857_wrapped_payload():
    payload = update_857_live.parse_jsonp('detail({"code":200,"data":{"ok":true}})')
    assert payload['code'] == 200
    assert payload['data']['ok'] is True


def test_flatten_rooms_deduplicates_nested_data_via_fetch_build_helpers(monkeypatch):
    rooms = update_857_live.flatten_rooms({'0': [{'roomNum': '1'}], '1': [{'roomNum': '2'}]})
    assert [room['roomNum'] for room in rooms] == ['1', '2']


def test_build_entry_prefers_hd_m3u8_and_decodes_auth_key():
    room = {'roomNum': '746673', 'title': '瑞典超 卡尔马 VS 代格福什', 'liveTypeParent': 1}
    detail = {
        'room': room,
        'stream': {
            'flv': 'https://example.com/a.flv?auth_key\\u003dold',
            'hdM3u8': 'https://example.com/a.m3u8?auth_key\\u003dnew',
        },
    }
    entry = update_857_live.build_entry(room, detail)
    assert entry['name'] == '瑞典超 卡尔马 VS 代格福什'
    assert entry['group'] == '足球'
    assert entry['url'] == 'https://example.com/a.m3u8?auth_key=new'


def test_render_outputs_valid_m3u():
    text = update_857_live.render([
        {'name': '中超 A vs B', 'group': '足球', 'url': 'https://example.com/live.m3u8', 'logo': ''}
    ])
    assert text.startswith('#EXTM3U')
    assert 'group-title="足球"' in text
    assert 'https://example.com/live.m3u8' in text


def test_resolve_m3u8_extracts_sub_playlist(monkeypatch):
    mock_resp = MagicMock()
    mock_resp.read.return_value = (
        b'#EXTM3U\n'
        b'#EXT-X-STREAM-INF:BANDWIDTH=2560000\n'
        b'https://abc123.livehwc4.com/path/stream_lhd.m3u8?sub_m3u8=true&auth_key=key\n'
    )
    mock_resp.__enter__.return_value = mock_resp

    def fake_urlopen(req, timeout=None):
        return mock_resp

    monkeypatch.setattr(update_857_live.urllib.request, 'urlopen', fake_urlopen)
    result = update_857_live.resolve_m3u8_url(
        'https://pullsgp.yyzb456.top/stream.m3u8?auth_key=key'
    )
    assert 'livehwc4.com' in result
    assert 'sub_m3u8=true' in result


def test_resolve_m3u8_falls_back_on_failure(monkeypatch):
    def fake_urlopen(req, timeout=None):
        raise OSError("fail")

    monkeypatch.setattr(update_857_live.urllib.request, 'urlopen', fake_urlopen)
    result = update_857_live.resolve_m3u8_url(
        'https://pullsgp.yyzb456.top/stream.m3u8?auth_key=key'
    )
    assert result == 'https://pullsgp.yyzb456.top/stream.m3u8?auth_key=key'


def test_resolve_m3u8_skips_flv():
    result = update_857_live.resolve_m3u8_url('https://example.com/stream.flv?auth_key=key')
    assert result == 'https://example.com/stream.flv?auth_key=key'
