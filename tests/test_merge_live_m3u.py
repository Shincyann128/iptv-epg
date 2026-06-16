import importlib.util
import sys
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'merge_live_m3u.py'
spec = importlib.util.spec_from_file_location('merge_live_m3u', MODULE_PATH)
merge_live_m3u = importlib.util.module_from_spec(spec)
sys.modules['merge_live_m3u'] = merge_live_m3u
spec.loader.exec_module(merge_live_m3u)


SAMPLE_M3U = '''#EXTM3U
#EXTINF:-1 group-title="看球通-足球",欧联杯 弗赖堡 vs 阿斯顿维拉 - 官方直播1 [欧联杯]
https://same.example/stream1.m3u8
#EXTINF:-1 tvg-name="欧联杯 弗赖堡 vs 阿斯顿维拉｜官方直播1" group-title="欧联杯",欧联杯 弗赖堡 vs 阿斯顿维拉｜官方直播1
https://same.example/stream1.m3u8
#EXTINF:-1 group-title="NBA 季后赛",NBA 骑士VS猛龙(G2) [篮球回放08] [HLS]
https://vod.example/replay.m3u8
'''


def test_parse_m3u_extracts_entries():
    entries = merge_live_m3u.parse_m3u(SAMPLE_M3U, source='样例源')

    assert len(entries) == 3
    assert entries[0]['group'] == '看球通-足球'
    assert entries[0]['name'] == '欧联杯 弗赖堡 vs 阿斯顿维拉 - 官方直播1 [欧联杯]'
    assert entries[0]['source'] == '样例源'


def test_merge_entries_deduplicates_by_url_and_sorts_live_before_replay():
    entries = merge_live_m3u.parse_m3u(SAMPLE_M3U, source='咖啡直播')

    sports_processed = merge_live_m3u.process_sports_entries(entries)
    # Stream1 (duplicate URL) → deduped to 1 entry
    # Replay with '回放' in name → filtered out by _FILTER_NAMES
    assert len(sports_processed) == 1
    assert sports_processed[0]['url'] == 'https://same.example/stream1.m3u8'
    assert sports_processed[0]['source_short'] == '咖啡'


def test_render_adds_source_prefix_and_header_summary():
    entries = [
        {
            'source': '看球通',
            'group': '足球',
            'name': '欧联杯 弗赖堡 vs 阿斯顿维拉',
            'url': 'https://live.example/a.m3u8',
            'attrs': 'group-title="足球"',
        },
        {
            'source': '看球吧',
            'group': '篮球回放',
            'name': 'NBA 骑士VS猛龙',
            'url': 'https://vod.example/b.m3u8',
            'attrs': 'group-title="篮球回放"',
        },
    ]

    text = merge_live_m3u.render_m3u(entries)

    assert '# Sources: 自用, 动态地方台, FWC4K, 看球通, 咖啡直播, 咪咕直播, 看球吧, 857直播, popo直播, live-event, PPV, damizhibo' in text
    assert '# Total streams: 2' in text
    assert '欧联杯 弗赖堡 vs 阿斯顿维拉' in text
    assert 'NBA 骑士VS猛龙' in text


def test_canonical_group_prefers_esports_keywords_over_basketball_terms():
    assert merge_live_m3u.render_sports_group_title({'sport': '电竞', 'group': '看球通-电竞'}) == '电竞'
