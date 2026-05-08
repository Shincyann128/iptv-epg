import importlib.util
import json
import sys
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'check_playlist_sync.py'
spec = importlib.util.spec_from_file_location('check_playlist_sync', MODULE_PATH)
check_playlist_sync = importlib.util.module_from_spec(spec)
sys.modules['check_playlist_sync'] = check_playlist_sync
spec.loader.exec_module(check_playlist_sync)


def test_extract_playlist_channels_deduplicates(tmp_path):
    playlist = tmp_path / 'sample.m3u'
    playlist.write_text(
        '#EXTM3U\n'
        '#EXTINF:-1,Channel A\nurl1\n'
        '#EXTINF:-1,Channel B\nurl2\n'
        '#EXTINF:-1,Channel A\nurl3\n',
        encoding='utf-8',
    )

    assert check_playlist_sync.extract_playlist_channels(playlist) == ['Channel A', 'Channel B']


def test_main_returns_nonzero_for_missing_channel_with_require_exact(tmp_path, monkeypatch, capsys):
    playlist = tmp_path / 'sample.m3u'
    playlist.write_text('#EXTM3U\n#EXTINF:-1,Channel A\nurl1\n', encoding='utf-8')

    channel_map = tmp_path / 'channel_map.json'
    channel_map.write_text(json.dumps({'channels': {'Channel B': {'enabled': False}}}), encoding='utf-8')

    monkeypatch.setattr(
        sys,
        'argv',
        ['check_playlist_sync.py', '--playlist', str(playlist), '--map', str(channel_map), '--require-exact'],
    )

    code = check_playlist_sync.main()
    out = capsys.readouterr().out

    assert code == 1
    assert 'Channel A' in out
    assert 'Channel B' in out
