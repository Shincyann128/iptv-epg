#!/usr/bin/env python3
"""popozhibo.xyz → M3U 直播源生成

从 popozhibo.xyz 拉取正在直播的体育赛事，解密并提取 M3U8 直连地址。

Usage:
    python3 popozhibo_m3u.py              # 输出 M3U 到 stdout
    python3 popozhibo_m3u.py -o xxx.m3u   # 输出到文件

集成到 merge_live_m3u.py:
    import popozhibo_m3u
    text = popozhibo_m3u.generate_m3u()
"""

import json
import re
import sys
import time
import base64
from urllib.parse import unquote, urlparse, parse_qs
from urllib.request import Request, urlopen

BASE = "https://www.popozhibo.xyz"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
TIMEOUT = 10

# ── HTTP ──────────────────────────────────────────────

def http_get(url, referer=None):
    headers = {"User-Agent": UA, "Accept": "text/html,application/json,*/*"}
    if referer:
        headers["Referer"] = referer
    req = Request(url, headers=headers)
    with urlopen(req, timeout=TIMEOUT) as resp:
        return resp.read().decode("utf-8", errors="replace")


def fetch_games():
    """从 /live 页面抓取比赛列表，返回 list[dict]"""
    html = http_get(f"{BASE}/live")
    games = []
    blocks = re.split(r"<li[^>]*>", html)
    for block in blocks:
        link = re.search(r"/live/(\d+)/play", block)
        if not link:
            continue
        time_m = re.search(r"(\d+-\d+\s+\d+:\d+)", block)
        league_m = re.search(r'game-name[^>]*>([^<]+)', block)
        left_m = re.search(r'left-team-name[^>]*>([^<]+)', block)
        right_m = re.search(r'right-team-name[^>]*>([^<]+)', block)
        status_m = re.search(r'game-status[^>]*>([^<]+)', block)
        if not left_m or not right_m:
            continue
        games.append({
            "id": link.group(1),
            "time": time_m.group(1) if time_m else "?",
            "league": league_m.group(1).strip() if league_m else "?",
            "home": left_m.group(1).strip(),
            "away": right_m.group(1).strip(),
            "status": status_m.group(1).strip() if status_m else "?",
        })
    return games


def decode_source(data_field):
    """解码 /source API 的 data 字段 → dict"""
    stripped = data_field[6:-2]
    decoded = base64.b64decode(stripped)
    text = unquote(decoded.decode("latin-1"))
    result = json.loads(text)
    for link in result.get("links", []):
        try:
            link["name"] = link["name"].encode("latin-1").decode("utf-8")
        except (UnicodeError, AttributeError):
            pass
    return result


def fetch_links(game_id):
    """获取一场比赛的直播源链接列表"""
    try:
        raw = http_get(f"{BASE}/live/{game_id}/source")
        data = json.loads(raw)
        decoded = decode_source(data["data"])
        return decoded.get("links", [])
    except Exception:
        return []


def extract_m3u8(links):
    """从 links 中提取直连 M3U8 URL。保留 txTime，并跳过已过期 URL。"""
    direct = []
    now = int(time.time())
    for link in links:
        url = link.get("url", "")
        name = link.get("name", "?")
        # 提取包裹在 88player.top 里的内层 URL。
        # 注意：popo 的 wrapper 没有 URL-encode 内层地址，导致 txTime 被 parse_qs
        # 解析成外层参数；必须手动拼回去，否则直链缺 txTime 会直接 404。
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        inner = qs.get("url", [""])[0]
        real_url = inner or url
        if inner and "txTime=" not in real_url and qs.get("txTime"):
            sep = "&" if "?" in real_url else "?"
            real_url = f"{real_url}{sep}txTime={qs['txTime'][0]}"

        real_qs = parse_qs(urlparse(real_url).query)
        tx_time = real_qs.get("txTime", [None])[0]
        if tx_time:
            try:
                # 留 60 秒安全边界，避免刚写进 M3U 就过期。
                if int(tx_time) <= now + 60:
                    continue
            except ValueError:
                pass

        # mayizhibo B-lines are Cloudflare-protected: curl/APTV get HTTP 403 even
        # with UA/Referer/Origin. Do not publish them into M3U as dead links.
        if "elive.mayizhibo.net" in real_url:
            continue

        real_path = urlparse(real_url).path.lower()
        if real_path.endswith(".m3u8"):
            direct.append((real_url, name))
        elif "play1nm.hnyongshun.cn" in real_url and real_path.endswith(".m3u8"):
            direct.append((real_url, name))
    return direct


# ── Sport detection ───────────────────────────────────

BASKETBALL_KW = ["篮球", "篮甲", "篮冠", "欧篮", "NBA", "CBA"]
BASEBALL_KW = ["韩职棒", "KBO", "职棒", "棒球"]
ESPORTS_KW = ["LPL", "LCK", "LEC", "LCS", "电竞", "英雄联盟", "王者荣耀", "DOTA"]
FOOTBALL_KW = ["足球", "中超", "英超", "西甲", "德甲", "意甲", "法甲", "欧冠", "亚冠",
               "日职", "韩K", "J1", "J2", "K1", "K2", "K3", "中甲", "女足",
               "越南甲", "菲MPBL", "城市足球", "超级联赛", "南韩", "世界杯"]


def detect_sport(league_name):
    # 当前总分类没有“棒球”，先归综合，避免误放进篮球区。
    for kw in BASEBALL_KW:
        if kw in league_name:
            return "综合"
    for kw in BASKETBALL_KW:
        if kw in league_name:
            return "篮球"
    for kw in ESPORTS_KW:
        if kw in league_name:
            return "电竞"
    for kw in FOOTBALL_KW:
        if kw in league_name:
            return "足球"
    return "综合"


# ── M3U generation ────────────────────────────────────

def generate_m3u():
    """抓取所有直播中比赛，返回 M3U 文本"""
    import concurrent.futures

    games = fetch_games()
    # 只保留直播中的比赛
    live_statuses = {"直播中", "进行中", "上半场", "下半场", "中场", "加时", "点球"}
    live_games = [g for g in games if g["status"] in live_statuses]
    if not live_games:
        return "#EXTM3U\n# No live games from popozhibo.xyz\n"

    # 并发获取源
    lines = ["#EXTM3U"]
    count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        future_map = {
            pool.submit(fetch_links, g["id"]): g for g in live_games
        }
        done, _ = concurrent.futures.wait(future_map, timeout=25)
        for future in done:
            g = future_map[future]
            try:
                links = future.result()
            except Exception:
                continue
            m3u8_urls = extract_m3u8(links)
            if not m3u8_urls:
                continue
            sport = detect_sport(g["league"])
            for url, line_name in m3u8_urls:
                title = f"{g['league']} {g['home']} vs {g['away']} | {line_name}"
                title = title.replace('"', "'")
                lines.append(f'#EXTINF:-1 group-title="{sport}",{title}')
                lines.append(url)
                count += 1

    if count == 0:
        return "#EXTM3U\n# No streams available from popozhibo.xyz\n"
    return "\n".join(lines) + "\n"


# ── Standalone CLI ────────────────────────────────────

def main():
    import argparse
    import os
    parser = argparse.ArgumentParser(description="popozhibo.xyz → M3U")
    parser.add_argument("-o", "--output", help="输出文件路径，默认输出到 stdout")
    args = parser.parse_args()

    print("Fetching popozhibo live games...", file=sys.stderr)
    text = generate_m3u()

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"Written to {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
