#!/usr/bin/env python3
"""Merge static + sports M3U with per-source parsing & match-grouped output.

Sports entries from 看球通/咖啡直播/看球吧/857直播 are parsed per-source,
clustered by match within the same sport+league, and rendered in
a uniform `[src] league team1 vs team2 | extra` format.

Static channels (自用) are left unchanged and placed first.
"""

import importlib.util
import io
import re
import sys
import unicodedata
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from tempfile import NamedTemporaryFile

BJ_TZ = timezone(timedelta(hours=8))

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
OUTPUT_FILE = REPO_ROOT / "live_merged.m3u"
MYSELF_CACHE = REPO_ROOT / "myself.m3u"
DYNAMIC_LOCAL_CACHE = REPO_ROOT / "dynamic_local_cache.m3u"
DYNAMIC_LOCAL_URL = "http://m3u.sjbox.cc/113.m3u"
DYNAMIC_LOCAL_CACHE_TTL = 3 * 3600  # 3 hours; sports merge still runs every 15 min
PPV_CACHE = REPO_ROOT / "ppv_cache.m3u"
PPV_CACHE_TTL = 12 * 3600  # 12 hours
FWC4K_URL = "http://82.156.243.185:33389/fwc.m3u"
TV1288_URL = "https://itv.tv1288.xyz"
TV1288_CACHE = REPO_ROOT / "tv1288_cache.txt"
TV1288_CACHE_MAX_AGE = 24 * 3600  # merge skips cache older than 24h
SOURCES = ["自用", "动态地方台", "FWC4K", "看球通", "咖啡直播", "咪咕直播", "看球吧", "857直播", "popo直播", "live-event", "PPV", "damizhibo"]
REPLAY_KEYWORDS = ("回放", "录像", "VOD")

DYNAMIC_LOCAL_TARGET_ORDER = [
    "青岛电视台1套", "青岛1", "青岛电视台2套", "青岛2",
    "青岛电视台3套", "青岛3", "青岛电视台4套", "青岛4",
    "城阳综合", "李沧TV", "即墨综合", "泰山TV",
    "北京新闻", "北京文艺", "北京生活", "北京影视", "北京财经",
    "北京体育休闲", "北京纪实科教", "北京纪实", "北京国际",
]
DYNAMIC_LOCAL_TARGETS = set(DYNAMIC_LOCAL_TARGET_ORDER)

# ── Source short names & ordering ──
SOURCE_SHORT = {"FWC4K": "4K杜比", "857直播": "857", "咖啡直播": "咖啡", "咪咕直播": "咪咕", "看球吧": "看球吧", "看球通": "看球通", "popo直播": "popo", "live-event": "看个球", "PPV": "PPV", "damizhibo": "dami"}
# Within output groups: 4K杜比 → 咖啡 → 咪咕 → 看个球 → popo → dami → 857 → 看球吧 → 看球通 → PPV
SOURCE_ORDER = {"FWC4K": -1, "咖啡直播": 0, "咪咕直播": 1, "live-event": 2, "popo直播": 3, "damizhibo": 4, "857直播": 5, "看球吧": 6, "看球通": 7, "PPV": 8}
SPORT_ORDER = {"足球": 0, "篮球": 1, "电竞": 2, "综合": 3, "回放": 4}

# ── Static category ordering (unchanged) ──
STATIC_CATEGORIES = {
    "央视频道": -10,
    "卫视频道": -9,
    "北京": -8,
    "日本": -7,
    "国际": -6,
    "体育": -5,
    "4K": -4,
    "新闻": -3,
    "其他": 99,
}

CATEGORY_ORDER = {
    "足球": 0,
    "篮球": 1,
    "电竞": 2,
    "综合": 3,
    "回放": 9,
    "其他": 8,
}

# ══════════════════════════════════════════════════
#  Normalization helpers
# ══════════════════════════════════════════════════

# Traditional → Simplified (sports-domain character set)
_TRAD_MAP = {
    '聯': '联', '賽': '赛', '爾': '尔', '羅': '罗', '薩': '萨', '馬': '马',
    '隊': '队', '體': '体', '籃': '篮', '電': '电', '競': '竞', '視': '视',
    '臺': '台', '灣': '湾', '國': '国', '亞': '亚', '歐': '欧', '場': '场',
    '節': '节', '級': '级', '組': '组', '總': '总', '決': '决', '戰': '战',
    '鬥': '斗', '門': '门', '開': '开', '關': '关', '實': '实', '際': '际',
    '動': '动', '畫': '画', '樂': '乐', '廣': '广', '東': '东', '華': '华',
    '風': '风', '雲': '云', '龍': '龙', '鳳': '凤', '陽': '阳', '陰': '阴',
    '黃': '黄', '綠': '绿', '藍': '蓝', '紅': '红', '金': '金', '銀': '银',
    '銅': '铜', '鐵': '铁', '鋼': '钢', '烏': '乌', '納': '纳', '維': '维',
    '貝': '贝', '蘭': '兰', '萬': '万', '與': '与', '對': '对', '為': '为',
    '會': '会', '們': '们', '來': '来', '時': '时', '現': '现', '發': '发',
    '見': '见', '說': '说', '話': '话', '語': '语', '讓': '让', '進': '进',
    '過': '过', '還': '还', '這': '这', '後': '后', '從': '从', '沒': '没',
    '裡': '里', '倫': '伦', '爾': '尔', '姆': '姆', '特': '特', '斯': '斯',
    '克': '克', '拉': '拉', '夫': '夫', '尼': '尼', '科': '科', '曼': '曼',
    '森': '森', '堡': '堡', '茨': '茨', '赫': '赫', '塔': '塔', '基': '基',
    '庫': '库', '普': '普', '托': '托', '格': '格', '迪': '迪', '卡': '卡',
    '加': '加', '達': '达', '頓': '顿', '確': '确', '約': '约', '議': '议',
    '點': '点', '張': '张', '楊': '杨', '趙': '赵', '吳': '吴', '劉': '刘',
    '陳': '陈', '孫': '孙', '爾': '尔',
    # Sports-specific phonetic normalization
    '莎': '萨',   # 莎索羅 → 萨索洛 (Sassuolo)
    '羅': '罗',   # already mapped but ensure
}
_TRAD_TABLE = str.maketrans(_TRAD_MAP)


def to_simplified(text: str) -> str:
    return text.translate(_TRAD_TABLE)


# Phonetic normalization for sports team name variants
_PHONETIC_FIXES = [
    ("莎索罗", "萨索洛"),   # 857 uses 莎索羅→莎索罗, standard is 萨索洛 (Sassuolo)
    ("莎索洛", "萨索洛"),
    ("萨索罗", "萨索洛"),   # after trad→simp: 莎→萨 but 罗≠洛
    ("史托港", "斯托克港"),  # 857 phonetic variant (Stockport)
]


def normalize_text(text: str) -> str:
    """NFKC + traditional→simplified + whitespace collapse + VS normalization."""
    # Replace punctuation BEFORE NFKC (NFKC converts ：→: and ｜→|)
    text = text.replace('：', ' ').replace('｜', ' | ')
    text = unicodedata.normalize("NFKC", text)
    text = to_simplified(text)
    # 857-specific: convert special separator symbols (must be BEFORE decorative removal)
    text = text.replace('✔', ' vs ').replace('▲', ' vs ')
    # Strip bracket-wrapped league prefixes: 【英超】→ 英超, 〖中超〗→ 中超
    text = re.sub(r'[【〖]([^】〗]*)[】〗]', r' \1 ', text)
    # Remove decorative symbols and junk prefixes
    text = re.sub(r'[🔥💗◆◇●○★☆🇭🇰🇨🇳🇯🇵🍒🍿🎯⚽🏀🏆🥇]', ' ', text)
    text = re.sub(r'^[a-zA-Z]{1,2}\s+', '', text)  # stray prefix like "a中超"
    # Normalize other separator-like symbols to " vs "
    text = re.sub(r'\s*[–—]\s*', ' vs ', text)  # en-dash / em-dash
    # Normalize unspaced dash separator (e.g. "伯恩利-狼队")
    text = re.sub(r'([\u4e00-\u9fff\u3400-\u4dbf])-([\u4e00-\u9fff\u3400-\u4dbf])',
                  r'\1 - \2', text)
    # Normalize VS variants (handles both Latin and CJK adjacency)
    # Step 1: word-bounded (Latin context)
    text = re.sub(r'(?i)\bvs\b', ' vs ', text)
    # Step 2: CJK-adjacent (e.g. 队VS队, 耀vs布)
    text = re.sub(r'(?i)([\u4e00-\u9fff\u3400-\u4dbf])vs([\u4e00-\u9fff\u3400-\u4dbf])',
                  r'\1 vs \2', text)
    text = re.sub(r'(?i)([\u4e00-\u9fff\u3400-\u4dbf])vs\s', r'\1 vs ', text)
    text = re.sub(r'(?i)\svs([\u4e00-\u9fff\u3400-\u4dbf])', r' vs \1', text)
    # Step 3: catch-all for non-alpha adjacent (e.g. ")vs史", "(中)vs斯")
    text = re.sub(r'(?i)([^a-zA-Z])vs([^a-zA-Z])', r'\1 vs \2', text)
    # Step 4: alpha-before-CJK (e.g. "FCvs休") and CJK-before-alpha
    text = re.sub(r'(?i)([a-zA-Z])vs([\u4e00-\u9fff])', r'\1 vs \2', text)
    text = re.sub(r'(?i)([\u4e00-\u9fff])vs([a-zA-Z])', r'\1 vs \2', text)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    # Apply phonetic fixes for known sports variants
    for old, new in _PHONETIC_FIXES:
        text = text.replace(old, new)
    return text


def clean_team(text: str) -> str:
    """Light team name cleanup — no aggressive deletion."""
    text = text.strip()
    text = re.sub(r'[（(][^)）]*[)）]', '', text)   # remove parentheticals inc. （中）
    text = re.sub(r'[\[\]【】]', '', text)          # remove brackets
    # Only remove these suffixes when they're at the very end
    text = re.sub(r'(队|FC|俱乐部)$', '', text).strip()
    return text


# ══════════════════════════════════════════════════
#  Source-specific parsers
# ══════════════════════════════════════════════════

def parse_857(raw_name: str, group: str) -> dict:
    """Parse 857直播 entry name.

    Handles: 联赛 TeamA VS TeamB, （粤） variants, emoji, messy spacing.
    Non-match: 聊球, 直播间, 清源, etc.
    """
    line_label = ""
    text = normalize_text(raw_name)

    # Extract language tag: （粤）or （粵）
    m = re.match(r'^[(（]([^)）]*)[)）]\s*', text)
    if m:
        lang = m.group(1)
        if any(c in lang for c in '粤粵'):
            line_label = "粤语"
        text = text[m.end():]

    # Permanently excluded entries (not sports content at all)
    exclude_kw = ["清源"]
    if any(kw in text for kw in exclude_kw):
        return None  # completely discard

    # Non-match detection (classified as 综合, kept in output)
    non_match_kw = ["聊球", "直播间"]
    if any(kw in text for kw in non_match_kw):
        return {"sport": "综合", "is_match": False}

    # Split on ' vs '
    parts = re.split(r'\s+vs\s+', text, maxsplit=1)
    if len(parts) != 2:
        # Fallback: try " - " separator (some 857 entries)
        if ' - ' in text:
            parts = text.split(' - ', 1)
        else:
            return {"sport": "综合", "is_match": False}
    if len(parts) != 2:
        return {"sport": "综合", "is_match": False}

    left, right = parts
    left_parts = left.strip().split()
    if len(left_parts) < 2:
        return {"sport": "综合", "is_match": False}

    league = left_parts[0]
    team1 = " ".join(left_parts[1:])
    team2 = right.strip()

    sport = group or "足球"
    # Detect basketball
    if any(kw in text for kw in ["篮球", "篮甲", "篮冠", "欧篮", "NBA", "CBA"]):
        sport = "篮球"
    if any(kw in text for kw in ["LPL", "LCK", "电竞", "LOL"]):
        sport = "电竞"

    return {
        "sport": sport, "is_match": True,
        "league": league, "team1": clean_team(team1), "team2": clean_team(team2),
        "line_label": line_label,
    }


def parse_kafei(raw_name: str, group: str) -> dict:
    """Parse 咖啡直播 entry name.

    Format: 联赛 TeamA vs TeamB｜原声直播N / ｜官方直播N
    """
    text = normalize_text(raw_name)

    # Split main body from line info
    if " | " in text:
        main, line_info = text.split(" | ", 1)
    elif "|" in text:
        main, line_info = text.split("|", 1)
    else:
        main, line_info = text, ""

    # Normalize line label
    if line_info:
        m = re.match(r'(原声直播|官方直播)(\d+)?', line_info.strip())
        if m:
            label = m.group(1)[:2]  # 原声 or 官方
            num = m.group(2) or ""
            line_info = label + num
        else:
            line_info = line_info.strip()
    main = main.strip()

    # Parse: league team1 vs team2
    m = re.match(r'^(.+?)\s+(.+?)\s+vs\s+(.+?)$', main)
    if not m:
        return {"sport": "综合", "is_match": False}

    league, team1, team2 = m.groups()

    # Detect sport
    sport = "足球"
    if any(kw in text for kw in ["篮球", "篮甲", "篮冠", "欧篮", "NBA", "CBA", "菲专员杯", "菲篮杯"]):
        sport = "篮球"
    if any(kw in text for kw in ["电竞", "LOL", "DOTA", "CS2", "LPL", "LCK"]):
        sport = "电竞"

    return {
        "sport": sport, "is_match": True,
        "league": league.strip(), "team1": clean_team(team1), "team2": clean_team(team2),
        "line_label": line_info,
    }


def parse_kanqiu(raw_name: str, group: str) -> dict:
    """Parse 看球吧 entry name.

    Format: 联赛 TeamA VS TeamB [主播] [FLV/HLS]
    Replays have '回放' in name.
    """
    text = normalize_text(raw_name)

    # Replay detection
    if "回放" in text:
        return {"sport": "回放", "is_match": False}

    # Esports
    esports_kw = ["LPL", "LCK", "LEC", "LCS", "DOTA", "CS2", "英雄联盟", "王者荣耀"]
    is_esports = any(kw in text for kw in esports_kw)

    # Extract trailing [format] and [anchor]
    fmt = ""
    anchor = ""
    # Format: last [FLV] or [HLS]
    m_fmt = re.search(r'\[(FLV|HLS)\]\s*$', text)
    if m_fmt:
        fmt = m_fmt.group(1)
        text = text[:m_fmt.start()].strip()
    # Anchor: last remaining [...] before format
    m_anchor = re.search(r'\[([^\]]+)\]\s*$', text)
    if m_anchor and not re.match(r'^(FLV|HLS)$', m_anchor.group(1)):
        anchor = m_anchor.group(1)
        text = text[:m_anchor.start()].strip()

    # Parse: league team1 vs team2
    m = re.match(r'^(.+?)\s+(.+?)\s+vs\s+(.+?)$', text)
    if not m:
        sport = "电竞" if is_esports else "综合"
        return {"sport": sport, "is_match": False}

    league, team1, team2 = m.groups()

    sport = "电竞" if is_esports else "足球"
    # Basketball detection
    if any(kw in text for kw in ["篮球", "篮甲", "篮冠", "欧篮", "NBA", "CBA"]):
        sport = "篮球"

    return {
        "sport": sport, "is_match": True,
        "league": league.strip(), "team1": clean_team(team1), "team2": clean_team(team2),
        "anchor": anchor, "format": fmt,
    }


def parse_kqt(raw_name: str, group: str) -> dict:
    """Parse 看球通 entry name.

    Format: TeamA vs TeamB - Anchor [League]
    Esports: LPL WBG⚔LGD - 粤语解说-广佬
    NBA: 🏀NBA TeamA vs TeamB 回放🏀 - anchor
    """
    text = normalize_text(raw_name)

    # Replay
    if "回放" in text:
        return {"sport": "回放", "is_match": False}

    # Esports: ⚔ symbol
    if "LPL" in text or "LCK" in text or "⚔" in text:
        text_clean = text.replace("⚔", " vs ").replace("️", "")
        m = re.match(r'^(.+?)\s+vs\s+(.+?)\s*-\s*(.+?)$', text_clean)
        if m:
            left, team2, anchor = m.groups()
            # left may contain league prefix like "LPL WBG"
            # Split: first word is league
            left_parts = left.strip().split()
            if len(left_parts) >= 2:
                league = left_parts[0]
                team1 = " ".join(left_parts[1:])
            else:
                league = ""
                team1 = left
            return {
                "sport": "电竞", "is_match": True,
                "league": league,
                "team1": clean_team(team1), "team2": clean_team(team2),
                "anchor": anchor.strip(),
            }
        return {"sport": "电竞", "is_match": False}

    # NBA / basketball
    if "NBA" in text or "🏀" in text:
        text_clean = text.replace("🏀", "").strip()
        m = re.match(r'^(.+?)\s+vs\s+(.+?)\s*-\s*(.+?)$', text_clean)
        if m:
            return {
                "sport": "篮球", "is_match": True,
                "league": "NBA",
                "team1": clean_team(m.group(1)), "team2": clean_team(m.group(2)),
                "anchor": m.group(3).strip(),
            }
        return {"sport": "篮球", "is_match": False}

    # Standard: TeamA vs TeamB - Anchor [League]
    m = re.match(r'^(.+?)\s+vs\s+(.+?)\s*-\s*(.+?)\s*\[(.+?)\]\s*$', text)
    if m:
        team1, team2, anchor, league = m.groups()
        return {
            "sport": "足球", "is_match": True,
            "league": league.strip(),
            "team1": clean_team(team1), "team2": clean_team(team2),
            "anchor": anchor.strip(),
        }

    # Try without league bracket (fallback)
    m = re.match(r'^(.+?)\s+vs\s+(.+?)\s*-\s*(.+?)$', text)
    if m:
        team1, team2, anchor = m.groups()
        # Try to infer league from anchor (e.g., "篮球原声全场回放" → basketball)
        sport = "足球"
        if any(kw in anchor for kw in ["篮球", "NBA"]):
            sport = "篮球"
        return {
            "sport": sport, "is_match": True,
            "league": "", "team1": clean_team(team1), "team2": clean_team(team2),
            "anchor": anchor.strip(),
        }

    return {"sport": "综合", "is_match": False}


def parse_liveevent(raw_name: str, group: str) -> dict:
    """Parse live-event (看个球) entry name.

    Format: league team1 vs team2
    group-title is already the sport category: 足球/篮球/电竞
    Non-match: 注意事项 placeholder.
    """
    text = normalize_text(raw_name)

    # Skip placeholder
    if text == "注意事项":
        return {"sport": "综合", "is_match": False}

    # Detect sport from group-title
    sport = group if group in ("足球", "篮球", "电竞") else "综合"

    # Parse: league team1 vs team2
    m = re.match(r'^(.+?)\s+(.+?)\s+vs\s+(.+?)$', text)
    if not m:
        # Try without league prefix
        m = re.match(r'^(.+?)\s+vs\s+(.+?)$', text)
        if m:
            return {
                "sport": sport, "is_match": True,
                "league": "",
                "team1": clean_team(m.group(1)), "team2": clean_team(m.group(2)),
            }
        return {"sport": sport, "is_match": False}

    league, team1, team2 = m.groups()
    return {
        "sport": sport, "is_match": True,
        "league": league.strip(), "team1": clean_team(team1), "team2": clean_team(team2),
    }


def parse_ppv(raw_name: str, group: str) -> dict:
    """Parse PPV TVK-format entry.

    Format: M/D HH:MM-HH:MM TeamA vs TeamB
    Non-match: channel names without time (e.g. "Roland-Garros: TNT Sports 1")
    Group contains "PPV|category" — extract sport from it.
    """
    # Extract sport from group (e.g. "PPV|足球" → "足球")
    sport = "综合"
    if "|" in group:
        sport = group.split("|", 1)[1].strip() or "综合"

    text = normalize_text(raw_name)

    # Strip time prefix: "5/31 08:00-11:00 " → keep it in display but parse teams after
    # Pattern: M/D HH:MM-HH:MM ...
    time_match = re.match(r'^(\d+/\d+\s+\d{2}:\d{2}-\d{2}:\d{2})\s+(.+)$', text)
    if not time_match:
        # Channel name without time (e.g. "Roland-Garros: TNT Sports 1")
        return {"sport": sport, "is_match": False, "display_extra": text}

    time_str = time_match.group(1)
    rest = time_match.group(2).strip()

    # Try to parse as match: TeamA vs TeamB
    m = re.match(r'^(.+?)\s+vs\s+(.+?)$', rest)
    if not m:
        return {"sport": sport, "is_match": False, "display_extra": f"{time_str} {rest}"}

    team1 = clean_team(m.group(1))
    team2 = clean_team(m.group(2))

    return {
        "sport": sport, "is_match": True,
        "league": time_str,  # store time in league field for sorting
        "team1": team1, "team2": team2,
    }


def parse_damizhibo(raw_name: str, group: str) -> dict:
    """Parse damizhibo.com entry name.

    Format: ⚽ MM-DD HH:MM league team1 vs team2 [line_label]
    or:     🏀 MM-DD HH:MM league team1 vs team2 [line_label]

    录像 (replay) entries are filtered upstream in fetch_damizhibo_entries().
    Placeholder entries (官网) are also filtered upstream.
    """
    text = normalize_text(raw_name)

    # Emoji → sport detection
    sport = "足球"
    if "🏀" in raw_name or "篮球" in raw_name:
        sport = "篮球"

    # Parse: MM-DD HH:MM league team1 vs team2 [line_label]
    # Line label is in brackets at the end
    line_label = ""
    m_label = re.search(r'\[([^\]]+)\]\s*$', text)
    if m_label:
        line_label = m_label.group(1).strip()
        text = text[:m_label.start()].strip()

    # Match: date time league team1 vs team2
    m = re.match(r'^(\d{2}-\d{2})\s+(\d{2}:\d{2})\s+(.+?)\s+(.+?)\s+vs\s+(.+?)$', text)
    if not m:
        return {"sport": "综合", "is_match": False}

    date_str, time_str, league, team1, team2 = m.groups()

    # Time filter: only include if match is within live window
    now = datetime.now(BJ_TZ)
    try:
        this_year = now.year
        match_dt = datetime.strptime(f"{this_year}-{date_str} {time_str}", "%Y-%m-%d %H:%M")
        match_dt = match_dt.replace(tzinfo=BJ_TZ)

        # Handle year boundary (Dec → Jan next year)
        if match_dt > now + timedelta(days=180):
            match_dt = match_dt.replace(year=this_year - 1)

        # Live window: 30 min before match to 3 hours after start
        window_start = match_dt - timedelta(minutes=30)
        window_end = match_dt + timedelta(hours=3)

        if not (window_start <= now <= window_end):
            return None  # not live, skip entirely

    except ValueError:
        return {"sport": "综合", "is_match": False}

    return {
        "sport": sport, "is_match": True,
        "league": league.strip(),
        "team1": clean_team(team1),
        "team2": clean_team(team2),
        "line_label": line_label,
    }


def parse_tv1288(raw_name: str, group: str) -> dict:
    text = normalize_text(raw_name)
    m_time = re.match(r'^(\d{1,2}:\d{2})\s+(.+)$', text)
    display_time = m_time.group(1) if m_time else ""
    body = m_time.group(2).strip() if m_time else text
    m = re.match(r'^(.+?)\s+(.+?)\s+vs\s+([^|]+?)(?:\s+(?:清流播出|赛场原声|原声|官方|\d号桌).*)?$', body)
    if m:
        league, team1, team2 = m.groups()
        sport = "篮球" if any(kw in body for kw in ["篮球", "U21", "CBA", "NBA", "篮"]) else "足球"
        if any(kw in body for kw in ["WTT", "网球", "WTA", "ATP", "斯诺克", "UFC", "女篮热身赛"]):
            sport = "综合"
        return {"sport": sport, "is_match": True, "league": league.strip(), "team1": clean_team(team1), "team2": clean_team(team2), "sort_time": display_time, "display_time": display_time}
    return {"sport": "综合", "is_match": False, "display_extra": f"{display_time} {body}".strip(), "sort_time": display_time}


def parse_fwc4k(raw_name: str, group: str) -> dict | None:
    """Parse FWC4K event entry; keep only live-window fixtures."""
    if group != "4K杜比视界世界杯正赛":
        return None

    text = raw_name.strip()
    m = re.match(r'^(.+?)_(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})_(\d+)$', text)
    if not m:
        return None

    teams, dt_str, _ts_ms = m.groups()
    parts = re.split(r'\s+v\.\s+', teams, maxsplit=1)
    if len(parts) != 2:
        return None

    try:
        match_dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=BJ_TZ)
    except ValueError:
        return None

    now = datetime.now(BJ_TZ)
    if not (match_dt - timedelta(minutes=10) <= now <= match_dt + timedelta(hours=2, minutes=45)):
        return None

    return {
        "sport": "足球",
        "is_match": True,
        "league": "世界杯",
        "team1": clean_team(parts[0]),
        "team2": clean_team(parts[1]),
        "sort_time": match_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "display_time": match_dt.strftime("%m-%d %H:%M"),
    }


# Source parser dispatch
SOURCE_PARSERS = {
    "857直播": parse_857,
    "咖啡直播": parse_kafei,
    "看球吧": parse_kanqiu,
    "看球通": parse_kqt,
    "popo直播": parse_kafei,   # popozhibo 格式与咖啡直播相同: 联赛 Team vs Team | 线路
    "live-event": parse_liveevent,
    "PPV": parse_ppv,
    "damizhibo": parse_damizhibo,
    "FWC4K": parse_fwc4k,
    "咪咕直播": parse_tv1288,
}

# ══════════════════════════════════════════════════
#  Clustering
# ══════════════════════════════════════════════════

def _team_sim(a: str, b: str) -> float:
    """Similarity between two team names, handling caster suffix."""
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    # Handle caster-name suffix (e.g. "辽宁铁人楠波湾" contains "辽宁铁人")
    if a in b or b in a:
        longer = a if len(a) > len(b) else b
        shorter = b if len(a) > len(b) else a
        # Only accept substring match if the difference is small (≤3 chars)
        # This prevents "北京" matching "北京国安" too easily
        if len(longer) - len(shorter) <= 3:
            return 0.82
    return SequenceMatcher(None, a, b).ratio()


def cluster_matches(entries: list[dict]) -> list[list[dict]]:
    """Group match entries by sport+league fuzzy clustering.

    Entries must have: sport, league (can be empty), team1, team2, is_match=True.
    Returns list of groups, each is a list of entries for the same match.
    Non-match entries form singleton groups.
    """
    groups: list[list[dict]] = []
    for entry in entries:
        if not entry.get("is_match"):
            groups.append([entry])
            continue

        sport = entry.get("sport", "综合")
        league = entry.get("league", "")

        best_group = None
        best_score = 0.0
        for g in groups:
            g0 = g[0]
            if not g0.get("is_match"):
                continue
            if g0.get("sport") != sport:
                continue
            if g0.get("league", "") != league:
                continue

            # Compare entry with group representative
            s_forward = min(
                _team_sim(entry["team1"], g0["team1"]),
                _team_sim(entry["team2"], g0["team2"]),
            )
            s_cross = min(
                _team_sim(entry["team1"], g0["team2"]),
                _team_sim(entry["team2"], g0["team1"]),
            )
            score = max(s_forward, s_cross)
            if score > best_score:
                best_score = score
                best_group = g

        if best_score >= 0.72:
            best_group.append(entry)
        else:
            groups.append([entry])

    return groups


# ══════════════════════════════════════════════════
#  Display-name rendering
# ══════════════════════════════════════════════════

def render_sports_display(entry: dict) -> str:
    """Build uniform display name for a sports entry.

    Format: [src_short] league team1 vs team2 | extra_info
    Non-match entries fall back to cleaned raw name.
    """
    src = entry.get("source_short", entry.get("source", "?"))
    league = entry.get("league", "")
    team1 = entry.get("team1", "")
    team2 = entry.get("team2", "")

    if not entry.get("is_match") or not (team1 and team2):
        # Non-match or parse failure: use display_extra or cleaned raw name
        extra = entry.get("display_extra", "")
        if extra:
            return f"[{src}] {extra}"
        raw = entry.get("raw_name", "")
        # Try to clean the raw name — strip source prefix
        m = re.match(r'^\[[^\]]+\]\s*(.*)$', raw)
        if m:
            raw = m.group(1)
        raw = normalize_text(raw) if raw else raw
        if not raw:
            raw = entry.get("name", src)
        return f"[{src}] {raw}"

    parts = [f"[{src}]"]
    if src == "4K杜比" and entry.get("display_time"):
        parts.append(entry["display_time"])
    else:
        # For PPV entries, show sport category before the match info
        sport = entry.get("sport", "")
        if sport and sport != "综合" and src == "PPV":
            parts.append(sport)
        if league:
            parts.append(league)
    parts.append(f"{team1} vs {team2}")

    base = " ".join(parts)

    # Extra info
    extras = []
    if entry.get("line_label"):
        extras.append(entry["line_label"])
    if entry.get("anchor"):
        extras.append(entry["anchor"])
    if entry.get("format"):
        extras.append(entry["format"])

    if extras:
        return f"{base} | {' | '.join(extras)}"
    return base


def render_sports_group_title(entry: dict) -> str:
    """Return uniform group-title for a sports entry."""
    sport = entry.get("sport", "综合")
    # Map to standard categories
    valid = {"足球", "篮球", "电竞", "综合", "回放"}
    return sport if sport in valid else "综合"


# ══════════════════════════════════════════════════
#  Sports entry processing pipeline
# ══════════════════════════════════════════════════

def process_sports_entries(raw_entries: list[dict]) -> list[dict]:
    """Parse, group by source, sort, and assign display names.

    Steps:
    1. URL dedup
    2. Source-specific parsing
    3. Assign display_name & group-title (source-based)
    4. Group by source, sort within each group
    5. Sort groups by SOURCE_ORDER
    """
    # 1. URL dedup
    seen_urls: set[str] = set()
    deduped: list[dict] = []
    for e in raw_entries:
        url = e.get("url", "").strip()
        if url and url not in seen_urls:
            seen_urls.add(url)
            deduped.append(dict(e))

    # 2. Parse
    parsed: list[dict] = []
    for e in deduped:
        source = e.get("source", "")
        parser = SOURCE_PARSERS.get(source)
        if not parser:
            continue
        name = e.get("name", "")
        m = re.match(r'^\[[^\]]+\]\s*(.*)$', name)
        raw = m.group(1) if m else name
        result = parser(raw, e.get("group", "综合"))
        if result:
            entry = {**e, **result}
            entry["source_short"] = SOURCE_SHORT.get(source, source)
            entry["raw_name"] = name
            parsed.append(entry)

    # Filter out replays, official event rooms, talk shows
    _FILTER_NAMES = ("回放", "官方活动", "一起来聊球")
    parsed = [e for e in parsed if not any(kw in e.get("raw_name", "") for kw in _FILTER_NAMES)]

    # 3. Assign display_name
    for e in parsed:
        e["display_name"] = render_sports_display(e)
        # Group by source short name (e.g. 咖啡, 看个球, 857...)
        e["output_group"] = e.get("source_short", e.get("source", "?"))

    # 4. Group by source, sort within each group
    groups: dict[str, list[dict]] = {}
    for e in parsed:
        gkey = e["output_group"]
        groups.setdefault(gkey, []).append(e)

    for gkey in groups:
        groups[gkey].sort(key=lambda x: (
            x.get("sort_time", ""),
            x.get("sport", ""),
            x.get("league", ""),
            x.get("team1", ""),
            x.get("team2", ""),
        ))

    # 5. Sort groups by SOURCE_ORDER, then flatten
    def _source_sort_key(gkey: str) -> int:
        # Map short names back to full source names for SOURCE_ORDER lookup
        reverse_short = {v: k for k, v in SOURCE_SHORT.items()}
        full_name = reverse_short.get(gkey, gkey)
        return SOURCE_ORDER.get(full_name, 99)

    sorted_keys = sorted(groups.keys(), key=_source_sort_key)
    result: list[dict] = []
    for gkey in sorted_keys:
        result.extend(groups[gkey])

    return result


# ══════════════════════════════════════════════════
#  Original functions (mostly unchanged)
# ══════════════════════════════════════════════════

def load_module(filename: str, module_name: str):
    path = SCRIPT_DIR / filename
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def parse_m3u(text: str, source: str) -> list[dict]:
    entries = []
    lines = [line.strip() for line in text.splitlines()]
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.startswith("#EXTINF"):
            i += 1
            continue
        j = i + 1
        while j < len(lines) and (not lines[j] or lines[j].startswith("#")):
            j += 1
        if j >= len(lines):
            break
        name = line.split(",", 1)[1].strip() if "," in line else source
        attrs = line.split(",", 1)[0].replace("#EXTINF:-1", "").strip()
        group_match = re.search(r'group-title="([^"]+)"', line)
        group = group_match.group(1).strip() if group_match else "其他"
        entries.append(
            {
                "source": source,
                "group": group,
                "name": name,
                "url": lines[j],
                "attrs": attrs,
            }
        )
        i = j + 1
    return entries


def normalize_self_group(entry: dict) -> str:
    """Normalize messy group titles from myself.m3u into clean categories."""
    group = entry.get("group", "未分类")
    name = entry.get("name", "")
    text = f"{group} {name}"

    name_upper = name.upper()

    # CCTV names may be lowercase in some hand-written lists (cctv5/cctv8k/etc.)
    if group in ("央视频道", "央视", "🔥[移动]央卫视直播") or name_upper.startswith("CCTV"):
        if "4K" in name_upper or "8K" in name_upper or "UHD" in name_upper:
            return "4K"
        return "央视频道"
    if group in ("4K", "UHD | 4K", "4KUHD-FIFA", "8K频道") or "4K" in name_upper or "8K" in name_upper or "UHD" in name_upper or name in ("Now616", "Now617"):
        return "4K"
    if group in ("卫视频道", "卫视") or "卫视" in name:
        return "卫视频道"
    if group in ("北京", "咪咕央视2") or name.startswith("北京"):
        if "体育" in name:
            return "体育"
        return "北京"
    if group == "山东频道" or "山东" in name or "青岛" in name or name in ("城阳综合", "即墨综合", "李沧TV", "泰山TV"):
        return "卫视频道"

    # Sports-like hand-written groups must be checked before broad international/news rules.
    sports_keywords = (
        "体育", "SPORT", "ESPN", "FS1", "F1", "FORMULA", "NBA", "WNBA",
        "MAVERICKS", "BUNDESLIGA", "NOW SPORTS", "TSN", "TNT SPORTS", "FOX SPORTS", "NOW618", "NOW619"
    )
    if group in ("体育", "体育频道", "【体育频道】", "UK Sports", "UK SPORTS", "NBA TEAMS", "NBA",
                 "F1 Formula", "体育竞技 - 北美", "Sports - NA", "Sports - EU", "FIFA World Cup 2026",
                 "🏀[联通]咪视界直播") or any(kw in name_upper for kw in sports_keywords):
        return "体育"

    jp_groups = ("Tokyo", "Kansai", "BS")
    jp_keywords = ("NHK", "NTV", "TBS", "FUJI TV", "TV ASAHI", "TV TOKYO", "TV OSAKA",
                   "KANSAI TV", "MBS", "YTV", "SUN", "KBS", "WOWOW")
    if group in jp_groups or any(kw in name_upper for kw in jp_keywords) or name in ("ABC (Primehome)",):
        if "4K" in name_upper:
            return "4K"
        return "日本"

    news_keywords = ("NEWS", "REUTERS", "CNN", "BBC NEWS", "NBC 5", "WFAA", "KDFW", "FOX LOCAL")
    if group == "新闻资讯" or any(kw in name_upper for kw in news_keywords):
        return "新闻"

    intl_keywords = ("BBC", "CNBC", "FOX", "NBC", "CBS", "ABC 8", "DALLAS", "LOCALNOW", "XUMO",
                     "UK:", "UK ")
    if any(kw in name_upper for kw in intl_keywords) or group in ("国际台", "国际", "英国综合",
                                                              "Xumo频道", "Undefined",
                                                              "LocalNow🇺🇸: More Cities"):
        return "国际"
    if group in ("💓专享源🅰️", "TV"):
        return "其他"
    if group in ("其他频道",):
        return "其他"
    return "其他"


def fetch_myself_m3u_text() -> str:
    if MYSELF_CACHE.exists():
        text = MYSELF_CACHE.read_text(encoding="utf-8")
        print(f"myself.m3u: using local cache ({len(text)} bytes)", file=sys.stderr)
        return text
    print("myself.m3u: cache not found, continuing without static channels", file=sys.stderr)
    return "#EXTM3U\n"


def fetch_dynamic_local_m3u_text() -> str:
    """Fetch sjbox local-channel M3U at most every 3h; fall back to cache."""
    import time
    import urllib.request

    if DYNAMIC_LOCAL_CACHE.exists():
        age = time.time() - DYNAMIC_LOCAL_CACHE.stat().st_mtime
        if age < DYNAMIC_LOCAL_CACHE_TTL:
            text = DYNAMIC_LOCAL_CACHE.read_text(encoding="utf-8")
            print(f"动态地方台: cache ({int(age // 60)}m old)", file=sys.stderr)
            return text

    try:
        req = urllib.request.Request(
            DYNAMIC_LOCAL_URL,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            text = resp.read().decode("utf-8", errors="replace")
        if "#EXTM3U" not in text or "#EXTINF" not in text:
            raise RuntimeError("返回内容不是 M3U")
        DYNAMIC_LOCAL_CACHE.write_text(text, encoding="utf-8")
        print(f"动态地方台: fetched {len(text)} bytes", file=sys.stderr)
        return text
    except Exception as exc:
        if DYNAMIC_LOCAL_CACHE.exists():
            text = DYNAMIC_LOCAL_CACHE.read_text(encoding="utf-8")
            print(f"WARN 动态地方台: {exc}; using cache", file=sys.stderr)
            return text
        raise RuntimeError(f"动态地方台不可用: {exc}")


def dynamic_target_key(name: str) -> str:
    name = normalize_text(name)
    name = re.sub(r"\s+", "", name)
    aliases = {
        "北京纪实": "北京纪实科教",
    }
    return aliases.get(name, name)


def dynamic_url_script(url: str) -> str:
    m = re.search(r"/(live_[a-z0-9_]+\.php)\?", url)
    return m.group(1) if m else ""


def is_dynamic_local_stream_usable(url: str) -> bool:
    """Probe sjbox dynamic local streams before exposing them to APTV."""
    if not url.startswith("http"):
        return False
    import urllib.request

    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Range": "bytes=0-2047",
            },
        )
        with urllib.request.urlopen(req, timeout=6) as resp:
            status = getattr(resp, "status", 200)
            data = resp.read(2048)
        if status >= 400:
            return False
        if not data:
            return False
        head = data[:512].decode("utf-8", errors="ignore")
        # Accept HLS playlists and obvious media bytes; reject HTML/error pages.
        if "<html" in head.lower() or "error" in head.lower():
            return False
        return True
    except Exception:
        return False


def dynamic_entry_score(entry: dict) -> int:
    """Prefer 山东/北京联通 direct live PHP URLs from the sjbox source."""
    group = entry.get("group", "")
    url = entry.get("url", "")
    name = entry.get("name", "")
    score = 0
    if group == "山东频道" and any(x in name for x in ("青岛", "城阳", "李沧", "即墨", "泰山")):
        score += 50
    if group == "北京联通" and name.startswith("北京"):
        score += 50
    if "live_sd.php" in url or "live_bjlt.php" in url:
        score += 20
    if url.startswith("http"):
        score += 5
    return score


def fetch_dynamic_local_entries() -> list[dict]:
    """Fetch selected daily-refreshed local channels as standalone entries.

    These streams are intentionally not written back into myself.m3u: sjbox URLs
    can rotate daily, so they are appended during merge only.
    """
    text = fetch_dynamic_local_m3u_text()
    entries = parse_m3u(text, source="动态地方台")
    wanted = {dynamic_target_key(n) for n in DYNAMIC_LOCAL_TARGETS}
    picked: dict[tuple[str, str], dict] = {}
    for entry in entries:
        name_key = dynamic_target_key(entry.get("name", ""))
        if name_key not in wanted:
            continue
        entry = dict(entry)
        # Normalize aliases so identical Beijing channels are rendered together.
        if name_key == "北京纪实科教" and entry.get("name") == "北京纪实":
            entry["name"] = "北京纪实科教"
        entry["source"] = "动态地方台"
        entry["group"] = normalize_self_group(entry)
        key = (name_key, dynamic_url_script(entry.get("url", "")))
        old = picked.get(key)
        if old is None or dynamic_entry_score(entry) > dynamic_entry_score(old):
            picked[key] = entry

    usable_picked: dict[tuple[str, str], dict] = {}
    disabled_markers = ("110.40.170.5:8889",)
    disabled = 0
    for key, entry in picked.items():
        url = entry.get("url", "")
        if any(marker in url for marker in disabled_markers):
            disabled += 1
            continue
        if is_dynamic_local_stream_usable(url):
            usable_picked[key] = entry
    skipped = len(picked) - len(usable_picked)
    picked = usable_picked
    if disabled:
        print(f"动态地方台: disabled {disabled} sjbox streams", file=sys.stderr)
    elif skipped:
        print(f"动态地方台: skipped {skipped} unusable streams", file=sys.stderr)

    found_names = {name for name, _script in picked}
    print(f"动态地方台: picked {len(found_names)}/{len(wanted)} names, {len(picked)} variants", file=sys.stderr)

    # Stable output order follows DYNAMIC_LOCAL_TARGETS display order, variants grouped by script.
    target_order = {dynamic_target_key(name): i for i, name in enumerate(DYNAMIC_LOCAL_TARGET_ORDER)}
    return sorted(
        picked.values(),
        key=lambda e: (target_order.get(dynamic_target_key(e.get("name", "")), 999), e.get("name", ""), dynamic_url_script(e.get("url", ""))),
    )


def fetch_myself_entries() -> list[dict]:
    text = fetch_myself_m3u_text()
    entries = parse_m3u(text, source="自用")
    # Global disabled-markers filter: block known-dead URLs from ANY source.
    _DISABLED_URL_MARKERS = ("110.40.170.5:8889",)
    filtered = []
    for entry in entries:
        url = entry.get("url", "")
        if any(marker in url for marker in _DISABLED_URL_MARKERS):
            continue
        entry["group"] = normalize_self_group(entry)
        filtered.append(entry)
    if len(filtered) < len(entries):
        print(f"自用: filtered out {len(entries) - len(filtered)} disabled-URL entries", file=sys.stderr)
    return filtered


def beijing_group_key(entry: dict) -> str:
    aliases = {
        "北京纪实": "北京纪实科教",
    }
    name = str(entry.get("name") or "")
    return aliases.get(name, name)


def is_beijing_entry(entry: dict) -> bool:
    return entry.get("name", "").startswith("北京")


def group_beijing_static_entries(entries: list[dict]) -> list[dict]:
    """Keep Beijing variants together while preserving non-Beijing static order."""
    first_beijing_index = next((i for i, e in enumerate(entries) if is_beijing_entry(e)), None)
    if first_beijing_index is None:
        return entries

    first_seen: dict[str, int] = {}
    for entry in entries:
        if is_beijing_entry(entry):
            first_seen.setdefault(beijing_group_key(entry), len(first_seen))

    beijing_entries = [e for e in entries if is_beijing_entry(e)]
    non_beijing_entries = [e for e in entries if not is_beijing_entry(e)]
    sorted_beijing = sorted(
        beijing_entries,
        key=lambda e: (
            first_seen.get(beijing_group_key(e), 999),
            beijing_group_key(e),
            0 if e.get("source") == "自用" else 1,
            e.get("url", ""),
        ),
    )

    insert_at = sum(1 for e in entries[:first_beijing_index] if not is_beijing_entry(e))
    return non_beijing_entries[:insert_at] + sorted_beijing + non_beijing_entries[insert_at:]


def merge_all_entries(myself_entries: list[dict], sports_entries: list[dict]) -> list[dict]:
    # Static entries: keep original order, URL dedup.
    # TSN 4K CA already represents TSN East; TSN East 4K CA uses the same URL and should not be duplicated.
    seen_urls: set[str] = set()
    myself_list: list[dict] = []
    for entry in myself_entries:
        url = entry["url"].strip()
        if url and url not in seen_urls:
            seen_urls.add(url)
            myself_list.append(dict(entry))

    myself_list = group_beijing_static_entries(myself_list)

    # Process sports entries through new pipeline
    sports_processed = process_sports_entries(sports_entries)

    return list(myself_list) + sports_processed


def render_m3u(entries: list[dict]) -> str:
    lines = [
        "#EXTM3U",
        "# Generated locally by merge_live_m3u.py",
        f"# Sources: {', '.join(SOURCES)}",
        f"# Static channels (自用): from GitHub Shincyann128/iptv myself.m3u",
        f"# Live sports: 看球通 + 咖啡直播 + 咪咕直播 + 看球吧 + 857直播 + popo直播 + 看个球 + PPV + damizhibo (refreshed every 15 minutes)",
        f"# Total streams: {len(entries)}",
        "",
    ]
    static_sources = {"自用", "动态地方台"}
    last_source = None
    for entry in entries:
        source = entry.get("source", "")
        if last_source in static_sources and source not in static_sources:
            lines.append("# ===== 体育直播（每15分钟刷新）=====")
            lines.append("")
        last_source = source

        if source in static_sources:
            # Static/local channels: keep original format
            display_name = entry["name"]
            group = entry.get("group", "其他")
        else:
            # Sports: use processed display_name & group
            display_name = entry.get("display_name", entry["name"])
            group = entry.get("output_group", entry.get("group", "综合"))

        escaped_name = display_name.replace('"', "'")
        escaped_group = group.replace('"', "'")
        lines.append(
            f'#EXTINF:-1 group-title="{escaped_group}" tvg-name="{escaped_name}",{escaped_name}'
        )
        lines.append(entry["url"])
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


# ══════════════════════════════════════════════════
#  Fetch functions (unchanged)
# ══════════════════════════════════════════════════

def fetch_kqt_entries() -> list[dict]:
    module = load_module("kqt_m3u.py", "kqt_m3u")
    rooms = module.fetch_all_rooms()
    text = module.generate_m3u(rooms)
    return parse_m3u(text, source="看球通")


def fetch_kafei_entries() -> list[dict]:
    module = load_module("update_kafeizhibo_live.py", "update_kafeizhibo_live")
    items = module.fetch_archors()
    from collections import Counter
    status_dist = Counter(i.get("status", "?") for i in items)
    entries = module.build_entries(items)
    if len(entries) < 10:
        print(
            f"  coffee DEBUG: raw={len(items)} status_dist={dict(status_dist)} built={len(entries)}",
            file=sys.stderr,
        )
    text = module.render(entries)
    return parse_m3u(text, source="咖啡直播")


def fetch_kanqiu_entries() -> list[dict]:
    module = load_module("kanqiu2m3u.py", "kanqiu2m3u")
    base, home_data = module.find_api()
    if not base:
        raise RuntimeError("看球吧 API 不可用")
    streams = module.extract_recommend_lives(home_data)
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        text = module.generate_m3u(streams)
    return parse_m3u(text, source="看球吧")


def fetch_857_entries() -> list[dict]:
    module = load_module("update_857_live.py", "update_857_live")
    rooms = module.fetch_rooms()
    entries = module.build_entries(rooms)
    print(f"  857 DEBUG: raw={len(rooms)} built={len(entries)}", file=sys.stderr)
    text = module.render(entries)
    return parse_m3u(text, source="857直播")


def fetch_popozhibo_entries() -> list[dict]:
    module = load_module("popozhibo_m3u.py", "popozhibo_m3u")
    text = module.generate_m3u()
    return parse_m3u(text, source="popo直播")


def _tv1288_cache_text() -> str:
    import time
    import urllib.request
    if TV1288_CACHE.exists():
        age = time.time() - TV1288_CACHE.stat().st_mtime
        if age <= TV1288_CACHE_MAX_AGE:
            text = TV1288_CACHE.read_text(encoding="utf-8")
            print(f"  tv1288 cache: {int(age // 60)}m old", file=sys.stderr)
            return text
        print(f"  tv1288 cache expired: {int(age // 3600)}h old; skip", file=sys.stderr)
        return ""
    try:
        req = urllib.request.Request(TV1288_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            text = resp.read().decode("utf-8", errors="replace")
        if "#genre#" not in text:
            raise RuntimeError("返回内容不是 tv1288 文本列表")
        TV1288_CACHE.write_text(text, encoding="utf-8")
        print(f"  tv1288 cache: bootstrapped {len(text)} bytes", file=sys.stderr)
        return text
    except Exception as exc:
        print(f"WARN tv1288 cache missing and fetch failed: {exc}", file=sys.stderr)
        return ""


def _tv1288_parse_plain(text: str) -> list[dict]:
    entries = []
    group = ""
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.endswith(",#genre#"):
            group = line.split(",", 1)[0].strip()
            continue
        if "," not in line:
            continue
        name, url = line.split(",", 1)
        name, url = name.strip(), url.strip()
        if url.startswith("http"):
            entries.append({"group": group, "name": name, "url": url})
    return entries


def _tv1288_match_time(name: str, group: str):
    m = list(re.finditer(r'(?:^|\s)(\d{1,2}:\d{2})(?:\s|$)', name))
    if not m:
        return None
    hhmm = m[-1].group(1)
    try:
        hh, mm = map(int, hhmm.split(":"))
    except ValueError:
        return None
    today = datetime.now(BJ_TZ).date()
    if group == "昨天":
        today = today - timedelta(days=1)
    elif group == "明天":
        today = today + timedelta(days=1)
    return datetime.combine(today, datetime.min.time().replace(hour=hh, minute=mm), BJ_TZ)


def _tv1288_is_hls(url: str) -> bool:
    import urllib.request
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=7) as resp:
            status = getattr(resp, "status", 200)
            data = resp.read(4096).decode("utf-8", errors="ignore")
        if status != 200:
            return False
        return "#EXTM3U" in data and ("#EXT-X-" in data or ".m3u8" in data)
    except Exception:
        return False


def fetch_tv1288_entries() -> list[dict]:
    text = _tv1288_cache_text()
    if not text:
        return []
    now = datetime.now(BJ_TZ)
    raw_entries = _tv1288_parse_plain(text)
    drop_kw = ("回放", "全场回放", "纯享版回放", "录像", "集锦", "录播", "明天", "预告")
    host_rank = {"121.23.132.38:7788": 0, "222.134.19.49:8090": 1, "14.153.176.157:1234": 2}
    candidates: dict[str, list[tuple[int, dict, datetime]]] = {}
    for e in raw_entries:
        if e.get("group") != "今天":
            continue
        name = e.get("name", "")
        if any(kw in name for kw in drop_kw):
            continue
        match_dt = _tv1288_match_time(name, "今天")
        if not match_dt:
            continue
        if not (match_dt - timedelta(minutes=20) <= now <= match_dt + timedelta(hours=3)):
            continue
        key = normalize_text(name)
        host = re.sub(r'^https?://([^/]+).*', r'\1', e.get("url", ""))
        candidates.setdefault(key, []).append((host_rank.get(host, 99), e, match_dt))
    result = []
    for key, variants in sorted(candidates.items(), key=lambda item: item[0]):
        if not variants:
            continue
        _best = min(variants, key=lambda item: item[0])
        _rank, e, match_dt = _best
        name = normalize_text(e["name"])
        hhmm = match_dt.strftime("%H:%M")
        if not name.startswith(hhmm):
            name = f"{hhmm} {name}"
        result.append({"source": "咪咕直播", "group": "咪咕", "name": name, "url": e["url"], "attrs": 'group-title="咪咕"'})
    print(f"  tv1288: raw={len(raw_entries)} candidates={len(candidates)} live={len(result)}", file=sys.stderr)
    return result


def fetch_liveevent_entries() -> list[dict]:
    """Fetch M3U from local live-event Docker container, resolve CDN URLs."""
    import urllib.request

    url = "http://127.0.0.1:28989/list.m3u"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            text = resp.read().decode("utf-8")
    except Exception as exc:
        raise RuntimeError(f"live-event Docker 不可用: {exc}")

    # Resolve each proxy URL to the actual CDN stream URL
    lines = text.splitlines()
    resolved = []
    skip_next = False
    for i, line in enumerate(lines):
        if skip_next:
            skip_next = False
            continue
        if line.startswith("#EXTINF") and "注意事项" in line:
            skip_next = True
            continue
        if line.startswith("http://127.0.0.1:28989/"):
            # Resolve through local proxy
            try:
                req = urllib.request.Request(line, method="HEAD")
                with urllib.request.urlopen(req, timeout=10) as resp:
                    cdn_url = resp.geturl()
                    if cdn_url and cdn_url != line:
                        line = cdn_url
            except Exception:
                pass  # keep original if resolve fails
        resolved.append(line)

    return parse_m3u("\n".join(resolved), source="live-event")


def _fwc4k_playlist_is_live(url: str) -> bool:
    """Return True only for real live event playlists, not end*.ts placeholders."""
    import urllib.request

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            text = resp.read(4096).decode("utf-8", errors="ignore")
    except Exception:
        return False

    if "#EXT-X-ENDLIST" in text:
        return False
    if re.search(r'(^|/)end\d+\.ts', text):
        return False
    return bool(".ts" in text or "/s?n=" in text or "#EXT-X-MEDIA-SEQUENCE" in text)


def fetch_fwc4k_entries() -> list[dict]:
    """Fetch FWC4K events and keep only currently live 4K Dolby World Cup matches."""
    import urllib.request

    try:
        req = urllib.request.Request(FWC4K_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            text = resp.read().decode("utf-8")
    except Exception as exc:
        raise RuntimeError(f"FWC4K 不可用: {exc}")

    raw_entries = parse_m3u(text, source="FWC4K")
    candidates = []
    for e in raw_entries:
        if e.get("group") != "4K杜比视界世界杯正赛":
            continue
        if not parse_fwc4k(e.get("name", ""), e.get("group", "")):
            continue
        if not _fwc4k_playlist_is_live(e.get("url", "")):
            continue
        candidates.append(e)

    print(f"  FWC4K: raw={len(raw_entries)} live={len(candidates)}", file=sys.stderr)
    return candidates


def fetch_ppv_entries() -> list[dict]:
    """Fetch PPV M3U from korice.eu.org, cached for 12 hours."""
    import os
    import time

    # Check cache
    try:
        cache_mtime = os.path.getmtime(PPV_CACHE)
        if time.time() - cache_mtime < PPV_CACHE_TTL:
            text = PPV_CACHE.read_text(encoding="utf-8")
            print("PPV: (cache)", file=sys.stderr)
            return parse_m3u(text, source="PPV")
    except (OSError, FileNotFoundError):
        pass

    # Fetch fresh
    import urllib.request

    url = "https://www.korice.eu.org/ppv_m3u.php"
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"}
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            text = resp.read().decode("utf-8")
    except Exception as exc:
        raise RuntimeError(f"PPV 不可用: {exc}")

    # Write cache
    PPV_CACHE.write_text(text, encoding="utf-8")
    return parse_m3u(text, source="PPV")


def fetch_damizhibo_entries() -> list[dict]:
    """Fetch M3U from damizhibo.com, filter to only live matches.

    Filters out:
    - 录像 (replay) entries
    - Placeholder entries (官网：)
    - Future matches not yet in live window
    """
    import urllib.request

    url = "https://damizhibo.com/iptv.m3u"
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            text = resp.read().decode("utf-8")
    except Exception as exc:
        raise RuntimeError(f"damizhibo 不可用: {exc}")

    entries = parse_m3u(text, source="damizhibo")

    # Filter: remove 录像 groups, placeholder entries, and non-live matches
    filtered = []
    for e in entries:
        # Skip 录像 groups
        if "录像" in e.get("group", ""):
            continue
        # Skip placeholder entries (官网：)
        name = e.get("name", "")
        if "官网" in name or "dami.live" in name or "ricetv" in name:
            continue
        filtered.append(e)

    print(f"  damizhibo: raw={len(entries)} filtered={len(filtered)}", file=sys.stderr)
    return filtered


def write_atomic(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=path.parent) as tmp:
        tmp.write(content)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def main() -> int:
    all_entries = []

    # 1) Static
    try:
        myself_entries = fetch_myself_entries()
        print(f"自用: {len(myself_entries)}", file=sys.stderr)
    except Exception as exc:
        print(f"WARN 自用: {exc}", file=sys.stderr)
        myself_entries = []

    # 2) Daily-refreshed local channels
    try:
        dynamic_local_entries = fetch_dynamic_local_entries()
        print(f"动态地方台: {len(dynamic_local_entries)}", file=sys.stderr)
    except Exception as exc:
        print(f"WARN 动态地方台: {exc}", file=sys.stderr)
        dynamic_local_entries = []

    # 3) Sports
    sports_entries = []
    for label, fn in (
        ("FWC4K", fetch_fwc4k_entries),
        ("看球通", fetch_kqt_entries),
        ("咖啡直播", fetch_kafei_entries),
        ("咪咕直播", fetch_tv1288_entries),
        ("看球吧", fetch_kanqiu_entries),
        ("857直播", fetch_857_entries),
        ("popo直播", fetch_popozhibo_entries),
        ("live-event", fetch_liveevent_entries),
        ("PPV", fetch_ppv_entries),
        ("damizhibo", fetch_damizhibo_entries),
    ):
        try:
            entries = fn()
            print(f"{label}: {len(entries)}", file=sys.stderr)
            sports_entries.extend(entries)
        except Exception as exc:
            print(f"WARN {label}: {exc}", file=sys.stderr)

    static_entries = myself_entries + dynamic_local_entries

    if not sports_entries and not static_entries:
        raise RuntimeError("所有源都没有抓到可用流")

    merged = merge_all_entries(static_entries, sports_entries)
    content = render_m3u(merged)
    write_atomic(OUTPUT_FILE, content)

    static_count = len(static_entries)
    sports_count = len(merged) - static_count
    print(
        f"Wrote {len(merged)} merged streams ({static_count} static + {sports_count} sports) to {OUTPUT_FILE}"
    )
    return len(merged)


if __name__ == "__main__":
    main()
