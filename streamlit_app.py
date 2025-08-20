# streamlit_app.py

import streamlit as st
import requests
import gspread
import time
import random
import xmlrpc.client
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from requests.auth import HTTPBasicAuth
import json
import re
import pandas as pd
from urllib.parse import urlparse
import tempfile
import os
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape as xml_escape
import io
from PIL import Image, ImageDraw, ImageFont
from scripts.blogger_client import post_to_blogger

# Optional: avoid SSL warning noise (REST verify=False を使うため)
try:
    requests.packages.urllib3.disable_warnings()  # type: ignore
except Exception:
    pass

# ========================
# 設定値（Secretsから取得）
# ========================
try:
    SHEET_ID = st.secrets["google"]["spreadsheet_id"]
    GEMINI_API_KEYS = [
        st.secrets["google"].get("gemini_api_key_1"),
        st.secrets["google"].get("gemini_api_key_2"),
    ]
    GEMINI_API_KEYS = [k for k in GEMINI_API_KEYS if k]
except KeyError as e:
    st.error(f"Secretsの設定が不足しています: {e}")
    st.stop()

# WP接続設定は Secrets を優先
WP_CONFIGS = {}
if "wp_configs" in st.secrets:
    WP_CONFIGS = {k: dict(v) for k, v in st.secrets["wp_configs"].items()}

# フォールバック（Secrets 未設定時のみ）
if not WP_CONFIGS:
    WP_CONFIGS = {
        'ykikaku': {
            'url': 'https://ykikaku.xsrv.jp/',
            'user': 'ykikaku',
            'password': 'lfXp BJNx Rvy8 rBlt Yjug ADRn'
        },
        'efdlqjtz': {
            'url': 'https://www.efdlqjtz.v2010.coreserver.jp/',
            'user': 'efdlqjtz',
            'password': 'KCIA cTyz TcdG U1Qs M4pd eezb'
        },
        'selectadvance': {
            'url': 'https://selectadvance.v2006.coreserver.jp/',
            'user': 'selectadvance',
            'password': '6HUY g7oZ Gow8 LBCu yzL8 cR3S'
        },
        'welkenraedt': {
            'url': 'https://www.welkenraedt-online.com/',
            'user': 'welkenraedtonline',
            'password': 'yzn4 6nlm vtrh 8N4v oxHl KUvf'
        },
        'ncepqvub': {
            'url': 'https://ncepqvub.v2009.coreserver.jp/',
            'user': 'ncepqvub',
            'password': 'DIZy ky10 UAhO NJ47 6Jww ImdE'
        },
        'kosagi': {
            'url': 'https://www.kosagi.jp/',
            'user': 'kosagi',
            'password': 'K2DZ ERIy aTLb K2Z0 gHi6 XdIN'
        },
        'selectad': {
            'url': 'https://selectad01.xsrv.jp/',
            'user': 'selectad01',
            'password': '8LhM laXm pDUx gkjV cg1f EXYr'
        },
        'thrones': {
            'url': 'https://thrones.v2009.coreserver.jp/',
            'user': 'thrones',
            'password': 'ETvJ VP2F jugd mxXU xJX0 wHVr'
        }
    }

# プロジェクト設定（あなたの命名を踏襲）
PROJECT_CONFIGS = {
    'biggift': {
        'worksheet': 'ビックギフト向け',
        'platforms': ['blogger', 'livedoor'],
        'max_posts': {'blogger': 20, 'livedoor': 15},
        'needs_k_column': True
    },
    'arigataya': {
        'worksheet': 'ありがた屋向け',
        'platforms': ['seesaa', 'fc2'],  # ← 許可はここで定義
        'max_posts': 20,
        'needs_k_column': True
    },
    'kaitori_life': {
        'worksheet': '買取LIFE向け',
        'platforms': ['wordpress'],
        'wp_sites': ['selectad', 'thrones'],
        'max_posts': 20,
        'needs_k_column': False
    },
    'osaifu_rescue': {
        'worksheet': 'お財布レスキュー向け',
        'platforms': ['wordpress'],
        'wp_sites': ['ykikaku', 'efdlqjtz'],
        'max_posts': 20,
        'needs_k_column': False
    },
    'kure_kaeru': {
        'worksheet': 'クレかえる向け',
        'platforms': ['wordpress'],
        'wp_sites': ['selectadvance', 'welkenraedt'],
        'max_posts': 20,
        'needs_k_column': False
    },
    'red_site': {
        'worksheet': '赤いサイト向け',
        'platforms': ['wordpress'],
        'wp_sites': ['ncepqvub', 'kosagi'],
        'max_posts': 20,
        'needs_k_column': False
    }
}

# 非WPプラットフォーム設定
PLATFORM_CONFIGS = {
    'seesaa': {
        'endpoint': os.environ.get('SEESAA_ENDPOINT', 'http://blog.seesaa.jp/rpc'),
        'username': os.environ.get('SEESAA_USERNAME', 'kyuuyo.fac@gmail.com'),
        'password': os.environ.get('SEESAA_PASSWORD', 'st13131094pao'),
        'blogid': os.environ.get('SEESAA_BLOGID', '7228801')
    },
    'fc2': {
        'endpoint': 'https://blog.fc2.com/xmlrpc.php',
        'blog_id': os.environ.get('FC2_BLOG_ID', 'genkinka1313'),
        'username': os.environ.get('FC2_USERNAME', 'esciresearch.com@gmail.com'),
        'password': os.environ.get('FC2_PASSWORD', 'st13131094pao')
    },
    'livedoor': {
        'blog_name': os.environ.get('LIVEDOOR_BLOG_NAME', 'radiochildcare'),
        'user_id': os.environ.get('LIVEDOOR_ID', 'radiochildcare'),
        'api_key': os.environ.get('LIVEDOOR_API_KEY', '5WF0Akclk2')
    },
    'blogger': {
        'blog_id': os.environ.get('BLOGGER_BLOG_ID', '3943718248369040188')
    }
}

# 投稿間隔（スパム回避） - Streamlit用に短縮
MIN_INTERVAL = 30
MAX_INTERVAL = 60

# ========================
# Streamlit 設定
# ========================
st.set_page_config(page_title="統合ブログ投稿ツール", page_icon="🚀", layout="wide")
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white; padding: 1.2rem 1.6rem; border-radius: 12px; margin-bottom: 1rem;
    }
    .logbox {
        height: 420px; overflow:auto; border:1px solid #ddd; padding:8px; border-radius:8px; background:#fafafa;
        font-family: ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;
        font-size: 12px; line-height: 1.5;
    }
</style>
""", unsafe_allow_html=True)

# ========================
# セッションステート
# ========================
if 'gemini_key_index' not in st.session_state:
    st.session_state.gemini_key_index = 0
if 'posting_projects' not in st.session_state:
    st.session_state.posting_projects = set()
if 'current_project' not in st.session_state:
    st.session_state.current_project = None
if 'realtime_logs' not in st.session_state:
    st.session_state.realtime_logs = []

def add_realtime_log(message: str):
    ts = datetime.now().strftime("%H:%M:%S")
    st.session_state.realtime_logs.append(f"[{ts}] {message}")

# ========================
# GSpread 認証
# ========================
@st.cache_resource
def get_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        if "gcp" in st.secrets:
            gcp_info = st.secrets["gcp"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(dict(gcp_info), scope)
            return gspread.authorize(creds)
    except Exception:
        pass
    creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if creds_json:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write(creds_json); temp_path = f.name
        creds = ServiceAccountCredentials.from_json_keyfile_name(temp_path, scope)
        os.unlink(temp_path)
        return gspread.authorize(creds)
    st.error("Google認証情報が設定されていません。Secretsの[gcp]セクションを確認してください。")
    st.stop()

# ========================
# 競合他社・その他リンク
# ========================
@st.cache_data(ttl=300)
def get_competitor_domains():
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SHEET_ID).worksheet('競合他社')
        rows = sheet.get_all_values()[1:]
        domains = []
        for row in rows:
            if row and row[0]:
                d = row[0].strip()
                if d.startswith('http'):
                    d = urlparse(d).netloc
                domains.append(d.lower())
        return domains
    except Exception:
        return []

@st.cache_data(ttl=300)
def get_other_links():
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SHEET_ID).worksheet('その他リンク先')
        rows = sheet.get_all_values()[1:]
        items = []
        for r in rows:
            if len(r) >= 2 and r[0] and r[1]:
                items.append({"url": r[0].strip(), "anchor": r[1].strip()})
        if not items:
            items = [
                {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
                {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"},
            ]
        return items
    except Exception:
        return [
            {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
            {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"},
        ]

def choose_other_link():
    others = get_other_links()
    comps = get_competitor_domains()
    candidates = []
    for site in others:
        dom = urlparse(site['url']).netloc.lower()
        if not any(c in dom for c in comps):
            candidates.append(site)
    if candidates:
        pick = random.choice(candidates)
        return pick['url'], pick['anchor']
    return None, None

# ========================
# ターゲット正規化
# ========================
def normalize_target(value: str) -> str:
    """投稿先の入力ゆらぎを正規化（UI/シート両対応）"""
    if not value:
        return ""
    s = str(value).strip().lower()
    # 全角→半角
    try:
        import unicodedata
        s = unicodedata.normalize("NFKC", s)
    except Exception:
        pass
    # 代表表記に寄せる
    aliases = {
        'fc2': ['fc2', 'fc-2', 'ｆｃ２', 'fc２', 'ＦＣ２', 'fc2ブログ', 'fc2 blog'],
        'seesaa': ['seesaa', 'シーサー', 'しーさー', 'seesaaブログ'],
        'livedoor': ['livedoor', 'ライブドア', 'live door', 'livedoorブログ'],
        'blogger': ['blogger', 'ブロガー', 'blogger.com'],
        'both': ['both', '両方', 'どちらも', 'all'],
    }
    for key, words in aliases.items():
        if s in words:
            return key
    return s  # 既に期待値ならそのまま

# ========================
# ユーティリティ
# ========================
def enforce_anchor_attrs(html: str) -> str:
    """<a> に target/_blank + rel を強制付与"""
    def add_attrs(m):
        tag = m.group(0)
        if re.search(r"\btarget\s*=", tag, flags=re.I) is None:
            tag = tag.replace("<a ", '<a target="_blank" ', 1)
        rel_m = re.search(r'\brel\s*=\s*"([^"]*)"', tag, flags=re.I)
        if rel_m:
            rel_val = rel_m.group(1)
            need = []
            for t in ("noopener", "noreferrer"):
                if t not in rel_val.split():
                    need.append(t)
            if need:
                new_rel = rel_val + " " + " ".join(need)
                tag = tag[:rel_m.start(1)] + new_rel + tag[rel_m.end(1):]
        else:
            tag = tag.replace("<a ", '<a rel="noopener noreferrer" ', 1)
        return tag
    return re.sub(r"<a\s+[^>]*>", add_attrs, html, flags=re.I)

def _normalize_target(s: str) -> str:
    """投稿先表記ゆれの正規化（全角→半角、小文字、同義吸収）"""
    if not s:
        return ""
    try:
        import unicodedata
        s = unicodedata.normalize('NFKC', s)
    except Exception:
        pass
    s = s.strip().lower()
    alias = {
        'fc-2': 'fc2', 'f c 2': 'fc2', 'ｆｃ２': 'fc2', 'ＦＣ２': 'fc2', 'fc2': 'fc2',
        'seesaa': 'seesaa', 'see saa': 'seesaa', 'シーサー': 'seesaa',
        'livedoor': 'livedoor', 'ライブドア': 'livedoor',
        'blogger': 'blogger', 'ブロガー': 'blogger',
        'both': 'both', '両方': 'both'
    }
    return alias.get(s, s)

# ========================
# アイキャッチ自動生成
# ========================
def _load_font_candidates():
    candidates = [
        "fonts/NotoSansJP-Bold.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansJP-Bold.otf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "C:/Windows/Fonts/meiryob.ttc",
        "C:/Windows/Fonts/meiryo.ttc",
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return None

def create_eyecatch_image(title: str, site_key: str) -> bytes:
    width, height = 600, 400
    site_color_schemes = {
        'selectadvance': [{'bg': '#2E7D32', 'accent': '#66BB6A', 'text': '#FFFFFF'},
                          {'bg': '#388E3C', 'accent': '#81C784', 'text': '#FFFFFF'}],
        'welkenraedt': [{'bg': '#1976D2', 'accent': '#64B5F6', 'text': '#FFFFFF'},
                        {'bg': '#303F9F', 'accent': '#7986CB', 'text': '#FFFFFF'}],
        'ykikaku': [{'bg': '#E91E63', 'accent': '#F48FB1', 'text': '#FFFFFF'},
                    {'bg': '#C2185B', 'accent': '#F8BBD9', 'text': '#FFFFFF'}],
        'efdlqjtz': [{'bg': '#FF5722', 'accent': '#FF8A65', 'text': '#FFFFFF'},
                     {'bg': '#D84315', 'accent': '#FFAB91', 'text': '#FFFFFF'}],
        'ncepqvub': [{'bg': '#B71C1C', 'accent': '#EF5350', 'text': '#FFFFFF'},
                     {'bg': '#C62828', 'accent': '#E57373', 'text': '#FFFFFF'}],
        'kosagi': [{'bg': '#B71C1C', 'accent': '#EF5350', 'text': '#FFFFFF'},
                   {'bg': '#C62828', 'accent': '#E57373', 'text': '#FFFFFF'}],
        'selectad': [{'bg': '#4A148C', 'accent': '#AB47BC', 'text': '#FFFFFF'},
                     {'bg': '#6A1B9A', 'accent': '#CE93D8', 'text': '#FFFFFF'}],
        'thrones': [{'bg': '#004D40', 'accent': '#26A69A', 'text': '#FFFFFF'},
                    {'bg': '#00695C', 'accent': '#4DB6AC', 'text': '#FFFFFF'}],
        'default': [{'bg': '#4CAF50', 'accent': '#8BC34A', 'text': '#FFFFFF'},
                    {'bg': '#689F38', 'accent': '#AED581', 'text': '#FFFFFF'}]
    }
    scheme = random.choice(site_color_schemes.get(site_key, site_color_schemes['default']))
    img = Image.new('RGB', (width, height), color=scheme['bg'])
    draw = ImageDraw.Draw(img)
    for i in range(height):
        alpha = i / height
        r = int(int(scheme['bg'][1:3], 16) * (1 - alpha * 0.3))
        g = int(int(scheme['bg'][3:5], 16) * (1 - alpha * 0.3))
        b = int(int(scheme['bg'][5:7], 16) * (1 - alpha * 0.3))
        draw.rectangle([(0, i), (width, i + 1)], fill=(r, g, b))
    draw.ellipse([-50, -50, 150, 150], fill=scheme['accent'])
    draw.ellipse([width-100, height-100, width+50, height+50], fill=scheme['accent'])
    font_path = _load_font_candidates()
    try:
        title_font = ImageFont.truetype(font_path if font_path else "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 28)
        subtitle_font = ImageFont.truetype(font_path if font_path else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 20)
    except Exception:
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
    # 改行
    lines = []
    if len(title) > 12:
        for sep in ['！','？','…','!','?','、','。','・','｜']:
            if sep in title:
                idx = title.find(sep)
                if 5 < idx < len(title) - 5:
                    lines = [title[:idx+1], title[idx+1:].strip()]
                    break
        if not lines:
            for sep in ['と','の','は','が','を','に','で']:
                if sep in title:
                    idx = title.find(sep)
                    if 5 < idx < len(title) - 5:
                        lines = [title[:idx], title[idx:]]
                        break
        if not lines:
            mid = len(title) // 2
            lines = [title[:mid], title[mid:]]
    else:
        lines = [title]
    y_start = (height - len(lines) * 50) // 2
    for i, line in enumerate(lines):
        try:
            bbox = draw.textbbox((0, 0), line, font=title_font)
            text_width = bbox[2] - bbox[0]
        except AttributeError:
            text_width, _ = draw.textsize(line, font=title_font)
        x = (width - text_width) // 2
        y = y_start + i * 50
        draw.text((x + 2, y + 2), line, font=title_font, fill=(0, 0, 0))
        draw.text((x, y), line, font=title_font, fill=scheme['text'])
    site_names = {
        'selectadvance': '後払いアプリ現金化攻略ブログ',
        'welkenraedt': 'マネーハック365',
        'ykikaku': 'お財布レスキュー',
        'efdlqjtz': 'キャッシュアドバイザー',
        'ncepqvub': 'あと払いスマートライフ',
        'kosagi': '金欠ブロガーの裏金策帖',
        'selectad': '買取LIFEサポート',
        'thrones': 'アセットマネジメント'
    }
    site_name = site_names.get(site_key, 'Financial Blog')
    try:
        bbox = draw.textbbox((0, 0), site_name, font=subtitle_font)
        text_width = bbox[2] - bbox[0]
    except AttributeError:
        text_width, _ = draw.textsize(site_name, font=subtitle_font)
    x = (width - text_width) // 2
    draw.text((x, height - 50), site_name, font=subtitle_font, fill=scheme['text'])
    draw.rectangle([50, 40, width-50, 42], fill=scheme['text'])
    bio = io.BytesIO()
    img.save(bio, format='JPEG', quality=90)
    bio.seek(0)
    return bio.getvalue()

# ========================
# Gemini
# ========================
def _get_gemini_key():
    if not GEMINI_API_KEYS:
        raise RuntimeError("Gemini APIキーが未設定です")
    key = GEMINI_API_KEYS[st.session_state.gemini_key_index % len(GEMINI_API_KEYS)]
    st.session_state.gemini_key_index += 1
    return key

# ==== PATCH: Gemini caller with retry & rotation ====
import math
def _sleep_with_log(sec: float):
    try:
        add_realtime_log(f"⏳ Gemini待機 {sec:.1f} 秒（レート制限/一時エラー）")
    except Exception:
        pass
    time.sleep(max(0.0, sec))

def call_gemini(prompt: str) -> str:
    """
    レート制限(429)や一時エラー(5xx)時に指数バックオフ＋キー自動ローテーションで再試行する。
    """
    if not GEMINI_API_KEYS:
        raise RuntimeError("Gemini APIキーが未設定です")

    max_attempts = 6            # 最大リトライ回数
    base_backoff = 2.0          # 初回待機秒
    last_err = None

    for attempt in range(1, max_attempts + 1):
        api_key = GEMINI_API_KEYS[st.session_state.gemini_key_index % len(GEMINI_API_KEYS)]
        # 次回のためにインデックスを進める（429時に別キーへ切替されやすくする）
        st.session_state.gemini_key_index += 1

        endpoint = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent?key={api_key}'
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.7}
        }

        try:
            resp = requests.post(endpoint, json=payload, timeout=60)
            # 正常
            if resp.status_code == 200:
                j = resp.json()
                return j['candidates'][0]['content']['parts'][0]['text']

            # 429 / 5xx はリトライ対象
            if resp.status_code in (429, 500, 502, 503, 504):
                # Retry-After があれば尊重
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait = float(retry_after)
                    except Exception:
                        wait = base_backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                else:
                    wait = base_backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.5)

                # 429はキーを回しながら待機（すでに上で回している）
                try:
                    add_realtime_log(f"⚠️ Gemini {resp.status_code}: リトライ {attempt}/{max_attempts}、待機 {wait:.1f}s")
                except Exception:
                    pass
                _sleep_with_log(wait)
                last_err = f"Gemini API エラー: {resp.status_code} / {resp.text[:200]}"
                continue

            # それ以外は即エラー
            last_err = f"Gemini API エラー: {resp.status_code} / {resp.text[:200]}"
            break

        except requests.RequestException as e:
            # ネットワーク一時障害もリトライ
            wait = base_backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            try:
                add_realtime_log(f"⚠️ Gemini通信エラー: {e} → リトライ {attempt}/{max_attempts}、待機 {wait:.1f}s")
            except Exception:
                pass
            _sleep_with_log(wait)
            last_err = str(e)
            continue

    # ここまで来たら失敗
    raise Exception(last_err or "Gemini API 呼び出しに失敗しました")

# ==== PATCH: cache wrapper ====
@st.cache_data(ttl=1800)  # 30分キャッシュ
def _cached_generate_article(theme: str, url: str, anchor: str) -> dict:
    return generate_article_with_link(theme, url, anchor)

def generate_article_with_link(theme: str, url: str, anchor_text: str) -> dict:
    auto_theme = False
    if not theme or theme.strip() == "":
        theme = "金融・投資・資産運用"; auto_theme = True
    theme_instruction = f"「{theme}」をテーマに" if not auto_theme else "金融系（投資、クレジットカード、ローン、資産運用など）から自由にテーマを選んで"
    prompt = f"""
# 命令書:
{theme_instruction}、読者に価値のある記事を作成してください。

# 記事に含めるリンク（1つのみ）:
URL: {url}
アンカーテキスト: {anchor_text}

# 出力形式:
・最初の行に魅力的なタイトルを出力（タグなし）
・その後、HTML形式で本文作成
・リンクを自然に挿入（1回のみ）

# HTML記法:
・見出し: <h2>, <h3>のみ使用（H1タグは使用禁止）
・段落: <p>タグで囲む
・リンク: <a href="URL" target="_blank" rel="noopener noreferrer">アンカーテキスト</a>
・リスト: <ul><li>

# 記事の要件:
・2000-2500文字
・専門的でありながら分かりやすい
・具体的な数値や事例を含める
・読者の悩みを解決する内容

# 重要:
・プレースホルダー（〇〇など）は使用禁止
・すべて具体的な内容で記述
・リンクは指定されたものを正確に使用
"""
    text = call_gemini(prompt)
    lines = text.strip().split('\n')
    title = (lines[0] if lines else "タイトル").strip()
    content = '\n'.join(lines[1:]).strip()
    content = re.sub(r'〇〇|××|△△', '', content)
    content = re.sub(r'（ここで.*?）', '', content)
    content = re.sub(r'<p>\s*</p>', '', content)
    content = enforce_anchor_attrs(content.strip())
    return {"title": title, "content": content, "theme": theme if not auto_theme else "金融"}

# ========================
# WordPress
# ========================
def upload_image_to_wordpress(image_data: bytes, filename: str, site_config: dict) -> int | None:
    media_endpoint = f'{site_config["url"]}wp-json/wp/v2/media'
    import string
    safe_filename = ''.join(c for c in filename if c in string.ascii_letters + string.digits + '-_.') or f"eyecatch_{int(time.time())}.jpg"
    if not safe_filename.endswith('.jpg'):
        safe_filename += '.jpg'
    headers = {'Content-Disposition': f'attachment; filename="{safe_filename}"','Content-Type': 'image/jpeg'}
    try:
        r = requests.post(media_endpoint, data=image_data, headers=headers,
                          auth=HTTPBasicAuth(site_config['user'], site_config['password']),
                          timeout=60, verify=False)
        return r.json()['id'] if r.status_code == 201 else None
    except Exception:
        return None

def get_category_id(site_config, category_name):
    if not category_name:
        return None
    try:
        r = requests.get(f"{site_config['url']}wp-json/wp/v2/categories", timeout=30, verify=False)
        if r.status_code == 200:
            for cat in r.json():
                if cat['name'] == category_name:
                    return cat['id']
    except Exception:
        pass
    return None

def generate_slug_from_title(title):
    keyword_map = {
        '投資': 'investment','資産': 'asset','運用': 'management','増やす': 'increase',
        '貯金': 'savings','節約': 'saving','クレジット': 'credit','カード': 'card',
        'ローン': 'loan','金融': 'finance','銀行': 'bank','保険': 'insurance','実践': 'practice',
        '方法': 'method','戦略': 'strategy','ガイド': 'guide','初心者': 'beginner','完全': 'complete',
        '効果': 'effect','成功': 'success','選び方': 'selection','比較': 'comparison','活用': 'utilization',
        'おすすめ': 'recommend','基礎': 'basic','知識': 'knowledge'
    }
    parts = ['money']
    for jp, en in keyword_map.items():
        if jp in title: parts.append(en); break
    if len(parts) == 1: parts.append('tips')
    return ('-'.join(parts) + '-' + datetime.now().strftime('%m%d') + f"-{random.randint(100,999)}").lower()

def post_to_wordpress(article: dict, site_key: str, category_name: str = None,
                      schedule_dt: datetime = None, enable_eyecatch: bool = True) -> str:
    if site_key not in WP_CONFIGS:
        st.error(f"不明なサイト: {site_key}"); return ""
    cfg = WP_CONFIGS[site_key]

    # kosagi は XML-RPC
    if site_key == 'kosagi':
        if schedule_dt and schedule_dt > datetime.now():
            wait_seconds = int((schedule_dt - datetime.now()).total_seconds())
            st.info(f"kosagi用: {schedule_dt.strftime('%H:%M')}まで待機（{wait_seconds}秒）")
            p = st.progress(0)
            for i in range(wait_seconds):
                p.progress((i+1)/max(1,wait_seconds)); time.sleep(1)
            st.success("予約時刻になりました。kosagiに投稿開始")
        try:
            server = xmlrpc.client.ServerProxy(f"{cfg['url']}xmlrpc.php")
            content = {"title": article['title'], "description": article['content'],
                       "mt_allow_comments":1, "mt_allow_pings":1, "post_type":"post"}
            post_id = server.metaWeblog.newPost(0, cfg['user'], cfg['password'], content, True)
            return f"{cfg['url']}?p={post_id}"
        except Exception as e:
            st.error(f"kosagi投稿エラー: {e}"); return ""

    # REST
    feat_id = None
    if enable_eyecatch:
        try:
            with st.spinner("アイキャッチ画像を生成中..."):
                img = create_eyecatch_image(article['title'], site_key)
                feat_id = upload_image_to_wordpress(img, f"{generate_slug_from_title(article['title'])}.jpg", cfg)
        except Exception as e:
            st.warning(f"アイキャッチ生成エラー: {e}")

    payload = {
        'title': article['title'],
        'content': article['content'],
        'slug': generate_slug_from_title(article['title']),
        'status': 'publish'
    }
    if category_name:
        cid = get_category_id(cfg, category_name)
        if cid: payload['categories'] = [cid]
    if feat_id:
        payload['featured_media'] = feat_id
    if schedule_dt and schedule_dt > datetime.now():
        payload['status'] = 'future'
        payload['date'] = schedule_dt.strftime('%Y-%m-%dT%H:%M:%S')

    try:
        r = requests.post(f"{cfg['url']}wp-json/wp/v2/posts",
                          auth=HTTPBasicAuth(cfg['user'], cfg['password']),
                          headers={'Content-Type':'application/json','User-Agent':'streamlit-app'},
                          data=json.dumps(payload), timeout=60, verify=False)
        if r.status_code in (200,201):
            return r.json().get('link','')
        else:
            st.error(f"WordPress投稿失敗({site_key}): {r.status_code}\n{r.text[:400]}...")
            return ""
    except Exception as e:
        st.error(f"WordPress投稿エラー({site_key}): {e}")
        return ""

def test_wordpress_connection(site_key):
    if site_key not in WP_CONFIGS:
        st.error(f"設定なし: {site_key}"); return False
    cfg = WP_CONFIGS[site_key]
    try:
        if site_key == 'kosagi':
            server = xmlrpc.client.ServerProxy(f"{cfg['url']}xmlrpc.php")
            _ = server.blogger.getUsersBlogs("", cfg['user'], cfg['password'])
            st.success("kosagi 接続成功 (XML-RPC)"); return True
        r = requests.get(f"{cfg['url']}wp-json/wp/v2/users/me",
                         auth=HTTPBasicAuth(cfg['user'], cfg['password']),
                         timeout=15, verify=False)
        if r.status_code == 200:
            st.success("✅ 接続成功"); return True
        st.error(f"❌ 接続失敗: {r.status_code}\n{r.text[:300]}...")
        return False
    except Exception as e:
        st.error(f"接続エラー: {e}"); return False

# ========================
# 非WP投稿
# ========================
def post_to_seesaa(article: dict, category_name: str = None) -> str:
    cfg = PLATFORM_CONFIGS['seesaa']
    server = xmlrpc.client.ServerProxy(cfg['endpoint'], allow_none=True)
    safe_html = enforce_anchor_attrs(article["content"])
    content = {"title": article["title"], "description": safe_html}
    try:
        post_id = server.metaWeblog.newPost(cfg['blogid'], cfg['username'], cfg['password'], content, True)
        if category_name:
            try:
                cats = server.mt.getCategoryList(cfg['blogid'], cfg['username'], cfg['password'])
                for c in cats:
                    if c.get("categoryName") == category_name:
                        server.mt.setPostCategories(post_id, cfg['username'], cfg['password'],
                                                    [{"categoryId": c.get("categoryId"), "isPrimary": True}])
                        break
            except Exception:
                pass
        try:
            post = server.metaWeblog.getPost(post_id, cfg['username'], cfg['password'])
            return post.get("permalink") or post.get("link") or ""
        except Exception:
            return f"post_id:{post_id}"
    except Exception as e:
        st.error(f"Seesaa投稿エラー: {e}")
        return ""

def post_to_fc2(article: dict, category_name: str = None) -> str:
    cfg = PLATFORM_CONFIGS['fc2']
    server = xmlrpc.client.ServerProxy(cfg['endpoint'])
    safe_html = enforce_anchor_attrs(article['content'])
    content = {'title': article['title'], 'description': safe_html}
    try:
        post_id = server.metaWeblog.newPost(cfg['blog_id'], cfg['username'], cfg['password'], content, True)
        if category_name:
            try:
                cats = server.mt.getCategoryList(cfg['blog_id'], cfg['username'], cfg['password'])
                for c in cats:
                    if c.get('categoryName') == category_name:
                        server.mt.setPostCategories(post_id, cfg['username'], cfg['password'], [c]); break
            except Exception:
                pass
        return f"https://{cfg['blog_id']}.blog.fc2.com/blog-entry-{post_id}.html"
    except Exception as e:
        st.error(f"FC2投稿エラー: {e}")
        return ""

def post_to_livedoor(article: dict, category_name: str = None) -> str:
    cfg = PLATFORM_CONFIGS['livedoor']
    root = f"https://livedoor.blogcms.jp/atompub/{cfg['blog_name']}"
    endpoint = f"{root}/article"
    title_xml = xml_escape(article["title"])
    safe_html = enforce_anchor_attrs(article["content"])
    content_xml = xml_escape(safe_html)
    cat_xml = f'<category term="{xml_escape(category_name)}"/>' if category_name else ""
    entry_xml = f'''<?xml version="1.0" encoding="utf-8"?>
<entry xmlns="http://www.w3.org/2005/Atom">
  <title>{title_xml}</title>
  <content type="html">{content_xml}</content>
  {cat_xml}
</entry>'''.encode("utf-8")
    try:
        r = requests.post(endpoint, data=entry_xml,
                          headers={"Content-Type": "application/atom+xml;type=entry"},
                          auth=HTTPBasicAuth(cfg['user_id'], cfg['api_key']),
                          timeout=30)
        if r.status_code in (200,201):
            try:
                root_xml = ET.fromstring(r.text)
                ns = {"atom":"http://www.w3.org/2005/Atom"}
                alt = root_xml.find(".//atom:link[@rel='alternate']", ns)
                return alt.get("href") if alt is not None else ""
            except Exception:
                return ""
        else:
            st.error(f"livedoor投稿失敗: {r.status_code}\n{r.text[:300]}...")
            return ""
    except Exception as e:
        st.error(f"livedoor投稿エラー: {e}")
        return ""

def post_to_blogger(article: dict) -> str:
    st.warning("Blogger投稿は未実装（認証が複雑なため）")
    return ""

# ========================
# シート I/O
# ========================
@st.cache_data(ttl=60)
def load_sheet_data(project_key):
    try:
        if project_key not in PROJECT_CONFIGS:
            return pd.DataFrame()
        client = get_sheets_client()
        cfg = PROJECT_CONFIGS[project_key]
        sheet = client.open_by_key(SHEET_ID).worksheet(cfg['worksheet'])
        rows = sheet.get_all_values()
        if len(rows) <= 1:
            return pd.DataFrame()
        headers = rows[0]
        data_rows = rows[1:]
        clean_headers = []
        for i, h in enumerate(headers):
            clean_headers.append(f"{h}_{i}" if h in clean_headers else h)
        filtered = []
        for row in data_rows:
            if len(row) >= 5 and row[1] and row[1].strip():
                status = row[4].strip().lower() if len(row) > 4 else ''
                if status in ['', '未処理']:
                    adj = row + [''] * (len(clean_headers) - len(row))
                    filtered.append(adj[:len(clean_headers)])
        if not filtered:
            return pd.DataFrame()
        df = pd.DataFrame(filtered, columns=clean_headers)
        if '選択' not in df.columns:
            df.insert(0, '選択', False)
        return df
    except Exception as e:
        st.error(f"データ読み込みエラー: {e}")
        return pd.DataFrame()

def update_sheet_row(project_key, row_data, updates):
    try:
        client = get_sheets_client()
        cfg = PROJECT_CONFIGS[project_key]
        sheet = client.open_by_key(SHEET_ID).worksheet(cfg['worksheet'])
        all_rows = sheet.get_all_values()
        promo_url = row_data.get('宣伝URL', '')
        for i, r in enumerate(all_rows[1:], start=2):
            if len(r) > 1 and r[1] == promo_url:
                for col, val in updates.items():
                    if col in all_rows[0]:
                        ci = all_rows[0].index(col) + 1
                        sheet.update_cell(i, ci, val)
                return True
        return False
    except Exception as e:
        st.error(f"スプレッドシート更新エラー: {e}")
        return False

def add_schedule_to_k_column(project_key, row_data, schedule_times):
    try:
        client = get_sheets_client()
        cfg = PROJECT_CONFIGS[project_key]
        sheet = client.open_by_key(SHEET_ID).worksheet(cfg['worksheet'])
        all_rows = sheet.get_all_values()
        promo_url = row_data.get('宣伝URL', '')
        for i, r in enumerate(all_rows[1:], start=2):
            if len(r) > 1 and r[1] == promo_url:
                col_num = 11  # K列
                for dt in schedule_times:
                    while col_num <= len(r) + 10:
                        try:
                            if not sheet.cell(i, col_num).value:
                                break
                        except Exception:
                            break
                        col_num += 1
                    sheet.update_cell(i, col_num, dt.strftime('%Y/%m/%d %H:%M'))
                    col_num += 1
                return True
        return False
    except Exception as e:
        st.error(f"K列記録エラー: {e}")
        return False

# ========================
# 投稿ロジック
# ========================
def get_max_posts_for_project(project_key, post_target=""):
    cfg = PROJECT_CONFIGS[project_key]
    mx = cfg['max_posts']
    if isinstance(mx, dict):
        if post_target.lower() == 'livedoor': return 15
        elif post_target.lower() == 'blogger': return 20
        else: return 20
    return mx

def execute_post(row_data, project_key, post_count=1, schedule_times=None, enable_eyecatch=True, ui_override_target:str=""):
    try:
        st.session_state.posting_projects.add(project_key)
        add_realtime_log(f"📋 {PROJECT_CONFIGS[project_key]['worksheet']} の投稿開始")
        cfg = PROJECT_CONFIGS[project_key]
        schedule_times = schedule_times or []
        current_counter = 0
        if 'カウンター' in row_data and row_data['カウンター']:
            try: 
                current_counter = int(row_data['カウンター'])
            except Exception: 
                current_counter = 0
        add_realtime_log(f"📊 現在のカウンター: {current_counter}")

        # --- 投稿先決定（UI最優先） ---
        override_target = row_data.get('__override_target__')  # UI 上書き
        if override_target:
            desired_target = _normalize_target(override_target)
        else:
            desired_target = _normalize_target(row_data.get('投稿先',''))

        add_realtime_log(f"🎯 指定投稿先(正規化): '{desired_target or '（未指定）'}'")

        max_posts = get_max_posts_for_project(project_key, desired_target)
        if current_counter >= max_posts:
            add_realtime_log(f"⚠️ 既に{max_posts}記事完了済み")
            st.warning(f"既に{max_posts}記事完了しています")
            return False

        posts_completed = 0
        progress_bar = st.progress(0)

        for i in range(post_count):
            if current_counter >= max_posts:
                add_realtime_log(f"⚠️ カウンター{current_counter}: 既に{max_posts}記事完了")
                break

            schedule_dt = schedule_times[i] if i < len(schedule_times) else None
            add_realtime_log(f"📝 記事{i+1}/{post_count}の処理開始")

            with st.expander(f"記事{i+1}/{post_count}の投稿", expanded=True):
                try:
                    # 使用リンク決定
                    if current_counter == max_posts - 1:
                        add_realtime_log(f"🎯 {max_posts}記事目 → 宣伝URL使用")
                        url = row_data.get('宣伝URL', '')
                        anchor = row_data.get('アンカーテキスト', 'サイト紹介')
                        category = row_data.get('カテゴリー', 'お金のマメ知識')
                    else:
                        add_realtime_log(f"🔗 {current_counter + 1}記事目 → その他リンク使用")
                        url, anchor = choose_other_link()
                        if not url:
                            add_realtime_log("❌ その他リンクが取得できません")
                            st.error("その他リンクが取得できません")
                            break
                        category = 'お金のマメ知識'

                    # 記事生成
                    add_realtime_log("🧠 記事を生成中...")
                    with st.spinner("記事を生成中..."):
                        theme = row_data.get('テーマ', '') or '金融・投資・資産運用'
                        article = _cached_generate_article(theme, url, anchor)
                    st.success(f"タイトル: {article['title']}")
                    st.info(f"使用リンク: {anchor}")

                    posted_urls = []
                    platforms = cfg['platforms']

                    if 'wordpress' in platforms:
                        # WP案件
                        sites = cfg.get('wp_sites', [])
                        if desired_target in ['both'] and len(sites) > 1:
                            wp_targets = sites[:]
                        elif desired_target in sites:
                            wp_targets = [desired_target]
                        elif desired_target in WP_CONFIGS:
                            wp_targets = [desired_target]
                        else:
                            # 未指定→最初のWPに出す（WPは誤爆リスク低）
                            wp_targets = [sites[0]] if sites else []

                        add_realtime_log(f"🧭 WP投稿ターゲット: {wp_targets}")
                        for tgt in wp_targets:
                            add_realtime_log(f"📤 {tgt} に投稿中...")
                            post_url = post_to_wordpress(article, tgt, category, schedule_dt, enable_eyecatch)
                            if post_url:
                                posted_urls.append(post_url)
                                add_realtime_log(f"✅ {tgt} 投稿成功 → {post_url}")
                            else:
                                add_realtime_log(f"❌ {tgt} 投稿失敗")

                    else:
                        # ★ 非WordPress案件の分岐（biggift / arigataya など）
                        # 1) UI最優先 → 2) シートの投稿先 → 3) フォールバック優先順位
                        desired_target_raw = ui_override_target or (row_data.get('投稿先', '') or '')
                        desired_target = normalize_target(desired_target_raw)

                        add_realtime_log(f"🎯 希望ターゲット(前処理) = '{desired_target_raw}' → 正規化 = '{desired_target}'")

                        valid_targets = [p for p in ['livedoor', 'blogger', 'seesaa', 'fc2'] if p in platforms]

                        def do_post(target_name: str):
                            nonlocal posted_urls
                            t = normalize_target(target_name)
                            if t == 'livedoor':
                                add_realtime_log("📤 livedoor へ投稿開始")
                                add_realtime_log(f"    endpoint: https://livedoor.blogcms.jp/atompub/{PLATFORM_CONFIGS['livedoor']['blog_name']}/article")
                                post_url = post_to_livedoor(article, category)
                            elif t == 'seesaa':
                                add_realtime_log("📤 seesaa へ投稿開始")
                                add_realtime_log(f"    endpoint: {PLATFORM_CONFIGS['seesaa']['endpoint']}")
                                post_url = post_to_seesaa(article, category)
                            elif t == 'fc2':
                                add_realtime_log("📤 fc2 へ投稿開始")
                                add_realtime_log(f"    endpoint: {PLATFORM_CONFIGS['fc2']['endpoint']}")
                                post_url = post_to_fc2(article, category)
                            elif t == 'blogger':
                                add_realtime_log("📤 blogger へ投稿開始")
                                post_url = post_to_blogger(article)
                            else:
                                add_realtime_log(f"❌ 未知のターゲット指定: {target_name}")
                                return
                            if post_url:
                                posted_urls.append(post_url)
                                add_realtime_log(f"✅ {t} 投稿成功 → {post_url}")
                            else:
                                add_realtime_log(f"❌ {t} 投稿失敗")

                        # 実際に投下するターゲット配列を決定
                        if desired_target == 'both':
                            targets = [t for t in ['livedoor', 'blogger'] if t in valid_targets] or valid_targets[:]
                        elif desired_target in valid_targets:
                            targets = [desired_target]
                        else:
                            # フォールバック：この順で最初の存在プラットフォームへ
                            fallback_order = [t for t in ['livedoor', 'blogger', 'seesaa', 'fc2'] if t in valid_targets]
                            targets = fallback_order[:1] if fallback_order else []

                        add_realtime_log(f"🧭 実際に投稿するターゲット = {targets}")

                        for t in targets:
                            do_post(t)

                    if not posted_urls:
                        add_realtime_log("❌ 投稿に失敗しました")
                        st.error("投稿に失敗しました")
                        break

                    # URLを画面に明示
                    st.success("投稿URL:")
                    for u in posted_urls:
                        st.write(f"• {u}")

                    current_counter += 1
                    posts_completed += 1

                    # シート更新
                    upd = {'カウンター': str(current_counter), '投稿URL': ', '.join(posted_urls)}
                    if current_counter >= max_posts:
                        upd['ステータス'] = '処理済み'
                    update_sheet_row(project_key, row_data, upd)

                    st.success(f"投稿完了 {posts_completed}/{post_count}")

                except Exception as e:
                    add_realtime_log(f"❌ 記事{i+1}の投稿エラー: {e}")
                    st.error(f"記事{i+1}の投稿エラー: {e}")
                    break

            progress_bar.progress(posts_completed / max(1, post_count))

            if i < post_count - 1:
                wait_time = random.randint(MIN_INTERVAL, MAX_INTERVAL)
                add_realtime_log(f"⏳ 次の投稿まで {wait_time} 秒待機")
                for _ in range(wait_time):
                    time.sleep(1)

        add_realtime_log(f"✅ 合計 {posts_completed} 件の投稿を完了")
        return posts_completed > 0
    except Exception as e:
        add_realtime_log(f"❌ 投稿処理エラー: {e}")
        st.error(f"投稿処理エラー: {e}")
        return False
    finally:
        st.session_state.posting_projects.discard(project_key)

# ========================
# UI
# ========================
def main():
    st.markdown('<div class="main-header"><h2>SEOブログ自動化ツール</h2><p>記事を自動生成して複数プラットフォームに投稿します</p></div>', unsafe_allow_html=True)

    project_options = {
        'biggift': 'ビックギフト（非WordPress・K列予約）',
        'arigataya': 'ありがた屋（非WordPress・K列予約）',
        'kaitori_life': '買取LIFE（WordPress・予約投稿）',
        'osaifu_rescue': 'お財布レスキュー（WordPress・予約投稿）',
        'kure_kaeru': 'クレかえる（WordPress・予約投稿）',
        'red_site': '赤いサイト（WordPress・kosagi特殊）'
    }
    left, right = st.columns([2,1])

    with left:
        project_key = st.selectbox(
            "プロジェクト選択:",
            options=list(project_options.keys()),
            format_func=lambda x: project_options[x],
            disabled=project_options.get("project_selector","biggift") in st.session_state.posting_projects,
            key="project_selector"
        )
    with right:
        st.caption("進行状況ログ")
        st.markdown('<div class="logbox" id="logbox">', unsafe_allow_html=True)
        for log in st.session_state.realtime_logs[-500:]:
            st.text(log)
        st.markdown('</div>', unsafe_allow_html=True)

    # プロジェクト切り替え時はデータキャッシュクリア
    current_project = st.session_state.get('current_project')
    if current_project != project_key and project_key not in st.session_state.posting_projects:
        st.session_state.current_project = project_key
        st.cache_data.clear()
        st.session_state.realtime_logs = []

    cfg = PROJECT_CONFIGS[project_key]
    c1, c2 = st.columns(2)
    with c1:
        st.info(f"**プロジェクト**: {cfg['worksheet']}")
        st.info(f"**プラットフォーム**: {', '.join(cfg['platforms'])}")
    with c2:
        if cfg['needs_k_column']:
            st.warning("**予約方式**: K列記録 → GitHub Actions実行")
        else:
            st.success("**予約方式**: WordPress予約投稿機能")

    # === 非WPのみ: UIで投稿先を明示指定できるように ===
    ui_override_target = ""
    if 'wordpress' not in cfg['platforms']:
        st.subheader("投稿先（非WordPress）")
        nonwp_targets = [p for p in ['livedoor', 'blogger', 'seesaa', 'fc2'] if p in cfg['platforms']]
        # 先頭に「自動（シート値を使用）」を入れる
        opts_label = ['自動（シートの「投稿先」列を使用）'] + [t.upper() if t!='blogger' else 'Blogger' for t in nonwp_targets]
        choice = st.radio(
            "UIで投稿先を固定したい場合は選択してください",
            options=opts_label,
            horizontal=True,
            help="ここで選ぶとシートの『投稿先』よりも優先されます"
        )
        if choice != '自動（シートの「投稿先」列を使用）':
            # 表示ラベル→キー名へ逆変換
            map_back = {('Blogger' if t=='blogger' else t.upper()): t for t in nonwp_targets}
            ui_override_target = map_back.get(choice, "")

    # WP接続テスト
    if not cfg['needs_k_column']:
        with st.expander("🔧 WordPress接続テスト", expanded=False):
            sites = cfg.get('wp_sites', [])
            if sites:
                test_site = st.selectbox("テスト対象サイト:", options=sites, key="test_site")
                if st.button(f"🔍 {test_site} 接続テスト", type="secondary"):
                    test_wordpress_connection(test_site)
                if len(sites) > 1 and st.button("🔍 全サイト一括テスト", type="secondary"):
                    for s in sites:
                        st.write(f"## {s}")
                        test_wordpress_connection(s)
                        st.write("---")
            else:
                st.write("このプロジェクトにはWordPressサイトの指定がありません。")

    # データ
    df = load_sheet_data(project_key)
    if df.empty:
        st.info("未処理のデータがありません")
        return

    st.header("データ一覧")
    edited_df = st.data_editor(
        df,
        use_container_width=True, hide_index=True,
        column_config={
            "選択": st.column_config.CheckboxColumn("選択", width="small"),
            "テーマ": st.column_config.TextColumn("テーマ", width="medium"),
            "宣伝URL": st.column_config.TextColumn("宣伝URL", width="large"),
            "投稿先": st.column_config.TextColumn("投稿先", width="small"),
            "アンカーテキスト": st.column_config.TextColumn("アンカー", width="medium"),
            "ステータス": st.column_config.TextColumn("ステータス", width="small"),
            "カウンター": st.column_config.TextColumn("カウンター", width="small")
        }
    )

    st.header("投稿設定")
    col1, col2 = st.columns(2)
    with col1:
        post_count = st.selectbox("投稿数", options=[1,2,3,4,5], help="一度に投稿する記事数を選択")
    with col2:
        enable_eyecatch = st.checkbox("アイキャッチ画像を自動生成", value=True)

    # 非WPは UI上書き選択肢を出す
    override_target_ui = None
    if 'wordpress' not in cfg['platforms']:
        with st.expander("投稿先を上書き（非WPのみ・任意）", expanded=False):
            allowed = PROJECT_CONFIGS[project_key]['platforms']
            override_target_ui = st.selectbox(
                "上書き投稿先", options=["（上書きしない）"] + allowed + (["both"] if len(allowed) > 1 else []),
                help="シートの『投稿先』を無視して、この選択を優先します。"
            )

    # 予約 UI
    if cfg['needs_k_column']:
        st.markdown("""<div class="warning-box"><strong>非WordPressプロジェクト</strong><br>
        予約時刻はK列に記録され、GitHub Actionsで定期実行されます。</div>""", unsafe_allow_html=True)
        enable_schedule = st.checkbox("予約投稿を使用する（K列記録）")
        schedule_times = []
        if enable_schedule:
            st.subheader("予約時刻設定")
            schedule_input = st.text_area("予約時刻（1行につき1件）",
                                          placeholder="10:30\n12:15\n14:00",
                                          help="HH:MM形式。今日の未来時刻のみ有効。")
            if schedule_input:
                lines = [x.strip() for x in schedule_input.split('\n') if x.strip()]
                now = datetime.now()
                for line in lines:
                    try:
                        t = datetime.strptime(line, '%H:%M')
                        dt = now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
                        if dt > now: 
                            schedule_times.append(dt)
                        else: 
                            st.error(f"過去時刻: {line}")
                    except ValueError:
                        st.error(f"無効な時刻形式: {line}")
                if schedule_times:
                    st.success(f"予約時刻 {len(schedule_times)}件")
                    for dt in schedule_times: 
                        st.write("• " + dt.strftime('%H:%M'))
                if len(schedule_times) < post_count and enable_schedule:
                    st.warning(f"投稿数{post_count}に対して予約時刻が{len(schedule_times)}件")
    else:
        st.markdown("""<div class="success-box"><strong>WordPressプロジェクト</strong><br>
        WordPressの予約投稿機能を使用します。</div>""", unsafe_allow_html=True)
        enable_schedule = st.checkbox("予約投稿を使用する")
        schedule_times = []
        if enable_schedule:
            st.subheader("予約時刻設定")
            schedule_input = st.text_area("予約時刻（1行につき1件）",
                                          placeholder="2025-08-20 10:30\n2025-08-20 12:15\n2025-08-20 14:00",
                                          help="YYYY-MM-DD HH:MM / YYYY/MM/DD HH:MM / HH:MM のいずれか")
            if schedule_input:
                lines = [x.strip() for x in schedule_input.split('\n') if x.strip()]
                now = datetime.now()
                for line in lines:
                    dt = None
                    for fmt in ('%Y-%m-%d %H:%M','%Y/%m/%d %H:%M','%H:%M'):
                        try:
                            if fmt == '%H:%M':
                                t = datetime.strptime(line, fmt)
                                dt = now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
                            else:
                                dt = datetime.strptime(line, fmt)
                            break
                        except ValueError:
                            continue
                    if dt and dt > now: 
                        schedule_times.append(dt)
                    elif dt: 
                        st.error(f"過去の時刻は指定不可: {line}")
                    else: 
                        st.error(f"無効な時刻形式: {line}")
                if schedule_times:
                    st.success(f"予約時刻 {len(schedule_times)}件")
                    for dt in schedule_times: 
                        st.write("• " + dt.strftime('%Y/%m/%d %H:%M'))
                if len(schedule_times) < post_count and enable_schedule:
                    st.warning(f"投稿数{post_count}に対して予約時刻が{len(schedule_times)}件")

    # 実行ボタン
    col_a, col_b = st.columns(2)
    with col_a:
        if cfg['needs_k_column'] and enable_schedule:
            button_text = "K列に予約時刻を記録"
        elif not cfg['needs_k_column'] and enable_schedule:
            button_text = "予約投稿"
        else:
            button_text = "即時投稿"

        if st.button(button_text, type="primary", use_container_width=True):
            sel = edited_df[edited_df['選択'] == True]
            if len(sel) == 0:
                st.error("投稿する行を選択してください")
            elif len(sel) > 1:
                st.error("1行のみ選択してください")
            else:
                row = sel.iloc[0].to_dict()
                # UI 上書きを仕込む（非WPのみ）
                if override_target_ui and override_target_ui != "（上書きしない）":
                    row['__override_target__'] = override_target_ui

                if cfg['needs_k_column'] and enable_schedule:
                    if not schedule_times:
                        st.error("予約時刻を入力してください")
                    else:
                        if add_schedule_to_k_column(project_key, row, schedule_times):
                            st.success("K列に予約時刻を記録しました。GitHub Actionsで実行されます。")
                            time.sleep(1.2)
                            st.cache_data.clear()
                            st.rerun()
                else:
                    ok = execute_post(
                        row, project_key,
                        post_count=post_count,
                        schedule_times=schedule_times if enable_schedule else [],
                        enable_eyecatch=enable_eyecatch,
                        ui_override_target=ui_override_target
                    )
                    if ok:
                        st.cache_data.clear()

    with col_b:
        if st.button("データ更新", use_container_width=True):
            st.cache_data.clear()
            st.success("データを更新しました")
            st.rerun()

    # GitHub Actions サンプル
    if cfg['needs_k_column']:
        with st.expander("GitHub Actions設定サンプル"):
            st.code("""
name: Auto Blog Post
on:
  schedule:
    - cron: '0,30 * * * *'
  workflow_dispatch:
jobs:
  post:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v4
        with: { python-version: '3.11' }
      - run: pip install -r requirements.txt
      - name: Run scheduled posts
        env:
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          GOOGLE_APPLICATION_CREDENTIALS_JSON: ${{ secrets.GOOGLE_APPLICATION_CREDENTIALS_JSON }}
        run: python scripts/post_executor.py
""", language="yaml")

    # メトリクス
    st.markdown("---")
    c3, c4, c5 = st.columns(3)
    with c3: 
        st.metric("未処理件数", len(edited_df))
    with c4: 
        st.metric("プラットフォーム", len(cfg['platforms']))
    with c5: 
        st.metric("最終更新", datetime.now().strftime("%H:%M:%S"))

if __name__ == "__main__":
    main()

