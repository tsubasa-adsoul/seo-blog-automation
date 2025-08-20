# streamlit_app.py
# -*- coding: utf-8 -*-

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
import io
import os
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

# ============================================================
# 0) Bloggerライブラリ(オプション)
# ============================================================
try:
    import pickle
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    BLOGGER_AVAILABLE = True
except Exception:
    BLOGGER_AVAILABLE = False


# ============================================================
# 1) フォント（文字化け防止・同梱最優先）
# ============================================================
def _jp_font(size: int) -> ImageFont.FreeTypeFont:
    """
    日本語フォントのロード。最優先で ./fonts/NotoSansJP-Bold.ttf を使う。
    失敗時は順にフォールバックして、最終的には PIL のデフォルトを返す。
    """
    candidates = [
        Path(__file__).resolve().parent / "fonts" / "NotoSansJP-Bold.ttf",  # 同梱フォント（最優先）
        Path("fonts/NotoSansJP-Bold.ttf"),                                   # 実行場所直下
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",            # Linuxよくある
        "/usr/share/fonts/truetype/fonts-japanese-gothic.ttf",               # Linux別名
        "C:/Windows/Fonts/meiryob.ttc",                                      # Windows Bold
        "C:/Windows/Fonts/meiryo.ttc",                                       # Windows
    ]
    for p in candidates:
        try:
            return ImageFont.truetype(str(p), size)
        except Exception:
            pass
    return ImageFont.load_default()


# ============================================================
# 2) 便利系
# ============================================================
def normalize_base_url(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    if not re.match(r"^https?://", u):
        u = "https://" + u
    if not u.endswith("/"):
        u += "/"
    return u


def generate_slug_from_title(title: str) -> str:
    """
    タイトルから英数字スラッグを生成（日本語OK）。red系キーワード優先。
    """
    keyword_map = {
        '投資':'investment','資産':'asset','運用':'management','増やす':'increase','貯金':'savings','節約':'saving',
        'クレジット':'credit','カード':'card','ローン':'loan','金融':'finance','銀行':'bank','保険':'insurance',
        '実践':'practice','方法':'method','戦略':'strategy','ガイド':'guide','初心者':'beginner','完全':'complete',
        '効果':'effect','成功':'success','選び方':'selection','比較':'comparison','活用':'utilization',
        'おすすめ':'recommend','基礎':'basic','知識':'knowledge','対策':'measures','解決':'solution',
        '買取':'kaitori','業者':'company','先払い':'sakibarai','爆速':'bakusoku','賢く':'smart','乗り切る':'survive',
        'お金':'money','困らない':'noworry','金欠':'shortage','現金化':'cash','即日':'sameday','審査':'screening',
        '申込':'application','利用':'use','安全':'safe','注意':'caution','危険':'danger','詐欺':'scam','違法':'illegal'
    }
    parts = ['money']
    for jp, en in keyword_map.items():
        if jp in title:
            parts.append(en)
            break
    if len(parts) == 1:
        parts.append('tips')
    mmdd = datetime.now().strftime('%m%d')
    rnd = random.randint(100, 999)
    return f"{'-'.join(parts)}-{mmdd}-{rnd}".lower()


def enforce_anchor_attrs(html_text: str) -> str:
    """
    すべての <a> に target/rel を追加・補完（セキュリティ＆SEO的に安全）
    """
    def add_attrs(m):
        tag = m.group(0)
        if re.search(r'\btarget\s*=', tag, flags=re.I) is None:
            tag = tag.replace('<a ', '<a target="_blank" ', 1)
        rel_m = re.search(r'\brel\s*=\s*"([^"]*)"', tag, flags=re.I)
        if rel_m:
            rel_val = rel_m.group(1)
            need = []
            for t in ('noopener', 'noreferrer'):
                if t not in rel_val.split():
                    need.append(t)
            if need:
                new_rel = rel_val + ' ' + ' '.join(need)
                tag = tag[:rel_m.start(1)] + new_rel + tag[rel_m.end(1):]
        else:
            tag = tag.replace('<a ', '<a rel="noopener noreferrer" ', 1)
        return tag

    return re.sub(r'<a\s+[^>]*>', add_attrs, html_text, flags=re.I)


# ============================================================
# 3) アイキャッチ自動生成（JPフォント・赤サイト配色）
# ============================================================
def create_eyecatch_image(title: str, site_key: str) -> bytes:
    """
    タイトルから600x400のアイキャッチ画像を生成。赤サイトは赤系、それ以外は緑系。
    """
    w, h = 600, 400
    if site_key in ['ncepqvub', 'kosagi']:
        schemes = [
            {'bg': '#B71C1C', 'accent': '#EF5350', 'text': '#FFFFFF'},
            {'bg': '#C62828', 'accent': '#FF5252', 'text': '#FFFFFF'},
            {'bg': '#D32F2F', 'accent': '#FF8A80', 'text': '#FFFFFF'},
            {'bg': '#E53935', 'accent': '#FFCDD2', 'text': '#FFFFFF'},
            {'bg': '#8B0000', 'accent': '#DC143C', 'text': '#FFFFFF'},
        ]
    else:
        schemes = [
            {'bg': '#2E7D32', 'accent': '#66BB6A', 'text': '#FFFFFF'},
            {'bg': '#388E3C', 'accent': '#81C784', 'text': '#FFFFFF'},
            {'bg': '#4CAF50', 'accent': '#8BC34A', 'text': '#FFFFFF'},
            {'bg': '#689F38', 'accent': '#AED581', 'text': '#FFFFFF'},
            {'bg': '#7CB342', 'accent': '#C5E1A5', 'text': '#2E7D32'},
        ]
    sc = random.choice(schemes)

    img = Image.new('RGB', (w, h), color=sc['bg'])
    draw = ImageDraw.Draw(img)

    # 簡易グラデーション
    r, g, b = int(sc['bg'][1:3], 16), int(sc['bg'][3:5], 16), int(sc['bg'][5:7], 16)
    for y in range(h):
        a = y / h * 0.3
        draw.rectangle([(0, y), (w, y + 1)],
                       fill=(int(r * (1 - a)), int(g * (1 - a)), int(b * (1 - a))))

    # 装飾
    draw.ellipse([-50, -50, 150, 150], fill=sc['accent'])
    draw.ellipse([w - 100, h - 100, w + 50, h + 50], fill=sc['accent'])

    f = _jp_font(28)

    # 2行化ロジック
    lines = []
    if len(title) > 12:
        for sep in ['！', '？', '…', '!', '?']:
            if sep in title:
                i = title.find(sep)
                if i > 0:
                    lines = [title[:i + 1], title[i + 1:].strip()]
                    break
        if not lines:
            for sep in ['と', '、', 'の', 'は', 'が', 'を', 'に', '…', 'で']:
                if sep in title:
                    i = title.find(sep)
                    if 5 < i < len(title) - 5:
                        lines = [title[:i], title[i:]]
                        break
        if not lines:
            m = len(title) // 2
            lines = [title[:m], title[m:]]
    else:
        lines = [title]

    y0 = (h - len(lines) * 50) // 2
    for idx, line in enumerate(lines):
        try:
            bbox = draw.textbbox((0, 0), line, font=f)
            tw = bbox[2] - bbox[0]
        except Exception:
            tw, _ = draw.textsize(line, font=f)
        x = (w - tw) // 2
        y = y0 + idx * 50
        draw.text((x + 2, y + 2), line, font=f, fill=(0, 0, 0))
        draw.text((x, y), line, font=f, fill=sc['text'])

    draw.rectangle([50, 40, w - 50, 42], fill=sc['text'])

    bio = io.BytesIO()
    img.save(bio, format='JPEG', quality=90)
    bio.seek(0)
    return bio.getvalue()


def upload_image_to_wordpress(image_bytes: bytes, filename: str, site_config: dict, log=None) -> int | None:
    def L(m): (st.write(m) if log is None else log(m))
    endpoint = f"{site_config['url']}wp-json/wp/v2/media"
    import string
    safe = ''.join(c for c in filename if c in string.ascii_letters + string.digits + '-_.')
    if not safe or safe == '.jpg':
        safe = f'eyecatch_{int(time.time())}.jpg'
    if not safe.endswith('.jpg'):
        safe += '.jpg'

    files = {'file': (safe, io.BytesIO(image_bytes), 'image/jpeg')}
    data = {'title': safe, 'alt_text': safe}
    try:
        r = requests.post(endpoint,
                          auth=HTTPBasicAuth(site_config['user'], site_config['password']),
                          files=files, data=data, timeout=60)
        if r.status_code == 201:
            mid = r.json().get('id')
            L(f"✅ アイキャッチUP成功: {safe} (ID: {mid})")
            return mid
        L(f"⚠️ アイキャッチUP失敗: {r.status_code} / {r.text[:200]}")
        return None
    except Exception as e:
        L(f"⚠️ アイキャッチUP例外: {e}")
        return None


# ============================================================
# 4) Secrets / Config
# ============================================================
try:
    SHEET_ID = st.secrets["google"]["spreadsheet_id"]
    GEMINI_API_KEYS = [
        st.secrets["google"]["gemini_api_key_1"],
        st.secrets["google"]["gemini_api_key_2"],
    ]
except KeyError as e:
    st.error(f"Secrets不足: {e}")
    st.stop()

BLOG_ID = st.secrets.get("BLOG_ID", "")
BLOGGER_CREDENTIALS_JSON = st.secrets.get("BLOGGER_CREDENTIALS", "").strip()
BLOGGER_TOKEN_B64 = st.secrets.get("BLOGGER_TOKEN", "").strip()

WP_CONFIGS = {
    "ykikaku": {"url": "https://ykikaku.xsrv.jp/", "user": "ykikaku", "password": "lfXp BJNx Rvy8 rBlt Yjug ADRn"},
    "efdlqjtz": {"url": "https://www.efdlqjtz.v2010.coreserver.jp/", "user": "efdlqjtz", "password": "KCIA cTyz TcdG U1Qs M4pd eezb"},
    "selectadvance": {"url": "https://selectadvance.v2006.coreserver.jp/", "user": "selectadvance", "password": "6HUY g7oZ Gow8 LBCu yzL8 cR3S"},
    "welkenraedt": {"url": "https://www.welkenraedt-online.com/", "user": "welkenraedtonline", "password": "yzn4 6nlm vtrh 8N4v oxHl KUvf"},
    "ncepqvub": {"url": "https://ncepqvub.v2009.coreserver.jp/", "user": "ncepqvub", "password": "DIZy ky10 UAhO NJ47 6Jww ImdE"},
    "kosagi": {"url": "https://www.kosagi.jp/", "user": "kosagi", "password": "K2DZ ERIy aTLb K2Z0 gHi6 XdIN"},
    "selectad01": {"url": "https://selectad01.xsrv.jp/", "user": "selectad01", "password": "8LhM laXm pDUx gkjV cg1f EXYr"},
    "thrones": {"url": "https://thrones.v2009.coreserver.jp/", "user": "thrones", "password": "ETvJ VP2F jugd mxXU xJX0 wHVr"},
}

PROJECT_CONFIGS = {
    'biggift': {
        'worksheet': 'ビックギフト向け',
        'platforms': ['blogger', 'livedoor'],
        'max_posts': {'blogger': 20, 'livedoor': 15},
        'needs_k_column': True
    },
    'arigataya': {
        'worksheet': 'ありがた屋向け',
        'platforms': ['seesaa', 'fc2'],
        'max_posts': {'seesaa': 20, 'fc2': 20},
        'needs_k_column': True
    },
    'kaitori_life': {
        'worksheet': '買取LIFE向け',
        'platforms': ['wordpress'],
        'wp_sites': ['selectad01', 'thrones'],
        'max_posts': {'wordpress': 20},
        'needs_k_column': False
    },
    'osaifu_rescue': {
        'worksheet': 'お財布レスキュー向け',
        'platforms': ['wordpress'],
        'wp_sites': ['ykikaku', 'efdlqjtz'],
        'max_posts': {'wordpress': 20},
        'needs_k_column': False
    },
    'kure_kaeru': {
        'worksheet': 'クレかえる向け',
        'platforms': ['wordpress'],
        'wp_sites': ['selectadvance', 'welkenraedt'],
        'max_posts': {'wordpress': 20},
        'needs_k_column': False
    },
    'red_site': {
        'worksheet': '赤いサイト向け',
        'platforms': ['wordpress'],
        'wp_sites': ['ncepqvub', 'kosagi'],
        'max_posts': {'wordpress': 20},
        'needs_k_column': False
    }
}

MIN_INTERVAL = 30
MAX_INTERVAL = 60


# ============================================================
# 5) ステート / 通知＆ログ
# ============================================================
for key, default in [
    ('gemini_key_index', 0),
    ('posting_projects', set()),
    ('current_project', None),
    ('realtime_logs', {}),
    ('all_posted_urls', {}),
    ('completion_results', {}),
    ('persistent_notifications', []),
    ('notification_counter', 0),
]:
    if key not in st.session_state:
        st.session_state[key] = default


def add_notification(message, notification_type="info", project_key=None):
    st.session_state.notification_counter += 1
    notification = {
        'id': st.session_state.notification_counter,
        'timestamp': datetime.now().strftime("%H:%M:%S"),
        'message': message,
        'type': notification_type,
        'project_key': project_key,
        'created_at': datetime.now()
    }
    st.session_state.persistent_notifications.append(notification)
    if len(st.session_state.persistent_notifications) > 40:
        st.session_state.persistent_notifications = st.session_state.persistent_notifications[-30:]


def show_notifications():
    if not st.session_state.persistent_notifications:
        return
    st.markdown("### 📢 通知")
    for n in st.session_state.persistent_notifications[-6:][::-1]:
        icon = "✅" if n['type']=="success" else "❌" if n['type']=="error" else "⚠️" if n['type']=="warning" else "ℹ️"
        project = f"[{n.get('project_key','')}] " if n.get('project_key') else ""
        st.write(f"{icon} **{n['timestamp']}** {project}{n['message']}")


def add_realtime_log(message, project_key):
    s = st.session_state.realtime_logs.setdefault(project_key, [])
    s.append(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
    if len(s) > 60:
        st.session_state.realtime_logs[project_key] = s[-45:]


def add_posted_url(counter, title, url, timestamp, project_key):
    st.session_state.all_posted_urls.setdefault(project_key, []).append(
        {'counter': counter, 'title': title, 'url': url, 'timestamp': timestamp}
    )


# ============================================================
# 6) Google Sheets
# ============================================================
@st.cache_resource
def get_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        if "gcp" in st.secrets:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(dict(st.secrets["gcp"]), scope)
            return gspread.authorize(creds)
    except Exception as e:
        add_notification(f"Google認証エラー: {e}", "error")
        st.stop()
    add_notification("Secrets[gcp] がありません。", "error")
    st.stop()


@st.cache_data(ttl=60)
def load_sheet_data(project_key):
    try:
        cfg = PROJECT_CONFIGS[project_key]
        sh = get_sheets_client().open_by_key(SHEET_ID).worksheet(cfg['worksheet'])
        rows = sh.get_all_values()
        if len(rows) <= 1:
            return pd.DataFrame()

        headers = rows[0]
        data = rows[1:]

        clean = []
        for i, h in enumerate(headers):
            c = h.replace('\n', '').replace('\r', '').replace('（', '').replace('）', '').replace('(', '').replace(')', '').strip()
            if 'テーマ' in h: c = 'テーマ'
            elif '宣伝URL' in h or 'URL' in h: c = '宣伝URL'
            elif '投稿先' in h: c = '投稿先'
            elif 'アンカー' in h: c = 'アンカーテキスト'
            elif 'ステータス' in h: c = 'ステータス'
            elif '投稿URL' in h: c = '投稿URL'
            elif 'カウンター' in h: c = 'カウンター'
            elif 'カテゴリー' in h: c = 'カテゴリー'
            elif 'パーマリンク' in h: c = 'パーマリンク'
            elif '日付' in h: c = '日付'
            if c in clean:
                c = f"{c}_{i}"
            clean.append(c)

        filtered = []
        for row in data:
            if len(row) >= 5 and row[1] and row[1].strip():
                status = row[4].strip().lower() if len(row) > 4 else ''
                if status in ['', '未処理']:
                    adj = row + [''] * (len(clean) - len(row))
                    filtered.append(adj[:len(clean)])

        if not filtered:
            return pd.DataFrame()

        df = pd.DataFrame(filtered, columns=clean)
        if '選択' not in df.columns:
            df.insert(0, '選択', False)
        return df
    except Exception as e:
        add_notification(f"データ読み込みエラー: {e}", "error")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_competitor_domains():
    try:
        sh = get_sheets_client().open_by_key(SHEET_ID).worksheet('競合他社')
        rows = sh.get_all_values()[1:]
        doms = []
        for r in rows:
            if r and r[0]:
                d = r[0].strip()
                if d.startswith('http'):
                    d = urlparse(d).netloc
                doms.append(d.lower())
        return doms
    except Exception:
        return []


@st.cache_data(ttl=300)
def get_other_links():
    try:
        sh = get_sheets_client().open_by_key(SHEET_ID).worksheet('その他リンク先')
        rows = sh.get_all_values()[1:]
        lst = [{"url": r[0].strip(), "anchor": r[1].strip()} for r in rows if len(r) >= 2 and r[0] and r[1]]
        if not lst:
            lst = [
                {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
                {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"},
            ]
        return lst
    except Exception:
        return [
            {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
            {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"},
        ]


def get_other_link():
    comps = get_competitor_domains()
    avail = []
    for s in get_other_links():
        domain = urlparse(s['url']).netloc.lower()
        if not any(c in domain for c in comps):
            avail.append(s)
    if avail:
        p = random.choice(avail)
        return p['url'], p['anchor']
    return None, None


# ============================================================
# 7) Gemini記事生成
# ============================================================
def _get_gemini_key():
    i = st.session_state.get('gemini_key_index', 0)
    k = GEMINI_API_KEYS[i % len(GEMINI_API_KEYS)]
    st.session_state['gemini_key_index'] = i + 1
    return k


def call_gemini(prompt: str) -> str:
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent?key={_get_gemini_key()}"
    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.7}}
    r = requests.post(endpoint, json=payload, timeout=60)
    if r.status_code != 200:
        raise Exception(f"Gemini API エラー: {r.status_code} - {r.text[:200]}")
    j = r.json()
    return j['candidates'][0]['content']['parts'][0]['text']


def generate_article_with_link(theme: str, url: str, anchor_text: str) -> dict:
    auto = False
    if not theme or theme.strip() == "":
        theme = "金融・投資・資産運用"
        auto = True
    inst = "金融系（投資、クレジットカード、ローン、資産運用など）から自由に" if auto else f"「{theme}」をテーマに"
    prompt = f"""
# 命令書:
{inst}、読者に価値のある記事を作成してください。

# 記事に含めるリンク（1つのみ）:
URL: {url}
アンカーテキスト: {anchor_text}

# 出力形式:
・最初の行に魅力的なタイトルを出力（タグなし）
・その後、HTML形式で本文作成
・リンクは本文中に1回のみ自然に挿入

# HTML記法:
・見出し: <h2>, <h3>（H1禁止）
・段落: <p>（空段落は禁止）
・リンク: <a href="URL" target="_blank" rel="noopener noreferrer">アンカーテキスト</a>
・リスト: <ul><li>

# 要件:
・2000-2500文字
・具体例や数値を適度に
・初心者にもわかりやすく
・プレースホルダー（〇〇等）禁止
"""
    res = call_gemini(prompt).strip().split('\n')
    title = res[0].strip()
    content = '\n'.join(res[1:]).strip()
    content = re.sub(r'〇〇|××|△△', '', content)
    content = re.sub(r'（ここで.*?）', '', content)
    content = re.sub(r'<p>\s*</p>', '', content)
    content = enforce_anchor_attrs(content)
    return {"title": title, "content": content, "theme": (theme if not auto else "金融")}


# ============================================================
# 8) プラットフォーム投稿
# ============================================================
def get_category_id(site_config, category_name):
    if not category_name:
        return None
    try:
        r = requests.get(f"{site_config['url']}wp-json/wp/v2/categories", timeout=30)
        if r.status_code == 200:
            for c in r.json():
                if c.get('name') == category_name:
                    return c.get('id')
    except Exception:
        pass
    return None


def post_to_wordpress(article: dict, site_key: str, category_name: str = None,
                      schedule_dt: datetime | None = None, enable_eyecatch: bool = True,
                      project_key: str = None) -> str:
    """
    WordPress投稿
      - kosagi: XML-RPC(metaWeblog) 即時公開（WAF対策のため最低限フィールド、アイキャッチ未対応）
                予約指定がある場合はアプリ側で待機→即時
      - 他: REST で下書き→公開/予約、アイキャッチアップロード＆設定
    """
    if site_key not in WP_CONFIGS:
        add_notification(f"不明なサイト: {site_key}", "error", project_key)
        return ""
    site = WP_CONFIGS[site_key]
    base = normalize_base_url(site['url'])
    add_notification(f"ベースURL: {base}", "info", project_key)

    # -----------------------------------------
    # kosagi: XML-RPC（metaWeblog）: newPost→(可能なら)カテゴリ
    # -----------------------------------------
    if site_key == 'kosagi':
        # 予約 → 待機してから即時公開
        if schedule_dt and schedule_dt > datetime.now():
            wait = max(0, int((schedule_dt - datetime.now()).total_seconds()))
            add_notification(f"kosagi待機: {wait}秒", "info", project_key)
            if wait > 0:
                pb = st.progress(0)
                for i in range(wait):
                    pb.progress((i + 1) / wait)
                    time.sleep(1)

        try:
            server = xmlrpc.client.ServerProxy(f"{base}xmlrpc.php", allow_none=True)
            content_struct = {
                'title': article['title'],
                'description': article['content'],
            }
            # 即時公開 = publish=True
            post_id = server.metaWeblog.newPost(0, site['user'], site['password'], content_struct, True)

            # カテゴリー設定（任意）
            if category_name:
                try:
                    cats = server.mt.getCategoryList(0, site['user'], site['password'])
                    target = None
                    for c in cats:
                        if c.get("categoryName") == category_name:
                            target = c
                            break
                    if target:
                        server.mt.setPostCategories(
                            post_id, site['user'], site['password'],
                            [{"categoryId": target.get("categoryId"), "isPrimary": True}]
                        )
                except Exception:
                    pass

            # URL取得
            try:
                post = server.metaWeblog.getPost(post_id, site['user'], site['password'])
                post_url = post.get("permalink") or post.get("link") or f"{base}?p={post_id}"
            except Exception:
                post_url = f"{base}?p={post_id}"

            add_notification(f"kosagi投稿成功: {post_url}", "success", project_key)
            return post_url

        except xmlrpc.client.Fault as f:
            add_notification(f"kosagi XMLRPC Fault: {f.faultString}", "error", project_key)
            return ""
        except Exception as e:
            add_notification(f"kosagi XMLRPC投稿エラー: {e}", "error", project_key)
            return ""

    # -----------------------------------------
    # 通常WP: REST（下書き→公開/予約）＋アイキャッチ
    # -----------------------------------------
    endpoint = f"{base}wp-json/wp/v2/posts"
    slug = generate_slug_from_title(article['title'])
    add_notification(f"REST API: {endpoint} / スラッグ: {slug}", "info", project_key)

    data = {'title': article['title'], 'content': article['content'], 'status': 'draft', 'slug': slug}

    cat_id = get_category_id(site, category_name) if category_name else None
    if cat_id:
        data['categories'] = [cat_id]
        add_notification(f"カテゴリー: {category_name} (ID: {cat_id})", "info", project_key)

    # アイキャッチ
    if enable_eyecatch:
        try:
            img = create_eyecatch_image(article['title'], site_key)
            mid = upload_image_to_wordpress(
                img, f"{slug}.jpg", site_config=site,
                log=lambda m: add_notification(m, "info", project_key)
            )
            if mid:
                data['featured_media'] = mid
        except Exception as e:
            add_notification(f"アイキャッチ処理エラー: {e}", "warning", project_key)

    try:
        # Step1: 下書き作成
        r = requests.post(endpoint, auth=HTTPBasicAuth(site['user'], site['password']),
                          headers={'Content-Type': 'application/json'}, data=json.dumps(data), timeout=60)
        if r.status_code not in (200, 201):
            try:
                msg = r.json().get('message', 'Unknown')
            except Exception:
                msg = r.text[:300]
            add_notification(f"{site_key} 下書き失敗 {r.status_code}: {msg}", "error", project_key)
            return ""

        post_id = r.json()['id']
        add_notification(f"下書き作成成功 (ID:{post_id})", "success", project_key)

        # Step2: 公開/予約
        upd = f"{base}wp-json/wp/v2/posts/{post_id}"
        upd_data = {'slug': slug}
        if schedule_dt and schedule_dt > datetime.now():
            upd_data['status'] = 'future'
            upd_data['date'] = schedule_dt.strftime('%Y-%m-%dT%H:%M:%S')
            add_notification(f"予約設定: {upd_data['date']}", "info", project_key)
        else:
            upd_data['status'] = 'publish'

        ur = requests.post(
            upd, auth=HTTPBasicAuth(site['user'], site['password']),
            headers={'Content-Type': 'application/json'}, data=json.dumps(upd_data), timeout=60
        )
        if ur.status_code not in (200, 201):
            add_notification(f"公開/予約失敗 {ur.status_code}: {ur.text[:200]}", "error", project_key)
            return ""

        link = ur.json().get('link', '') or f"{base}{slug}/"
        add_notification(
            ("予約投稿成功" if upd_data['status'] == 'future' else "投稿成功") + f" ({site_key}): {link}",
            "success", project_key
        )
        return link

    except Exception as e:
        add_notification(f"{site_key} 投稿エラー: {e}", "error", project_key)
        return ""


# ============================================================
# 9) 投稿ロジック
# ============================================================
def get_max_posts_for_project(project_key, _=""):
    cfg = PROJECT_CONFIGS[project_key]['max_posts']
    if isinstance(cfg, dict):
        return cfg.get('wordpress', 20)
    return cfg


def execute_post(row, project_key, post_count=1, schedule_times=None, enable_eyecatch=True):
    """
    1行のデータに対して post_count 回記事投稿
      - 1〜19件: その他リンク
      - 20件目: 宣伝URL
      - WP予約: REST→future
      - kosagi予約: アプリ側で待機→即時公開
    """
    try:
        st.session_state.posting_projects.add(project_key)
        add_realtime_log(f"📋 {PROJECT_CONFIGS[project_key]['worksheet']} の投稿開始", project_key)
        add_notification(f"{PROJECT_CONFIGS[project_key]['worksheet']} の投稿開始", "info", project_key)

        cfg = PROJECT_CONFIGS[project_key]
        schedule_times = schedule_times or []

        # カウンター
        current = 0
        cv = row.get('カウンター', '') or row.get('カウンタ', '') or ''
        try:
            current = int(str(cv).strip()) if cv else 0
        except Exception:
            current = 0
        add_realtime_log(f"📊 現在カウンター: {current}", project_key)

        # 投稿先
        post_target = (row.get('投稿先', '') or '').strip().lower()
        add_notification(f"投稿先指定: '{post_target}'", "info", project_key)
        if not post_target:
            add_notification("投稿先が空白です", "error", project_key)
            st.session_state.posting_projects.discard(project_key)
            return False

        if 'wordpress' in cfg['platforms']:
            if post_target not in cfg.get('wp_sites', []):
                add_notification(
                    f"投稿先 '{post_target}' はこのプロジェクト({project_key})に未登録。利用可能: {', '.join(cfg.get('wp_sites', []))}",
                    "error", project_key
                )
                st.session_state.posting_projects.discard(project_key)
                return False
        else:
            add_notification("未対応プラットフォームです", "error", project_key)
            st.session_state.posting_projects.discard(project_key)
            return False

        max_posts = get_max_posts_for_project(project_key)
        if current >= max_posts:
            add_notification(f"既に{max_posts}記事完了", "warning", project_key)
            st.session_state.posting_projects.discard(project_key)
            return False

        progress_bar = st.progress(0)
        done = 0

        for i in range(post_count):
            if current >= max_posts:
                add_notification(f"{max_posts}記事到達", "warning", project_key)
                break

            schedule_dt = schedule_times[i] if i < len(schedule_times) else None

            with st.expander(f"記事{i + 1}/{post_count}", expanded=True):
                # 20件目は宣伝URL
                if current == max_posts - 1:
                    url = row.get('宣伝URL', '') or ''
                    anchor = row.get('アンカーテキスト', '') or row.get('アンカー', '') or project_key
                    category = row.get('カテゴリー', '') or 'お金のマメ知識'
                    st.info(f"{max_posts}記事目 → 宣伝URL使用")
                else:
                    url, anchor = get_other_link()
                    if not url:
                        add_notification("その他リンク取得失敗", "error", project_key)
                        break
                    category = row.get('カテゴリー', '') or 'お金のマメ知識'
                    st.info(f"{current + 1}記事目 → その他リンク使用")

                theme = row.get('テーマ', '') or '金融・投資・資産運用'
                st.write("🧠 記事を生成中…")
                article = generate_article_with_link(theme, url, anchor)
                st.success(f"タイトル: {article['title']}")
                st.info(f"使用リンク: {anchor}")

                # kosagi のアイキャッチは強制OFF
                enable_eye = enable_eyecatch and (post_target != 'kosagi')
                post_url = post_to_wordpress(article, post_target, category, schedule_dt, enable_eye, project_key)
                if not post_url:
                    add_notification("投稿に失敗", "error", project_key)
                    break

                ts = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                add_posted_url(current + 1, article['title'], post_url, ts, project_key)

                # カウンター更新（G列=7）
                try:
                    cli = get_sheets_client()
                    sh = cli.open_by_key(SHEET_ID).worksheet(cfg['worksheet'])
                    allrows = sh.get_all_values()
                    promo = row.get('宣伝URL', '') or ''
                    for rix, rrow in enumerate(allrows[1:], start=2):
                        if len(rrow) > 1 and rrow[1] == promo:
                            current += 1
                            sh.update_cell(rix, 7, str(current)); time.sleep(0.25)
                            if current >= max_posts:
                                # E列: ステータス
                                sh.update_cell(rix, 5, "処理済み"); time.sleep(0.25)
                                # F列: 投稿URL（20本目）
                                finals = [it['url'] for it in st.session_state.all_posted_urls[project_key] if it['counter'] == max_posts]
                                sh.update_cell(rix, 6, ', '.join(finals)); time.sleep(0.25)
                                # I列: 完了日時
                                sh.update_cell(rix, 9, datetime.now().strftime("%Y/%m/%d %H:%M")); time.sleep(0.25)
                                add_notification(f"{max_posts}記事完了", "success", project_key)
                            else:
                                add_notification(f"カウンター更新: {current}/{max_posts}", "success", project_key)
                            break
                except Exception as e:
                    add_notification(f"シート更新エラー: {e}", "warning", project_key)

                done += 1
                progress_bar.progress(done / post_count)

                # 次記事待機
                if current < max_posts and i < post_count - 1:
                    wt = random.randint(MIN_INTERVAL, MAX_INTERVAL)
                    st.info(f"次の記事まで{wt}秒待機…")
                    time.sleep(wt)

        st.session_state.posting_projects.discard(project_key)
        add_notification(f"{done}記事の投稿が完了", "success", project_key)
        return True

    except Exception as e:
        st.session_state.posting_projects.discard(project_key)
        add_notification(f"投稿処理エラー: {e}", "error", project_key)
        return False


# ============================================================
# 10) UI
# ============================================================
st.set_page_config(page_title="統合ブログ投稿ツール", page_icon="🚀", layout="wide")

st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.4rem 1.2rem;
        border-radius: 10px;
        text-align: center;
        margin-bottom: 1.2rem;
    }
    .stButton > button {
        background: linear-gradient(135deg, #4CAF50, #66BB6A);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.6rem 1.8rem;
        font-weight: bold;
        font-size: 16px;
    }
    .stButton > button:hover {
        background: linear-gradient(135deg, #66BB6A, #4CAF50);
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
  <h1>統合ブログ投稿管理システム</h1>
  <p>WordPress / Seesaa / FC2 / livedoor / Blogger（必要に応じて）</p>
</div>
""", unsafe_allow_html=True)

show_notifications()

# 完了プロジェクト表示
if st.session_state.completion_results:
    st.markdown("## 🎉 投稿完了プロジェクト")
    for proj_key, result in st.session_state.completion_results.items():
        with st.expander(f"✅ {result['project_name']} - 完了: {result['completed_at']}", expanded=False):
            st.markdown(f"**投稿完了**: {result['total_posts']}記事")
            st.markdown("### 📋 投稿完了記事一覧")
            for item in result['all_urls']:
                st.write(f"**{item['counter']}記事目**: {item['title']}")
                st.write(f"🔗 [{item['url']}]({item['url']})")
                st.write(f"⏰ {item['timestamp']}")
                st.markdown("---")
            if st.button(f"OK（{result['project_name']}の結果を閉じる）", key=f"close_{proj_key}"):
                del st.session_state.completion_results[proj_key]
                st.rerun()

# プロジェクト選択
project_key = st.selectbox(
    "プロジェクト選択",
    options=list(PROJECT_CONFIGS.keys()),
    format_func=lambda x: f"{PROJECT_CONFIGS[x]['worksheet']} ({', '.join(PROJECT_CONFIGS[x]['platforms'])})",
    key="project_selector"
)

cfg = PROJECT_CONFIGS[project_key]
col1, col2 = st.columns(2)
with col1:
    st.info(f"**プロジェクト**: {cfg['worksheet']}")
    st.info(f"**プラットフォーム**: {', '.join(cfg['platforms'])}")
with col2:
    if cfg['needs_k_column']:
        st.warning("**予約方式**: K列記録 → 外部実行")
    else:
        st.success("**予約方式**: WordPress 予約投稿機能（kosagiは待機→即時）")

# データ読み込み
df = load_sheet_data(project_key)
if df.empty:
    st.info("未処理のデータがありません")
    st.stop()

st.header("データ一覧")
edited_df = st.data_editor(
    df, use_container_width=True, hide_index=True,
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

# 投稿設定
st.header("投稿設定")
colA, colB = st.columns(2)
with colA:
    post_count = st.selectbox("投稿数", options=[1, 2, 3, 4, 5], help="一度に投稿する記事数")
with colB:
    enable_eyecatch = st.checkbox("アイキャッチ画像を自動生成", value=True)

# 予約入力（共通）
enable_schedule = st.checkbox("予約投稿を使用（kosagiは待機後に即時公開）")
schedule_times = []
if enable_schedule:
    st.subheader("予約時刻設定（YYYY-MM-DD HH:MM / HH:MM）")
    lines = [l.strip() for l in st.text_area("予約時刻（1行につき1件）", placeholder="2025-08-20 10:30\n14:00").split('\n') if l.strip()]
    now = datetime.now()
    for s in lines:
        dt = None
        for fmt in ['%Y-%m-%d %H:%M', '%Y/%m/%d %H:%M', '%H:%M']:
            try:
                if fmt == '%H:%M':
                    t = datetime.strptime(s, fmt)
                    dt = now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
                else:
                    dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
        if dt and dt > now:
            schedule_times.append(dt)
        elif dt:
            add_notification(f"過去の時刻は指定不可: {s}", "error")
        else:
            add_notification(f"無効な時刻形式: {s}", "error")
    if schedule_times:
        st.success(f"予約 {len(schedule_times)}件")
        for dt in schedule_times:
            st.write(f"• {dt.strftime('%Y/%m/%d %H:%M')}")

# 実行ボタン
col1, col2 = st.columns(2)
with col1:
    if st.button("投稿実行", type="primary", use_container_width=True):
        sel = edited_df[edited_df['選択'] == True]
        if len(sel) == 0:
            add_notification("投稿する行を選択してください", "error")
        elif len(sel) > 1:
            add_notification("1行のみ選択してください", "error")
        else:
            ok = execute_post(
                sel.iloc[0].to_dict(), project_key,
                post_count=post_count, schedule_times=schedule_times, enable_eyecatch=enable_eyecatch
            )
            if ok:
                time.sleep(1.0); st.cache_data.clear(); st.rerun()
with col2:
    if st.button("データ更新", use_container_width=True):
        st.cache_data.clear()
        add_notification("データを更新しました", "success")
        st.rerun()
