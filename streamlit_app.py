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
import tempfile
import os
import io
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
import base64
import html

# =====================================
# オプション: Blogger ライブラリ有無
# =====================================
try:
    import pickle
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    BLOGGER_AVAILABLE = True
except Exception:
    BLOGGER_AVAILABLE = False


# =====================================
# 共通: 日本語フォント解決（同梱優先）
# =====================================
def _jp_font(size: int) -> ImageFont.FreeTypeFont:
    candidates = [
        Path(__file__).resolve().parent / "fonts" / "NotoSansJP-Bold.ttf",  # 同梱（最優先）
        Path("fonts/NotoSansJP-Bold.ttf"),
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/fonts-japanese-gothic.ttf",
        "C:/Windows/Fonts/meiryob.ttc",
        "C:/Windows/Fonts/meiryo.ttc",
    ]
    for p in candidates:
        try:
            return ImageFont.truetype(str(p), size)
        except Exception:
            pass
    return ImageFont.load_default()


# =====================================
# URL正規化
# =====================================
def normalize_base_url(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    if not re.match(r'^https?://', u):
        u = 'https://' + u
    if not u.endswith('/'):
        u += '/'
    return u


# =====================================
# スラッグ生成（英数字）
# =====================================
def generate_slug_from_title(title: str) -> str:
    keyword_map = {
        '投資': 'investment', '資産': 'asset', '運用': 'management', '増やす': 'increase',
        '貯金': 'savings', '節約': 'saving', 'クレジット': 'credit', 'カード': 'card',
        'ローン': 'loan', '金融': 'finance', '銀行': 'bank', '保険': 'insurance',
        '実践': 'practice', '方法': 'method', '戦略': 'strategy', 'ガイド': 'guide',
        '初心者': 'beginner', '完全': 'complete', '効果': 'effect', '成功': 'success',
        '選び方': 'selection', '比較': 'comparison', '活用': 'utilization', 'おすすめ': 'recommend',
        '基礎': 'basic', '知識': 'knowledge', '対策': 'measures', '解決': 'solution',
        '買取': 'kaitori', '業者': 'company', '先払い': 'sakibarai', '爆速': 'bakusoku',
        '賢く': 'smart', '乗り切る': 'survive', 'お金': 'money', '困らない': 'noworry',
        '金欠': 'shortage', '現金化': 'cash', '即日': 'sameday', '審査': 'screening',
        '申込': 'application', '利用': 'use', '安全': 'safe', '注意': 'caution',
        '危険': 'danger', '詐欺': 'scam', '違法': 'illegal'
    }
    slug_parts = ['money']
    for jp, en in keyword_map.items():
        if jp in title:
            slug_parts.append(en); break
    if len(slug_parts) == 1:
        slug_parts.append('tips')
    date_str = datetime.now().strftime('%m%d')
    random_num = random.randint(100, 999)
    return ('-'.join(slug_parts) + f'-{date_str}-{random_num}').lower()


# =====================================
# アイキャッチ自動生成（サイト別カラー）
# =====================================
def create_eyecatch_image(title: str, site_key: str) -> bytes:
    width, height = 600, 400

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

    scheme = random.choice(schemes)

    img = Image.new('RGB', (width, height), color=scheme['bg'])
    draw = ImageDraw.Draw(img)

    # 簡易縦グラデーション
    br, bg, bb = int(scheme['bg'][1:3], 16), int(scheme['bg'][3:5], 16), int(scheme['bg'][5:7], 16)
    for i in range(height):
        alpha = i / height * 0.3
        draw.rectangle([(0, i), (width, i + 1)],
                       fill=(int(br*(1-alpha)), int(bg*(1-alpha)), int(bb*(1-alpha))))

    # アクセント円
    draw.ellipse([-50, -50, 150, 150], fill=scheme['accent'])
    draw.ellipse([width-100, height-100, width+50, height+50], fill=scheme['accent'])

    # フォント
    title_font = _jp_font(28)

    # タイトル2行化ロジック
    lines = []
    if len(title) > 12:
        for sep in ['！', '？', '…', '!', '?']:
            if sep in title:
                idx = title.find(sep)
                if idx > 0:
                    lines = [title[:idx+1], title[idx+1:].strip()]
                    break
        if not lines:
            for sep in ['と', '、', 'の', 'は', 'が', 'を', 'に', '…', 'で']:
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
        except Exception:
            text_width, _ = draw.textsize(line, font=title_font)
        x = (width - text_width) // 2
        y = y_start + i * 50
        draw.text((x + 2, y + 2), line, font=title_font, fill=(0, 0, 0))
        draw.text((x, y), line, font=title_font, fill=scheme['text'])

    draw.rectangle([50, 40, width-50, 42], fill=scheme['text'])

    bio = io.BytesIO()
    img.save(bio, format='JPEG', quality=90)
    bio.seek(0)
    return bio.getvalue()


# =====================================
# 画像アップロード（multipart 方式）
# =====================================
def upload_image_to_wordpress(image_bytes: bytes, filename: str, site_config: dict, log=None) -> int | None:
    def logp(m): st.write(m) if log is None else log(m)
    endpoint = f'{site_config["url"]}wp-json/wp/v2/media'

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
            logp(f"✅ アイキャッチUP成功: {safe} (ID: {mid})")
            return mid
        else:
            logp(f"⚠️ アイキャッチUP失敗: {r.status_code} / {r.text[:200]}")
            return None
    except Exception as e:
        logp(f"⚠️ アイキャッチUP例外: {e}")
        return None


# =====================================
# aタグ属性統一
# =====================================
def enforce_anchor_attrs(html_text: str) -> str:
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


# =====================================
# WordPress設定（Secretsから）
# =====================================
try:
    SHEET_ID = st.secrets["google"]["spreadsheet_id"]
    GEMINI_API_KEYS = [
        st.secrets["google"]["gemini_api_key_1"],
        st.secrets["google"]["gemini_api_key_2"],
    ]
except KeyError as e:
    st.error(f"Secretsの設定が不足しています: {e}")
    st.stop()

BLOG_ID = st.secrets.get("BLOG_ID", "")
BLOGGER_CREDENTIALS_JSON = st.secrets.get("BLOGGER_CREDENTIALS", "").strip()
BLOGGER_TOKEN_B64 = st.secrets.get("BLOGGER_TOKEN", "").strip()

WP_CONFIGS = {
    "ykikaku": {
        "url": "https://ykikaku.xsrv.jp/",
        "user": "ykikaku",
        "password": "lfXp BJNx Rvy8 rBlt Yjug ADRn"
    },
    "efdlqjtz": {
        "url": "https://www.efdlqjtz.v2010.coreserver.jp/",
        "user": "efdlqjtz",
        "password": "KCIA cTyz TcdG U1Qs M4pd eezb"
    },
    "selectadvance": {
        "url": "https://selectadvance.v2006.coreserver.jp/",
        "user": "selectadvance",
        "password": "6HUY g7oZ Gow8 LBCu yzL8 cR3S"
    },
    "welkenraedt": {
        "url": "https://www.welkenraedt-online.com/",
        "user": "welkenraedtonline",
        "password": "yzn4 6nlm vtrh 8N4v oxHl KUvf"
    },
    "ncepqvub": {
        "url": "https://ncepqvub.v2009.coreserver.jp/",
        "user": "ncepqvub",
        "password": "DIZy ky10 UAhO NJ47 6Jww ImdE"
    },
    "kosagi": {
        "url": "https://www.kosagi.jp/",
        "user": "kosagi",
        "password": "K2DZ ERIy aTLb K2Z0 gHi6 XdIN"
    },
    "selectad01": {
        "url": "https://selectad01.xsrv.jp/",
        "user": "selectad01",
        "password": "8LhM laXm pDUx gkjV cg1f EXYr"
    },
    "thrones": {
        "url": "https://thrones.v2009.coreserver.jp/",
        "user": "thrones",
        "password": "ETvJ VP2F jugd mxXU xJX0 wHVr"
    }
}

PROJECT_CONFIGS = {
    'red_site': {
        'worksheet': '赤いサイト向け',
        'platforms': ['wordpress'],
        'wp_sites': ['ncepqvub', 'kosagi'],
        'max_posts': {'wordpress': 20},
        'needs_k_column': False
    },
    # 必要なら他プロジェクトもここへ（省略）
}

MIN_INTERVAL = 30
MAX_INTERVAL = 60


# =====================================
# 通知ユーティリティ
# =====================================
if 'persistent_notifications' not in st.session_state:
    st.session_state.persistent_notifications = []
if 'notification_counter' not in st.session_state:
    st.session_state.notification_counter = 0
if 'realtime_logs' not in st.session_state:
    st.session_state.realtime_logs = {}
if 'all_posted_urls' not in st.session_state:
    st.session_state.all_posted_urls = {}
if 'completion_results' not in st.session_state:
    st.session_state.completion_results = {}

def add_notification(message, notification_type="info", project_key=None):
    st.session_state.notification_counter += 1
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.persistent_notifications.append({
        'id': st.session_state.notification_counter,
        'timestamp': timestamp,
        'message': message,
        'type': notification_type,
        'project_key': project_key,
        'created_at': datetime.now()
    })
    if len(st.session_state.persistent_notifications) > 30:
        st.session_state.persistent_notifications = st.session_state.persistent_notifications[-25:]

def add_realtime_log(message, project_key):
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_message = f"[{timestamp}] {message}"
    if project_key not in st.session_state.realtime_logs:
        st.session_state.realtime_logs[project_key] = []
    st.session_state.realtime_logs[project_key].append(log_message)
    if len(st.session_state.realtime_logs[project_key]) > 50:
        st.session_state.realtime_logs[project_key] = st.session_state.realtime_logs[project_key][-30:]

def add_posted_url(counter, title, url, timestamp, project_key):
    if project_key not in st.session_state.all_posted_urls:
        st.session_state.all_posted_urls[project_key] = []
    st.session_state.all_posted_urls[project_key].append({
        'counter': counter, 'title': title, 'url': url, 'timestamp': timestamp
    })

def show_notifications():
    if not st.session_state.persistent_notifications:
        return
    st.markdown("### 📢 直近通知")
    for n in st.session_state.persistent_notifications[-5:][::-1]:
        icon = "✅" if n['type']=="success" else "❌" if n['type']=="error" else "⚠️" if n['type']=="warning" else "ℹ️"
        project_text = f"[{n.get('project_key','')}] " if n.get('project_key') else ""
        st.write(f"{icon} **{n['timestamp']}** {project_text}{n['message']}")


# =====================================
# Google Sheets クライアント
# =====================================
@st.cache_resource
def get_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        if "gcp" in st.secrets:
            gcp_info = st.secrets["gcp"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(dict(gcp_info), scope)
            return gspread.authorize(creds)
    except Exception as e:
        add_notification(f"Google認証エラー: {e}", "error")
        st.stop()
    add_notification("Google認証情報が見つかりません（Secrets[gcp]）", "error")
    st.stop()


@st.cache_data(ttl=60)
def load_sheet_data(project_key):
    try:
        config = PROJECT_CONFIGS[project_key]
        client = get_sheets_client()
        sheet = client.open_by_key(SHEET_ID).worksheet(config['worksheet'])
        rows = sheet.get_all_values()
        if len(rows) <= 1:
            return pd.DataFrame()

        headers = rows[0]
        data_rows = rows[1:]

        clean_headers = []
        for i, header in enumerate(headers):
            clean_header = header.replace('\n','').replace('\r','').replace('（','').replace('）','').replace('(', '').replace(')','').strip()
            if 'テーマ' in header: clean_header = 'テーマ'
            elif '宣伝URL' in header or 'URL' in header: clean_header = '宣伝URL'
            elif '投稿先' in header: clean_header = '投稿先'
            elif 'アンカー' in header: clean_header = 'アンカーテキスト'
            elif 'ステータス' in header: clean_header = 'ステータス'
            elif '投稿URL' in header: clean_header = '投稿URL'
            elif 'カウンター' in header: clean_header = 'カウンター'
            elif 'カテゴリー' in header: clean_header = 'カテゴリー'
            elif 'パーマリンク' in header: clean_header = 'パーマリンク'
            elif '日付' in header: clean_header = '日付'
            if clean_header in clean_headers: clean_header = f"{clean_header}_{i}"
            clean_headers.append(clean_header)

        filtered_rows = []
        for row in data_rows:
            if len(row) >= 5 and row[1] and row[1].strip():
                status = row[4].strip().lower() if len(row) > 4 else ''
                if status in ['', '未処理']:
                    adjusted = row + [''] * (len(clean_headers) - len(row))
                    filtered_rows.append(adjusted[:len(clean_headers)])

        if not filtered_rows:
            return pd.DataFrame()

        df = pd.DataFrame(filtered_rows, columns=clean_headers)
        if '選択' not in df.columns:
            df.insert(0, '選択', False)
        return df
    except Exception as e:
        add_notification(f"データ読み込みエラー: {e}", "error")
        return pd.DataFrame()


# =====================================
# リンク候補（その他リンク先）
# =====================================
@st.cache_data(ttl=300)
def get_other_links():
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SHEET_ID).worksheet('その他リンク先')
        rows = sheet.get_all_values()[1:]
        other_sites = []
        for row in rows:
            if len(row) >= 2 and row[0] and row[1]:
                other_sites.append({"url": row[0].strip(), "anchor": row[1].strip()})
        if not other_sites:
            other_sites = [
                {"url":"https://www.fsa.go.jp/","anchor":"金融庁"},
                {"url":"https://www.boj.or.jp/","anchor":"日本銀行"},
            ]
        return other_sites
    except Exception:
        return [
            {"url":"https://www.fsa.go.jp/","anchor":"金融庁"},
            {"url":"https://www.boj.or.jp/","anchor":"日本銀行"},
        ]


@st.cache_data(ttl=300)
def get_competitor_domains():
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SHEET_ID).worksheet('競合他社')
        competitors = sheet.get_all_values()[1:]
        domains = []
        for row in competitors:
            if row and row[0]:
                domain = row[0].strip()
                if domain.startswith('http'):
                    parsed = urlparse(domain)
                    domain = parsed.netloc
                domains.append(domain.lower())
        return domains
    except Exception:
        return []


def get_other_link():
    other_sites = get_other_links()
    comp = get_competitor_domains()
    available = []
    for site in other_sites:
        site_domain = urlparse(site['url']).netloc.lower()
        if not any(c in site_domain for c in comp):
            available.append(site)
    if available:
        site = random.choice(available)
        return site['url'], site['anchor']
    return None, None


# =====================================
# Gemini 記事生成
# =====================================
def _get_gemini_key():
    idx = st.session_state.get('gemini_key_index', 0)
    key = GEMINI_API_KEYS[idx % len(GEMINI_API_KEYS)]
    st.session_state['gemini_key_index'] = idx + 1
    return key

def call_gemini(prompt: str) -> str:
    api_key = _get_gemini_key()
    endpoint = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent?key={api_key}'
    payload = {"contents":[{"parts":[{"text": prompt}]}], "generationConfig":{"temperature":0.7}}
    response = requests.post(endpoint, json=payload, timeout=60)
    if response.status_code != 200:
        raise Exception(f"Gemini API エラー: {response.status_code} - {response.text[:200]}")
    result = response.json()
    return result['candidates'][0]['content']['parts'][0]['text']

def generate_article_with_link(theme: str, url: str, anchor_text: str) -> dict:
    if not theme or theme.strip() == "":
        theme = "金融・投資・資産運用"; auto_theme = True
    else:
        auto_theme = False
    theme_instruction = "金融系（投資、クレジットカード、ローン、資産運用など）から自由にテーマを選んで" if auto_theme else f"「{theme}」をテーマに"
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
    response = call_gemini(prompt)
    lines = response.strip().split('\n')
    title = lines[0].strip()
    content = '\n'.join(lines[1:]).strip()
    content = re.sub(r'〇〇|××|△△', '', content)
    content = re.sub(r'（ここで.*?）', '', content)
    content = re.sub(r'<p>\s*</p>', '', content).strip()
    content = enforce_anchor_attrs(content)
    return {"title": title, "content": content, "theme": theme if not auto_theme else "金融"}


# =====================================
# WordPress 投稿（REST / XML-RPC）
# =====================================
def get_category_id(site_config, category_name):
    if not category_name:
        return None
    try:
        endpoint = f"{site_config['url']}wp-json/wp/v2/categories"
        r = requests.get(endpoint, timeout=30)
        if r.status_code == 200:
            for cat in r.json():
                if cat.get('name') == category_name:
                    return cat.get('id')
    except Exception:
        pass
    return None


def post_to_wordpress(article_data: dict, site_key: str, category_name: str = None,
                      schedule_dt: datetime = None, enable_eyecatch: bool = True,
                      project_key: str = None) -> str:
    if site_key not in WP_CONFIGS:
        add_notification(f"不明なサイト: {site_key}", "error", project_key)
        return ""

    site_config = WP_CONFIGS[site_key]
    base_url = normalize_base_url(site_config['url'])
    add_notification(f"ベースURL: {base_url}", "info", project_key)

    # =========================
    # kosagi: XML-RPC (最小構成)
    # =========================
    if site_key == 'kosagi':
        # 予約はXML-RPC側で不可 → ここで待機（必要時）
        if schedule_dt and schedule_dt > datetime.now():
            wait_seconds = max(0, int((schedule_dt - datetime.now()).total_seconds()))
            add_notification(f"kosagi待機: {wait_seconds}秒", "info", project_key)
            progress_bar = st.progress(0)
            total = max(wait_seconds, 1)
            for i in range(wait_seconds):
                progress_bar.progress((i+1)/total)
                time.sleep(1)
            add_notification("kosagi投稿開始", "success", project_key)

        try:
            endpoint = f"{base_url}xmlrpc.php"
            server = xmlrpc.client.ServerProxy(endpoint, allow_none=True)

            # metaWeblog.newPost(blogid, user, pass, struct, publish)
            content_struct = {
                'title': article_data['title'],
                'description': article_data['content'],   # HTML可
            }
            post_id = server.metaWeblog.newPost(0, site_config['user'], site_config['password'], content_struct, True)

            # permalink取得（安全に）
            try:
                post = server.metaWeblog.getPost(post_id, site_config['user'], site_config['password'])
                post_url = post.get('permalink') or post.get('link') or f"{base_url}?p={post_id}"
            except Exception:
                post_url = f"{base_url}?p={post_id}"

            add_notification(f"kosagi投稿成功: {post_url}", "success", project_key)
            return post_url

        except xmlrpc.client.Fault as fault:
            add_notification(f"kosagi XMLRPC Fault: {fault.faultString}", "error", project_key)
            return ""
        except Exception as e:
            add_notification(f"kosagi XMLRPC投稿エラー: {e}", "error", project_key)
            return ""

    # =========================
    # その他WP: REST + 2段階更新
    # =========================
    endpoint = f"{base_url}wp-json/wp/v2/posts"
    add_notification(f"REST API エンドポイント: {endpoint}", "info", project_key)

    slug = generate_slug_from_title(article_data['title'])
    add_notification(f"生成スラッグ: {slug}", "info", project_key)

    post_data = {
        'title': article_data['title'],
        'content': article_data['content'],
        'status': 'draft',
        'slug': slug
    }

    # カテゴリー
    cat_id = get_category_id(site_config, category_name) if category_name else None
    if cat_id:
        post_data['categories'] = [cat_id]
        add_notification(f"カテゴリー設定: {category_name} (ID: {cat_id})", "info", project_key)

    # アイキャッチ
    if enable_eyecatch:
        try:
            add_notification(f"アイキャッチ生成: {site_key}", "info", project_key)
            eyecatch_data = create_eyecatch_image(article_data['title'], site_key)
            media_id = upload_image_to_wordpress(eyecatch_data, f"{slug}.jpg", site_config,
                                                 log=lambda m: add_notification(m, "info", project_key))
            if media_id:
                post_data['featured_media'] = media_id
            else:
                add_notification("アイキャッチなしで続行", "warning", project_key)
        except Exception as e:
            add_notification(f"アイキャッチ処理エラー: {e}", "warning", project_key)

    try:
        # Step1: 下書き作成
        add_notification(f"{site_key}下書き投稿開始", "info", project_key)
        r = requests.post(endpoint,
                          auth=HTTPBasicAuth(site_config['user'], site_config['password']),
                          headers={'Content-Type':'application/json'},
                          data=json.dumps(post_data), timeout=60)
        if r.status_code not in (200, 201):
            msg = ""
            try: msg = r.json().get('message','Unknown')
            except Exception: msg = r.text[:300]
            add_notification(f"{site_key}投稿失敗: HTTP {r.status_code} - {msg}", "error", project_key)
            return ""

        res = r.json()
        post_id = res['id']
        add_notification(f"下書き作成成功 (ID: {post_id})", "success", project_key)

        # Step2: スラッグ維持したまま公開/予約
        update_endpoint = f"{base_url}wp-json/wp/v2/posts/{post_id}"
        update_data = {'slug': slug}
        if schedule_dt and schedule_dt > datetime.now():
            update_data['status'] = 'future'
            update_data['date'] = schedule_dt.strftime('%Y-%m-%dT%H:%M:%S')
            add_notification(f"予約投稿設定: {update_data['date']}", "info", project_key)
        else:
            update_data['status'] = 'publish'

        ur = requests.post(update_endpoint,
                           auth=HTTPBasicAuth(site_config['user'], site_config['password']),
                           headers={'Content-Type':'application/json'},
                           data=json.dumps(update_data), timeout=60)
        if ur.status_code not in (200, 201):
            add_notification(f"スラッグ更新/公開失敗: HTTP {ur.status_code} - {ur.text[:200]}", "error", project_key)
            return ""

        final = ur.json()
        post_url = final.get('link', '') or f"{base_url}{slug}/"
        if schedule_dt and schedule_dt > datetime.now():
            add_notification(f"予約投稿成功 ({site_key}): {post_url}", "success", project_key)
        else:
            add_notification(f"投稿成功 ({site_key}): {post_url}", "success", project_key)
        return post_url

    except Exception as e:
        add_notification(f"{site_key}投稿エラー: {e}", "error", project_key)
        return ""


# =====================================
# 投稿数上限ユーティリティ
# =====================================
def get_max_posts_for_project(project_key, post_target=""):
    cfg = PROJECT_CONFIGS[project_key]['max_posts']
    if isinstance(cfg, dict):
        key = (post_target or '').strip().lower()
        if key in cfg: return cfg[key]
        if key in ('wordpress',): return cfg.get('wordpress', 20)
        return max(cfg.values()) if cfg else 20
    return cfg


# =====================================
# メイン処理（1行から複数投稿）
# =====================================
def execute_post(row_data, project_key, post_count=1, schedule_times=None, enable_eyecatch=True):
    try:
        if project_key not in st.session_state.realtime_logs:
            st.session_state.realtime_logs[project_key] = []
        if project_key not in st.session_state.all_posted_urls:
            st.session_state.all_posted_urls[project_key] = []

        add_realtime_log(f"📋 {PROJECT_CONFIGS[project_key]['worksheet']} の投稿開始", project_key)
        add_notification(f"{PROJECT_CONFIGS[project_key]['worksheet']} の投稿を開始しました", "info", project_key)

        config = PROJECT_CONFIGS[project_key]
        schedule_times = schedule_times or []

        # カウンター
        current_counter = 0
        counter_value = row_data.get('カウンター', '') or row_data.get('カウンタ', '') or ''
        if counter_value:
            try: current_counter = int(str(counter_value).strip())
            except: current_counter = 0
        add_realtime_log(f"📊 現在のカウンター: {current_counter}", project_key)

        # 投稿先（WPサイト）
        post_target_raw = row_data.get('投稿先', '') or ''
        post_target = post_target_raw.strip().lower()
        add_notification(f"投稿先指定: '{post_target_raw}'", "info", project_key)

        # 上限数
        max_posts = get_max_posts_for_project(project_key, 'wordpress')
        if current_counter >= max_posts:
            add_notification(f"既に{max_posts}記事完了しています", "warning", project_key)
            return False

        progress_bar = st.progress(0)
        posts_completed = 0

        for i in range(post_count):
            if current_counter >= max_posts:
                add_notification(f"カウンター{current_counter}: 既に{max_posts}記事完了", "warning", project_key)
                break

            schedule_dt = schedule_times[i] if i < len(schedule_times) else None

            with st.expander(f"記事{i+1}/{post_count}の投稿", expanded=True):
                # リンク決定
                if current_counter == max_posts - 1:
                    url = row_data.get('宣伝URL', '') or ''
                    anchor = row_data.get('アンカーテキスト', '') or row_data.get('アンカー', '') or project_key
                    category = row_data.get('カテゴリー', '') or 'お金のマメ知識'
                    st.info(f"{max_posts}記事目 → 宣伝URL使用")
                else:
                    url, anchor = get_other_link()
                    if not url:
                        add_notification("その他リンクが取得できません", "error", project_key)
                        break
                    category = row_data.get('カテゴリー', '') or 'お金のマメ知識'
                    st.info(f"{current_counter + 1}記事目 → その他リンク使用")

                # 記事生成
                st.write("🧠 記事生成中…")
                theme = row_data.get('テーマ', '') or '金融・投資・資産運用'
                article = generate_article_with_link(theme, url, anchor)
                st.success(f"タイトル: {article['title']}")

                # ルーティング
                posted_urls = []
                if 'wordpress' in config['platforms']:
                    wp_sites = config.get('wp_sites', [])
                    if not post_target:
                        add_notification("投稿先が空白です。投稿先サイトを指定してください", "error", project_key)
                        break
                    if post_target in wp_sites:
                        st.write(f"📤 WordPress({post_target})に投稿")
                        post_url = post_to_wordpress(article, post_target, category, schedule_dt, enable_eyecatch and (post_target!='kosagi'), project_key)
                        if post_url:
                            posted_urls.append(post_url)
                    else:
                        add_notification(f"投稿先 '{post_target}' はこのプロジェクトに未登録。利用可能: {', '.join(wp_sites)}", "error", project_key)
                        break
                else:
                    add_notification("未対応のプラットフォーム", "error", project_key)
                    break

                if not posted_urls:
                    add_notification("投稿に失敗しました", "error", project_key)
                    break

                timestamp = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                for url_item in posted_urls:
                    add_posted_url(current_counter + 1, article['title'], url_item, timestamp, project_key)

                # カウンター更新（G列=7）
                try:
                    client = get_sheets_client()
                    sheet = client.open_by_key(SHEET_ID).worksheet(PROJECT_CONFIGS[project_key]['worksheet'])
                    all_rows = sheet.get_all_values()
                    promo_url = row_data.get('宣伝URL', '') or ''
                    for row_idx, row in enumerate(all_rows[1:], start=2):
                        if len(row) > 1 and row[1] == promo_url:
                            current_counter += 1
                            sheet.update_cell(row_idx, 7, str(current_counter)); time.sleep(0.3)
                            if current_counter >= max_posts:
                                # E列=5: ステータス、F列=6: 投稿URL、I列=9: 完了日時
                                sheet.update_cell(row_idx, 5, "処理済み"); time.sleep(0.3)
                                finals = [it['url'] for it in st.session_state.all_posted_urls[project_key] if it['counter'] == max_posts]
                                sheet.update_cell(row_idx, 6, ', '.join(finals)); time.sleep(0.3)
                                completion_time = datetime.now().strftime("%Y/%m/%d %H:%M")
                                sheet.update_cell(row_idx, 9, completion_time); time.sleep(0.3)
                                add_notification(f"{max_posts}記事完了！", "success", project_key)
                            break
                except Exception as e:
                    add_notification(f"スプレッドシート更新エラー: {e}", "warning", project_key)

                posts_completed += 1
                progress_bar.progress(posts_completed / post_count)

                if current_counter < max_posts and i < post_count - 1:
                    wait_time = random.randint(MIN_INTERVAL, MAX_INTERVAL)
                    st.info(f"次の記事まで{wait_time}秒待機中…")
                    time.sleep(wait_time)

        add_notification(f"{posts_completed}記事の投稿が完了しました", "success", project_key)
        return True

    except Exception as e:
        add_notification(f"投稿処理エラー: {e}", "error", project_key)
        return False


# =====================================
# UI
# =====================================
st.set_page_config(page_title="統合ブログ投稿ツール", page_icon="🚀", layout="wide")

st.markdown("""
<style>
    .stButton > button { background: linear-gradient(135deg, #4CAF50, #66BB6A); color: white; border: none; border-radius: 8px; padding: 0.6rem 1.2rem; font-weight: bold; }
    .stButton > button:hover { background: linear-gradient(135deg, #66BB6A, #4CAF50); }
</style>
""", unsafe_allow_html=True)

show_notifications()

st.markdown("## 統合ブログ投稿管理（WordPress/赤サイト対応）")

project_key = st.selectbox(
    "プロジェクト選択",
    options=list(PROJECT_CONFIGS.keys()),
    format_func=lambda x: f"{PROJECT_CONFIGS[x]['worksheet']} ({', '.join(PROJECT_CONFIGS[x]['platforms'])})",
)

config = PROJECT_CONFIGS[project_key]
st.info(f"**プロジェクト**: {config['worksheet']} / **プラットフォーム**: {', '.join(config['platforms'])}")

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

st.header("投稿設定")
col1, col2 = st.columns(2)
with col1:
    post_count = st.selectbox("投稿数", options=[1,2,3,4,5], help="一度に投稿する記事数")
with col2:
    enable_eyecatch = st.checkbox("アイキャッチ画像を自動生成", value=True)

# 予約（WPのみ、有効サイトはREST側のみ。kosagiは内部で待機→即時）
enable_schedule = st.checkbox("予約投稿を使用する（kosagiは待機後に即時公開）")
schedule_times = []
if enable_schedule:
    st.subheader("予約時刻（YYYY-MM-DD HH:MM / HH:MM）")
    schedule_input = st.text_area("1行につき1件", placeholder="2025-08-20 10:30\n14:00")
    if schedule_input:
        lines = [l.strip() for l in schedule_input.split('\n') if l.strip()]
        now = datetime.now()
        for line in lines:
            dt = None
            for fmt in ['%Y-%m-%d %H:%M', '%Y/%m/%d %H:%M', '%H:%M']:
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
                add_notification(f"過去の時刻は指定不可: {line}", "error")
            else:
                add_notification(f"無効な時刻形式: {line}", "error")
        if schedule_times:
            st.success(f"予約時刻 {len(schedule_times)}件を設定")
            for dt in schedule_times: st.write(f"• {dt.strftime('%Y/%m/%d %H:%M')}")

col_a, col_b = st.columns(2)
with col_a:
    if st.button("投稿実行", type="primary", use_container_width=True):
        selected_rows = edited_df[edited_df['選択'] == True]
        if len(selected_rows) == 0:
            add_notification("投稿する行を選択してください", "error")
        elif len(selected_rows) > 1:
            add_notification("1行のみ選択してください", "error")
        else:
            row = selected_rows.iloc[0]
            success = execute_post(
                row.to_dict(), project_key,
                post_count=post_count, schedule_times=schedule_times,
                enable_eyecatch=enable_eyecatch
            )
            if success:
                time.sleep(1.2)
                st.cache_data.clear()
                st.rerun()
with col_b:
    if st.button("データ更新", use_container_width=True):
        st.cache_data.clear()
        add_notification("データを更新しました", "success")
        st.rerun()

    st.markdown("---")
    col_info1, col_info2, col_info3 = st.columns(3)
    with col_info1: st.metric("未処理件数", len(df))
    with col_info2: st.metric("プラットフォーム", len(config['platforms']))
    with col_info3: st.metric("最終更新", datetime.now().strftime("%H:%M:%S"))

if __name__ == "__main__":
    main()









