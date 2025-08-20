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
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape as xml_escape
import io
from PIL import Image, ImageDraw, ImageFont
import base64

# ==============
# 可用ライブラリ検出（Blogger系）
# ==============
try:
    import pickle
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    BLOGGER_AVAILABLE = True
except Exception:
    BLOGGER_AVAILABLE = False


# URL正規化ユーティリティ
def normalize_base_url(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    # スキーム付与
    if not re.match(r'^https?://', u):
        u = 'https://' + u
    # 末尾スラッシュ付与
    if not u.endswith('/'):
        u += '/'
    return u


# ========================
# ページ設定
# ========================
st.set_page_config(page_title="統合ブログ投稿ツール", page_icon="🚀", layout="wide")

st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem;
        border-radius: 10px;
        text-align: center;
        margin-bottom: 2rem;
    }
    .stButton > button {
        background: linear-gradient(135deg, #4CAF50, #66BB6A);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.8rem 2rem;
        font-weight: bold;
        font-size: 16px;
    }
    .stButton > button:hover {
        background: linear-gradient(135deg, #66BB6A, #4CAF50);
    }
    .warning-box { background: #fff3cd; border: 1px solid #ffc107; color: #856404; padding: 1rem; border-radius: 8px; margin: 1rem 0; }
    .success-box { background: #d4edda; border: 1px solid #28a745; color: #155724; padding: 1rem; border-radius: 8px; margin: 1rem 0; }
    .error-box { background: #f8d7da; border: 1px solid #dc3545; color: #721c24; padding: 1rem; border-radius: 8px; margin: 1rem 0; }
    .notification-container { position: sticky; top: 0; z-index: 1000; background: white; padding: 1rem; border-bottom: 1px solid #ddd; margin-bottom: 1rem; }
</style>
""", unsafe_allow_html=True)

# ========================
# Secrets 読み込み
# ========================
try:
    SHEET_ID = st.secrets["google"]["spreadsheet_id"]
    GEMINI_API_KEYS = [
        st.secrets["google"]["gemini_api_key_1"],
        st.secrets["google"]["gemini_api_key_2"],
    ]
except KeyError as e:
    st.error(f"Secretsの設定が不足しています: {e}")
    st.stop()

# Blogger Secrets
BLOG_ID = st.secrets.get("BLOG_ID", "")
BLOGGER_CREDENTIALS_JSON = st.secrets.get("BLOGGER_CREDENTIALS", "").strip()
BLOGGER_TOKEN_B64 = st.secrets.get("BLOGGER_TOKEN", "").strip()

# プラットフォーム（全て Secrets から読む）
PLATFORM_CONFIGS = {
    "livedoor": {
        "blog_name": st.secrets.get("LIVEDOOR_BLOG_NAME", ""),
        "user_id":   st.secrets.get("LIVEDOOR_ID", ""),
        "api_key":   st.secrets.get("LIVEDOOR_API_KEY", "")
    },
    "seesaa": {
        "endpoint": "http://blog.seesaa.jp/rpc",
        "username": st.secrets.get("SEESAA_USERNAME", ""),
        "password": st.secrets.get("SEESAA_PASSWORD", ""),
        "blogid":   st.secrets.get("SEESAA_BLOGID", "")
    },
    "fc2": {
        "endpoint": "https://blog.fc2.com/xmlrpc.php",
        "blog_id":  st.secrets.get("FC2_BLOG_ID", ""),
        "username": st.secrets.get("FC2_USERNAME", ""),
        "password": st.secrets.get("FC2_PASSWORD", "")
    },
    "blogger": {
        "blog_id": BLOG_ID
    }
}

# WordPressサイト（Secrets から構築）
WP_CONFIGS = {
    "ykikaku": {
        "url":      st.secrets.get("WP_YKIKAKU_URL", ""),
        "user":     st.secrets.get("WP_YKIKAKU_USER", ""),
        "password": st.secrets.get("WP_YKIKAKU_PASSWORD", "")
    },
    "efdlqjtz": {
        "url":      st.secrets.get("WP_EFDLQJTZ_URL", ""),
        "user":     st.secrets.get("WP_EFDLQJTZ_USER", ""),
        "password": st.secrets.get("WP_EFDLQJTZ_PASSWORD", "")
    },
    "selectadvance": {
        "url":      st.secrets.get("WP_SELECTADVANCE_URL", ""),
        "user":     st.secrets.get("WP_SELECTADVANCE_USER", ""),
        "password": st.secrets.get("WP_SELECTADVANCE_PASSWORD", "")
    },
    "welkenraedt": {
        "url":      st.secrets.get("WP_WELKENRAEDT_URL", ""),
        "user":     st.secrets.get("WP_WELKENRAEDT_USER", ""),
        "password": st.secrets.get("WP_WELKENRAEDT_PASSWORD", "")
    },
    "ncepqvub": {
        "url":      st.secrets.get("WP_NCEPQVUB_URL", ""),
        "user":     st.secrets.get("WP_NCEPQVUB_USER", ""),
        "password": st.secrets.get("WP_NCEPQVUB_PASSWORD", "")
    },
    "kosagi": {
        "url":      st.secrets.get("WP_KOSAGI_URL", ""),
        "user":     st.secrets.get("WP_KOSAGI_USER", ""),
        "password": st.secrets.get("WP_KOSAGI_PASSWORD", "")
    },
    "selectad01": {
        "url":      st.secrets.get("WP_SELECTAD_URL", ""),
        "user":     st.secrets.get("WP_SELECTAD_USER", ""),
        "password": st.secrets.get("WP_SELECTAD_PASSWORD", "")
    },
    "thrones": {
        "url":      st.secrets.get("WP_THRONES_URL", ""),
        "user":     st.secrets.get("WP_THRONES_USER", ""),
        "password": st.secrets.get("WP_THRONES_PASSWORD", "")
    }
}

# プロジェクト設定
PROJECT_CONFIGS = {
    'biggift': {
        'worksheet': 'ビックギフト向け',
        'platforms': ['blogger', 'livedoor'],  # C列の投稿先に依存
        # プラットフォーム別の最大件数
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

# 投稿間隔（スパム対策）
MIN_INTERVAL = 30
MAX_INTERVAL = 60

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
    st.session_state.realtime_logs = {}
if 'all_posted_urls' not in st.session_state:
    st.session_state.all_posted_urls = {}
if 'completion_results' not in st.session_state:
    st.session_state.completion_results = {}
if 'persistent_notifications' not in st.session_state:
    st.session_state.persistent_notifications = []
if 'notification_counter' not in st.session_state:
    st.session_state.notification_counter = 0

# ========================
# 通知ユーティリティ
# ========================
def add_notification(message, notification_type="info", project_key=None):
    st.session_state.notification_counter += 1
    timestamp = datetime.now().strftime("%H:%M:%S")
    notification = {
        'id': st.session_state.notification_counter,
        'timestamp': timestamp,
        'message': message,
        'type': notification_type,
        'project_key': project_key,
        'created_at': datetime.now()
    }
    st.session_state.persistent_notifications.append(notification)
    if len(st.session_state.persistent_notifications) > 30:
        st.session_state.persistent_notifications = st.session_state.persistent_notifications[-25:]

def show_notifications():
    if not st.session_state.persistent_notifications:
        return
    st.markdown('<div class="notification-container">', unsafe_allow_html=True)
    st.markdown("### 📢 通知一覧")
    recent_notifications = st.session_state.persistent_notifications[-5:]
    for notification in reversed(recent_notifications):
        timestamp = notification['timestamp']
        message = notification['message']
        ntype = notification['type']
        project = notification.get('project_key', '')
        if ntype == "success":
            icon = "✅"; css_class = "success-box"
        elif ntype == "error":
            icon = "❌"; css_class = "error-box"
        elif ntype == "warning":
            icon = "⚠️"; css_class = "warning-box"
        else:
            icon = "ℹ️"; css_class = "success-box"
        project_text = f"[{project}] " if project else ""
        st.markdown(f"""<div class="{css_class}"><strong>{icon} {timestamp}</strong> {project_text}{message}</div>""", unsafe_allow_html=True)
    if len(st.session_state.persistent_notifications) > 5:
        with st.expander(f"全通知を表示 ({len(st.session_state.persistent_notifications)}件)"):
            for n in reversed(st.session_state.persistent_notifications):
                icon = "✅" if n['type']=="success" else "❌" if n['type']=="error" else "⚠️" if n['type']=="warning" else "ℹ️"
                project_text = f"[{n.get('project_key','')}] " if n.get('project_key') else ""
                st.write(f"{icon} **{n['timestamp']}** {project_text}{n['message']}")
    col1, _ = st.columns([1,4])
    with col1:
        if st.button("🗑️ 通知クリア", key="clear_notifications"):
            st.session_state.persistent_notifications = []
            st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

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

# ========================
# Google Sheets
# ========================
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
    add_notification("Google認証情報が見つかりません。Secretsの[gcp]を確認してください。", "error")
    st.stop()

@st.cache_data(ttl=60)
def load_sheet_data(project_key):
    try:
        if project_key not in PROJECT_CONFIGS:
            return pd.DataFrame()
        client = get_sheets_client()
        config = PROJECT_CONFIGS[project_key]
        sheet = client.open_by_key(SHEET_ID).worksheet(config['worksheet'])
        rows = sheet.get_all_values()
        if len(rows) <= 1:
            return pd.DataFrame()
        headers = rows[0]
        data_rows = rows[1:]

        # ヘッダ重複回避
        clean_headers = []
        for i, header in enumerate(headers):
            clean_headers.append(f"{header}_{i}" if header in clean_headers else header)

        # ステータス 未処理 のみ
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

def update_sheet_row(project_key, row_data, updates):
    try:
        client = get_sheets_client()
        config = PROJECT_CONFIGS[project_key]
        sheet = client.open_by_key(SHEET_ID).worksheet(config['worksheet'])
        all_rows = sheet.get_all_values()
        promo_url = row_data.get('宣伝URL', '')
        for i, row in enumerate(all_rows[1:], start=2):
            if len(row) > 1 and row[1] == promo_url:
                for col_name, value in updates.items():
                    if col_name in all_rows[0]:
                        col_idx = all_rows[0].index(col_name) + 1
                        sheet.update_cell(i, col_idx, value)
                        time.sleep(0.3)
                add_realtime_log(f"✅ スプレッドシート更新: 行{i}", project_key)
                add_notification(f"スプレッドシート更新完了: 行{i}", "success", project_key)
                return True
        add_notification("対象行が見つかりません", "error", project_key)
        return False
    except Exception as e:
        add_notification(f"スプレッドシート更新エラー: {e}", "error", project_key)
        return False

def add_schedule_to_k_column(project_key, row_data, schedule_times):
    try:
        client = get_sheets_client()
        config = PROJECT_CONFIGS[project_key]
        sheet = client.open_by_key(SHEET_ID).worksheet(config['worksheet'])
        all_rows = sheet.get_all_values()
        promo_url = row_data.get('宣伝URL', '')
        for i, row in enumerate(all_rows[1:], start=2):
            if len(row) > 1 and row[1] == promo_url:
                col_num = 11  # K列
                for schedule_dt in schedule_times:
                    while True:
                        try:
                            val = sheet.cell(i, col_num).value
                            if not val:
                                break
                        except Exception:
                            break
                        col_num += 1
                    sheet.update_cell(i, col_num, schedule_dt.strftime('%Y/%m/%d %H:%M'))
                    col_num += 1
                add_notification(f"K列以降に予約時刻を記録: 行{i}", "success", project_key)
                return True
        add_notification("対象行が見つかりません", "error", project_key)
        return False
    except Exception as e:
        add_notification(f"K列記録エラー: {e}", "error", project_key)
        return False

# ========================
# 競合/その他リンク
# ========================
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

def get_other_link():
    other_sites = get_other_links()
    competitor_domains = get_competitor_domains()
    available = []
    for site in other_sites:
        site_domain = urlparse(site['url']).netloc.lower()
        if not any(comp in site_domain for comp in competitor_domains):
            available.append(site)
    if available:
        site = random.choice(available)
        return site['url'], site['anchor']
    return None, None

# ========================
# Gemini 記事生成
# ========================
def _get_gemini_key():
    key = GEMINI_API_KEYS[st.session_state.gemini_key_index % len(GEMINI_API_KEYS)]
    st.session_state.gemini_key_index += 1
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
    return {"title": title, "content": content, "theme": theme if not auto_theme else "金融"}

# ========================
# HTMLアンカー属性統一
# ========================
def enforce_anchor_attrs(html: str) -> str:
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
    return re.sub(r'<a\s+[^>]*>', add_attrs, html, flags=re.I)

# ========================
# アイキャッチ自動生成
# ========================
def create_eyecatch_image(title: str, site_key: str) -> bytes:
    width, height = 600, 400
    color_schemes = [
        {'bg': '#2E7D32', 'accent': '#66BB6A', 'text': '#FFFFFF'},
        {'bg': '#388E3C', 'accent': '#81C784', 'text': '#FFFFFF'},
        {'bg': '#4CAF50', 'accent': '#8BC34A', 'text': '#FFFFFF'},
        {'bg': '#689F38', 'accent': '#AED581', 'text': '#FFFFFF'},
        {'bg': '#7CB342', 'accent': '#C5E1A5', 'text': '#2E7D32'},
    ]
    scheme = random.choice(color_schemes)
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
    try:
        title_font = ImageFont.truetype("C:/Windows/Fonts/meiryob.ttc", 28)
        subtitle_font = ImageFont.truetype("C:/Windows/Fonts/meiryob.ttc", 20)
    except:
        try:
            title_font = ImageFont.truetype("C:/Windows/Fonts/meiryo.ttc", 28)
            subtitle_font = ImageFont.truetype("C:/Windows/Fonts/meiryo.ttc", 20)
        except:
            title_font = ImageFont.load_default()
            subtitle_font = ImageFont.load_default()
    lines = []
    if len(title) > 12:
        for sep in ['！','？','…','!','?']:
            if sep in title:
                idx = title.find(sep)
                if idx > 0:
                    lines = [title[:idx+1], title[idx+1:].strip()]
                    break
        if not lines:
            for sep in ['と','、','の','は','が','を','に','…','で']:
                if sep in title:
                    idx = title.find(sep)
                    if 5 < idx < len(title)-5:
                        lines = [title[:idx], title[idx:]]
                        break
        if not lines:
            mid = len(title)//2
            lines = [title[:mid], title[mid:]]
    else:
        lines = [title]
    y_start = (height - len(lines)*50)//2
    for i, line in enumerate(lines):
        try:
            bbox = draw.textbbox((0,0), line, font=title_font)
            text_width = bbox[2]-bbox[0]
        except AttributeError:
            text_width, _ = draw.textsize(line, font=title_font)
        x = (width - text_width)//2
        y = y_start + i*50
        draw.text((x+2,y+2), line, font=title_font, fill=(0,0,0))
        draw.text((x,y), line, font=title_font, fill=scheme['text'])
    site_names = {
        'selectadvance':'Select Advance','welkenraedt':'Welkenraedt Online','ykikaku':'YK企画',
        'efdlqjtz':'EFDLQJTZ','ncepqvub':'NCEPQVUB','kosagi':'Kosagi','selectad01':'Select AD',
        'thrones':'Thrones'
    }
    site_name = site_names.get(site_key, 'Financial Blog')
    try:
        bbox = draw.textbbox((0,0), site_name, font=subtitle_font); text_width = bbox[2]-bbox[0]
    except AttributeError:
        text_width, _ = draw.textsize(site_name, font=subtitle_font)
    x = (width - text_width)//2
    draw.text((x, height-50), site_name, font=subtitle_font, fill=scheme['text'])
    draw.rectangle([50, 40, width-50, 42], fill=scheme['text'])
    buf = io.BytesIO()
    img.save(buf, format='JPEG', quality=90)
    buf.seek(0)
    return buf.getvalue()

# ========================
# 各プラットフォーム投稿
# ========================
def post_to_seesaa(article: dict, category_name: str = None, project_key: str = None) -> str:
    config = PLATFORM_CONFIGS['seesaa']
    server = xmlrpc.client.ServerProxy(config['endpoint'], allow_none=True)
    content = {"title": article["title"], "description": article["content"]}
    try:
        add_notification("Seesaa投稿を開始します", "info", project_key)
        post_id = server.metaWeblog.newPost(config['blogid'], config['username'], config['password'], content, True)
        if category_name:
            try:
                cats = server.mt.getCategoryList(config['blogid'], config['username'], config['password'])
                for c in cats:
                    if c.get("categoryName") == category_name:
                        server.mt.setPostCategories(post_id, config['username'], config['password'],
                                                    [{"categoryId": c.get("categoryId"), "isPrimary": True}])
                        break
            except Exception as cat_error:
                add_notification(f"Seesaaカテゴリ設定エラー: {cat_error}", "warning", project_key)
        try:
            post = server.metaWeblog.getPost(post_id, config['username'], config['password'])
            post_url = post.get("permalink") or post.get("link") or ""
            if post_url:
                add_notification(f"Seesaa投稿成功: {post_url}", "success", project_key)
            return post_url
        except Exception:
            add_notification(f"Seesaa投稿成功 (post_id: {post_id})", "success", project_key)
            return f"post_id:{post_id}"
    except Exception as e:
        add_notification(f"Seesaa投稿エラー: {e}", "error", project_key)
        return ""

def post_to_fc2(article: dict, category_name: str = None, project_key: str = None) -> str:
    config = PLATFORM_CONFIGS['fc2']
    server = xmlrpc.client.ServerProxy(config['endpoint'])
    content = {'title': article['title'], 'description': article['content']}
    try:
        add_notification("FC2投稿を開始します", "info", project_key)
        post_id = server.metaWeblog.newPost(config['blog_id'], config['username'], config['password'], content, True)
        if category_name:
            try:
                cats = server.mt.getCategoryList(config['blog_id'], config['username'], config['password'])
                for c in cats:
                    if c.get('categoryName') == category_name:
                        server.mt.setPostCategories(post_id, config['username'], config['password'], [c])
                        break
            except Exception as cat_error:
                add_notification(f"FC2カテゴリ設定エラー: {cat_error}", "warning", project_key)
        post_url = f"https://{config['blog_id']}.blog.fc2.com/blog-entry-{post_id}.html"
        add_notification(f"FC2投稿成功: {post_url}", "success", project_key)
        return post_url
    except Exception as e:
        add_notification(f"FC2投稿エラー: {e}", "error", project_key)
        return ""

def post_to_livedoor(article: dict, category_name: str = None, project_key: str = None) -> str:
    config = PLATFORM_CONFIGS['livedoor']
    root = f"https://livedoor.blogcms.jp/atompub/{config['blog_name']}"
    endpoint = f"{root}/article"
    add_notification("livedoor投稿を開始します", "info", project_key)
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
        response = requests.post(
            endpoint,
            data=entry_xml,
            headers={"Content-Type": "application/atom+xml;type=entry"},
            auth=HTTPBasicAuth(config['user_id'], config['api_key']),
            timeout=30,
        )
        if response.status_code in (200, 201):
            try:
                root_xml = ET.fromstring(response.text)
                ns = {"atom": "http://www.w3.org/2005/Atom"}
                alt = root_xml.find(".//atom:link[@rel='alternate']", ns)
                if alt is not None:
                    post_url = alt.get("href")
                    add_notification(f"livedoor投稿成功: {post_url}", "success", project_key)
                    return post_url
                else:
                    add_notification("livedoor投稿成功 (URL取得失敗)", "success", project_key)
                    return ""
            except Exception as parse_error:
                add_notification(f"livedoor投稿成功 (レスポンス解析エラー: {parse_error})", "warning", project_key)
                return ""
        else:
            add_notification(f"livedoor投稿失敗: HTTP {response.status_code} - {response.text[:200]}", "error", project_key)
            return ""
    except Exception as e:
        add_notification(f"livedoor投稿エラー: {e}", "error", project_key)
        return ""

def post_to_blogger(article: dict, project_key: str = None) -> str:
    if not BLOGGER_AVAILABLE:
        add_notification("Bloggerライブラリがありません（requirements.txtを確認）", "error", project_key)
        return ""
    if not BLOG_ID:
        add_notification("Blogger BLOG_ID が未設定です（Secrets）", "error", project_key)
        return ""
    try:
        add_notification("Blogger認証処理を開始します", "info", project_key)
        creds = None
        token_file = '/tmp/blogger_token.pickle'
        # SecretsのBase64から復元
        if BLOGGER_TOKEN_B64:
            with open(token_file, 'wb') as f:
                f.write(base64.b64decode(BLOGGER_TOKEN_B64))
        if os.path.exists(token_file):
            with open(token_file, 'rb') as token:
                creds = pickle.load(token)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                add_notification("Bloggerトークンを更新中...", "info", project_key)
                creds.refresh(Request())
                with open(token_file, 'wb') as token:
                    pickle.dump(creds, token)
            else:
                # 初回認証フローはCloudで実行不可。ローカルでtoken作成→Secrets(BLOGGER_TOKEN)に登録が必要。
                add_notification("Blogger初回認証が必要です（ローカルでtoken作成→SecretsへBase64登録）", "error", project_key)
                return ""
        add_notification("Blogger認証成功", "success", project_key)
        service = build('blogger', 'v3', credentials=creds)
        post_data = {'title': article['title'], 'content': article['content'], 'labels': [article.get('theme','金融')]}
        add_notification(f"Blogger投稿実行: {article['title'][:30]}...", "info", project_key)
        response = service.posts().insert(blogId=BLOG_ID, body=post_data, isDraft=False).execute()
        if response and 'url' in response:
            post_url = response['url']
            add_notification(f"Blogger投稿成功: {post_url}", "success", project_key)
            return post_url
        else:
            add_notification("Blogger投稿失敗: レスポンスにURLなし", "error", project_key)
            return ""
    except Exception as e:
        add_notification(f"Blogger投稿エラー: {e}", "error", project_key)
        return ""

def post_to_wordpress(article_data: dict, site_key: str, category_name: str = None,
                      schedule_dt: datetime = None, enable_eyecatch: bool = True, project_key: str = None) -> str:
    if site_key not in WP_CONFIGS:
        add_notification(f"不明なサイト: {site_key}", "error", project_key)
        return ""
    site_config = WP_CONFIGS[site_key]
    
    # ベースURLの正規化（末尾スラッシュ確保）
    base_url = site_config['url'].rstrip('/') + '/'
    
    # kosagi: XMLRPCで即時 or 待機→即時
    if site_key == 'kosagi':
        if schedule_dt and schedule_dt > datetime.now():
            wait_seconds = max(0, int((schedule_dt - datetime.now()).total_seconds()))
            add_notification(f"kosagi待機: {wait_seconds}秒", "info", project_key)
            progress_bar = st.progress(0)
            total = max(wait_seconds, 1)
            for i in range(wait_seconds):
                progress_bar.progress((i+1)/total)
                time.sleep(1)
            add_notification("kosagi投稿開始", "success", project_key)
        
        endpoint = f"{base_url}xmlrpc.php"
        import html
        escaped_title = html.escape(article_data['title'])
        xml_request = f"""<?xml version="1.0" encoding="UTF-8"?>
<methodCall>
    <methodName>wp.newPost</methodName>
    <params>
        <param><value><int>0</int></value></param>
        <param><value><string>{site_config['user']}</string></value></param>
        <param><value><string>{site_config['password']}</string></value></param>
        <param>
            <value>
                <struct>
                    <member><name>post_type</name><value><string>post</string></value></member>
                    <member><name>post_status</name><value><string>publish</string></value></member>
                    <member><name>post_title</name><value><string>{escaped_title}</string></value></member>
                    <member><name>post_content</name><value><string><![CDATA[{article_data['content']}]]></string></value></member>
                </struct>
            </value>
        </param>
    </params>
</methodCall>"""
        try:
            add_notification("kosagi XMLRPC投稿...", "info", project_key)
            response = requests.post(
                endpoint, data=xml_request.encode('utf-8'),
                headers={'Content-Type':'text/xml; charset=UTF-8','User-Agent':'WordPress XML-RPC Client'},
                timeout=60
            )
            if response.status_code == 200:
                if '<name>faultCode</name>' in response.text:
                    fault_match = re.search(r'<name>faultString</name>.*?<string>(.*?)</string>', response.text, re.DOTALL)
                    fault_msg = fault_match.group(1) if fault_match else "不明なエラー"
                    add_notification(f"kosagi XMLRPC投稿エラー: {fault_msg}", "error", project_key)
                    return ""
                match = re.search(r'<string>(\d+)</string>', response.text)
                if match:
                    post_id = match.group(1)
                    post_url = f"{base_url}?p={post_id}"
                    add_notification(f"kosagi投稿成功: {post_url}", "success", project_key)
                    return post_url
                add_notification("kosagi投稿成功 (ID抽出失敗)", "success", project_key)
                return base_url
            else:
                add_notification(f"kosagi投稿失敗: HTTP {response.status_code} - {response.text[:300]}", "error", project_key)
                return ""
        except Exception as e:
            add_notification(f"kosagi投稿エラー: {e}", "error", project_key)
            return ""
    
    # 通常WP: REST API
    endpoint = f"{base_url}wp-json/wp/v2/posts"
    post_data = {'title': article_data['title'], 'content': article_data['content'], 'status': 'publish'}
    
    if schedule_dt and schedule_dt > datetime.now():
        post_data['status'] = 'future'
        post_data['date'] = schedule_dt.strftime('%Y-%m-%dT%H:%M:%S')
        add_notification(f"予約投稿設定: {schedule_dt.strftime('%Y/%m/%d %H:%M')}", "info", project_key)
    
    if enable_eyecatch:
        try:
            add_notification(f"アイキャッチ生成: {site_key}", "info", project_key)
            eyecatch_data = create_eyecatch_image(article_data['title'], site_key)
            media_endpoint = f"{base_url}wp-json/wp/v2/media"
            files = {'file': ('eyecatch.jpg', eyecatch_data, 'image/jpeg')}
            media_data = {'title': f"アイキャッチ: {article_data['title'][:30]}...", 'alt_text': article_data['title']}
            media_response = requests.post(media_endpoint, auth=HTTPBasicAuth(site_config['user'], site_config['password']),
                                           files=files, data=media_data, timeout=60)
            if media_response.status_code == 201:
                media_info = media_response.json()
                post_data['featured_media'] = media_info['id']
                add_notification(f"アイキャッチUP成功 ({site_key})", "success", project_key)
            else:
                add_notification(f"アイキャッチUP失敗 ({site_key}): {media_response.status_code}", "warning", project_key)
        except Exception as e:
            add_notification(f"アイキャッチ処理エラー ({site_key}): {e}", "warning", project_key)
    
    try:
        add_notification(f"{site_key}投稿開始（REST）", "info", project_key)
        response = requests.post(endpoint, auth=HTTPBasicAuth(site_config['user'], site_config['password']),
                                 headers={'Content-Type':'application/json'}, data=json.dumps(post_data), timeout=60)
        if response.status_code in (200, 201):
            try:
                data = response.json()
                post_url = data.get('link','')
                if schedule_dt and schedule_dt > datetime.now():
                    add_notification(f"予約投稿成功 ({site_key})", "success", project_key)
                else:
                    add_notification(f"投稿成功 ({site_key}): {post_url}", "success", project_key)
                return post_url
            except Exception as e:
                add_notification(f"{site_key}投稿成功だがJSON解析エラー: {e}", "warning", project_key)
                return base_url
        elif response.status_code == 401:
            add_notification(f"{site_key}認証エラー(401)", "error", project_key)
        elif response.status_code == 403:
            add_notification(f"{site_key}権限エラー(403)", "error", project_key)
        elif response.status_code == 404:
            add_notification(f"{site_key}API未有効/URL誤り(404)", "error", project_key)
        else:
            try:
                msg = response.json().get('message','Unknown')
            except Exception:
                msg = response.text[:300]
            add_notification(f"{site_key}投稿失敗: HTTP {response.status_code} - {msg}", "error", project_key)
        return ""
    except Exception as e:
        add_notification(f"{site_key}投稿エラー: {e}", "error", project_key)
        return ""

# ========================
# 投稿数ロジック
# ========================
def get_max_posts_for_project(project_key, post_target=""):
    cfg = PROJECT_CONFIGS[project_key]['max_posts']
    if isinstance(cfg, dict):
        key = (post_target or '').strip().lower()
        if key in cfg:
            return cfg[key]
        # wordpressはサイト単位だが全体上限を適用
        if key in ('wordpress',):
            return cfg.get('wordpress', 20)
        # 既定
        return max(cfg.values()) if cfg else 20
    return cfg

# ========================
# 実行メイン（投稿処理）
# ========================
def execute_post(row_data, project_key, post_count=1, schedule_times=None, enable_eyecatch=True):
    try:
        st.session_state.posting_projects.add(project_key)
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
        if 'カウンター' in row_data and row_data['カウンター']:
            try:
                current_counter = int(str(row_data['カウンター']).strip())
            except:
                current_counter = 0
        add_realtime_log(f"📊 現在のカウンター: {current_counter}", project_key)

        # C列「投稿先」
        post_target_raw = row_data.get('投稿先', '').strip()
        post_target = post_target_raw.lower()
        add_notification(f"投稿先指定: '{post_target_raw}'", "info", project_key)

        # 最大投稿数
        max_posts = get_max_posts_for_project(project_key, post_target)

        if current_counter >= max_posts:
            add_realtime_log(f"⚠️ 既に{max_posts}記事完了済み", project_key)
            add_notification(f"既に{max_posts}記事完了しています", "warning", project_key)
            st.session_state.posting_projects.discard(project_key)
            return False

        posts_completed = 0
        add_realtime_log(f"🚀 {post_count}記事の投稿を開始", project_key)
        progress_bar = st.progress(0)

        for i in range(post_count):
            if current_counter >= max_posts:
                add_realtime_log(f"⚠️ カウンター{current_counter}: 既に{max_posts}記事完了済み", project_key)
                add_notification(f"カウンター{current_counter}: 既に{max_posts}記事完了済み", "warning", project_key)
                break

            schedule_dt = schedule_times[i] if i < len(schedule_times) else None
            add_realtime_log(f"📝 記事{i+1}/{post_count}の処理開始", project_key)

            with st.expander(f"記事{i+1}/{post_count}の投稿", expanded=True):
                try:
                    # 宣伝URL or その他リンク
                    if current_counter == max_posts - 1:
                        add_realtime_log(f"🎯 {max_posts}記事目 → 宣伝URL使用", project_key)
                        st.info(f"{max_posts}記事目 → 宣伝URL使用")
                        url = row_data.get('宣伝URL', '')
                        anchor = row_data.get('アンカーテキスト', project_key)
                        category = row_data.get('カテゴリー', 'お金のマメ知識')
                    else:
                        add_realtime_log(f"🔗 {current_counter + 1}記事目 → その他リンク使用", project_key)
                        st.info(f"{current_counter + 1}記事目 → その他リンク使用")
                        url, anchor = get_other_link()
                        if not url:
                            add_realtime_log("❌ その他リンクが取得できません", project_key)
                            add_notification("その他リンクが取得できません", "error", project_key)
                            break
                        category = row_data.get('カテゴリー', 'お金のマメ知識')

                    # 記事生成
                    add_realtime_log("🧠 記事を生成中...", project_key)
                    with st.spinner("記事を生成中..."):
                        theme = row_data.get('テーマ', '') or '金融・投資・資産運用'
                        article = generate_article_with_link(theme, url, anchor)

                    add_realtime_log(f"✅ 記事生成完了: {article['title'][:30]}...", project_key)
                    st.success(f"タイトル: {article['title']}")
                    st.info(f"使用リンク: {anchor}")

                    # === ここが重要：投稿先の厳密ルーティング ===
                    posted_urls = []
                    allowed = config['platforms']

                    # WordPress群（プロジェクトがWP）の場合
                    if 'wordpress' in allowed:
                        wp_sites = config.get('wp_sites', [])
                        
                        # C列が空白の場合はエラー
                        if not post_target or post_target in ('wordpress', ''):
                            add_notification("C列「投稿先」が空白です。投稿先サイトを指定してください", "error", project_key)
                            break
                        
                        # C列に特定site_keyが入っている場合のみ投稿
                        if post_target in WP_CONFIGS and post_target in wp_sites:
                            add_realtime_log(f"📤 WordPress({post_target})のみに投稿", project_key)
                            add_notification(f"指定WPサイト '{post_target}' に投稿します", "info", project_key)
                            post_url = post_to_wordpress(article, post_target, category, schedule_dt, enable_eyecatch, project_key)
                            if post_url:
                                posted_urls.append(post_url)
                        else:
                            add_notification(f"C列のWPサイト '{post_target}' はこのプロジェクトに登録されていません", "error", project_key)
                            break

                    # 非WPプロジェクト（Blogger / livedoor / Seesaa / FC2）
                    elif post_target in allowed:
                        if post_target == 'blogger':
                            add_realtime_log("📤 Bloggerに投稿中...", project_key)
                            post_url = post_to_blogger(article, project_key)
                            if post_url: posted_urls.append(post_url)
                        elif post_target == 'livedoor':
                            add_realtime_log("📤 livedoorに投稿中...", project_key)
                            post_url = post_to_livedoor(article, category, project_key)
                            if post_url: posted_urls.append(post_url)
                        elif post_target == 'seesaa':
                            add_realtime_log("📤 Seesaaに投稿中...", project_key)
                            post_url = post_to_seesaa(article, category, project_key)
                            if post_url: posted_urls.append(post_url)
                        elif post_target == 'fc2':
                            add_realtime_log("📤 FC2に投稿中...", project_key)
                            post_url = post_to_fc2(article, category, project_key)
                            if post_url: posted_urls.append(post_url)
                        else:
                            add_notification(f"未対応の投稿先: {post_target}", "error", project_key)
                            break
                    else:
                        add_notification(f"未対応または空白の投稿先: '{post_target_raw}'", "error", project_key)
                        break

                    if not posted_urls:
                        add_realtime_log("❌ 投稿に失敗しました", project_key)
                        add_notification("投稿に失敗しました", "error", project_key)
                        break

                    # 記録
                    timestamp = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                    for url_item in posted_urls:
                        add_posted_url(current_counter + 1, article['title'], url_item, timestamp, project_key)
                        add_realtime_log(f"📋 記事{current_counter + 1}記録 → {url_item}", project_key)

                    # カウンター更新
                    current_counter += 1
                    posts_completed += 1

                    # スプレッドシート更新（カウンター・完了処理）
                    client = get_sheets_client()
                    config_sheet = PROJECT_CONFIGS[project_key]
                    sheet = client.open_by_key(SHEET_ID).worksheet(config_sheet['worksheet'])
                    all_rows = sheet.get_all_values()
                    promo_url = row_data.get('宣伝URL', '')
                    for row_idx, row in enumerate(all_rows[1:], start=2):
                        if len(row) > 1 and row[1] == promo_url:
                            # G列: カウンター
                            sheet.update_cell(row_idx, 7, str(current_counter))
                            time.sleep(0.3)
                            if current_counter >= max_posts:
                                # E列: ステータス
                                sheet.update_cell(row_idx, 5, "処理済み"); time.sleep(0.3)
                                # F列: 投稿URL (最終記事のURL群)
                                final_urls = [it['url'] for it in st.session_state.all_posted_urls[project_key] if it['counter'] == max_posts]
                                sheet.update_cell(row_idx, 6, ', '.join(final_urls)); time.sleep(0.3)
                                # I列: 完了日時
                                completion_time = datetime.now().strftime("%Y/%m/%d %H:%M")
                                sheet.update_cell(row_idx, 9, completion_time); time.sleep(0.3)
                                add_realtime_log(f"🎉 {max_posts}記事完了！シート更新完了", project_key)
                                add_notification(f"{max_posts}記事完了！プロジェクト完了", "success", project_key)
                                st.session_state.completion_results[project_key] = {
                                    'project_name': PROJECT_CONFIGS[project_key]['worksheet'],
                                    'completed_at': completion_time,
                                    'total_posts': max_posts,
                                    'all_urls': st.session_state.all_posted_urls[project_key].copy()
                                }
                                st.balloons()
                                st.success(f"{max_posts}記事完了!")
                                st.session_state.posting_projects.discard(project_key)
                                return True
                            else:
                                add_notification(f"カウンター更新: {current_counter}/{max_posts}", "success", project_key)
                            break

                    progress_bar.progress(posts_completed / post_count)

                    if current_counter < max_posts and i < post_count - 1:
                        wait_time = random.randint(MIN_INTERVAL, MAX_INTERVAL)
                        add_realtime_log(f"⏳ 次の記事まで{wait_time}秒待機中...", project_key)
                        st.info(f"次の記事まで{wait_time}秒待機中...")
                        time.sleep(wait_time)

                except Exception as e:
                    add_realtime_log(f"❌ 記事{i+1}の投稿エラー: {e}", project_key)
                    add_notification(f"記事{i+1}の投稿エラー: {e}", "error", project_key)
                    st.session_state.posting_projects.discard(project_key)
                    break

        st.session_state.posting_projects.discard(project_key)
        add_realtime_log(f"✅ {posts_completed}記事の投稿が完了しました", project_key)
        add_notification(f"{posts_completed}記事の投稿が完了しました", "success", project_key)
        return True

    except Exception as e:
        st.session_state.posting_projects.discard(project_key)
        add_realtime_log(f"❌ 投稿処理エラー: {e}", project_key)
        add_notification(f"投稿処理エラー: {e}", "error", project_key)
        return False

# ========================
# UI
# ========================
def main():
    show_notifications()

    st.markdown("""
    <div class="main-header">
        <h1>統合ブログ投稿管理システム</h1>
        <p>WordPress / Seesaa / FC2 / livedoor / Blogger 対応</p>
    </div>
    """, unsafe_allow_html=True)

    if not BLOGGER_AVAILABLE:
        add_notification("Blogger投稿を使うには requirements.txt に google-* ライブラリを追加してください", "warning")

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

    # 進行中プロジェクト
    posting_projects = st.session_state.get('posting_projects', set())
    if posting_projects:
        st.markdown("## 🚀 投稿中プロジェクト")
        if len(posting_projects) > 1:
            tabs = st.tabs([f"{PROJECT_CONFIGS[pk]['worksheet']}" for pk in posting_projects])
            for i, proj_key in enumerate(posting_projects):
                with tabs[i]:
                    if proj_key in st.session_state.get('realtime_logs', {}):
                        st.markdown(f"### 📋 {PROJECT_CONFIGS[proj_key]['worksheet']} 進行状況")
                        if proj_key in st.session_state.get('all_posted_urls', {}):
                            posted_count = len(st.session_state.all_posted_urls[proj_key])
                            max_posts = get_max_posts_for_project(proj_key)
                            if max_posts > 0:
                                progress = min(posted_count / max_posts, 1.0)
                                st.progress(progress, f"{posted_count}/{max_posts} 記事完了")
                        logs = st.session_state.realtime_logs[proj_key][-10:]
                        for log in logs:
                            st.text(log)
        else:
            proj_key = list(posting_projects)[0]
            st.warning(f"🚀 {PROJECT_CONFIGS[proj_key]['worksheet']} 投稿処理中です。")
            if proj_key in st.session_state.get('realtime_logs', {}):
                if proj_key in st.session_state.get('all_posted_urls', {}):
                    posted_count = len(st.session_state.all_posted_urls[proj_key])
                    max_posts = get_max_posts_for_project(proj_key)
                    if max_posts > 0:
                        progress = min(posted_count / max_posts, 1.0)
                        st.progress(progress, f"{posted_count}/{max_posts} 記事完了")
                with st.expander("📋 リアルタイム進行状況", expanded=True):
                    logs = st.session_state.realtime_logs[proj_key][-10:]
                    for log in logs:
                        st.text(log)

    # プロジェクト選択
    project_key = st.selectbox(
        "プロジェクト選択",
        options=list(PROJECT_CONFIGS.keys()),
        format_func=lambda x: f"{PROJECT_CONFIGS[x]['worksheet']} ({', '.join(PROJECT_CONFIGS[x]['platforms'])})",
        key="project_selector"
    )
    if st.session_state.current_project != project_key and project_key not in st.session_state.get('posting_projects', set()):
        st.session_state.current_project = project_key
        st.cache_data.clear()

    config = PROJECT_CONFIGS[project_key]
    col1, col2 = st.columns(2)
    with col1:
        st.info(f"**プロジェクト**: {config['worksheet']}")
        st.info(f"**プラットフォーム**: {', '.join(config['platforms'])}")
    with col2:
        if config['needs_k_column']:
            st.warning("**予約方式**: K列記録 → GitHub Actions / 外部実行")
        else:
            st.success("**予約方式**: WordPress 予約投稿機能")

    # データ読み込み
    df = load_sheet_data(project_key)
    if df.empty:
        st.info("未処理のデータがありません")
        return

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
    col1, col2 = st.columns(2)
    with col1:
        post_count = st.selectbox("投稿数", options=[1,2,3,4,5], help="一度に投稿する記事数")
    with col2:
        enable_eyecatch = st.checkbox("アイキャッチ画像を自動生成", value=True)

    # 予約設定
    if config['needs_k_column']:
        st.markdown("""
        <div class="warning-box">
        <strong>非WordPressプロジェクト</strong><br>
        予約時刻はK列に記録され、外部実行で投稿されます。
        </div>
        """, unsafe_allow_html=True)
        enable_schedule = st.checkbox("予約投稿を使用する（K列記録）")
        schedule_times = []
        if enable_schedule:
            st.subheader("予約時刻設定（今日の未来時刻）")
            schedule_input = st.text_area("予約時刻（1行につき1件）", placeholder="10:30\n12:15\n14:00")
            if schedule_input:
                lines = [l.strip() for l in schedule_input.split('\n') if l.strip()]
                now = datetime.now()
                for line in lines:
                    try:
                        t = datetime.strptime(line, '%H:%M')
                        dt = now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
                        if dt > now:
                            schedule_times.append(dt)
                        else:
                            add_notification(f"過去の時刻は無効: {line}", "error")
                    except ValueError:
                        add_notification(f"無効な時刻形式: {line}", "error")
                if schedule_times:
                    st.success(f"予約時刻 {len(schedule_times)}件を設定")
                    for dt in schedule_times: st.write(f"• {dt.strftime('%H:%M')}")
    else:
        st.markdown("""
        <div class="success-box">
        <strong>WordPressプロジェクト</strong><br>
        WordPressの予約投稿機能を使用します。
        </div>
        """, unsafe_allow_html=True)
        enable_schedule = st.checkbox("予約投稿を使用する")
        schedule_times = []
        if enable_schedule:
            st.subheader("予約時刻設定（YYYY-MM-DD HH:MM または HH:MM）")
            schedule_input = st.text_area("予約時刻（1行につき1件）", placeholder="2025-08-20 10:30\n2025-08-20 12:15\n14:00")
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

    # 実行ボタン
    col_a, col_b = st.columns(2)
    with col_a:
        if config['needs_k_column'] and enable_schedule:
            button_text = "K列に予約時刻を記録"
        elif not config['needs_k_column'] and enable_schedule:
            button_text = "予約投稿"
        else:
            button_text = "即時投稿"
        if st.button(button_text, type="primary", use_container_width=True):
            selected_rows = edited_df[edited_df['選択'] == True]
            if len(selected_rows) == 0:
                add_notification("投稿する行を選択してください", "error")
            elif len(selected_rows) > 1:
                add_notification("1行のみ選択してください", "error")
            else:
                row = selected_rows.iloc[0]
                if config['needs_k_column'] and enable_schedule:
                    if not schedule_times:
                        add_notification("予約時刻を入力してください", "error")
                    else:
                        success = add_schedule_to_k_column(project_key, row.to_dict(), schedule_times)
                        if success:
                            add_notification("K列に予約時刻を記録しました", "success", project_key)
                            time.sleep(1.5); st.cache_data.clear(); st.rerun()
                else:
                    success = execute_post(
                        row.to_dict(), project_key,
                        post_count=post_count, schedule_times=schedule_times, enable_eyecatch=enable_eyecatch
                    )
                    if success:
                        time.sleep(1.5); st.cache_data.clear(); st.rerun()
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



