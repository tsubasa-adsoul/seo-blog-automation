#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
統合ブログ投稿管理システム - 完全予約投稿対応版
PCシャットダウン対応・GitHub Actions連携
"""

# ========================
# 基本インポート
# ========================
import os
import re
import io
import json
import time
import base64
import random
import logging
import tempfile
import xmlrpc.client
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from datetime import datetime, timedelta

import pandas as pd
import requests
import gspread
import streamlit as st
from PIL import Image, ImageDraw, ImageFont
from oauth2client.service_account import ServiceAccountCredentials
from requests.auth import HTTPBasicAuth

# ========================
# ログ設定（Streamlit Cloud対応）
# ========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ========================
# ページ設定
# ========================
st.set_page_config(
    page_title="📝 統合ブログ投稿管理システム",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========================
# セッション状態（確実に初期化）
# ========================
_defaults = {
    'authenticated': False,
    'username': None,
    'is_admin': False,
}
for _k, _v in _defaults.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ========================
# 定数・設定
# ========================
# スプレッドシートID（st.secrets優先・環境変数フォールバック）
SPREADSHEET_ID = None
try:
    SPREADSHEET_ID = st.secrets.google.spreadsheet_id
except Exception:
    SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '')

# 投稿間隔（予約実行時の連投緩和用）
MIN_INTERVAL = 60   # 60秒
MAX_INTERVAL = 120  # 120秒

# プロジェクト設定（UI表示用）
PROJECTS = {
    'ビックギフト': {
        'worksheet': 'ビックギフト向け',
        'icon': '🎁',
        'color': '#ff8c00',
        'platforms': ['blogger', 'livedoor'],
        'wp_sites': [],
        'max_posts': {'blogger': 20, 'livedoor': 15}
    },
    'ありがた屋': {
        'worksheet': 'ありがた屋向け',
        'icon': '☕',
        'color': '#8b4513',
        'platforms': ['seesaa', 'fc2'],
        'wp_sites': [],
        'max_posts': 20
    },
    '買取LIFE': {
        'worksheet': '買取LIFE向け',
        'icon': '💰',
        'color': '#ffd700',
        'platforms': ['wordpress'],
        'wp_sites': ['selectad', 'thrones'],
        'max_posts': 20
    },
    'お財布レスキュー': {
        'worksheet': 'お財布レスキュー向け',
        'icon': '💖',
        'color': '#ff6b9d',
        'platforms': ['wordpress'],
        'wp_sites': ['ykikaku', 'efdlqjtz'],
        'max_posts': 20
    },
    'クレかえる': {
        'worksheet': 'クレかえる向け',
        'icon': '🐸',
        'color': '#7ed321',
        'platforms': ['wordpress'],
        'wp_sites': ['selectadvance', 'welkenraedt'],
        'max_posts': 20
    },
    '赤いサイト': {
        'worksheet': '赤いサイト向け',
        'icon': '🛒',
        'color': '#ff4444',
        'platforms': ['wordpress'],
        'wp_sites': ['ncepqvub', 'kosagi'],
        'max_posts': 20
    }
}

# WordPress接続情報（st.secrets優先・環境変数フォールバック）
def _get_secret(path: List[str], env_name: Optional[str] = None) -> Optional[str]:
    """st.secretsのネストを安全に取り出し。なければ環境変数へフォールバック"""
    try:
        cur = st.secrets
        for k in path:
            cur = cur[k]
        if cur:
            return str(cur)
    except Exception:
        pass
    if env_name:
        return os.environ.get(env_name)
    return None

WP_CONFIGS = {
    'ykikaku': {
        'url': _get_secret(['wp_ykikaku', 'url'], 'WP_YKIKAKU_URL'),
        'user': _get_secret(['wp_ykikaku', 'user'], 'WP_YKIKAKU_USER'),
        'password': _get_secret(['wp_ykikaku', 'password'], 'WP_YKIKAKU_PASSWORD'),
    },
    'efdlqjtz': {
        'url': _get_secret(['wp_efdlqjtz', 'url'], 'WP_EFDLQJTZ_URL'),
        'user': _get_secret(['wp_efdlqjtz', 'user'], 'WP_EFDLQJTZ_USER'),
        'password': _get_secret(['wp_efdlqjtz', 'password'], 'WP_EFDLQJTZ_PASSWORD'),
    },
    'selectadvance': {
        'url': _get_secret(['wp_selectadvance', 'url'], 'WP_SELECTADVANCE_URL'),
        'user': _get_secret(['wp_selectadvance', 'user'], 'WP_SELECTADVANCE_USER'),
        'password': _get_secret(['wp_selectadvance', 'password'], 'WP_SELECTADVANCE_PASSWORD'),
    },
    'welkenraedt': {
        'url': _get_secret(['wp_welkenraedt', 'url'], 'WP_WELKENRAEDT_URL'),
        'user': _get_secret(['wp_welkenraedt', 'user'], 'WP_WELKENRAEDT_USER'),
        'password': _get_secret(['wp_welkenraedt', 'password'], 'WP_WELKENRAEDT_PASSWORD'),
    },
    'ncepqvub': {
        'url': _get_secret(['wp_ncepqvub', 'url'], 'WP_NCEPQVUB_URL'),
        'user': _get_secret(['wp_ncepqvub', 'user'], 'WP_NCEPQVUB_USER'),
        'password': _get_secret(['wp_ncepqvub', 'password'], 'WP_NCEPQVUB_PASSWORD'),
    },
    'kosagi': {
        'url': _get_secret(['wp_kosagi', 'url'], 'WP_KOSAGI_URL'),
        'user': _get_secret(['wp_kosagi', 'user'], 'WP_KOSAGI_USER'),
        'password': _get_secret(['wp_kosagi', 'password'], 'WP_KOSAGI_PASSWORD'),
    },
    'selectad': {
        'url': _get_secret(['wp_selectad', 'url'], 'WP_SELECTAD_URL'),
        'user': _get_secret(['wp_selectad', 'user'], 'WP_SELECTAD_USER'),
        'password': _get_secret(['wp_selectad', 'password'], 'WP_SELECTAD_PASSWORD'),
    },
    'thrones': {
        'url': _get_secret(['wp_thrones', 'url'], 'WP_THRONES_URL'),
        'user': _get_secret(['wp_thrones', 'user'], 'WP_THRONES_USER'),
        'password': _get_secret(['wp_thrones', 'password'], 'WP_THRONES_PASSWORD'),
    },
}

# ========================
# Google Sheets クライアント
# ========================
@st.cache_resource
def get_sheets_client():
    """Google Sheetsクライアントを取得"""
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    # service account を secrets から一時ファイルに書き出し
    try:
        creds_dict = st.secrets.gcp.to_dict()
    except Exception:
        # 環境変数 JSON 文字列からでもOK（任意）
        creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON', '')
        if not creds_json:
            raise RuntimeError("GCPの認証情報が見つかりません（st.secrets.gcp か 環境変数 GOOGLE_APPLICATION_CREDENTIALS_JSON）")
        creds_dict = json.loads(creds_json)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(creds_dict, f)
        temp_creds_file = f.name

    creds = ServiceAccountCredentials.from_json_keyfile_name(temp_creds_file, scope)
    client = gspread.authorize(creds)

    try:
        os.unlink(temp_creds_file)
    except Exception:
        pass

    return client

def load_sheet_data(worksheet_name: str) -> pd.DataFrame:
    """シート読み込み（ヘッダ1行前提）"""
    client = get_sheets_client()
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(worksheet_name)
    data = sheet.get_all_values()
    if len(data) <= 1:
        return pd.DataFrame()
    return pd.DataFrame(data[1:], columns=data[0])

def update_sheet_cell(worksheet_name: str, row: int, col: int, value: str) -> bool:
    """セル更新（1始まり）"""
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(worksheet_name)
        sheet.update_cell(row, col, value)
        return True
    except Exception as e:
        logger.error(f"更新エラー: {e}")
        return False

def add_schedule_to_sheet(worksheet_name: str, row_num: int, schedule_times: List[datetime]) -> bool:
    """予約時刻をK列(11)以降に追記"""
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(worksheet_name)
        # K=11 から順に空セルへ書き込み
        row_values = sheet.row_values(row_num)
        col_num = 11  # K列
        for dt in schedule_times:
            # 既存の予約がある場合は空き列を探す
            while col_num <= max(11, len(row_values)):
                val = row_values[col_num - 1] if col_num - 1 < len(row_values) else ''
                if not val:
                    break
                col_num += 1
            sheet.update_cell(row_num, col_num, dt.strftime('%Y/%m/%d %H:%M'))
            col_num += 1
        return True
    except Exception as e:
        logger.error(f"予約記録エラー: {e}")
        return False

# ========================
# 競合・その他リンク
# ========================
def get_competitor_domains() -> List[str]:
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet('競合他社')
        rows = sheet.get_all_values()[1:]
        domains = []
        for r in rows:
            if not r or not r[0]:
                continue
            domain = r[0].strip()
            if domain.startswith('http'):
                domain = urlparse(domain).netloc
            domains.append(domain.lower())
        return domains
    except Exception as e:
        logger.warning(f"競合他社取得失敗: {e}")
        return []

def get_other_links() -> List[Dict]:
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet('その他リンク先')
        rows = sheet.get_all_values()[1:]
        sites = []
        for r in rows:
            if len(r) >= 2 and r[0] and r[1]:
                sites.append({"url": r[0].strip(), "anchor": r[1].strip()})
        if sites:
            return sites
    except Exception as e:
        logger.warning(f"その他リンク取得失敗: {e}")
    return [
        {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
        {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"},
    ]

def choose_other_link(other_links: List[Dict], competitor_domains: List[str]) -> Optional[Dict]:
    pool = []
    for site in other_links:
        d = urlparse(site['url']).netloc.lower()
        if not any(comp in d for comp in competitor_domains):
            pool.append(site)
    return random.choice(pool) if pool else None

# ========================
# 記事生成（Gemini）
# ========================
def _get_gemini_key() -> Optional[str]:
    for path, env in [
        (['google', 'gemini_api_key_1'], 'GEMINI_API_KEY_1'),
        (['google', 'gemini_api_key_2'], 'GEMINI_API_KEY_2'),
        (['gemini', 'api_key'], 'GEMINI_API_KEY'),
    ]:
        v = _get_secret(path, env)
        if v:
            return v
    return None

def call_gemini(prompt: str) -> str:
    api_key = _get_gemini_key()
    if not api_key:
        raise RuntimeError("Gemini APIキーが設定されていません")
    endpoint = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent?key={api_key}'
    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.7}}
    r = requests.post(endpoint, json=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Gemini API エラー: {r.status_code} {r.text[:200]}")
    data = r.json()
    return data['candidates'][0]['content']['parts'][0]['text']

def generate_article_with_link(theme: str, url: str, anchor_text: str) -> Dict:
    if not theme or theme.strip() == "":
        theme = "金融・投資・資産運用"
        auto_theme = True
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
・見出し: <h2>, <h3>
・段落: <p>
・リンク: <a href="URL" target="_blank" rel="noopener noreferrer">アンカーテキスト</a>
・リスト: <ul><li>

# 記事の要件:
・2000-2500文字
・専門的でありながら分かりやすい
・具体的な数値や事例を含める
・プレースホルダー（〇〇等）禁止
"""
    response = call_gemini(prompt)
    lines = [ln for ln in response.strip().split('\n') if ln.strip() != ""]
    title = lines[0].strip()
    content = '\n'.join(lines[1:]).strip()
    # 簡易クレンジング
    content = re.sub(r'〇〇|××|△△', '', content)
    content = re.sub(r'<p>\s*</p>', '', content)
    return {"title": title, "content": content, "theme": theme if not auto_theme else "金融"}

# ========================
# 各プラットフォーム投稿（実行時 = その場で公開する）
# 予約そのものは "K列に時刻を書くだけ"。投稿は check_and_execute_scheduled_posts() が時間到来時に行う。
# ========================
def post_to_wordpress(article: Dict, site_key: str) -> str:
    """WordPress（REST API, publish）"""
    cfg = WP_CONFIGS.get(site_key)
    if not cfg or not cfg.get('url') or not cfg.get('user'):
        logger.warning(f"WP設定不足: {site_key}")
        return ""
    endpoint = f"{cfg['url']}wp-json/wp/v2/posts"
    post_data = {'title': article['title'], 'content': article['content'], 'status': 'publish'}
    try:
        r = requests.post(endpoint, auth=HTTPBasicAuth(cfg['user'], cfg['password']),
                          headers={'Content-Type': 'application/json'}, data=json.dumps(post_data), timeout=60)
        if r.status_code in (200, 201):
            data = r.json()
            return data.get('link', '')
        logger.error(f"WordPress投稿失敗({site_key}): {r.status_code} {r.text[:200]}")
    except Exception as e:
        logger.error(f"WordPress投稿エラー({site_key}): {e}")
    return ""

def post_to_blogger(article: Dict) -> str:
    """Blogger: 実装環境依存のため簡易スタブ（失敗時は空文字返却）"""
    try:
        from googleapiclient.discovery import build  # 依存がなければImportError
        # 実運用に合わせて認証を整備してください
    except Exception as e:
        logger.warning(f"Blogger投稿未設定: {e}")
        return ""
    # 実装例は環境と認証に依存。ここでは未実装扱い。
    return ""

def post_to_livedoor(article: Dict) -> str:
    """livedoor: AtomPub"""
    LIVEDOOR_BLOG_NAME = os.environ.get('LIVEDOOR_BLOG_NAME', '')
    LIVEDOOR_ID = os.environ.get('LIVEDOOR_ID', '')
    LIVEDOOR_API_KEY = os.environ.get('LIVEDOOR_API_KEY', '')
    if not (LIVEDOOR_BLOG_NAME and LIVEDOOR_ID and LIVEDOOR_API_KEY):
        return ""
    root_url = f"https://livedoor.blogcms.jp/atompub/{LIVEDOOR_BLOG_NAME}"
    endpoint = f"{root_url}/article"
    from xml.sax.saxutils import escape as xml_escape
    import xml.etree.ElementTree as ET
    entry_xml = f'''<?xml version="1.0" encoding="utf-8"?>
<entry xmlns="http://www.w3.org/2005/Atom">
  <title>{xml_escape(article["title"])}</title>
  <content type="html">{xml_escape(article["content"])}</content>
</entry>'''.encode('utf-8')
    try:
        resp = requests.post(endpoint, data=entry_xml,
                             headers={"Content-Type": "application/atom+xml;type=entry"},
                             auth=HTTPBasicAuth(LIVEDOOR_ID, LIVEDOOR_API_KEY), timeout=30)
        if resp.status_code in (200, 201):
            root = ET.fromstring(resp.text)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            alt = root.find(".//atom:link[@rel='alternate']", ns)
            return alt.get("href") if alt is not None else ""
    except Exception as e:
        logger.error(f"livedoor投稿エラー: {e}")
    return ""

def post_to_seesaa(article: Dict) -> str:
    """Seesaa: XML-RPC"""
    endpoint = "http://blog.seesaa.jp/rpc"
    USER = os.environ.get('SEESAA_USERNAME', '')
    PASS = os.environ.get('SEESAA_PASSWORD', '')
    BLOGID = os.environ.get('SEESAA_BLOGID', '')
    if not (USER and PASS and BLOGID):
        return ""
    server = xmlrpc.client.ServerProxy(endpoint, allow_none=True)
    content = {"title": article["title"], "description": article["content"]}
    try:
        post_id = server.metaWeblog.newPost(BLOGID, USER, PASS, content, True)
        try:
            post = server.metaWeblog.getPost(post_id, USER, PASS)
            return post.get("permalink") or post.get("link") or ""
        except Exception:
            return f"post_id:{post_id}"
    except Exception as e:
        logger.error(f"Seesaa投稿エラー: {e}")
        return ""

def post_to_fc2(article: Dict) -> str:
    """FC2: XML-RPC"""
    endpoint = 'https://blog.fc2.com/xmlrpc.php'
    BLOGID = os.environ.get('FC2_BLOG_ID', '')
    USER = os.environ.get('FC2_USERNAME', '')
    PASS = os.environ.get('FC2_PASSWORD', '')
    if not (BLOGID and USER and PASS):
        return ""
    server = xmlrpc.client.ServerProxy(endpoint)
    content = {'title': article['title'], 'description': article['content']}
    try:
        post_id = server.metaWeblog.newPost(BLOGID, USER, PASS, content, True)
        return f"https://{BLOGID}.blog.fc2.com/blog-entry-{post_id}.html"
    except Exception as e:
        logger.error(f"FC2投稿エラー: {e}")
        return ""

# ========================
# 予約投稿 実行ロジック（GitHub Actions 等から呼ぶ想定）
# ========================
def _get_value(row: List[str], idx: int) -> str:
    return row[idx].strip() if len(row) > idx and row[idx] else ""

def _to_int(s: str, default: int = 0) -> int:
    try:
        return int(s)
    except Exception:
        return default

def _max_posts_of(config: Dict, platform_hint: str) -> int:
    max_posts = config.get('max_posts', 20)
    if isinstance(max_posts, dict):
        return max_posts.get(platform_hint.lower(), 20)
    return int(max_posts)

def _pick_wp_target(config: Dict, row: List[str]) -> Optional[str]:
    """行の「投稿先」列（2=0始まりで3列目）から対象WPサイトを1つだけ決める"""
    target = _get_value(row, 2).lower()
    sites = [s.lower() for s in config.get('wp_sites', [])]
    if not sites:
        return None
    if target and target in sites:
        return target
    return sites[0]  # 指定が無ければ先頭を既定

def _build_article(row: List[str], project_name: str, current_counter: int, max_posts: int,
                   other_links: List[Dict], competitor_domains: List[str]) -> Optional[Dict]:
    """カウンターに応じてリンクを切替えて記事作成（1〜19: その他 / 20: 宣伝URL）"""
    theme = _get_value(row, 0)
    if current_counter == max_posts - 1:
        # 20本目：宣伝URL＋アンカーテキスト
        url = _get_value(row, 1)
        anchor = _get_value(row, 3) or project_name
    else:
        # 1〜19本目：その他リンク
        site = choose_other_link(other_links, competitor_domains)
        if not site:
            return None
        url, anchor = site['url'], site['anchor']
    return generate_article_with_link(theme, url, anchor)

def execute_single_scheduled_post(row: List[str], project_name: str, config: Dict,
                                  sheet, row_idx_1based: int, col_num_1based: int,
                                  competitor_domains: List[str], other_links: List[Dict]) -> bool:
    """
    単一予約を実行:
      - 記事生成
      - 投稿（プラットフォーム1つに限定）
      - カウンター更新
      - 予約セル(K以降)を「完了」に上書き（※col_num_1basedをそのまま使う）
      - 20本目が投稿されたらステータス=処理済み＆最終日時
    """
    # 現カウンター
    current_counter = _to_int(_get_value(row, 6), 0)

    # 最大投稿数（プラットフォームで分岐）
    post_target = _get_value(row, 2).lower()
    if 'wordpress' in config['platforms']:
        mp = _max_posts_of(config, 'wordpress')
    elif post_target:
        mp = _max_posts_of(config, post_target)
    else:
        # 複数の可能性があるが、基準値として20
        mp = _max_posts_of(config, 'wordpress')

    if current_counter >= mp:
        logger.info("最大投稿数に到達済み。スキップ")
        return False

    # 記事生成（1〜19:その他 / 20:宣伝URL）
    article = _build_article(row, project_name, current_counter, mp, other_links, competitor_domains)
    if not article:
        logger.error("その他リンクが取得できず記事生成不可")
        return False

    posted = False
    # プラットフォーム別に “1つだけ” 投稿
    if 'wordpress' in config['platforms']:
        site = _pick_wp_target(config, row)
        if site:
            url = post_to_wordpress(article, site)
            if url:
                posted = True
                logger.info(f"WordPress投稿成功({site}): {url}")
    elif 'blogger' in config['platforms']:
        if post_target in ['blogger', '両方', '']:
            url = post_to_blogger(article)
            if url:
                posted = True
                logger.info(f"Blogger投稿成功: {url}")
    elif 'livedoor' in config['platforms']:
        if post_target in ['livedoor', '両方', '']:
            url = post_to_livedoor(article)
            if url:
                posted = True
                logger.info(f"livedoor投稿成功: {url}")
    elif 'seesaa' in config['platforms']:
        if post_target in ['seesaa', '']:
            url = post_to_seesaa(article)
            if url:
                posted = True
                logger.info(f"Seesaa投稿成功: {url}")
    elif 'fc2' in config['platforms']:
        if post_target in ['fc2']:
            url = post_to_fc2(article)
            if url:
                posted = True
                logger.info(f"FC2投稿成功: {url}")

    if not posted:
        logger.error("全プラットフォーム投稿失敗")
        return False

    # 投稿成功時：カウンター +1
    current_counter += 1
    sheet.update_cell(row_idx_1based, 7, str(current_counter))

    # 予約セル（K以降）の該当セルを「完了」
    # ※ col_num_1based は 1始まりの正しい列番号（K=11など）。+1 しない！
    sheet.update_cell(row_idx_1based, col_num_1based, "完了")

    # 20本目に到達したら ステータス=処理済み＆最終実行日時
    if current_counter >= mp:
        sheet.update_cell(row_idx_1based, 5, "処理済み")
        sheet.update_cell(row_idx_1based, 9, datetime.now().strftime("%Y/%m/%d %H:%M"))
        logger.info(f"🎯 完了: {project_name} 行{row_idx_1based} は {mp}本を投稿し終えました")

    return True

def check_and_execute_scheduled_posts(window_minutes: int = 30) -> Dict[str, int]:
    """
    予約投稿の定期実行（GitHub Actions等で30分ごと実行想定）
      - ステータスは途中で変えない（「予約済み」は使わない）
      - 行のK列(=11)以降を走査し、「完了」以外の日時が 今〜今+window 内なら実行
    """
    logger.info("⏰ 予約投稿チェック開始")
    client = get_sheets_client()
    now = datetime.now()
    window_end = now + timedelta(minutes=window_minutes)

    competitor_domains = get_competitor_domains()
    other_links = get_other_links()

    executed_total = 0
    skipped_total = 0

    for project_name, cfg in PROJECTS.items():
        try:
            sheet = client.open_by_key(SPREADSHEET_ID).worksheet(cfg['worksheet'])
            rows = sheet.get_all_values()
            if len(rows) <= 1:
                continue

            for r_idx, row in enumerate(rows[1:], start=2):
                status = _get_value(row, 4)
                # すでに処理済みならスキップ
                if status == '処理済み':
                    continue

                # K列(11)以降のセルを走査（row は0始まりアクセス）
                for col_pos_0based in range(10, len(row)):
                    raw = row[col_pos_0based].strip() if row[col_pos_0based] else ''
                    if not raw or raw == '完了':
                        continue
                    try:
                        scheduled_time = datetime.strptime(raw, '%Y/%m/%d %H:%M')
                    except Exception:
                        continue

                    if now <= scheduled_time <= window_end:
                        logger.info(f"🚀 実行: {cfg['worksheet']} 行{r_idx} {scheduled_time.strftime('%Y/%m/%d %H:%M')}")
                        ok = execute_single_scheduled_post(
                            row=row,
                            project_name=project_name,
                            config=cfg,
                            sheet=sheet,
                            row_idx_1based=r_idx,
                            col_num_1based=col_pos_0based + 1,  # ← ここは1始まりの列番号へ変換。+1は“0始まり→1始まり”の変換であり、K→11を守る！
                            competitor_domains=competitor_domains,
                            other_links=other_links
                        )
                        if ok:
                            executed_total += 1
                            # 連投防止
                            time.sleep(random.randint(MIN_INTERVAL, MAX_INTERVAL))
                        else:
                            skipped_total += 1

        except Exception as e:
            logger.error(f"ワークシート処理エラー({cfg['worksheet']}): {e}")

    logger.info(f"⏰ 予約投稿チェック完了: 実行 {executed_total} / スキップ {skipped_total}")
    return {"executed": executed_total, "skipped": skipped_total}

# ========================
# 認証UI
# ========================
def check_authentication() -> bool:
    if not st.session_state.authenticated:
        st.markdown("""
        <style>
        .auth-container {
            max-width: 420px;
            margin: auto;
            padding: 1.6rem;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 12px;
            color: #fff;
        }
        </style>
        """, unsafe_allow_html=True)

        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown('<div class="auth-container">', unsafe_allow_html=True)
            st.markdown("### 🔐 ログイン")
            username = st.text_input("ユーザー名", key="login_user")
            password = st.text_input("パスワード", type="password", key="login_pass")
            if st.button("ログイン", type="primary", use_container_width=True):
                try:
                    if username == "admin" and password == st.secrets.auth.admin_password:
                        st.session_state.authenticated = True
                        st.session_state.username = username
                        st.session_state.is_admin = True
                        st.rerun()
                    elif username in st.secrets.auth.client_passwords:
                        if password == st.secrets.auth.client_passwords[username]:
                            st.session_state.authenticated = True
                            st.session_state.username = username
                            st.session_state.is_admin = False
                            st.rerun()
                        else:
                            st.error("認証に失敗しました")
                    else:
                        st.error("認証に失敗しました")
                except Exception:
                    # secrets無しの環境では admin/任意で通す（検証用途）
                    if username == "admin":
                        st.session_state.authenticated = True
                        st.session_state.username = username
                        st.session_state.is_admin = True
                        st.rerun()
                    st.error("認証情報が設定されていません")
            st.markdown('</div>', unsafe_allow_html=True)
        return False
    return True

# ========================
# UI本体
# ========================
def main():
    # 認証
    if not check_authentication():
        return

    # スタイル
    st.markdown("""
    <style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.4rem 1.2rem;
        border-radius: 12px;
        margin-bottom: 1.2rem;
        color: white;
        text-align: center;
    }
    .warning-box {background: #fff3cd; border-left: 4px solid #ffc107; padding: 1rem; margin: 1rem 0;}
    .success-box {background: #d4edda; border-left: 4px solid #28a745; padding: 1rem; margin: 1rem 0;}
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="main-header">
        <h1>📝 統合ブログ投稿管理システム</h1>
        <p>完全予約投稿対応版 - PCシャットダウンOK（GitHub Actions等で実行）</p>
    </div>
    """, unsafe_allow_html=True)

    # サイドバー
    with st.sidebar:
        st.markdown(f"### 👤 {st.session_state.username or 'ゲスト'}")
        if st.button("🚪 ログアウト", use_container_width=True):
            st.session_state.authenticated = False
            st.session_state.username = None
            st.rerun()
        st.divider()
        st.markdown("### 🎯 プロジェクト選択")
        project_names = list(PROJECTS.keys())
        selected_project = st.selectbox("プロジェクトを選択", project_names, key="project_selector")
        project_info = PROJECTS[selected_project]
        st.markdown(f"""
        <div style="background:{project_info['color']}20;padding:1rem;border-radius:8px;border-left:4px solid {project_info['color']};">
            <h4>{project_info['icon']} {selected_project}</h4>
            <p>プラットフォーム: {', '.join(project_info['platforms'])}</p>
            <p>WPサイト: {', '.join(project_info.get('wp_sites', []) or ['-'])}</p>
        </div>
        """, unsafe_allow_html=True)

        st.divider()
        if st.button("⏱ 予約チェックを今すぐ実行（開発用）", use_container_width=True):
            res = check_and_execute_scheduled_posts(window_minutes=60)  # 手動実行時は幅広く
            st.success(f"実行: {res['executed']} / スキップ: {res['skipped']}")

    # タブ
    tabs = st.tabs(["⏰ 予約投稿", "📊 ダッシュボード", "⚙️ 設定"])

    # 予約投稿タブ
    with tabs[0]:
        st.markdown("### ⏰ 予約投稿（K列へ記録。実行はGitHub Actions等）")

        df = load_sheet_data(project_info['worksheet'])
        if df.empty:
            st.info("データがありません")
        else:
            # 列名を正規化
            df.columns = [str(c).strip() if c else f"列{i+1}" for i, c in enumerate(df.columns)]
            # 選択列を追加
            if '選択' not in df.columns:
                df.insert(0, '選択', False)

            st.markdown("#### 📋 投稿対象を選択")
            edited_df = st.data_editor(
                df,
                use_container_width=True,
                hide_index=True,
                key="schedule_data_editor",
                column_config={
                    "選択": st.column_config.CheckboxColumn("選択", help="予約投稿する行を選択", default=False)
                }
            )

            st.markdown("#### 🕐 予約スケジュール設定")
            col1, col2 = st.columns([3, 2])
            with col1:
                # デフォルト候補（今日の残り時間帯 or 明日）
                defaults = []
                now = datetime.now()
                for h in [9, 12, 15, 18]:
                    dt = now.replace(hour=h, minute=0, second=0, microsecond=0)
                    if dt > now:
                        defaults.append(dt.strftime('%Y/%m/%d %H:%M'))
                if not defaults:
                    tomorrow = now + timedelta(days=1)
                    for h in [9, 12, 15, 18]:
                        dt = tomorrow.replace(hour=h, minute=0, second=0, microsecond=0)
                        defaults.append(dt.strftime('%Y/%m/%d %H:%M'))

                schedule_input = st.text_area("予約日時（1行1件 / 形式: YYYY/MM/DD HH:MM）",
                                              value='\n'.join(defaults), height=200)
                posts_per_time = st.number_input("各時刻での投稿数", min_value=1, max_value=5, value=1, step=1)
            with col2:
                st.markdown("#### 📊 予約サマリー")
                schedule_times: List[datetime] = []
                for line in schedule_input.strip().split('\n'):
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        dt = datetime.strptime(s, '%Y/%m/%d %H:%M')
                        if dt > datetime.now():
                            schedule_times.append(dt)
                    except Exception:
                        pass
                if schedule_times:
                    st.success(f"✅ {len(schedule_times)}回の投稿を予約予定")
                    for dt in schedule_times[:5]:
                        st.write(f"• {dt.strftime('%m/%d %H:%M')}")
                    if len(schedule_times) > 5:
                        st.write(f"... 他 {len(schedule_times)-5}件")
                else:
                    st.warning("有効な予約時刻がありません")
                selected_count = len(edited_df[edited_df['選択'] == True]) if '選択' in edited_df.columns else 0
                st.info(f"選択行数: {selected_count}")
                if selected_count > 0 and schedule_times:
                    st.metric("総投稿数", selected_count * len(schedule_times) * posts_per_time)

            if st.button("🚀 予約時刻をK列に記録（投稿は実行時）", type="primary", use_container_width=True):
                selected_rows = edited_df[edited_df['選択'] == True] if '選択' in edited_df.columns else pd.DataFrame()
                if len(selected_rows) == 0:
                    st.error("投稿する行を選択してください")
                elif not schedule_times:
                    st.error("有効な予約時刻を入力してください")
                else:
                    progress = st.progress(0)
                    total = len(selected_rows)
                    done = 0
                    for idx, row in selected_rows.iterrows():
                        row_num = idx + 2  # シート上の行番号（1始まり）
                        add_schedule_to_sheet(project_info['worksheet'], row_num, schedule_times)
                        done += 1
                        progress.progress(done / total)
                    st.success("K列以降に予約時刻を記録しました。ステータスは変更しません（実行時のみ更新）。")

    # ダッシュボード
    with tabs[1]:
        st.markdown("### 📊 ダッシュボード")
        df = load_sheet_data(project_info['worksheet'])
        if df.empty:
            st.info("データがありません")
        else:
            status_col = 'ステータス' if 'ステータス' in df.columns else (df.columns[4] if len(df.columns) > 4 else None)
            total_urls = len(df)
            if status_col:
                completed = len(df[df[status_col] == '処理済み'])
                processing = total_urls - completed
            else:
                completed = 0
                processing = total_urls
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("総URL数", total_urls)
            with c2:
                st.metric("処理済み", completed)
            with c3:
                st.metric("未完了", processing)

            st.markdown("#### 予約（K列以降）の状況（先頭5列のみ表示）")
            # K列(=11)以降の推定列名（データエディタで自動付与された場合に “列xx”）
            extra_cols = [c for c in df.columns if re.match(r'^列\d+$', c) and int(c.replace('列', '')) >= 11]
            show_cols = ['宣伝URL'] + ([status_col] if status_col else []) + extra_cols[:5]
            show_cols = [c for c in show_cols if c in df.columns]
            if show_cols:
                st.dataframe(df[show_cols], use_container_width=True)
            else:
                st.info("K列以降の表示対象列が見つかりませんでした。")

    # 設定
    with tabs[2]:
        st.markdown("### ⚙️ 設定 / GitHub Actions")
        st.code("""
# .github/workflows/auto_post.yml
name: Auto Blog Post

on:
  schedule:
    - cron: '0,30 * * * *'  # 30分ごと
  workflow_dispatch:

jobs:
  post:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run scheduled posts
        env:
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          # WP/Livedoor/Seesaa/FC2/認証 等も secrets に
          GOOGLE_APPLICATION_CREDENTIALS_JSON: ${{ secrets.GOOGLE_APPLICATION_CREDENTIALS_JSON }}
        run: |
          python - <<'PY'
from streamlit_app import check_and_execute_scheduled_posts
print(check_and_execute_scheduled_posts())
PY
""", language="yaml")
        st.info("※ 予約はK列に記録するだけ。実際の投稿は上記ワークフローで実行時刻に合わせて行います。")

# ========================
# エントリーポイント
# ========================
if __name__ == "__main__":
    main()
