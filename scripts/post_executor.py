#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
自動投稿実行スクリプト（GitHub Actions用）
- K列(=11)以降の予約時刻を見て、今〜+30分の枠に入ったものだけ投稿
- ステータス「予約済み」は使わない
- 1〜19本目：その他リンク、20本目：宣伝URL（被リンク）
- 20本目が投稿された瞬間にのみ「処理済み」
- WordPressはデフォルト：即時公開（Actionsが時刻どおり実行する方式）
  ※オプションでネイティブの予約投稿（status=future）も可
"""

import os
import re
import io
import json
import time
import base64
import pickle
import random
import logging
import argparse
import requests
import gspread
import xmlrpc.client
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone
from oauth2client.service_account import ServiceAccountCredentials
from requests.auth import HTTPBasicAuth

# ----------------------------
# ログ
# ----------------------------
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/post_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ----------------------------
# 環境変数
# ----------------------------
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '')
CREDENTIALS_FILE = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_FILE', 'credentials.json')
GOOGLE_APPLICATION_CREDENTIALS_JSON = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON', '')

# Gemini
GEMINI_KEYS = [k for k in [
    os.environ.get('GEMINI_API_KEY_1'),
    os.environ.get('GEMINI_API_KEY_2'),
    os.environ.get('GEMINI_API_KEY'),
] if k]
if not GEMINI_KEYS:
    logger.warning("⚠️ GEMINI_API_KEY が設定されていません（記事生成は失敗します）")
_gemini_idx = 0

# 投稿間隔（スパム回避）
MIN_INTERVAL = int(os.environ.get('POST_MIN_INTERVAL', '60'))   # 60s
MAX_INTERVAL = int(os.environ.get('POST_MAX_INTERVAL', '120'))  # 120s

# WPネイティブ予約を使うか（通常は False：Actionsが指定時刻に実行する）
USE_WP_NATIVE_SCHEDULE = os.environ.get('USE_WP_NATIVE_SCHEDULE', 'false').lower() == 'true'

# ----------------------------
# プロジェクト設定
# ----------------------------
PROJECT_CONFIGS = {
    'biggift': {
        'worksheet': 'ビックギフト向け',
        'platforms': ['blogger', 'livedoor'],
        'max_posts': {'blogger': 20, 'livedoor': 15},
    },
    'arigataya': {
        'worksheet': 'ありがた屋向け',
        'platforms': ['seesaa', 'fc2'],
        'max_posts': {'seesaa': 20, 'fc2': 20},
    },
    'kaitori_life': {
        'worksheet': '買取LIFE向け',
        'platforms': ['wordpress'],
        'wp_sites': ['selectad', 'thrones'],
        'max_posts': 20,
    },
    'osaifu_rescue': {
        'worksheet': 'お財布レスキュー向け',
        'platforms': ['wordpress'],
        'wp_sites': ['ykikaku', 'efdlqjtz'],
        'max_posts': 20,
    },
    'kure_kaeru': {
        'worksheet': 'クレかえる向け',
        'platforms': ['wordpress'],
        'wp_sites': ['selectadvance', 'welkenraedt'],
        'max_posts': 20,
    },
    'red_site': {
        'worksheet': '赤いサイト向け',
        'platforms': ['wordpress'],
        'wp_sites': ['ncepqvub', 'kosagi'],
        'max_posts': 20,
    },
}

WP_CONFIGS = {
    'ykikaku': {
        'url': os.environ.get('WP_YKIKAKU_URL'),
        'user': os.environ.get('WP_YKIKAKU_USER'),
        'password': os.environ.get('WP_YKIKAKU_PASSWORD'),
    },
    'efdlqjtz': {
        'url': os.environ.get('WP_EFDLQJTZ_URL'),
        'user': os.environ.get('WP_EFDLQJTZ_USER'),
        'password': os.environ.get('WP_EFDLQJTZ_PASSWORD'),
    },
    'selectadvance': {
        'url': os.environ.get('WP_SELECTADVANCE_URL'),
        'user': os.environ.get('WP_SELECTADVANCE_USER'),
        'password': os.environ.get('WP_SELECTADVANCE_PASSWORD'),
    },
    'welkenraedt': {
        'url': os.environ.get('WP_WELKENRAEDT_URL'),
        'user': os.environ.get('WP_WELKENRAEDT_USER'),
        'password': os.environ.get('WP_WELKENRAEDT_PASSWORD'),
    },
    'ncepqvub': {
        'url': os.environ.get('WP_NCEPQVUB_URL'),
        'user': os.environ.get('WP_NCEPQVUB_USER'),
        'password': os.environ.get('WP_NCEPQVUB_PASSWORD'),
    },
    'kosagi': {
        'url': os.environ.get('WP_KOSAGI_URL'),
        'user': os.environ.get('WP_KOSAGI_USER'),
        'password': os.environ.get('WP_KOSAGI_PASSWORD'),
    },
    'selectad': {
        'url': os.environ.get('WP_SELECTAD_URL'),
        'user': os.environ.get('WP_SELECTAD_USER'),
        'password': os.environ.get('WP_SELECTAD_PASSWORD'),
    },
    'thrones': {
        'url': os.environ.get('WP_THRONES_URL'),
        'user': os.environ.get('WP_THRONES_USER'),
        'password': os.environ.get('WP_THRONES_PASSWORD'),
    },
}

# ----------------------------
# Google Sheets
# ----------------------------
def get_sheets_client():
    """GCP認証: 環境変数JSON or ファイル のどちらでも"""
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    if GOOGLE_APPLICATION_CREDENTIALS_JSON:
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write(GOOGLE_APPLICATION_CREDENTIALS_JSON)
            temp_path = f.name
        creds = ServiceAccountCredentials.from_json_keyfile_name(temp_path, scope)
        client = gspread.authorize(creds)
        try:
            os.unlink(temp_path)
        except Exception:
            pass
        return client
    # fallback: ローカルのcredentials.json
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    return gspread.authorize(creds)

# ----------------------------
# 補助（スプレッドシート列）
# ----------------------------
def _val(row: List[str], idx: int) -> str:
    return row[idx].strip() if len(row) > idx and row[idx] else ""

def _to_int(s: str, default: int = 0) -> int:
    try:
        return int(s)
    except Exception:
        return default

def _max_posts_of(config: Dict, platform_hint: str) -> int:
    max_posts = config.get('max_posts', 20)
    if isinstance(max_posts, dict):
        return int(max_posts.get(platform_hint.lower(), 20))
    return int(max_posts)

# ----------------------------
# 競合 / その他リンク
# ----------------------------
def get_competitor_domains(client) -> List[str]:
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet('競合他社')
        rows = sheet.get_all_values()[1:]
        doms = []
        for r in rows:
            if r and r[0]:
                d = r[0].strip()
                if d.startswith('http'):
                    d = urlparse(d).netloc
                doms.append(d.lower())
        logger.info(f"競合 {len(doms)}件")
        return doms
    except Exception as e:
        logger.warning(f"競合取得失敗: {e}")
        return []

def get_other_links(client) -> List[Dict]:
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet('その他リンク先')
        rows = sheet.get_all_values()[1:]
        items = []
        for r in rows:
            if len(r) >= 2 and r[0] and r[1]:
                items.append({"url": r[0].strip(), "anchor": r[1].strip()})
        if items:
            logger.info(f"その他リンク {len(items)}件")
            return items
    except Exception as e:
        logger.warning(f"その他リンク取得失敗: {e}")
    return [
        {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
        {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"},
    ]

def choose_other_link(other_links: List[Dict], competitor_domains: List[str]) -> Optional[Dict]:
    pool = []
    for s in other_links:
        d = urlparse(s['url']).netloc.lower()
        if not any(comp in d for comp in competitor_domains):
            pool.append(s)
    return random.choice(pool) if pool else None

# ----------------------------
# Gemini
# ----------------------------
def _gemini_key() -> Optional[str]:
    global _gemini_idx
    if not GEMINI_KEYS:
        return None
    k = GEMINI_KEYS[_gemini_idx % len(GEMINI_KEYS)]
    _gemini_idx += 1
    return k

def call_gemini(prompt: str) -> str:
    key = _gemini_key()
    if not key:
        raise RuntimeError("Gemini APIキー未設定")
    url = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent?key={key}'
    payload = {"contents": [{"parts": [{"text": prompt}]}]], "generationConfig": {"temperature": 0.7}}
    r = requests.post(url, json=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Gemini API エラー: {r.status_code} {r.text[:160]}")
    js = r.json()
    return js['candidates'][0]['content']['parts'][0]['text']

def generate_article_with_link(theme: str, url: str, anchor_text: str) -> Dict:
    if not theme or theme.strip() == "":
        theme = "金融・投資・資産運用"
        auto_theme = True
    else:
        auto_theme = False
    theme_instruction = "金融系（投資、クレジットカード、ローン、資産運用など）から自由に" if auto_theme else f"「{theme}」をテーマに"

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
・具体的な数値や事例
・プレースホルダー（〇〇等）禁止
"""
    text = call_gemini(prompt)
    lines = [ln for ln in text.split('\n') if ln.strip()]
    title = lines[0].strip()
    content = '\n'.join(lines[1:]).strip()
    content = re.sub(r'〇〇|××|△△', '', content)
    content = re.sub(r'<p>\s*</p>', '', content)
    return {"title": title, "content": content, "theme": theme if not auto_theme else "金融"}

# ----------------------------
# 投稿（各プラットフォーム）
# ----------------------------
def post_to_wordpress(article: Dict, site_key: str) -> str:
    cfg = WP_CONFIGS.get(site_key, {})
    if not cfg or not cfg.get('url') or not cfg.get('user'):
        return ""
    endpoint = f"{cfg['url']}wp-json/wp/v2/posts"
    data = {'title': article['title'], 'content': article['content'], 'status': 'publish'}
    try:
        r = requests.post(endpoint, auth=HTTPBasicAuth(cfg['user'], cfg['password']),
                          headers={'Content-Type': 'application/json'}, data=json.dumps(data), timeout=60)
        if r.status_code in (200, 201):
            return r.json().get('link', '')
        logger.error(f"WP公開失敗({site_key}): {r.status_code} {r.text[:160]}")
    except Exception as e:
        logger.error(f"WP公開エラー({site_key}): {e}")
    return ""

def post_to_wordpress_future(article: Dict, site_key: str, schedule_dt: datetime) -> str:
    """WPネイティブ予約（希望時のみ）。WP側Cron有効が前提。"""
    cfg = WP_CONFIGS.get(site_key, {})
    if not cfg or not cfg.get('url') or not cfg.get('user'):
        return ""
    endpoint = f"{cfg['url']}wp-json/wp/v2/posts"
    # RESTでは site のタイムゾーン設定に依存。安全にするなら date_gmt を使う
    schedule_gmt = schedule_dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:00')
    data = {
        'title': article['title'],
        'content': article['content'],
        'status': 'future',
        'date_gmt': schedule_gmt,   # or 'date'（サイトTZ）でもOK
    }
    try:
        r = requests.post(endpoint, auth=HTTPBasicAuth(cfg['user'], cfg['password']),
                          headers={'Content-Type': 'application/json'}, data=json.dumps(data), timeout=60)
        if r.status_code in (200, 201):
            return r.json().get('link', '')
        logger.error(f"WP予約失敗({site_key}): {r.status_code} {r.text[:160]}")
    except Exception as e:
        logger.error(f"WP予約エラー({site_key}): {e}")
    return ""

def post_to_livedoor(article: Dict) -> str:
    from xml.sax.saxutils import escape as xml_escape
    import xml.etree.ElementTree as ET
    BLOG = os.environ.get('LIVEDOOR_BLOG_NAME', '')
    UID = os.environ.get('LIVEDOOR_ID', '')
    KEY = os.environ.get('LIVEDOOR_API_KEY', '')
    if not (BLOG and UID and KEY):
        return ""
    endpoint = f"https://livedoor.blogcms.jp/atompub/{BLOG}/article"
    xml = f'''<?xml version="1.0" encoding="utf-8"?>
<entry xmlns="http://www.w3.org/2005/Atom">
  <title>{xml_escape(article["title"])}</title>
  <content type="html">{xml_escape(article["content"])}</content>
</entry>'''.encode('utf-8')
    r = requests.post(endpoint, data=xml,
                      headers={"Content-Type": "application/atom+xml;type=entry"},
                      auth=HTTPBasicAuth(UID, KEY), timeout=30)
    if r.status_code in (200, 201):
        try:
            root = ET.fromstring(r.text)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            alt = root.find(".//atom:link[@rel='alternate']", ns)
            return alt.get("href") if alt is not None else ""
        except Exception:
            pass
    return ""

def post_to_seesaa(article: Dict) -> str:
    ep = "http://blog.seesaa.jp/rpc"
    USER = os.environ.get('SEESAA_USERNAME', '')
    PASS = os.environ.get('SEESAA_PASSWORD', '')
    BLOGID = os.environ.get('SEESAA_BLOGID', '')
    if not (USER and PASS and BLOGID):
        return ""
    server = xmlrpc.client.ServerProxy(ep, allow_none=True)
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
    ep = 'https://blog.fc2.com/xmlrpc.php'
    BLOGID = os.environ.get('FC2_BLOG_ID', '')
    USER = os.environ.get('FC2_USERNAME', '')
    PASS = os.environ.get('FC2_PASSWORD', '')
    if not (BLOGID and USER and PASS):
        return ""
    server = xmlrpc.client.ServerProxy(ep)
    content = {'title': article['title'], 'description': article['content']}
    try:
        post_id = server.metaWeblog.newPost(BLOGID, USER, PASS, content, True)
        return f"https://{BLOGID}.blog.fc2.com/blog-entry-{post_id}.html"
    except Exception as e:
        logger.error(f"FC2投稿エラー: {e}")
        return ""

def post_to_blogger(article: Dict) -> str:
    # 環境依存。必要なら適宜実装
    return ""

# ----------------------------
# 予約投稿の実行
# ----------------------------
def _pick_wp_target(config: Dict, row: List[str]) -> Optional[str]:
    """行の「投稿先」列（2=0始まりで3列目）から対象WPサイトを1つに限定"""
    target = _val(row, 2).lower()
    sites = [s.lower() for s in config.get('wp_sites', [])]
    if not sites:
        return None
    if target and target in sites:
        return target
    return sites[0]  # 未指定なら先頭

def _build_article_by_counter(row: List[str], project_key: str, current_counter: int,
                              max_posts: int, other_links: List[Dict], competitor_domains: List[str]) -> Optional[Dict]:
    theme = _val(row, 0)
    if current_counter == max_posts - 1:
        # 20本目: 宣伝URL＋アンカー
        url = _val(row, 1)
        anchor = _val(row, 3) or project_key
    else:
        site = choose_other_link(other_links, competitor_domains)
        if not site:
            return None
        url, anchor = site['url'], site['anchor']
    return generate_article_with_link(theme, url, anchor)

def execute_single_scheduled_post(row: List[str], project_key: str, config: Dict, sheet,
                                  row_idx_1based: int, schedule_col_1based: int,
                                  scheduled_time: datetime,
                                  other_links: List[Dict], competitor_domains: List[str]) -> bool:
    """
    - 記事生成（1〜19: その他、20: 宣伝URL）
    - プラットフォーム1つだけ投稿
    - 成功したらカウンター+1、Kセル「完了」、20本目で「処理済み」＆完了日時
    """
    # 現カウンター
    cnt = _to_int(_val(row, 6), 0)

    # 最大数
    if 'wordpress' in config['platforms']:
        mp = _max_posts_of(config, 'wordpress')
    else:
        # 代表値
        mp = _max_posts_of(config, 'blogger')

    if cnt >= mp:
        logger.info("最大投稿数に到達済み → スキップ")
        return False

    # 記事
    article = _build_article_by_counter(row, project_key, cnt, mp, other_links, competitor_domains)
    if not article:
        logger.error("記事生成失敗（その他リンク不足）")
        return False

    posted = False
    post_target = _val(row, 2).lower()

    # WordPress系（1サイト限定）
    if 'wordpress' in config['platforms']:
        site = _pick_wp_target(config, row)
        if site:
            if USE_WP_NATIVE_SCHEDULE:
                url = post_to_wordpress_future(article, site, scheduled_time)
            else:
                url = post_to_wordpress(article, site)
            if url:
                posted = True
                logger.info(f"WP投稿成功({site}): {url}")

    # Blogger / Livedoor / Seesaa / FC2
    elif 'blogger' in config['platforms'] and (post_target in ['blogger', '両方', '']):
        url = post_to_blogger(article)
        if url:
            posted = True
            logger.info(f"Blogger成功: {url}")
    elif 'livedoor' in config['platforms'] and (post_target in ['livedoor', '両方', '']):
        url = post_to_livedoor(article)
        if url:
            posted = True
            logger.info(f"livedoor成功: {url}")
    elif 'seesaa' in config['platforms'] and (post_target in ['seesaa', '']):
        url = post_to_seesaa(article)
        if url:
            posted = True
            logger.info(f"Seesaa成功: {url}")
    elif 'fc2' in config['platforms'] and (post_target in ['fc2']):
        url = post_to_fc2(article)
        if url:
            posted = True
            logger.info(f"FC2成功: {url}")

    if not posted:
        logger.error("全プラットフォーム投稿失敗")
        return False

    # 成功後：カウンター+1、Kセル=完了
    cnt += 1
    sheet.update_cell(row_idx_1based, 7, str(cnt))
    sheet.update_cell(row_idx_1based, schedule_col_1based, "完了")

    # 20本目に到達 → 処理済み
    if cnt >= mp:
        sheet.update_cell(row_idx_1based, 5, "処理済み")
        sheet.update_cell(row_idx_1based, 9, datetime.now().strftime("%Y/%m/%d %H:%M"))
        logger.info(f"🎯 行{row_idx_1based} 完了（{mp}本）")

    return True

def check_and_execute_scheduled_posts(window_minutes: int = 30):
    """K列以降の予約を見て、今〜+window分のものを実行。ステータスには依存しない。"""
    logger.info("⏰ 予約チェック開始")
    client = get_sheets_client()
    now = datetime.now()
    window_end = now + timedelta(minutes=window_minutes)

    competitor_domains = get_competitor_domains(client)
    other_links = get_other_links(client)

    total_exec = 0
    total_skip = 0

    for project_key, cfg in PROJECT_CONFIGS.items():
        try:
            sheet = client.open_by_key(SPREADSHEET_ID).worksheet(cfg['worksheet'])
            rows = sheet.get_all_values()
            if len(rows) <= 1:
                continue

            for r_idx, row in enumerate(rows[1:], start=2):
                # すでに処理済みはスキップ
                if _val(row, 4) == '処理済み':
                    continue

                # K列(=11)以降を走査
                for col0 in range(10, len(row)):
                    raw = _val(row, col0)
                    if not raw or raw == '完了':
                        continue
                    try:
                        sched = datetime.strptime(raw, '%Y/%m/%d %H:%M')
                    except Exception:
                        continue

                    # 実行対象（今〜+window）
                    if now <= sched <= window_end:
                        ok = execute_single_scheduled_post(
                            row=row,
                            project_key=project_key,
                            config=cfg,
                            sheet=sheet,
                            row_idx_1based=r_idx,
                            schedule_col_1based=col0 + 1,  # 0→1始まり
                            scheduled_time=sched,
                            other_links=other_links,
                            competitor_domains=competitor_domains
                        )
                        if ok:
                            total_exec += 1
                            time.sleep(random.randint(MIN_INTERVAL, MAX_INTERVAL))
                        else:
                            total_skip += 1

        except Exception as e:
            logger.error(f"ワークシート処理エラー({cfg['worksheet']}): {e}")

    logger.info(f"⏰ 予約チェック完了: 実行 {total_exec} / スキップ {total_skip}")

# ----------------------------
# 即時投稿（任意）
# ----------------------------
def process_project(project_key: str, post_count: int):
    """未処理行から即時に n件 投稿（検証/緊急用）"""
    if project_key not in PROJECT_CONFIGS:
        logger.error(f"未知のプロジェクト: {project_key}")
        return

    cfg = PROJECT_CONFIGS[project_key]
    client = get_sheets_client()
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(cfg['worksheet'])
    rows = sheet.get_all_values()[1:]

    competitor_domains = get_competitor_domains(client)
    other_links = get_other_links(client)

    done = 0
    for i, row in enumerate(rows):
        if done >= post_count:
            break
        # ステータス列（E=5）: 未処理のみ
        if len(row) >= 5 and (_val(row, 4) in ['', '未処理']):
            row_num = i + 2
            cnt = _to_int(_val(row, 6), 0)

            # 最大
            if 'wordpress' in cfg['platforms']:
                mp = _max_posts_of(cfg, 'wordpress')
            else:
                mp = _max_posts_of(cfg, 'blogger')
            if cnt >= mp:
                continue

            try:
                article = _build_article_by_counter(row, project_key, cnt, mp, other_links, competitor_domains)
                if not article:
                    continue

                posted = False
                if 'wordpress' in cfg['platforms']:
                    site = _pick_wp_target(cfg, row)
                    if site:
                        url = post_to_wordpress(article, site)
                        if url:
                            posted = True
                elif 'blogger' in cfg['platforms']:
                    if _val(row, 2).lower() in ['blogger', '両方', '']:
                        url = post_to_blogger(article)
                        posted = bool(url)
                elif 'livedoor' in cfg['platforms']:
                    if _val(row, 2).lower() in ['livedoor', '両方', '']:
                        url = post_to_livedoor(article)
                        posted = bool(url)
                elif 'seesaa' in cfg['platforms']:
                    if _val(row, 2).lower() in ['seesaa', '']:
                        url = post_to_seesaa(article)
                        posted = bool(url)
                elif 'fc2' in cfg['platforms']:
                    if _val(row, 2).lower() in ['fc2']:
                        url = post_to_fc2(article)
                        posted = bool(url)

                if posted:
                    cnt += 1
                    sheet.update_cell(row_num, 7, str(cnt))
                    if cnt >= mp:
                        sheet.update_cell(row_num, 5, "処理済み")
                        sheet.update_cell(row_num, 9, datetime.now().strftime("%Y/%m/%d %H:%M"))
                    done += 1
                    if done < post_count:
                        time.sleep(random.randint(MIN_INTERVAL, MAX_INTERVAL))
            except Exception as e:
                logger.error(f"即時投稿エラー: {e}")
                sheet.update_cell(row_num, 5, "エラー")

    logger.info(f"即時投稿完了: {done}/{post_count}")

# ----------------------------
# CLI
# ----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', default='scheduled', help='scheduled / immediate')
    parser.add_argument('--project', default='all')
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--window', type=int, default=30, help='予約実行のウィンドウ（分）')
    args = parser.parse_args()

    if args.mode == 'scheduled':
        logger.info("⏰ 予約実行モード")
        check_and_execute_scheduled_posts(window_minutes=args.window)
    else:
        logger.info(f"📝 即時投稿モード: {args.project}, count={args.count}")
        if args.project == 'all':
            for k in PROJECT_CONFIGS.keys():
                process_project(k, args.count)
                time.sleep(random.randint(30, 60))
        else:
            process_project(args.project, args.count)

if __name__ == '__main__':
    main()
