#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
自動投稿実行スクリプト（GitHub Actions用）
"""

import os
import sys
import json
import time
import random
import argparse
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import xmlrpc.client
from urllib.parse import urlparse
from requests.auth import HTTPBasicAuth
import base64
import pickle
import re

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/post_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 環境変数から設定を読み込み
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '1sV0r6LavB4BgU7jGaa5C-GdyogUpWr_y42a-tNZXuFo')
CREDENTIALS_FILE = 'credentials.json'

# Gemini API設定
GEMINI_KEYS = [
    os.environ.get('GEMINI_API_KEY_1'),
    os.environ.get('GEMINI_API_KEY_2')
]
current_gemini_key_index = 0

# 投稿間隔設定
MIN_INTERVAL = 300  # 5分
MAX_INTERVAL = 600  # 10分

# ========================
# Google Sheets認証
# ========================
def get_sheets_client():
    """Google Sheetsクライアントを取得"""
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    return gspread.authorize(creds)

# ========================
# プロジェクト設定
# ========================
PROJECT_CONFIGS = {
    'biggift': {
        'worksheet': 'ビックギフト向け',
        'platforms': ['blogger', 'livedoor'],
        'max_posts': {'blogger': 20, 'livedoor': 15}
    },
    'arigataya': {
        'worksheet': 'ありがた屋向け',
        'platforms': ['seesaa', 'fc2'],
        'max_posts': {'seesaa': 20, 'fc2': 20}
    },
    'kaitori_life': {
        'worksheet': '買取LIFE向け',
        'platforms': ['wordpress'],
        'wp_sites': ['selectad', 'thrones'],
        'max_posts': 20
    },
    'osaifu_rescue': {
        'worksheet': 'お財布レスキュー向け',
        'platforms': ['wordpress'],
        'wp_sites': ['ykikaku', 'efdlqjtz'],
        'max_posts': 20
    },
    'kure_kaeru': {
        'worksheet': 'クレかえる向け',
        'platforms': ['wordpress'],
        'wp_sites': ['selectadvance', 'welkenraedt'],
        'max_posts': 20
    },
    'red_site': {
        'worksheet': '赤いサイト向け',
        'platforms': ['wordpress'],
        'wp_sites': ['ncepqvub', 'kosagi'],
        'max_posts': 20
    }
}

# WordPress設定
WP_CONFIGS = {
    'ykikaku': {
        'url': os.environ.get('WP_YKIKAKU_URL'),
        'user': os.environ.get('WP_YKIKAKU_USER'),
        'password': os.environ.get('WP_YKIKAKU_PASSWORD')
    },
    'efdlqjtz': {
        'url': os.environ.get('WP_EFDLQJTZ_URL'),
        'user': os.environ.get('WP_EFDLQJTZ_USER'),
        'password': os.environ.get('WP_EFDLQJTZ_PASSWORD')
    },
    'selectadvance': {
        'url': os.environ.get('WP_SELECTADVANCE_URL'),
        'user': os.environ.get('WP_SELECTADVANCE_USER'),
        'password': os.environ.get('WP_SELECTADVANCE_PASSWORD')
    },
    'welkenraedt': {
        'url': os.environ.get('WP_WELKENRAEDT_URL'),
        'user': os.environ.get('WP_WELKENRAEDT_USER'),
        'password': os.environ.get('WP_WELKENRAEDT_PASSWORD')
    },
    'ncepqvub': {
        'url': os.environ.get('WP_NCEPQVUB_URL'),
        'user': os.environ.get('WP_NCEPQVUB_USER'),
        'password': os.environ.get('WP_NCEPQVUB_PASSWORD')
    },
    'kosagi': {
        'url': os.environ.get('WP_KOSAGI_URL'),
        'user': os.environ.get('WP_KOSAGI_USER'),
        'password': os.environ.get('WP_KOSAGI_PASSWORD')
    },
    'selectad': {
        'url': os.environ.get('WP_SELECTAD_URL'),
        'user': os.environ.get('WP_SELECTAD_USER'),
        'password': os.environ.get('WP_SELECTAD_PASSWORD')
    },
    'thrones': {
        'url': os.environ.get('WP_THRONES_URL'),
        'user': os.environ.get('WP_THRONES_USER'),
        'password': os.environ.get('WP_THRONES_PASSWORD')
    }
}

# ========================
# 共通関数
# ========================
def get_competitor_domains(client) -> List[str]:
    """競合他社ドメインリストを取得"""
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet('競合他社')
        competitors = sheet.get_all_values()[1:]
        domains = []
        for row in competitors:
            if row and row[0]:
                domain = row[0].strip()
                if domain.startswith('http'):
                    parsed = urlparse(domain)
                    domain = parsed.netloc
                domains.append(domain.lower())
        logger.info(f"競合他社ドメイン {len(domains)}件を読み込みました")
        return domains
    except Exception as e:
        logger.warning(f"競合他社リスト取得エラー: {e}")
        return []

def get_other_links(client) -> List[Dict]:
    """その他のリンク先を取得"""
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet('その他リンク先')
        rows = sheet.get_all_values()[1:]
        other_sites = []
        for row in rows:
            if len(row) >= 2 and row[0] and row[1]:
                other_sites.append({
                    "url": row[0].strip(),
                    "anchor": row[1].strip()
                })
        if not other_sites:
            other_sites = [
                {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
                {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"}
            ]
        logger.info(f"その他リンク先 {len(other_sites)}件を読み込みました")
        return other_sites
    except Exception as e:
        logger.warning(f"その他リンク先の読み込みエラー: {e}")
        return [
            {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
            {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"}
        ]

def choose_other_link(other_links: List[Dict], competitor_domains: List[str]) -> Optional[Dict]:
    """競合以外のリンクを選択"""
    available_sites = []
    for site in other_links:
        site_domain = urlparse(site['url']).netloc.lower()
        if not any(comp in site_domain for comp in competitor_domains):
            available_sites.append(site)
    
    if available_sites:
        return random.choice(available_sites)
    return None

# ========================
# Gemini API
# ========================
def call_gemini(prompt: str) -> str:
    """Gemini APIを呼び出し"""
    global current_gemini_key_index
    
    # 利用可能なAPIキーを選択
    api_key = GEMINI_KEYS[current_gemini_key_index % len(GEMINI_KEYS)]
    current_gemini_key_index += 1
    
    endpoint = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent?key={api_key}'
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.7}
    }
    
    response = requests.post(endpoint, json=payload, timeout=60)
    if response.status_code != 200:
        raise Exception(f"Gemini API エラー: {response.status_code}")
    
    result = response.json()
    return result['candidates'][0]['content']['parts'][0]['text']

def generate_article_with_link(theme: str, url: str, anchor_text: str) -> Dict:
    """記事を生成"""
    if not theme or theme.strip() == "":
        theme = "金融・投資・資産運用"
        auto_theme = True
    else:
        auto_theme = False
    
    if auto_theme:
        theme_instruction = "金融系（投資、クレジットカード、ローン、資産運用など）から自由にテーマを選んで"
    else:
        theme_instruction = f"「{theme}」をテーマに"
    
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
・読者の悩みを解決する内容

# 重要:
・プレースホルダー（〇〇など）は使用禁止
・すべて具体的な内容で記述
・リンクは指定されたものを正確に使用
"""
    
    try:
        response = call_gemini(prompt)
        lines = response.strip().split('\n')
        title = lines[0].strip()
        content = '\n'.join(lines[1:]).strip()
        
        # HTMLコンテンツの検証
        content = re.sub(r'〇〇|××|△△', '', content)
        content = re.sub(r'（ここで.*?）', '', content)
        content = re.sub(r'<p>\s*</p>', '', content)
        
        return {
            "title": title,
            "content": content.strip(),
            "theme": theme if not auto_theme else "金融"
        }
    except Exception as e:
        logger.error(f"記事生成エラー: {e}")
        raise

# ========================
# 各プラットフォーム投稿関数
# ========================

def post_to_blogger(article_data: Dict) -> str:
    """Bloggerに投稿"""
    try:
        # 環境変数から認証情報を取得
        BLOG_ID = os.environ.get('BLOGGER_BLOG_ID')
        credentials_base64 = os.environ.get('BLOGGER_CREDENTIALS')
        token_base64 = os.environ.get('BLOGGER_TOKEN')
        
        if credentials_base64:
            # Base64デコード
            credentials_json = base64.b64decode(credentials_base64).decode('utf-8')
            with open('blogger_credentials.json', 'w') as f:
                f.write(credentials_json)
        
        if token_base64:
            # Base64デコード
            token_data = base64.b64decode(token_base64)
            with open('blogger_token.pickle', 'wb') as f:
                f.write(token_data)
        
        # Blogger API使用
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
        
        SCOPES = ['https://www.googleapis.com/auth/blogger']
        creds = None
        
        if os.path.exists('blogger_token.pickle'):
            with open('blogger_token.pickle', 'rb') as token:
                creds = pickle.load(token)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                with open('blogger_token.pickle', 'wb') as token:
                    pickle.dump(creds, token)
        
        service = build('blogger', 'v3', credentials=creds)
        
        post = {
            'title': article_data['title'],
            'content': article_data['content'],
            'labels': [article_data.get('theme', '金融')]
        }
        
        response = service.posts().insert(
            blogId=BLOG_ID,
            body=post,
            isDraft=False
        ).execute()
        
        if response and 'url' in response:
            logger.info(f"Blogger投稿成功: {response['url']}")
            return response['url']
        else:
            logger.error("Blogger投稿失敗")
            return ""
            
    except Exception as e:
        logger.error(f"Blogger投稿エラー: {e}")
        return ""

def post_to_livedoor(article: Dict) -> str:
    """livedoorブログに投稿"""
    from xml.sax.saxutils import escape as xml_escape
    import xml.etree.ElementTree as ET
    
    LIVEDOOR_BLOG_NAME = os.environ.get('LIVEDOOR_BLOG_NAME')
    LIVEDOOR_ID = os.environ.get('LIVEDOOR_ID')
    LIVEDOOR_API_KEY = os.environ.get('LIVEDOOR_API_KEY')
    
    root_url = f"https://livedoor.blogcms.jp/atompub/{LIVEDOOR_BLOG_NAME}"
    endpoint = f"{root_url}/article"
    
    title_xml = xml_escape(article["title"])
    content_xml = xml_escape(article["content"])
    
    entry_xml = f'''<?xml version="1.0" encoding="utf-8"?>
<entry xmlns="http://www.w3.org/2005/Atom">
  <title>{title_xml}</title>
  <content type="html">{content_xml}</content>
</entry>'''.encode("utf-8")
    
    response = requests.post(
        endpoint,
        data=entry_xml,
        headers={"Content-Type": "application/atom+xml;type=entry"},
        auth=HTTPBasicAuth(LIVEDOOR_ID, LIVEDOOR_API_KEY),
        timeout=30
    )
    
    if response.status_code in (200, 201):
        try:
            root_xml = ET.fromstring(response.text)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            alt = root_xml.find(".//atom:link[@rel='alternate']", ns)
            url = alt.get("href") if alt is not None else ""
            if url:
                logger.info(f"livedoor投稿成功: {url}")
                return url
        except:
            pass
    
    logger.error(f"livedoor投稿失敗: {response.status_code}")
    return ""

def post_to_seesaa(article: Dict) -> str:
    """Seesaaブログに投稿"""
    SEESAA_ENDPOINT = "http://blog.seesaa.jp/rpc"
    SEESAA_USERNAME = os.environ.get('SEESAA_USERNAME')
    SEESAA_PASSWORD = os.environ.get('SEESAA_PASSWORD')
    SEESAA_BLOGID = os.environ.get('SEESAA_BLOGID')
    
    server = xmlrpc.client.ServerProxy(SEESAA_ENDPOINT, allow_none=True)
    
    content = {
        "title": article["title"],
        "description": article["content"]
    }
    
    try:
        post_id = server.metaWeblog.newPost(
            SEESAA_BLOGID,
            SEESAA_USERNAME,
            SEESAA_PASSWORD,
            content,
            True
        )
        
        logger.info(f"Seesaa投稿成功: post_id={post_id}")
        
        # URLを取得
        try:
            post = server.metaWeblog.getPost(post_id, SEESAA_USERNAME, SEESAA_PASSWORD)
            url = post.get("permalink") or post.get("link") or ""
            if url:
                return url
        except:
            pass
        
        return f"post_id:{post_id}"
        
    except Exception as e:
        logger.error(f"Seesaa投稿エラー: {e}")
        return ""

def post_to_fc2(article: Dict) -> str:
    """FC2ブログに投稿"""
    FC2_ENDPOINT = 'https://blog.fc2.com/xmlrpc.php'
    FC2_BLOG_ID = os.environ.get('FC2_BLOG_ID')
    FC2_USERNAME = os.environ.get('FC2_USERNAME')
    FC2_PASSWORD = os.environ.get('FC2_PASSWORD')
    
    server = xmlrpc.client.ServerProxy(FC2_ENDPOINT)
    
    content = {
        'title': article['title'],
        'description': article['content']
    }
    
    try:
        post_id = server.metaWeblog.newPost(
            FC2_BLOG_ID,
            FC2_USERNAME,
            FC2_PASSWORD,
            content,
            True
        )
        
        logger.info(f"FC2投稿成功: post_id={post_id}")
        return f"https://{FC2_BLOG_ID}.blog.fc2.com/blog-entry-{post_id}.html"
        
    except Exception as e:
        logger.error(f"FC2投稿エラー: {e}")
        return ""

def post_to_wordpress(article_data: Dict, site_key: str) -> str:
    """WordPressに投稿"""
    if site_key not in WP_CONFIGS:
        logger.error(f"不明なサイト: {site_key}")
        return ""
    
    site_config = WP_CONFIGS[site_key]
    
    if not site_config['user']:
        logger.warning(f"{site_key}の認証情報が設定されていません")
        return ""
    
    # kosagi は XMLRPC を使用
    if site_key == 'kosagi':
        return post_to_wordpress_xmlrpc(article_data, site_config)
    
    # その他は REST API を使用
    endpoint = f"{site_config['url']}wp-json/wp/v2/posts"
    
    post_data = {
        'title': article_data['title'],
        'content': article_data['content'],
        'status': 'publish'
    }
    
    try:
        response = requests.post(
            endpoint,
            auth=HTTPBasicAuth(site_config['user'], site_config['password']),
            headers={'Content-Type': 'application/json'},
            data=json.dumps(post_data)
        )
        
        if response.status_code in (201, 200):
            data = response.json()
            url = data.get('link', '')
            logger.info(f"WordPress投稿成功 ({site_key}): {url}")
            return url
        else:
            logger.error(f"WordPress投稿失敗 ({site_key}): {response.status_code}")
            return ""
            
    except Exception as e:
        logger.error(f"WordPress投稿エラー ({site_key}): {e}")
        return ""

def post_to_wordpress_xmlrpc(article_data: Dict, site_config: Dict) -> str:
    """WordPress XMLRPC投稿（kosagi用）"""
    import html
    
    endpoint = f"{site_config['url']}xmlrpc.php"
    escaped_title = html.escape(article_data['title'])
    content = article_data['content']
    
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
                    <member>
                        <name>post_type</name>
                        <value><string>post</string></value>
                    </member>
                    <member>
                        <name>post_status</name>
                        <value><string>publish</string></value>
                    </member>
                    <member>
                        <name>post_title</name>
                        <value><string>{escaped_title}</string></value>
                    </member>
                    <member>
                        <name>post_content</name>
                        <value><string><![CDATA[{content}]]></string></value>
                    </member>
                </struct>
            </value>
        </param>
    </params>
</methodCall>"""
    
    try:
        response = requests.post(
            endpoint,
            data=xml_request.encode('utf-8'),
            headers={
                'Content-Type': 'text/xml; charset=UTF-8',
                'User-Agent': 'WordPress XML-RPC Client'
            }
        )
        
        if response.status_code == 200:
            match = re.search(r'<string>(\d+)</string>', response.text)
            if match:
                post_id = match.group(1)
                post_url = f"{site_config['url']}?p={post_id}"
                logger.info(f"WordPress投稿成功 (XMLRPC): {post_url}")
                return post_url
                
    except Exception as e:
        logger.error(f"WordPress XMLRPC投稿エラー: {e}")
    
    return ""

# ========================
# メイン処理
# ========================
def process_project(project_name: str, post_count: int):
    """プロジェクトの投稿処理"""
    if project_name not in PROJECT_CONFIGS:
        logger.error(f"不明なプロジェクト: {project_name}")
        return
    
    config = PROJECT_CONFIGS[project_name]
    client = get_sheets_client()
    
    # シートからデータを取得
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(config['worksheet'])
    rows = sheet.get_all_values()[1:]
    
    # 競合ドメインとその他リンクを取得
    competitor_domains = get_competitor_domains(client)
    other_links = get_other_links(client)
    
    posts_completed = 0
    
    for idx, row in enumerate(rows):
        if posts_completed >= post_count:
            break
        
        # 未処理の行を探す
        if len(row) >= 5 and row[1] and row[4].strip().lower() in ['', '未処理']:
            row_num = idx + 2
            
            # カウンター取得
            current_counter = 0
            if len(row) >= 7 and row[6]:
                try:
                    current_counter = int(row[6])
                except:
                    current_counter = 0
            
            # 最大投稿数チェック
            max_posts = config.get('max_posts', 20)
            if isinstance(max_posts, dict):
                platform = row[2] if len(row) > 2 else list(max_posts.keys())[0]
                max_posts = max_posts.get(platform.lower(), 20)
            
            if current_counter >= max_posts:
                continue
            
            # 記事生成
            try:
                if current_counter == max_posts - 1:
                    # 最終記事：宣伝URLを使用
                    logger.info(f"最終記事 ({max_posts}記事目) を生成中...")
                    article = generate_article_with_link(
                        row[0] if row[0] else '',
                        row[1],
                        row[3] if len(row) >= 4 and row[3] else project_name
                    )
                else:
                    # その他リンクを使用
                    logger.info(f"記事 {current_counter + 1}/{max_posts-1} を生成中...")
                    other_link = choose_other_link(other_links, competitor_domains)
                    if not other_link:
                        logger.error("その他リンクが取得できません")
                        continue
                    
                    article = generate_article_with_link(
                        row[0] if row[0] else '',
                        other_link['url'],
                        other_link['anchor']
                    )
                
                # 投稿処理
                posted = False
                post_target = row[2] if len(row) > 2 else ''
                
                if 'blogger' in config['platforms']:
                    if post_target.lower() in ['blogger', '両方', '']:
                        url = post_to_blogger(article)
                        if url:
                            posted = True
                            logger.info(f"Blogger投稿成功: {url}")
                
                if 'livedoor' in config['platforms']:
                    if post_target.lower() in ['livedoor', '両方', '']:
                        url = post_to_livedoor(article)
                        if url:
                            posted = True
                            logger.info(f"livedoor投稿成功: {url}")
                
                if 'seesaa' in config['platforms']:
                    if post_target.lower() in ['seesaa', '']:
                        url = post_to_seesaa(article)
                        if url:
                            posted = True
                            logger.info(f"Seesaa投稿成功: {url}")
                
                if 'fc2' in config['platforms']:
                    if post_target.lower() in ['fc2']:
                        url = post_to_fc2(article)
                        if url:
                            posted = True
                            logger.info(f"FC2投稿成功: {url}")
                
                if 'wordpress' in config['platforms']:
                    for wp_site in config.get('wp_sites', []):
                        if post_target.lower() in [wp_site, '両方', '']:
                            url = post_to_wordpress(article, wp_site)
                            if url:
                                posted = True
                                logger.info(f"WordPress ({wp_site}) 投稿成功: {url}")
                
                # カウンター更新
                if posted:
                    current_counter += 1
                    sheet.update_cell(row_num, 7, str(current_counter))
                    
                    # 最終記事の場合は処理済みにする
                    if current_counter >= max_posts:
                        sheet.update_cell(row_num, 5, "処理済み")
                        sheet.update_cell(row_num, 9, datetime.now().strftime("%Y/%m/%d %H:%M"))
                    
                    posts_completed += 1
                    logger.info(f"投稿完了: {posts_completed}/{post_count}")
                    
                    # 待機
                    if posts_completed < post_count:
                        wait_time = random.randint(MIN_INTERVAL, MAX_INTERVAL)
                        logger.info(f"{wait_time}秒待機中...")
                        time.sleep(wait_time)
                
            except Exception as e:
                logger.error(f"投稿処理エラー: {e}")
                sheet.update_cell(row_num, 5, "エラー")
    
    logger.info(f"プロジェクト {project_name} の処理完了: {posts_completed}記事投稿")

def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(description='ブログ自動投稿スクリプト')
    parser.add_argument('--project', default='all', help='プロジェクト名')
    parser.add_argument('--count', type=int, default=1, help='投稿数')
    args = parser.parse_args()
    
    # ログディレクトリ作成
    os.makedirs('logs', exist_ok=True)
    
    logger.info(f"自動投稿開始: project={args.project}, count={args.count}")
    
    if args.project == 'all':
        # 全プロジェクトを処理
        for project_name in PROJECT_CONFIGS.keys():
            logger.info(f"プロジェクト {project_name} を処理中...")
            process_project(project_name, args.count)
            
            # プロジェクト間の待機
            if project_name != list(PROJECT_CONFIGS.keys())[-1]:
                wait_time = random.randint(60, 120)
                logger.info(f"次のプロジェクトまで {wait_time}秒待機...")
                time.sleep(wait_time)
    else:
        # 指定プロジェクトのみ処理
        process_project(args.project, args.count)
    
    logger.info("自動投稿完了")

if __name__ == "__main__":
    main()
