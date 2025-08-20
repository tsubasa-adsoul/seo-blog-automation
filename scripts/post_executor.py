#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
K列予約投稿実行スクリプト（GitHub Actions用）
- K列以降の予約時刻をチェック
- 該当時刻の投稿を実行
- 非WordPressプラットフォーム専用（Seesaa、FC2、livedoor、Blogger）
"""

import os
import re
import json
import time
import random
import logging
import requests
import gspread
import xmlrpc.client
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional
from urllib.parse import urlparse
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from requests.auth import HTTPBasicAuth
from xml.sax.saxutils import escape as xml_escape
import tempfile
import argparse

# ----------------------------
# ログ設定
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ----------------------------
# 環境変数
# ----------------------------
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '1sV0r6LavB4BgU7jGaa5C-GdyogUpWr_y42a-tNZXuFo')
GOOGLE_APPLICATION_CREDENTIALS_JSON = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON', '')

# Gemini API設定
GEMINI_KEYS = [k for k in [
    os.environ.get('GEMINI_API_KEY_1'),
    os.environ.get('GEMINI_API_KEY_2'),
    os.environ.get('GEMINI_API_KEY_3'),
] if k]

if not GEMINI_KEYS:
    GEMINI_KEYS = ['AIzaSyBCxQruA6WrmfZHoZ6pTBPRVqkALKvdsT0']

_gemini_idx = 0

# 投稿間隔（スパム回避）
MIN_INTERVAL = int(os.environ.get('POST_MIN_INTERVAL', '60'))
MAX_INTERVAL = int(os.environ.get('POST_MAX_INTERVAL', '120'))

# ----------------------------
# プロジェクト設定（非WordPressのみ）
# ----------------------------
NON_WP_PROJECTS = {
    'biggift': {
        'worksheet': 'ビックギフト向け',
        'platforms': ['blogger', 'livedoor'],
        'max_posts': {'blogger': 20, 'livedoor': 15}
    },
    'arigataya': {
        'worksheet': 'ありがた屋向け',
        'platforms': ['seesaa', 'fc2'],
        'max_posts': 20
    }
}

# プラットフォーム設定
PLATFORM_CONFIGS = {
    'seesaa': {
        'endpoint': "http://blog.seesaa.jp/rpc",
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

# ----------------------------
# Google Sheets認証
# ----------------------------
def get_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    if not GOOGLE_APPLICATION_CREDENTIALS_JSON:
        logger.error("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON環境変数が設定されていません")
        raise RuntimeError("Google認証情報が設定されていません")
    
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

# ----------------------------
# 競合他社・その他リンク管理
# ----------------------------
def get_competitor_domains():
    try:
        client = get_sheets_client()
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
        
        logger.info(f"📋 競合他社ドメイン {len(domains)}件を読み込みました")
        return domains
    except Exception as e:
        logger.warning(f"⚠️ 競合他社リスト取得エラー: {e}")
        return []

def get_other_links():
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet('その他リンク先')
        rows = sheet.get_all_values()[1:]
        
        other_sites = []
        for row in rows:
            if len(row) >= 2 and row[0] and row[1]:
                other_sites.append({
                    "url": row[0].strip(),
                    "anchor": row[1].strip()
                })
        
        logger.info(f"📋 その他リンク先 {len(other_sites)}件を読み込みました")
        
        if not other_sites:
            other_sites = [
                {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
                {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"},
            ]
        
        return other_sites
        
    except Exception as e:
        logger.error(f"❌ その他リンク先の読み込みエラー: {e}")
        return [
            {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
            {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"},
        ]

def choose_other_link(other_links: List[Dict], competitor_domains: List[str]) -> Optional[Dict]:
    available_sites = []
    for site in other_links:
        site_domain = urlparse(site['url']).netloc.lower()
        if not any(comp in site_domain for comp in competitor_domains):
            available_sites.append(site)
    
    if available_sites:
        return random.choice(available_sites)
    
    return None

# ----------------------------
# Gemini記事生成
# ----------------------------
def _get_gemini_key() -> Optional[str]:
    global _gemini_idx
    if not GEMINI_KEYS:
        return None
    key = GEMINI_KEYS[_gemini_idx % len(GEMINI_KEYS)]
    _gemini_idx += 1
    return key

def call_gemini(prompt: str) -> str:
    api_key = _get_gemini_key()
    if not api_key:
        raise RuntimeError("Gemini APIキーが設定されていません")
    
    endpoint = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent?key={api_key}'
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.7}
    }
    
    response = requests.post(endpoint, json=payload, timeout=60)
    if response.status_code != 200:
        raise RuntimeError(f"Gemini API エラー: {response.status_code} {response.text[:200]}")
    
    result = response.json()
    return result['candidates'][0]['content']['parts'][0]['text']

def generate_article_with_link(theme: str, url: str, anchor_text: str) -> Dict:
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
    
    try:
        response = call_gemini(prompt)
        
        lines = response.strip().split('\n')
        title = lines[0].strip()
        content = '\n'.join(lines[1:]).strip()
        
        # HTML内容の検証と修正
        content = re.sub(r'〇〇|××|△△', '', content)
        content = re.sub(r'（ここで.*?）', '', content)
        content = re.sub(r'<p>\s*</p>', '', content)
        content = content.strip()
        
        return {
            "title": title,
            "content": content,
            "theme": theme if not auto_theme else "金融"
        }
        
    except Exception as e:
        logger.error(f"❌ 記事生成エラー: {e}")
        raise

# ----------------------------
# 各プラットフォーム投稿関数
# ----------------------------
def post_to_seesaa(article: dict, category_name: str = None) -> str:
    config = PLATFORM_CONFIGS['seesaa']
    server = xmlrpc.client.ServerProxy(config['endpoint'], allow_none=True)
    content = {"title": article["title"], "description": article["content"]}
    
    try:
        post_id = server.metaWeblog.newPost(
            config['blogid'], 
            config['username'], 
            config['password'], 
            content, 
            True
        )
        
        if category_name:
            try:
                cats = server.mt.getCategoryList(config['blogid'], config['username'], config['password'])
                for c in cats:
                    if c.get("categoryName") == category_name:
                        server.mt.setPostCategories(
                            post_id, config['username'], config['password'],
                            [{"categoryId": c.get("categoryId"), "isPrimary": True}]
                        )
                        break
            except Exception:
                pass
        
        try:
            post = server.metaWeblog.getPost(post_id, config['username'], config['password'])
            return post.get("permalink") or post.get("link") or ""
        except Exception:
            return f"post_id:{post_id}"
            
    except Exception as e:
        logger.error(f"❌ Seesaa投稿エラー: {e}")
        return ""

def post_to_fc2(article: dict, category_name: str = None) -> str:
    config = PLATFORM_CONFIGS['fc2']
    server = xmlrpc.client.ServerProxy(config['endpoint'])
    content = {'title': article['title'], 'description': article['content']}
    
    try:
        post_id = server.metaWeblog.newPost(
            config['blog_id'], 
            config['username'], 
            config['password'], 
            content, 
            True
        )
        
        if category_name:
            try:
                cats = server.mt.getCategoryList(config['blog_id'], config['username'], config['password'])
                for c in cats:
                    if c.get('categoryName') == category_name:
                        server.mt.setPostCategories(post_id, config['username'], config['password'], [c])
                        break
            except Exception:
                pass
        
        return f"https://{config['blog_id']}.blog.fc2.com/blog-entry-{post_id}.html"
        
    except Exception as e:
        logger.error(f"❌ FC2投稿エラー: {e}")
        return ""

def post_to_livedoor(article: dict, category_name: str = None) -> str:
    config = PLATFORM_CONFIGS['livedoor']
    root = f"https://livedoor.blogcms.jp/atompub/{config['blog_name']}"
    endpoint = f"{root}/article"
    
    title_xml = xml_escape(article["title"])
    content_xml = xml_escape(article["content"])
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
                return alt.get("href") if alt is not None else ""
            except Exception:
                return ""
        else:
            logger.error(f"❌ livedoor投稿失敗: {response.status_code}")
            return ""
            
    except Exception as e:
        logger.error(f"❌ livedoor投稿エラー: {e}")
        return ""

def post_to_blogger(article: dict) -> str:
    """Blogger投稿（簡易実装）"""
    logger.warning("⚠️ Blogger投稿は未実装（認証が複雑なため）")
    return ""

# ----------------------------
# ユーティリティ関数
# ----------------------------
def get_value_safe(row: List[str], idx: int) -> str:
    return row[idx].strip() if len(row) > idx and row[idx] else ""

def to_int_safe(s: str, default: int = 0) -> int:
    try:
        return int(s)
    except Exception:
        return default

def get_max_posts_for_project(project_key: str, post_target: str = "") -> int:
    config = NON_WP_PROJECTS[project_key]
    max_posts = config['max_posts']
    
    if isinstance(max_posts, dict):
        # ビックギフトの場合
        if post_target.lower() == 'livedoor':
            return 15
        elif post_target.lower() == 'blogger':
            return 20
        else:
            return 20  # デフォルト
    else:
        return max_posts

# ----------------------------
# K列予約実行メイン処理
# ----------------------------
def execute_scheduled_post(row: List[str], project_key: str, sheet, row_idx: int, 
                         col_idx: int, other_links: List[Dict], competitor_domains: List[str]) -> bool:
    """
    単一の予約投稿を実行
    - 記事生成（1〜N-1: その他リンク / N: 宣伝URL）
    - プラットフォーム投稿
    - カウンター更新
    - K列該当セルを「完了」に更新
    - 最終記事時に「処理済み」＆I列日時記録
    """
    try:
        config = NON_WP_PROJECTS[project_key]
        
        # 現在のカウンター取得
        current_counter = to_int_safe(get_value_safe(row, 6), 0)
        
        # 投稿先取得
        post_target = get_value_safe(row, 2) or (config['platforms'][0] if config['platforms'] else '')
        max_posts = get_max_posts_for_project(project_key, post_target)
        
        if current_counter >= max_posts:
            logger.info(f"⚠️ 既に{max_posts}記事完了済み - スキップ")
            return False
        
        # 記事内容決定
        if current_counter == max_posts - 1:
            # 最終記事：宣伝URL
            logger.info(f"📊 {max_posts}記事目 → 宣伝URL使用")
            url = get_value_safe(row, 1)
            anchor = get_value_safe(row, 3) or project_key
            category = get_value_safe(row, 7) if len(row) >= 8 else None
        else:
            # 1〜N-1記事目：その他リンク
            logger.info(f"📊 {current_counter + 1}記事目 → その他リンク使用")
            chosen_link = choose_other_link(other_links, competitor_domains)
            if not chosen_link:
                logger.error("❌ その他リンクが取得できません")
                return False
            
            url = chosen_link['url']
            anchor = chosen_link['anchor']
            category = 'お金のマメ知識'
        
        # 記事生成
        logger.info("🧠 記事を生成中...")
        theme = get_value_safe(row, 0) or '金融・投資・資産運用'
        article = generate_article_with_link(theme, url, anchor)
        
        logger.info(f"📝 タイトル: {article['title']}")
        logger.info(f"🔗 使用リンク: {anchor}")
        
        # プラットフォーム別投稿
        posted_urls = []
        
        if 'seesaa' in config['platforms'] and (not post_target or post_target == 'seesaa'):
            logger.info("📤 Seesaaに投稿中...")
            url_result = post_to_seesaa(article, category)
            if url_result:
                posted_urls.append(url_result)
                logger.info(f"✅ Seesaa投稿成功: {url_result}")
        
        if 'fc2' in config['platforms'] and (not post_target or post_target == 'fc2'):
            logger.info("📤 FC2に投稿中...")
            url_result = post_to_fc2(article, category)
            if url_result:
                posted_urls.append(url_result)
                logger.info(f"✅ FC2投稿成功: {url_result}")
        
        if 'livedoor' in config['platforms'] and (not post_target or post_target == 'livedoor'):
            logger.info("📤 livedoorに投稿中...")
            url_result = post_to_livedoor(article, category)
            if url_result:
                posted_urls.append(url_result)
                logger.info(f"✅ livedoor投稿成功: {url_result}")
        
        if 'blogger' in config['platforms'] and (not post_target or post_target == 'blogger'):
            logger.info("📤 Bloggerに投稿中...")
            url_result = post_to_blogger(article)
            if url_result:
                posted_urls.append(url_result)
                logger.info(f"✅ Blogger投稿成功: {url_result}")
        
        if not posted_urls:
            logger.error("❌ 全プラットフォーム投稿失敗")
            return False
        
        # スプレッドシート更新
        new_counter = current_counter + 1
        
        # カウンター更新（G列=7）
        sheet.update_cell(row_idx, 7, str(new_counter))
        
        # K列該当セルを「完了」に更新
        sheet.update_cell(row_idx, col_idx, "完了")
        
        # 最終記事完了時の処理
        if new_counter >= max_posts:
            # ステータス「処理済み」（E列=5）
            sheet.update_cell(row_idx, 5, "処理済み")
            
            # 投稿URL（F列=6）
            sheet.update_cell(row_idx, 6, ', '.join(posted_urls))
            
            # I列（9列目）に日時記録
            completion_time = datetime.now().strftime("%Y/%m/%d %H:%M")
            sheet.update_cell(row_idx, 9, completion_time)
            
            logger.info(f"🎯 {project_key} 行{row_idx} 完了（{max_posts}記事達成）")
        else:
            logger.info(f"📊 カウンター更新: {new_counter}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 投稿実行エラー: {e}")
        return False

def check_and_execute_k_column_schedules(window_minutes: int = 30) -> Dict[str, int]:
    """
    K列予約投稿チェック＆実行
    - 非WordPressプロジェクトのK列以降をチェック
    - 現在時刻〜+window_minutes内の予約を実行
    """
    logger.info("⏰ K列予約投稿チェック開始")
    client = get_sheets_client()
    now = datetime.now()
    window_end = now + timedelta(minutes=window_minutes)
    
    # 共通データ取得
    competitor_domains = get_competitor_domains()
    other_links = get_other_links()
    
    executed_total = 0
    skipped_total = 0
    
    for project_key, config in NON_WP_PROJECTS.items():
        try:
            logger.info(f"📋 {project_key} ({config['worksheet']}) チェック中...")
            sheet = client.open_by_key(SPREADSHEET_ID).worksheet(config['worksheet'])
            rows = sheet.get_all_values()
            
            if len(rows) <= 1:
                logger.info(f"⚠️ {config['worksheet']} にデータなし")
                continue
            
            for row_idx, row in enumerate(rows[1:], start=2):
                # すでに処理済みはスキップ
                status = get_value_safe(row, 4)
                if status == '処理済み':
                    continue
                
                # K列(11)以降をチェック
                for col_idx_0based in range(10, len(row)):
                    cell_value = row[col_idx_0based].strip() if row[col_idx_0based] else ''
                    
                    if not cell_value or cell_value == '完了':
                        continue
                    
                    # 日時解析
                    try:
                        scheduled_time = datetime.strptime(cell_value, '%Y/%m/%d %H:%M')
                    except Exception:
                        continue
                    
                    # 実行対象判定（現在時刻〜+window_minutes内）
                    if now <= scheduled_time <= window_end:
                        logger.info(f"🚀 実行対象: {project_key} 行{row_idx} 列{col_idx_0based+1} {scheduled_time.strftime('%Y/%m/%d %H:%M')}")
                        
                        success = execute_scheduled_post(
                            row=row,
                            project_key=project_key,
                            sheet=sheet,
                            row_idx=row_idx,
                            col_idx=col_idx_0based + 1,  # 1始まりの列番号
                            other_links=other_links,
                            competitor_domains=competitor_domains
                        )
                        
                        if success:
                            executed_total += 1
                            # 連投防止の待機
                            if executed_total % 3 == 0:  # 3投稿ごとに休憩
                                wait_time = random.randint(MIN_INTERVAL, MAX_INTERVAL)
                                logger.info(f"⏳ 連投防止: {wait_time}秒待機中...")
                                time.sleep(wait_time)
                        else:
                            skipped_total += 1
        
        except Exception as e:
            logger.error(f"❌ {project_key} 処理エラー: {e}")
    
    logger.info(f"⏰ K列予約チェック完了: 実行 {executed_total} / スキップ {skipped_total}")
    return {"executed": executed_total, "skipped": skipped_total}

# ----------------------------
# メイン実行
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description='K列予約投稿実行スクリプト（GitHub Actions用）')
    parser.add_argument('--window', type=int, default=30, help='実行ウィンドウ（分）')
    parser.add_argument('--test', action='store_true', help='テストモード')
    
    args = parser.parse_args()
    
    if args.test:
        logger.info("🧪 テストモード - 実際の投稿は行いません")
        # テスト用のデータチェックのみ
        try:
            client = get_sheets_client()
            logger.info("✅ Google Sheets接続成功")
            
            competitor_domains = get_competitor_domains()
            other_links = get_other_links()
            logger.info(f"✅ データ取得成功: 競合{len(competitor_domains)}件、その他{len(other_links)}件")
            
            logger.info("🧪 テストモード完了")
        except Exception as e:
            logger.error(f"❌ テスト失敗: {e}")
            exit(1)
        return
    
    logger.info(f"🚀 K列予約投稿チェック開始: ウィンドウ={args.window}分")
    
    try:
        result = check_and_execute_k_column_schedules(window_minutes=args.window)
        logger.info(f"✅ 処理完了: {result}")
        
        # GitHub Actions用の出力
        print(f"::set-output name=executed::{result['executed']}")
        print(f"::set-output name=skipped::{result['skipped']}")
        
    except Exception as e:
        logger.error(f"❌ メイン処理エラー: {e}")
        exit(1)

if __name__ == '__main__':
    main()
