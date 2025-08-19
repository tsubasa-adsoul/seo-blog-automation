#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
統合ブログ投稿管理システム - Streamlit版（予約投稿対応完全版）
"""

import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import random
from datetime import datetime, timedelta
import requests
from typing import Dict, List, Optional
import json
import base64
from urllib.parse import urlparse
import re
import io
from PIL import Image, ImageDraw, ImageFont
import xmlrpc.client
import tempfile
import os
import threading
import queue

# ページ設定
st.set_page_config(
    page_title="📝 統合ブログ投稿管理システム",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========================
# セッション状態の初期化
# ========================
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'username' not in st.session_state:
    st.session_state.username = None
if 'posting_status' not in st.session_state:
    st.session_state.posting_status = {}
if 'selected_project' not in st.session_state:
    st.session_state.selected_project = None
if 'scheduled_posts' not in st.session_state:
    st.session_state.scheduled_posts = []
if 'scheduler_running' not in st.session_state:
    st.session_state.scheduler_running = False

# ========================
# 設定
# ========================
SPREADSHEET_ID = st.secrets.google.spreadsheet_id

# プロジェクト設定
PROJECTS = {
    'ビックギフト': {
        'worksheet': 'ビックギフト向け',
        'icon': '🎁',
        'color': '#ff8c00',
        'platforms': ['Blogger', 'livedoor'],
        'max_posts': {'Blogger': 20, 'livedoor': 15}
    },
    'ありがた屋': {
        'worksheet': 'ありがた屋向け',
        'icon': '☕',
        'color': '#8b4513',
        'platforms': ['Seesaa', 'FC2'],
        'max_posts': 20
    },
    '買取LIFE': {
        'worksheet': '買取LIFE向け',
        'icon': '💰',
        'color': '#ffd700',
        'platforms': ['WordPress'],
        'sites': ['selectad', 'thrones'],
        'max_posts': 20
    },
    'お財布レスキュー': {
        'worksheet': 'お財布レスキュー向け',
        'icon': '💖',
        'color': '#ff6b9d',
        'platforms': ['WordPress'],
        'sites': ['ykikaku', 'efdlqjtz'],
        'max_posts': 20
    },
    'クレかえる': {
        'worksheet': 'クレかえる向け',
        'icon': '🐸',
        'color': '#7ed321',
        'platforms': ['WordPress'],
        'sites': ['selectadvance', 'welkenraedt'],
        'max_posts': 20
    },
    '赤いサイト': {
        'worksheet': '赤いサイト向け',
        'icon': '🛒',
        'color': '#ff4444',
        'platforms': ['WordPress'],
        'sites': ['ncepqvub', 'kosagi'],
        'max_posts': 20
    }
}

# ========================
# 認証
# ========================
def check_authentication():
    """ユーザー認証"""
    if not st.session_state.authenticated:
        st.markdown("""
        <style>
        .auth-container {
            max-width: 400px;
            margin: auto;
            padding: 2rem;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 10px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
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
                # 管理者認証
                if username == "admin" and password == st.secrets.auth.admin_password:
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.session_state.is_admin = True
                    st.rerun()
                # クライアント認証
                elif username in st.secrets.auth.client_passwords:
                    if password == st.secrets.auth.client_passwords[username]:
                        st.session_state.authenticated = True
                        st.session_state.username = username
                        st.session_state.is_admin = False
                        st.rerun()
                else:
                    st.error("認証に失敗しました")
            
            st.markdown('</div>', unsafe_allow_html=True)
        return False
    return True

# ========================
# Google Sheets接続
# ========================
@st.cache_resource
def get_sheets_client():
    """Google Sheetsクライアントを取得"""
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    # Secretsから認証情報を取得してファイルに書き込み
    creds_dict = st.secrets.gcp.to_dict()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(creds_dict, f)
        temp_creds_file = f.name
    
    creds = ServiceAccountCredentials.from_json_keyfile_name(temp_creds_file, scope)
    client = gspread.authorize(creds)
    
    # 一時ファイルを削除
    os.unlink(temp_creds_file)
    
    return client

def load_sheet_data(worksheet_name: str) -> pd.DataFrame:
    """スプレッドシートからデータを読み込み"""
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(worksheet_name)
        data = sheet.get_all_values()
        
        if len(data) > 1:
            df = pd.DataFrame(data[1:], columns=data[0])
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"データ読み込みエラー: {e}")
        return pd.DataFrame()

def update_sheet_cell(worksheet_name: str, row: int, col: int, value: str):
    """スプレッドシートのセルを更新"""
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(worksheet_name)
        sheet.update_cell(row, col, value)
        return True
    except Exception as e:
        st.error(f"更新エラー: {e}")
        return False

def update_sheet_immediately(worksheet_name: str, df: pd.DataFrame):
    """データフレーム全体を即座にスプレッドシートに保存"""
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(worksheet_name)
        
        # DataFrameをリストに変換（ヘッダー含む）
        values = [df.columns.tolist()] + df.values.tolist()
        
        # 全体を更新
        sheet.clear()
        sheet.update('A1', values)
        
        return True
    except Exception as e:
        st.error(f"自動保存エラー: {e}")
        return False

# ========================
# その他のリンク取得
# ========================
def get_other_links() -> List[Dict]:
    """その他のリンク先を取得"""
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
        if not other_sites:
            other_sites = [
                {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
                {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"}
            ]
        return other_sites
    except:
        return [
            {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
            {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"}
        ]

def get_competitor_domains() -> List[str]:
    """競合他社ドメインリストを取得"""
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
        return domains
    except:
        return []

# ========================
# 記事生成
# ========================
def call_gemini(prompt: str) -> str:
    """Gemini APIを呼び出し"""
    # APIキーを交互に使用
    api_keys = [st.secrets.google.gemini_api_key_1, st.secrets.google.gemini_api_key_2]
    api_key = random.choice(api_keys)
    
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

def generate_article(theme: str, url: str, anchor_text: str) -> Dict:
    """記事を生成"""
    if not theme or theme.strip() == "":
        theme = "金融・投資・資産運用"
    
    prompt = f"""
# 命令書:
「{theme}」をテーマに、読者に価値のある記事を作成してください。

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
"""
    
    try:
        response = call_gemini(prompt)
        lines = response.strip().split('\n')
        title = lines[0].strip()
        content = '\n'.join(lines[1:]).strip()
        
        return {
            "title": title,
            "content": content,
            "theme": theme
        }
    except Exception as e:
        st.error(f"記事生成エラー: {e}")
        return None

# ========================
# 各プラットフォーム投稿関数
# ========================

def post_to_blogger(article: Dict) -> str:
    """Bloggerに投稿（簡易版）"""
    try:
        # TODO: Blogger API実装
        st.info("Blogger投稿機能は準備中です")
        return ""
    except Exception as e:
        st.error(f"Blogger投稿エラー: {e}")
        return ""

def post_to_livedoor(article: Dict) -> str:
    """livedoorブログに投稿"""
    from xml.sax.saxutils import escape as xml_escape
    import xml.etree.ElementTree as ET
    
    try:
        config = st.secrets.livedoor
        root_url = f"https://livedoor.blogcms.jp/atompub/{config.blog_name}"
        endpoint = f"{root_url}/article"
        
        title_xml = xml_escape(article["title"])
        content_xml = xml_escape(article["content"])
        
        entry_xml = f'''<?xml version="1.0" encoding="utf-8"?>
<entry xmlns="http://www.w3.org/2005/Atom">
  <title>{title_xml}</title>
  <content type="html">{content_xml}</content>
</entry>'''.encode("utf-8")
        
        from requests.auth import HTTPBasicAuth
        response = requests.post(
            endpoint,
            data=entry_xml,
            headers={"Content-Type": "application/atom+xml;type=entry"},
            auth=HTTPBasicAuth(config.id, config.api_key),
            timeout=30
        )
        
        if response.status_code in (200, 201):
            root_xml = ET.fromstring(response.text)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            alt = root_xml.find(".//atom:link[@rel='alternate']", ns)
            url = alt.get("href") if alt is not None else ""
            if url:
                return url
    except Exception as e:
        st.error(f"livedoor投稿エラー: {e}")
    
    return ""

def post_to_seesaa(article: Dict) -> str:
    """Seesaaブログに投稿"""
    try:
        config = st.secrets.seesaa
        server = xmlrpc.client.ServerProxy("http://blog.seesaa.jp/rpc", allow_none=True)
        
        content = {
            "title": article["title"],
            "description": article["content"]
        }
        
        post_id = server.metaWeblog.newPost(
            config.blogid,
            config.username,
            config.password,
            content,
            True
        )
        
        # URLを取得
        try:
            post = server.metaWeblog.getPost(post_id, config.username, config.password)
            url = post.get("permalink") or post.get("link") or ""
            if url:
                return url
        except:
            pass
        
        return f"post_id:{post_id}"
        
    except Exception as e:
        st.error(f"Seesaa投稿エラー: {e}")
        return ""

def post_to_fc2(article: Dict) -> str:
    """FC2ブログに投稿"""
    try:
        config = st.secrets.fc2
        server = xmlrpc.client.ServerProxy('https://blog.fc2.com/xmlrpc.php')
        
        content = {
            'title': article['title'],
            'description': article['content']
        }
        
        post_id = server.metaWeblog.newPost(
            config.blog_id,
            config.username,
            config.password,
            content,
            True
        )
        
        return f"https://{config.blog_id}.blog.fc2.com/blog-entry-{post_id}.html"
        
    except Exception as e:
        st.error(f"FC2投稿エラー: {e}")
        return ""

def post_to_wordpress(article: Dict, site_key: str, schedule_dt: Optional[datetime] = None) -> str:
    """WordPressに投稿（予約投稿対応）"""
    try:
        # サイトごとの設定を取得
        wp_configs = {
            'ykikaku': st.secrets.wp_ykikaku,
            'efdlqjtz': st.secrets.wp_efdlqjtz,
            'selectadvance': st.secrets.wp_selectadvance,
            'welkenraedt': st.secrets.wp_welkenraedt,
            'ncepqvub': st.secrets.wp_ncepqvub,
            'kosagi': st.secrets.wp_kosagi,
            'selectad': st.secrets.wp_selectad,
            'thrones': st.secrets.wp_thrones,
        }
        
        if site_key not in wp_configs:
            st.error(f"不明なサイト: {site_key}")
            return ""
        
        config = wp_configs[site_key]
        
        # XML-RPC投稿
        server = xmlrpc.client.ServerProxy(f"{config.url}xmlrpc.php")
        
        post = {
            'post_title': article['title'],
            'post_content': article['content'],
            'post_type': 'post'
        }
        
        # 予約投稿の設定
        if schedule_dt and schedule_dt > datetime.now():
            post['post_status'] = 'future'
            post['post_date'] = schedule_dt.strftime('%Y%m%dT%H:%M:%S')
        else:
            post['post_status'] = 'publish'
        
        post_id = server.wp.newPost(
            0,
            config.user,
            config.password,
            post
        )
        
        return f"{config.url}?p={post_id}"
        
    except Exception as e:
        st.error(f"WordPress投稿エラー ({site_key}): {e}")
        return ""

# ========================
# 予約投稿管理
# ========================
class ScheduledPost:
    """予約投稿データクラス"""
    def __init__(self, project_name: str, row_data: Dict, schedule_time: datetime, post_count: int = 1):
        self.project_name = project_name
        self.row_data = row_data
        self.schedule_time = schedule_time
        self.post_count = post_count
        self.status = "待機中"
        self.result = None

def execute_scheduled_post(scheduled_post: ScheduledPost):
    """予約投稿を実行"""
    try:
        scheduled_post.status = "実行中"
        project_config = PROJECTS[scheduled_post.project_name]
        
        results = []
        for i in range(scheduled_post.post_count):
            result = process_post_for_project(
                scheduled_post.row_data,
                scheduled_post.project_name,
                project_config
            )
            results.extend(result)
            
            # 連投防止
            if i < scheduled_post.post_count - 1:
                time.sleep(random.randint(300, 600))
        
        scheduled_post.status = "完了"
        scheduled_post.result = results
        return True
        
    except Exception as e:
        scheduled_post.status = "エラー"
        scheduled_post.result = str(e)
        return False

def schedule_monitor():
    """予約投稿を監視して実行"""
    while st.session_state.scheduler_running:
        now = datetime.now()
        
        for scheduled_post in st.session_state.scheduled_posts:
            if scheduled_post.status == "待機中" and scheduled_post.schedule_time <= now:
                execute_scheduled_post(scheduled_post)
        
        time.sleep(30)  # 30秒ごとにチェック

# ========================
# 投稿処理
# ========================
def process_post_for_project(row_data: Dict, project_name: str, project_config: Dict, schedule_dt: Optional[datetime] = None) -> List[str]:
    """プロジェクトに応じた投稿処理"""
    results = []
    
    # カウンター取得
    counter = 0
    if 'カウンター' in row_data:
        try:
            counter = int(row_data['カウンター'])
        except:
            counter = 0
    
    # 最大投稿数チェック
    max_posts = project_config.get('max_posts', 20)
    if isinstance(max_posts, dict):
        max_posts = list(max_posts.values())[0]
    
    # リンク決定（20記事目は宣伝URL、それ以外はその他リンク）
    if counter == max_posts - 1:
        # 最終記事：宣伝URLを使用
        url = row_data.get('宣伝URL', '')
        anchor = row_data.get('アンカーテキスト', project_name)
    else:
        # その他リンクを使用
        other_links = get_other_links()
        competitor_domains = get_competitor_domains()
        
        # 競合を除外
        available_links = []
        for link in other_links:
            link_domain = urlparse(link['url']).netloc.lower()
            if not any(comp in link_domain for comp in competitor_domains):
                available_links.append(link)
        
        if available_links:
            selected = random.choice(available_links)
            url = selected['url']
            anchor = selected['anchor']
        else:
            st.error("その他リンクが見つかりません")
            return []
    
    # 記事生成
    theme = row_data.get('テーマ', '')
    article = generate_article(theme, url, anchor)
    
    if not article:
        st.error("記事生成に失敗しました")
        return []
    
    # プロジェクトごとの投稿処理
    if project_name == 'ビックギフト':
        if 'Blogger' in project_config['platforms']:
            result = post_to_blogger(article)
            if result:
                results.append(result)
        if 'livedoor' in project_config['platforms']:
            result = post_to_livedoor(article)
            if result:
                results.append(result)
    
    elif project_name == 'ありがた屋':
        target = row_data.get('投稿先', 'Seesaa')
        if target == 'Seesaa':
            result = post_to_seesaa(article)
            if result:
                results.append(result)
        elif target == 'FC2':
            result = post_to_fc2(article)
            if result:
                results.append(result)
    
    elif 'WordPress' in project_config['platforms']:
        # WordPress系プロジェクト
        for site in project_config.get('sites', []):
            result = post_to_wordpress(article, site, schedule_dt)
            if result:
                results.append(result)
    
    return results

# ========================
# メインUI
# ========================
def main():
    """メインアプリケーション"""
    
    # 認証チェック
    if not check_authentication():
        return
    
    # カスタムCSS
    st.markdown("""
    <style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        color: white;
        text-align: center;
    }
    .schedule-card {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # ヘッダー
    st.markdown("""
    <div class="main-header">
        <h1>📝 統合ブログ投稿管理システム</h1>
        <p>AI-Powered Content Generation & Publishing Platform</p>
    </div>
    """, unsafe_allow_html=True)
    
    # サイドバー
    with st.sidebar:
        st.markdown(f"### 👤 {st.session_state.username}")
        
        if st.button("🚪 ログアウト", use_container_width=True):
            st.session_state.authenticated = False
            st.session_state.username = None
            st.rerun()
        
        st.divider()
        
        # プロジェクト選択
        st.markdown("### 🎯 プロジェクト選択")
        
        project_names = list(PROJECTS.keys())
        selected_project = st.selectbox(
            "プロジェクトを選択",
            project_names,
            key="project_selector"
        )
        
        project_info = PROJECTS[selected_project]
        st.markdown(f"""
        <div style="background: {project_info['color']}20; padding: 1rem; border-radius: 8px; border-left: 4px solid {project_info['color']};">
            <h4>{project_info['icon']} {selected_project}</h4>
            <p>プラットフォーム: {', '.join(project_info['platforms'])}</p>
        </div>
        """, unsafe_allow_html=True)
    
    # メインコンテンツ
    tabs = st.tabs(["📊 ダッシュボード", "⏰ 予約投稿", "📝 即時投稿", "📈 分析"])
    
    # ダッシュボードタブ
    with tabs[0]:
        st.markdown("### 📊 ダッシュボード")
        
        # メトリクス表示
        col1, col2, col3, col4 = st.columns(4)
        
        # データ読み込み
        df = load_sheet_data(project_info['worksheet'])
        
        if not df.empty:
            total_urls = len(df)
            completed = len(df[df.get('ステータス', df.columns[4] if len(df.columns) > 4 else 'ステータス') == '処理済み']) if not df.empty else 0
            processing = total_urls - completed
            scheduled = len(st.session_state.scheduled_posts)
            
            with col1:
                st.metric("総URL数", total_urls)
            with col2:
                st.metric("処理済み", completed)
            with col3:
                st.metric("未処理", processing)
            with col4:
                st.metric("予約中", scheduled)
        
        # 予約投稿状況
        st.markdown("### 📅 予約投稿状況")
        
        if st.session_state.scheduled_posts:
            for post in st.session_state.scheduled_posts:
                status_color = {
                    "待機中": "🟡",
                    "実行中": "🔵",
                    "完了": "🟢",
                    "エラー": "🔴"
                }.get(post.status, "⚪")
                
                st.markdown(f"""
                {status_color} **{post.project_name}** - {post.schedule_time.strftime('%Y/%m/%d %H:%M')}
                - 投稿数: {post.post_count}
                - ステータス: {post.status}
                """)
        else:
            st.info("予約投稿はありません")
    
    # 予約投稿タブ（メイン機能）
    with tabs[1]:
        st.markdown("### ⏰ 予約投稿設定")
        
        # データ読み込み
        df = load_sheet_data(project_info['worksheet'])
        
        if not df.empty:
            # 列名のクリーンアップ
            df.columns = [str(col).strip() if col else f"列{i+1}" for i, col in enumerate(df.columns)]
            
            # 選択列を追加
            if '選択' not in df.columns:
                df.insert(0, '選択', False)
            
            # データ表示
            st.markdown("#### 📋 投稿対象を選択")
            
            edited_df = st.data_editor(
                df,
                use_container_width=True,
                hide_index=True,
                key="schedule_data_editor",
                column_config={
                    "選択": st.column_config.CheckboxColumn(
                        "選択",
                        help="予約投稿する行を選択",
                        default=False,
                    )
                }
            )
            
            # 予約設定
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown("#### 🕐 投稿スケジュール")
                
                # 複数の日時設定
                schedule_input = st.text_area(
                    "予約日時（1行1件）",
                    value=datetime.now().strftime('%Y/%m/%d 09:00\n%Y/%m/%d 12:00\n%Y/%m/%d 15:00\n%Y/%m/%d 18:00'),
                    height=200,
                    help="形式: YYYY/MM/DD HH:MM または HH:MM（本日）"
                )
                
                # 投稿数設定
                post_count_per_schedule = st.number_input(
                    "各時刻での投稿数",
                    min_value=1,
                    max_value=5,
                    value=1,
                    help="各予約時刻で何記事投稿するか"
                )
            
            with col2:
                st.markdown("#### ⚙️ オプション")
                
                # 投稿間隔
                interval_min = st.number_input(
                    "最小間隔（秒）",
                    min_value=60,
                    max_value=3600,
                    value=300,
                    step=60
                )
                
                interval_max = st.number_input(
                    "最大間隔（秒）",
                    min_value=60,
                    max_value=3600,
                    value=600,
                    step=60
                )
                
                # WordPressの場合の予約投稿
                use_wp_schedule = st.checkbox(
                    "WordPress予約投稿を使用",
                    value=False,
                    help="WordPressの予約投稿機能を使用（WordPress系のみ）"
                )
            
            # 予約実行ボタン
            if st.button("🚀 予約投稿を設定", type="primary", use_container_width=True):
                # 選択された行を取得
                selected_rows = edited_df[edited_df['選択'] == True] if '選択' in edited_df.columns else pd.DataFrame()
                
                if len(selected_rows) == 0:
                    st.warning("投稿する行を選択してください")
                else:
                    # 予約時刻をパース
                    schedule_times = []
                    now = datetime.now()
                    
                    for line in schedule_input.strip().split('\n'):
                        line = line.strip()
                        if not line:
                            continue
                        
                        # 時刻のパース
                        try:
                            # YYYY/MM/DD HH:MM形式
                            if '/' in line:
                                dt = datetime.strptime(line, '%Y/%m/%d %H:%M')
                            # HH:MM形式（本日）
                            elif ':' in line:
                                time_parts = line.split(':')
                                hour = int(time_parts[0])
                                minute = int(time_parts[1])
                                dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                                
                                # 過去の時刻なら翌日に設定
                                if dt <= now:
                                    dt += timedelta(days=1)
                            else:
                                continue
                            
                            schedule_times.append(dt)
                        except:
                            st.warning(f"無効な日時形式: {line}")
                    
                    if not schedule_times:
                        st.error("有効な予約時刻がありません")
                    else:
                        # 予約を作成
                        created_count = 0
                        
                        for idx, row in selected_rows.iterrows():
                            for schedule_time in schedule_times:
                                scheduled_post = ScheduledPost(
                                    project_name=selected_project,
                                    row_data=row.to_dict(),
                                    schedule_time=schedule_time,
                                    post_count=post_count_per_schedule
                                )
                                st.session_state.scheduled_posts.append(scheduled_post)
                                created_count += 1
                        
                        st.success(f"✅ {created_count}件の予約投稿を設定しました")
                        
                        # スケジューラーを開始
                        if not st.session_state.scheduler_running:
                            st.session_state.scheduler_running = True
                            scheduler_thread = threading.Thread(target=schedule_monitor, daemon=True)
                            scheduler_thread.start()
                            st.info("⏰ 予約投稿スケジューラーを開始しました")
                        
                        time.sleep(2)
                        st.rerun()
        else:
            st.info("データがありません")
        
        # 予約一覧
        st.divider()
        st.markdown("### 📅 予約投稿一覧")
        
        if st.session_state.scheduled_posts:
            # 予約をテーブル表示
            schedule_data = []
            for post in st.session_state.scheduled_posts:
                schedule_data.append({
                    "プロジェクト": post.project_name,
                    "予約日時": post.schedule_time.strftime('%Y/%m/%d %H:%M'),
                    "投稿数": post.post_count,
                    "ステータス": post.status,
                    "URL": post.row_data.get('宣伝URL', '')[:30] + '...' if post.row_data.get('宣伝URL', '') else ''
                })
            
            schedule_df = pd.DataFrame(schedule_data)
            st.dataframe(schedule_df, use_container_width=True)
            
            # 予約クリアボタン
            col1, col2, col3 = st.columns([1, 1, 2])
            with col1:
                if st.button("🗑️ 完了済みをクリア", use_container_width=True):
                    st.session_state.scheduled_posts = [
                        post for post in st.session_state.scheduled_posts
                        if post.status != "完了"
                    ]
                    st.rerun()
            
            with col2:
                if st.button("🔄 全てクリア", use_container_width=True):
                    st.session_state.scheduled_posts = []
                    st.session_state.scheduler_running = False
                    st.rerun()
        else:
            st.info("予約投稿はありません")
    
    # 即時投稿タブ
    with tabs[2]:
        st.markdown("### 📝 即時投稿")
        
        # データ読み込み
        df = load_sheet_data(project_info['worksheet'])
        
        if not df.empty:
            # 列名のクリーンアップ
            df.columns = [str(col).strip() if col else f"列{i+1}" for i, col in enumerate(df.columns)]
            
            # 選択列を追加
            if '選択' not in df.columns:
                df.insert(0, '選択', False)
            
            # データ表示
            edited_df = st.data_editor(
                df,
                use_container_width=True,
                hide_index=True,
                key="immediate_data_editor",
                column_config={
                    "選択": st.column_config.CheckboxColumn(
                        "選択",
                        help="投稿する行を選択",
                        default=False,
                    )
                }
            )
            
            # 投稿ボタン
            col1, col2, col3 = st.columns([1, 1, 3])
            
            with col1:
                post_count = st.number_input("投稿数", min_value=1, max_value=5, value=1, key="immediate_post_count")
            
            with col2:
                if st.button("📤 今すぐ投稿", type="primary", use_container_width=True):
                    # 選択された行を取得
                    selected_rows = edited_df[edited_df['選択'] == True] if '選択' in edited_df.columns else pd.DataFrame()
                    
                    if len(selected_rows) == 0:
                        st.warning("投稿する行を選択してください")
                    else:
                        with st.spinner(f"{len(selected_rows)}件を投稿中..."):
                            success_count = 0
                            
                            for idx, row in selected_rows.iterrows():
                                # 投稿処理
                                results = process_post_for_project(
                                    row.to_dict(),
                                    selected_project,
                                    project_info
                                )
                                
                                if results:
                                    st.success(f"✅ 投稿成功: {row.get('宣伝URL', '')[:30]}...")
                                    
                                    # ステータス更新
                                    row_num = idx + 2
                                    
                                    # カウンター更新
                                    current_counter = 0
                                    if 'カウンター' in row:
                                        try:
                                            current_counter = int(row['カウンター'])
                                        except:
                                            current_counter = 0
                                    
                                    update_sheet_cell(project_info['worksheet'], row_num, 7, str(current_counter + 1))
                                    
                                    # 最大投稿数に達したら処理済みに
                                    max_posts = project_info.get('max_posts', 20)
                                    if isinstance(max_posts, dict):
                                        max_posts = list(max_posts.values())[0]
                                    
                                    if current_counter + 1 >= max_posts:
                                        update_sheet_cell(project_info['worksheet'], row_num, 5, '処理済み')
                                        update_sheet_cell(project_info['worksheet'], row_num, 9, datetime.now().strftime("%Y/%m/%d %H:%M"))
                                    
                                    success_count += 1
                                else:
                                    st.error(f"❌ 投稿失敗: {row.get('宣伝URL', '')[:30]}...")
                                
                                # 連投防止
                                if idx < len(selected_rows) - 1:
                                    time.sleep(5)
                            
                            st.info(f"投稿完了: {success_count}/{len(selected_rows)}件成功")
                            time.sleep(2)
                            st.rerun()
        else:
            st.info("データがありません")
    
    # 分析タブ
    with tabs[3]:
        st.markdown("### 📈 分析")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🎯 投稿実績")
            
            # 予約投稿の実績
            completed_posts = [p for p in st.session_state.scheduled_posts if p.status == "完了"]
            error_posts = [p for p in st.session_state.scheduled_posts if p.status == "エラー"]
            waiting_posts = [p for p in st.session_state.scheduled_posts if p.status == "待機中"]
            
            metrics_data = pd.DataFrame({
                'ステータス': ['完了', 'エラー', '待機中'],
                '件数': [len(completed_posts), len(error_posts), len(waiting_posts)]
            })
            
            st.bar_chart(metrics_data.set_index('ステータス'))
        
        with col2:
            st.markdown("#### 📊 プロジェクト別統計")
            
            # プロジェクト別の予約数
            project_stats = {}
            for post in st.session_state.scheduled_posts:
                if post.project_name not in project_stats:
                    project_stats[post.project_name] = 0
                project_stats[post.project_name] += 1
            
            if project_stats:
                stats_df = pd.DataFrame({
                    'プロジェクト': list(project_stats.keys()),
                    '予約数': list(project_stats.values())
                })
                st.bar_chart(stats_df.set_index('プロジェクト'))
            else:
                st.info("データがありません")

# ========================
# エントリーポイント
# ========================
if __name__ == "__main__":
    main()
