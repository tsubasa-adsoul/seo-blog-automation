#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
統合ブログ投稿管理システム - 完全予約投稿対応版
PCシャットダウン対応・GitHub Actions連携
"""

import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import random
from datetime import datetime, timedelta
import requests
from typing import Dict, List, Optional, Tuple
import json
import base64
from urllib.parse import urlparse
import re
import io
from PIL import Image, ImageDraw, ImageFont
import xmlrpc.client
import tempfile
import os
import logging
from requests.auth import HTTPBasicAuth

# ログ設定（Streamlit Cloud対応）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # コンソール出力のみ
    ]
)
logger = logging.getLogger(__name__)

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
        'supports_schedule': {'Blogger': True, 'livedoor': False},
        'max_posts': {'Blogger': 20, 'livedoor': 15}
    },
    'ありがた屋': {
        'worksheet': 'ありがた屋向け',
        'icon': '☕',
        'color': '#8b4513',
        'platforms': ['Seesaa', 'FC2'],
        'supports_schedule': {'Seesaa': False, 'FC2': False},
        'max_posts': 20
    },
    '買取LIFE': {
        'worksheet': '買取LIFE向け',
        'icon': '💰',
        'color': '#ffd700',
        'platforms': ['WordPress'],
        'sites': ['selectad', 'thrones'],
        'supports_schedule': True,
        'max_posts': 20
    },
    'お財布レスキュー': {
        'worksheet': 'お財布レスキュー向け',
        'icon': '💖',
        'color': '#ff6b9d',
        'platforms': ['WordPress'],
        'sites': ['ykikaku', 'efdlqjtz'],
        'supports_schedule': True,
        'max_posts': 20
    },
    'クレかえる': {
        'worksheet': 'クレかえる向け',
        'icon': '🐸',
        'color': '#7ed321',
        'platforms': ['WordPress'],
        'sites': ['selectadvance', 'welkenraedt'],
        'supports_schedule': True,
        'max_posts': 20
    },
    '赤いサイト': {
        'worksheet': '赤いサイト向け',
        'icon': '🛒',
        'color': '#ff4444',
        'platforms': ['WordPress'],
        'sites': ['ncepqvub', 'kosagi'],
        'supports_schedule': True,
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
    
    creds_dict = st.secrets.gcp.to_dict()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(creds_dict, f)
        temp_creds_file = f.name
    
    creds = ServiceAccountCredentials.from_json_keyfile_name(temp_creds_file, scope)
    client = gspread.authorize(creds)
    
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

def add_schedule_to_sheet(worksheet_name: str, row_num: int, schedule_times: List[datetime]):
    """予約情報をスプレッドシートに記録（K列以降）"""
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(worksheet_name)
        
        # K列（11列目）から予約時刻を記録
        for i, dt in enumerate(schedule_times):
            col = 11 + i  # K列から開始
            sheet.update_cell(row_num, col, dt.strftime('%Y/%m/%d %H:%M'))
        
        return True
    except Exception as e:
        st.error(f"予約記録エラー: {e}")
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
# 予約投稿対応の各プラットフォーム投稿関数
# ========================

def post_to_wordpress_scheduled(article: Dict, site_key: str, schedule_dt: datetime) -> Tuple[bool, str]:
    """WordPressに予約投稿（真の予約投稿）"""
    try:
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
            return False, f"不明なサイト: {site_key}"
        
        config = wp_configs[site_key]
        server = xmlrpc.client.ServerProxy(f"{config.url}xmlrpc.php")
        
        # WordPressの予約投稿設定
        post_data = {
            'post_title': article['title'],
            'post_content': article['content'],
            'post_type': 'post',
            'post_status': 'future',  # 予約投稿
            'post_date': xmlrpc.client.DateTime(schedule_dt)  # 予約日時
        }
        
        post_id = server.wp.newPost(
            0,
            config.user,
            config.password,
            post_data
        )
        
        return True, f"{config.url}?p={post_id} (予約: {schedule_dt.strftime('%Y/%m/%d %H:%M')})"
        
    except Exception as e:
        return False, f"WordPress予約投稿エラー ({site_key}): {e}"

def post_to_blogger_scheduled(article: Dict, schedule_dt: datetime) -> Tuple[bool, str]:
    """Bloggerに予約投稿"""
    # TODO: Blogger API実装（予約投稿対応）
    return False, "Blogger予約投稿は開発中です"

def process_scheduled_posts(row_data: Dict, project_name: str, project_config: Dict, 
                          schedule_times: List[datetime]) -> Dict:
    """予約投稿を一括処理"""
    results = {
        'success': [],
        'failed': [],
        'github_actions_needed': []
    }
    
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
    
    # 各予約時刻に対して記事を生成・投稿
    for schedule_dt in schedule_times:
        if counter >= max_posts:
            results['failed'].append(f"最大投稿数({max_posts})に達しました")
            break
        
        # リンク決定
        if counter == max_posts - 1:
            # 最終記事：宣伝URLを使用
            url = row_data.get('宣伝URL', '')
            anchor = row_data.get('アンカーテキスト', project_name)
        else:
            # その他リンクを使用
            other_links = get_other_links()
            competitor_domains = get_competitor_domains()
            
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
                results['failed'].append("その他リンクが見つかりません")
                continue
        
        # 記事生成
        theme = row_data.get('テーマ', '')
        article = generate_article(theme, url, anchor)
        
        if not article:
            results['failed'].append(f"{schedule_dt.strftime('%H:%M')} - 記事生成失敗")
            continue
        
        # プラットフォームごとの処理
        if 'WordPress' in project_config['platforms']:
            # WordPress：真の予約投稿
            for site in project_config.get('sites', []):
                success, message = post_to_wordpress_scheduled(article, site, schedule_dt)
                if success:
                    results['success'].append(f"{site}: {message}")
                else:
                    results['failed'].append(f"{site}: {message}")
        
        elif project_name == 'ビックギフト':
            if 'Blogger' in project_config['platforms']:
                success, message = post_to_blogger_scheduled(article, schedule_dt)
                if success:
                    results['success'].append(message)
                else:
                    # Bloggerの予約投稿が未実装の場合はGitHub Actions用に記録
                    results['github_actions_needed'].append({
                        'platform': 'Blogger',
                        'schedule': schedule_dt,
                        'article': article
                    })
            
            if 'livedoor' in project_config['platforms']:
                # livedoorは予約投稿非対応
                results['github_actions_needed'].append({
                    'platform': 'livedoor',
                    'schedule': schedule_dt,
                    'article': article
                })
        
        elif project_name == 'ありがた屋':
            # Seesaa/FC2は予約投稿非対応
            target = row_data.get('投稿先', 'Seesaa')
            results['github_actions_needed'].append({
                'platform': target,
                'schedule': schedule_dt,
                'article': article
            })
        
        counter += 1
    
    return results

# ========================
# メインUI
# ========================
def main():
　initialize_session_state() 
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
    .warning-box {
        background: #fff3cd;
        border-left: 4px solid #ffc107;
        padding: 1rem;
        margin: 1rem 0;
    }
    .success-box {
        background: #d4edda;
        border-left: 4px solid #28a745;
        padding: 1rem;
        margin: 1rem 0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # ヘッダー
    st.markdown("""
    <div class="main-header">
        <h1>📝 統合ブログ投稿管理システム</h1>
        <p>完全予約投稿対応版 - PCシャットダウンOK</p>
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
        
        # 予約投稿対応状況を表示
        supports_schedule = project_info.get('supports_schedule', False)
        if isinstance(supports_schedule, dict):
            schedule_status = "一部対応"
            schedule_color = "#ff9800"
        elif supports_schedule:
            schedule_status = "完全対応"
            schedule_color = "#4caf50"
        else:
            schedule_status = "GitHub Actions必要"
            schedule_color = "#f44336"
        
        st.markdown(f"""
        <div style="background: {project_info['color']}20; padding: 1rem; border-radius: 8px; border-left: 4px solid {project_info['color']};">
            <h4>{project_info['icon']} {selected_project}</h4>
            <p>プラットフォーム: {', '.join(project_info['platforms'])}</p>
            <p style="color: {schedule_color};">予約投稿: {schedule_status}</p>
        </div>
        """, unsafe_allow_html=True)
    
    # メインコンテンツ
    tabs = st.tabs(["⏰ 予約投稿", "📝 即時投稿", "📊 ダッシュボード", "⚙️ 設定"])
    
    # 予約投稿タブ（メイン）
    with tabs[0]:
        st.markdown("### ⏰ 完全予約投稿システム")
        
        # 予約投稿の説明
        if project_info.get('supports_schedule') == True or \
           (isinstance(project_info.get('supports_schedule'), dict) and 'WordPress' in str(project_info.get('supports_schedule'))):
            st.markdown("""
            <div class="success-box">
            ✅ <b>このプロジェクトは完全予約投稿対応</b><br>
            投稿予約後、PCをシャットダウンしても自動投稿されます。
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="warning-box">
            ⚠️ <b>このプロジェクトはGitHub Actions設定が必要</b><br>
            予約情報はスプレッドシートに記録されます。<br>
            GitHub Actionsで定期実行することで予約投稿が可能です。
            </div>
            """, unsafe_allow_html=True)
        
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
            st.markdown("#### 🕐 予約スケジュール設定")
            
            col1, col2 = st.columns([3, 2])
            
            with col1:
                # 複数の日時設定
                default_times = []
                now = datetime.now()
                for hour in [9, 12, 15, 18]:
                    dt = now.replace(hour=hour, minute=0, second=0, microsecond=0)
                    if dt > now:
                        default_times.append(dt.strftime('%Y/%m/%d %H:%M'))
                
                if not default_times:
                    # 今日の時間が全て過ぎている場合は明日の時間
                    tomorrow = now + timedelta(days=1)
                    for hour in [9, 12, 15, 18]:
                        dt = tomorrow.replace(hour=hour, minute=0, second=0, microsecond=0)
                        default_times.append(dt.strftime('%Y/%m/%d %H:%M'))
                
                schedule_input = st.text_area(
                    "予約日時（1行1件）",
                    value='\n'.join(default_times),
                    height=200,
                    help="形式: YYYY/MM/DD HH:MM"
                )
                
                # 各時刻での投稿数
                posts_per_time = st.number_input(
                    "各時刻での投稿数",
                    min_value=1,
                    max_value=5,
                    value=1,
                    step=1,
                    help="通常は1記事ずつ投稿（カウンターが進みます）"
                )
            
            with col2:
                st.markdown("#### 📊 予約サマリー")
                
                # 入力された時刻を解析
                schedule_times = []
                for line in schedule_input.strip().split('\n'):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        dt = datetime.strptime(line, '%Y/%m/%d %H:%M')
                        if dt > datetime.now():
                            schedule_times.append(dt)
                    except:
                        pass
                
                if schedule_times:
                    st.success(f"✅ {len(schedule_times)}回の投稿を予約")
                    for dt in schedule_times[:5]:  # 最初の5件を表示
                        st.write(f"• {dt.strftime('%m/%d %H:%M')}")
                    if len(schedule_times) > 5:
                        st.write(f"... 他 {len(schedule_times) - 5}件")
                else:
                    st.warning("有効な予約時刻がありません")
                
                # 選択行数
                selected_count = len(edited_df[edited_df['選択'] == True]) if '選択' in edited_df.columns else 0
                st.info(f"選択行数: {selected_count}")
                
                if selected_count > 0 and schedule_times:
                    total_posts = selected_count * len(schedule_times) * posts_per_time
                    st.metric("総投稿数", total_posts)
            
            # 予約実行ボタン
            if st.button("🚀 予約投稿を実行", type="primary", use_container_width=True):
                selected_rows = edited_df[edited_df['選択'] == True] if '選択' in edited_df.columns else pd.DataFrame()
                
                if len(selected_rows) == 0:
                    st.error("投稿する行を選択してください")
                elif not schedule_times:
                    st.error("有効な予約時刻を入力してください")
                else:
                    # プログレスバー
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    total_tasks = len(selected_rows) * len(schedule_times)
                    current_task = 0
                    
                    all_results = []
                    
                    for idx, row in selected_rows.iterrows():
                        row_num = idx + 2  # スプレッドシートの行番号
                        
                        status_text.text(f"処理中: {row.get('宣伝URL', '')[:30]}...")
                        
                        # 予約投稿を処理
                        results = process_scheduled_posts(
                            row.to_dict(),
                            selected_project,
                            project_info,
                            schedule_times
                        )
                        
                        all_results.append(results)
                        
                        # スプレッドシートに予約情報を記録
                        if results['success'] or results['github_actions_needed']:
                            add_schedule_to_sheet(project_info['worksheet'], row_num, schedule_times)
                            update_sheet_cell(project_info['worksheet'], row_num, 5, '予約済み')
                        
                        current_task += len(schedule_times)
                        progress_bar.progress(current_task / total_tasks)
                    
                    # 結果表示
                    st.markdown("### 📊 予約結果")
                    
                    total_success = sum(len(r['success']) for r in all_results)
                    total_failed = sum(len(r['failed']) for r in all_results)
                    total_github = sum(len(r['github_actions_needed']) for r in all_results)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("✅ 成功", total_success)
                    with col2:
                        st.metric("❌ 失敗", total_failed)
                    with col3:
                        st.metric("⏰ GitHub Actions待ち", total_github)
                    
                    if total_success > 0:
                        st.success(f"""
                        ✅ {total_success}件の予約投稿が完了しました。
                        PCをシャットダウンしても、指定時刻に自動投稿されます。
                        """)
                    
                    if total_github > 0:
                        st.warning(f"""
                        ⚠️ {total_github}件はGitHub Actionsでの処理が必要です。
                        スプレッドシートのK列以降に予約時刻が記録されました。
                        """)
                    
                    time.sleep(3)
                    st.rerun()
        else:
            st.info("データがありません")
    
    # 即時投稿タブ
    with tabs[1]:
        st.markdown("### 📝 即時投稿")
        st.info("即時投稿機能は簡易版です。予約投稿をご利用ください。")
    
    # ダッシュボードタブ
    with tabs[2]:
        st.markdown("### 📊 ダッシュボード")
        
        df = load_sheet_data(project_info['worksheet'])
        
        if not df.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            total_urls = len(df)
            status_col = 'ステータス' if 'ステータス' in df.columns else df.columns[4] if len(df.columns) > 4 else None
            
            if status_col:
                completed = len(df[df[status_col] == '処理済み'])
                scheduled = len(df[df[status_col] == '予約済み'])
                processing = total_urls - completed - scheduled
            else:
                completed = 0
                scheduled = 0
                processing = total_urls
            
            with col1:
                st.metric("総URL数", total_urls)
            with col2:
                st.metric("処理済み", completed)
            with col3:
                st.metric("予約済み", scheduled)
            with col4:
                st.metric("未処理", processing)
            
            # 予約状況の詳細
            st.markdown("### 📅 予約状況")
            
            if status_col and scheduled > 0:
                scheduled_df = df[df[status_col] == '予約済み']
                
                # K列以降の予約時刻を表示
                schedule_cols = [col for col in df.columns if col.startswith('列') and int(col.replace('列', '')) >= 11]
                if schedule_cols:
                    st.dataframe(scheduled_df[['宣伝URL', status_col] + schedule_cols[:5]], use_container_width=True)
            else:
                st.info("予約済みの投稿はありません")
    
    # 設定タブ
    with tabs[3]:
        st.markdown("### ⚙️ 設定")
        
        if st.session_state.is_admin:
            st.markdown("#### 🤖 GitHub Actions設定")
            
            st.code("""
# .github/workflows/auto_post.yml
name: Auto Blog Post

on:
  schedule:
    - cron: '0,30 * * * *'  # 30分ごとに実行
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
        run: python scripts/post_executor.py --mode scheduled
        env:
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          # 他の環境変数...
            """, language="yaml")
            
            st.info("""
            上記のGitHub Actionsを設定することで、Seesaa/FC2/livedoorの予約投稿が可能になります。
            30分ごとにスプレッドシートをチェックし、予約時刻になった投稿を自動実行します。
            """)

# ========================
# エントリーポイント
# ========================
if __name__ == "__main__":
    main()

