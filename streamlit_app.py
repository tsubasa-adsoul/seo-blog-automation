#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
統合ブログ投稿管理システム - Streamlit版
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
    import json
    import tempfile
    
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    # Secretsから認証情報を取得してファイルに書き込み
    creds_dict = st.secrets.gcp.to_dict()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(creds_dict, f)
        temp_creds_file = f.name
    
    creds = ServiceAccountCredentials.from_json_keyfile_name(temp_creds_file, scope)
    client = gspread.authorize(creds)
    
    # 一時ファイルを削除
    import os
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
# アイキャッチ画像生成
# ========================
def create_eyecatch_image(title: str, project_name: str) -> bytes:
    """プロジェクトに応じたアイキャッチ画像を生成"""
    width, height = 600, 400
    
    # プロジェクトごとの色設定
    project_colors = {
        'ビックギフト': ['#FF8C00', '#FFA500'],  # オレンジ系
        'ありがた屋': ['#8B4513', '#CD853F'],    # ブラウン系
        '買取LIFE': ['#FFD700', '#FFF59D'],      # イエロー系
        'お財布レスキュー': ['#FF69B4', '#FFB6C1'],  # ピンク系
        'クレかえる': ['#7CB342', '#AED581'],    # グリーン系
        '赤いサイト': ['#FF4444', '#FF8888']     # レッド系
    }
    
    colors = project_colors.get(project_name, ['#667eea', '#764ba2'])
    
    # 画像作成
    img = Image.new('RGB', (width, height), color=colors[0])
    draw = ImageDraw.Draw(img)
    
    # グラデーション背景
    for i in range(height):
        alpha = i / height
        r1 = int(colors[0][1:3], 16)
        g1 = int(colors[0][3:5], 16)
        b1 = int(colors[0][5:7], 16)
        r2 = int(colors[1][1:3], 16)
        g2 = int(colors[1][3:5], 16)
        b2 = int(colors[1][5:7], 16)
        
        r = int(r1 * (1 - alpha) + r2 * alpha)
        g = int(g1 * (1 - alpha) + g2 * alpha)
        b = int(b1 * (1 - alpha) + b2 * alpha)
        
        draw.rectangle([(0, i), (width, i + 1)], fill=(r, g, b))
    
    # 装飾
    draw.ellipse([-50, -50, 150, 150], fill=colors[1])
    draw.ellipse([width-100, height-100, width+50, height+50], fill=colors[1])
    
    # フォント設定
    try:
        font = ImageFont.truetype("C:/Windows/Fonts/meiryob.ttc", 32)
    except:
        font = ImageFont.load_default()
    
    # タイトル描画
    lines = []
    if len(title) > 15:
        mid = len(title) // 2
        lines = [title[:mid], title[mid:]]
    else:
        lines = [title]
    
    y_start = (height - len(lines) * 50) // 2
    for i, line in enumerate(lines):
        try:
            bbox = draw.textbbox((0, 0), line, font=font)
            text_width = bbox[2] - bbox[0]
        except:
            text_width = len(line) * 20
        
        x = (width - text_width) // 2
        y = y_start + i * 50
        draw.text((x, y), line, font=font, fill='white')
    
    # バイトデータとして返す
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='JPEG', quality=90)
    img_byte_arr.seek(0)
    
    return img_byte_arr.getvalue()

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
    .project-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
        transition: transform 0.3s;
    }
    .project-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 5px 20px rgba(0,0,0,0.2);
    }
    .metric-card {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 1rem;
        border-radius: 8px;
        color: white;
        text-align: center;
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
        if not st.session_state.is_admin:
            # クライアントは特定のプロジェクトのみ
            # ここでクライアントごとのプロジェクト制限を実装
            pass
        
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
    tabs = st.tabs(["📊 ダッシュボード", "📝 投稿管理", "⏰ 予約設定", "📈 分析", "⚙️ 設定"])
    
    # ダッシュボードタブ
    with tabs[0]:
        st.markdown("### 📊 ダッシュボード")
        
        # メトリクス表示
        col1, col2, col3, col4 = st.columns(4)
        
        # データ読み込み
        df = load_sheet_data(project_info['worksheet'])
        
        if not df.empty:
            # ステータス列のインデックスを取得（通常はE列=4）
            status_col = 'ステータス' if 'ステータス' in df.columns else df.columns[4] if len(df.columns) > 4 else None
            counter_col = 'カウンター' if 'カウンター' in df.columns else df.columns[6] if len(df.columns) > 6 else None
            
            total_urls = len(df)
            if status_col:
                completed = len(df[df[status_col] == '処理済み'])
                processing = len(df[df[status_col].isin(['処理中', '未処理', ''])])
            else:
                completed = 0
                processing = total_urls
            
            with col1:
                st.metric("総URL数", total_urls, delta=None)
            with col2:
                st.metric("処理済み", completed, delta=f"{completed/total_urls*100:.1f}%")
            with col3:
                st.metric("未処理", processing, delta=None)
            with col4:
                st.metric("本日の投稿", "0", delta=None)  # TODO: 実装
        
        # グラフ表示
        st.markdown("### 📈 投稿推移")
        
        # ダミーデータでグラフ表示（実際のデータに置き換え）
        import numpy as np
        dates = pd.date_range(start='2025-08-01', periods=30)
        data = pd.DataFrame({
            '日付': dates,
            '投稿数': np.random.randint(0, 10, 30)
        })
        st.line_chart(data.set_index('日付'))
    
    # 投稿管理タブ
    with tabs[1]:
        st.markdown("### 📝 投稿管理")
        
        # データ表示
        df = load_sheet_data(project_info['worksheet'])
        
        if not df.empty:
            # 列名のクリーンアップ（エラー対策）
            df.columns = [str(col).strip() if col else f"列{i+1}" for i, col in enumerate(df.columns)]
            
            # 重複する列名を修正
            seen = {}
            new_columns = []
            for col in df.columns:
                if col in seen:
                    seen[col] += 1
                    new_columns.append(f"{col}_{seen[col]}")
                else:
                    seen[col] = 0
                    new_columns.append(col)
            df.columns = new_columns
            
            # デバッグ用（問題があれば表示）
            # st.write("列名:", df.columns.tolist())
            
            # 編集可能なデータエディタ
            edited_df = st.data_editor(
                df,
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True,
                key="data_editor",
                column_config={
                    "選択": st.column_config.CheckboxColumn(
                        "選択",
                        help="投稿する行を選択",
                        default=False,
                    ),
                    "宣伝URL": st.column_config.LinkColumn(
                        "宣伝URL",
                        help="宣伝するURL",
                        max_chars=50,
                    ),
                    "ステータス": st.column_config.SelectboxColumn(
                        "ステータス",
                        help="処理状況",
                        options=["未処理", "処理中", "処理済み", "エラー"],
                        default="未処理",
                    ),
                    "カウンター": st.column_config.NumberColumn(
                        "カウンター",
                        help="投稿済み記事数",
                        min_value=0,
                        max_value=20,
                        step=1,
                        format="%d",
                    ),
                }
            )
            
            # 自動保存機能（データが変更されたら即座に保存）
            if edited_df is not None and not df.equals(edited_df):
                if update_sheet_immediately(project_info['worksheet'], edited_df):
                    st.success("✅ 変更を自動保存しました", icon="💾")
                    st.rerun()
            
            # 投稿ボタン
            col1, col2, col3 = st.columns([1, 1, 3])
            
            with col1:
                post_count = st.number_input("投稿数", min_value=1, max_value=5, value=1)
            
            with col2:
                if st.button("📤 選択行を投稿", type="primary", use_container_width=True):
                    st.info("投稿処理を開始します...")
                    # TODO: 投稿処理の実装
            
            # 更新ボタン
            if st.button("💾 変更を保存", use_container_width=True):
                # スプレッドシートに変更を保存
                client = get_sheets_client()
                sheet = client.open_by_key(SPREADSHEET_ID).worksheet(project_info['worksheet'])
                
                # DataFrameをリストに変換
                values = [edited_df.columns.tolist()] + edited_df.values.tolist()
                
                # 全体を更新
                sheet.clear()
                sheet.update('A1', values)
                
                st.success("変更を保存しました！")
                st.rerun()
        else:
            st.info("データがありません")

    
    # 予約設定タブ
    with tabs[2]:
        st.markdown("### ⏰ 予約投稿設定")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("#### 📅 予約スケジュール")
            
            # カレンダー風の表示
            selected_date = st.date_input("日付を選択", datetime.now())
            
            # 時刻設定
            times = st.text_area(
                "投稿時刻（1行1時刻）",
                value="09:00\n12:00\n15:00\n18:00",
                height=150
            )
            
            if st.button("📅 予約を設定", type="primary", use_container_width=True):
                st.success("予約を設定しました！")
        
        with col2:
            st.markdown("#### 📊 予約状況")
            
            # 予約済みの表示
            st.info("""
            **本日の予約**
            - 09:00 - 1記事
            - 12:00 - 1記事
            - 15:00 - 1記事
            - 18:00 - 1記事
            """)
    
    # 分析タブ
    with tabs[3]:
        st.markdown("### 📈 分析")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🎯 投稿先別統計")
            
            # ダミーデータ
            platform_data = pd.DataFrame({
                'プラットフォーム': project_info['platforms'],
                '投稿数': [random.randint(50, 200) for _ in project_info['platforms']]
            })
            
            st.bar_chart(platform_data.set_index('プラットフォーム'))
        
        with col2:
            st.markdown("#### 📊 カテゴリ別統計")
            
            categories = ['お金の豆知識', '投資', 'クレジットカード', 'ローン', 'その他']
            category_data = pd.DataFrame({
                'カテゴリ': categories,
                '記事数': [random.randint(10, 50) for _ in categories]
            })
            
            st.bar_chart(category_data.set_index('カテゴリ'))
    
    # 設定タブ
    with tabs[4]:
        st.markdown("### ⚙️ 設定")
        
        if st.session_state.is_admin:
            st.markdown("#### 🔑 API設定")
            
            # API設定の表示（読み取り専用）
            with st.expander("Gemini API設定"):
                st.text_input("API Key 1", value="*" * 20, disabled=True)
                st.text_input("API Key 2", value="*" * 20, disabled=True)
            
            with st.expander("投稿先設定"):
                for platform in ['Blogger', 'livedoor', 'Seesaa', 'FC2', 'WordPress']:
                    st.text_input(f"{platform} 認証情報", value="*" * 20, disabled=True)
            
            st.markdown("#### 🔄 自動投稿設定")
            
            auto_post_enabled = st.checkbox("自動投稿を有効化", value=False)
            
            if auto_post_enabled:
                interval = st.slider("投稿間隔（時間）", min_value=1, max_value=24, value=2)
                st.info(f"{interval}時間ごとに自動投稿を実行します")
        else:
            st.info("管理者のみ設定を変更できます")

# ========================
# エントリーポイント
# ========================
if __name__ == "__main__":
    main()




