#!/usr/bin/env python
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time, random, json, os, re, tempfile, logging, xmlrpc.client
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
import requests
from requests.auth import HTTPBasicAuth

# ------------------------------------------------------------
# 基本設定
# ------------------------------------------------------------
st.set_page_config(page_title="📝 統合ブログ投稿管理システム", page_icon="🚀", layout="wide", initial_sidebar_state="expanded")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("blog-automation")

# セッション状態の初期化（関数にせず確実に）
_defaults = {
    'authenticated': False,
    'username': None,
    'is_admin': False,
    'posting_status': {},
    'selected_project': None,
}
for _k, _v in _defaults.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ------------------------------------------------------------
# 環境変数/Secrets
# ------------------------------------------------------------
# Google Sheets
SPREADSHEET_ID = st.secrets.google.spreadsheet_id

# Gemini API keys（どちらか存在する想定）
GEMINI_KEYS = []
if 'gemini_api_key_1' in st.secrets.google and st.secrets.google.gemini_api_key_1:
    GEMINI_KEYS.append(st.secrets.google.gemini_api_key_1)
if 'gemini_api_key_2' in st.secrets.google and st.secrets.google.gemini_api_key_2:
    GEMINI_KEYS.append(st.secrets.google.gemini_api_key_2)
if not GEMINI_KEYS:
    GEMINI_KEYS = [""]  # 空だと失敗するが UI で表示するのでOK

# プロジェクト定義
PROJECTS = {
    'ビックギフト': {
        'worksheet': 'ビックギフト向け',
        'icon': '🎁',
        'color': '#ff8c00',
        'platforms': ['Blogger', 'livedoor'],
        'supports_schedule': {'Blogger': False, 'livedoor': False},
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

# ------------------------------------------------------------
# 認証
# ------------------------------------------------------------
def check_authentication() -> bool:
    if not st.session_state.authenticated:
        st.markdown("""
        <style>
        .auth-container {
            max-width: 420px; margin:auto; padding: 1.5rem;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 10px; color: #fff;
        }
        </style>
        """, unsafe_allow_html=True)
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
            st.error("認証に失敗しました")
        st.markdown('</div>', unsafe_allow_html=True)
        return False
    return True

# ------------------------------------------------------------
# Sheets クライアント
# ------------------------------------------------------------
@st.cache_resource
def get_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_dict = st.secrets.gcp.to_dict()
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(creds_dict, f)
        key_path = f.name
    creds = ServiceAccountCredentials.from_json_keyfile_name(key_path, scope)
    os.unlink(key_path)
    return gspread.authorize(creds)

def load_sheet_data(worksheet_name: str) -> pd.DataFrame:
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(worksheet_name)
        values = sheet.get_all_values()
        if len(values) > 1:
            return pd.DataFrame(values[1:], columns=values[0])
        return pd.DataFrame()
    except Exception as e:
        st.error(f"データ読み込みエラー: {e}")
        return pd.DataFrame()

def update_sheet_cell(worksheet_name: str, row: int, col: int, value: str) -> bool:
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(worksheet_name)
        sheet.update_cell(row, col, value)
        return True
    except Exception as e:
        st.error(f"更新エラー: {e}")
        return False

def add_schedule_to_sheet(worksheet_name: str, row_num: int, schedule_times: List[datetime]) -> bool:
    """K列（11列目）から順に書き込む"""
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(worksheet_name)
        for i, dt in enumerate(schedule_times):
            col = 11 + i  # 11=K
            sheet.update_cell(row_num, col, dt.strftime('%Y/%m/%d %H:%M'))
        return True
    except Exception as e:
        st.error(f"予約記録エラー: {e}")
        return False

# ------------------------------------------------------------
# 付帯データ：その他リンク/競合
# ------------------------------------------------------------
def get_other_links() -> List[Dict]:
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet('その他リンク先')
        rows = sheet.get_all_values()[1:]
        out = []
        for r in rows:
            if len(r) >= 2 and r[0] and r[1]:
                out.append({"url": r[0].strip(), "anchor": r[1].strip()})
        if out:
            return out
    except:
        pass
    return [
        {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
        {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"}
    ]

def get_competitor_domains() -> List[str]:
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet('競合他社')
        comp = sheet.get_all_values()[1:]
        domains = []
        for row in comp:
            if row and row[0]:
                d = row[0].strip()
                if d.startswith('http'):
                    d = urlparse(d).netloc
                domains.append(d.lower())
        return domains
    except:
        return []

# ------------------------------------------------------------
# 記事生成（Gemini）
# ------------------------------------------------------------
def call_gemini(prompt: str) -> str:
    api_key = random.choice(GEMINI_KEYS)
    if not api_key:
        raise Exception("Gemini APIキーが設定されていません")
    endpoint = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent?key={api_key}'
    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.7}}
    r = requests.post(endpoint, json=payload, timeout=60)
    if r.status_code != 200:
        raise Exception(f"Gemini API エラー: {r.status_code} {r.text[:200]}")
    j = r.json()
    return j['candidates'][0]['content']['parts'][0]['text']

def generate_article(theme: str, url: str, anchor_text: str) -> Optional[Dict]:
    if not theme or not theme.strip():
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

# 要件:
・2000-2500文字
・専門的でありながら分かりやすい
・具体例と数値を適宜使用
"""
    try:
        txt = call_gemini(prompt).strip()
        lines = txt.split("\n")
        title = lines[0].strip()
        content = "\n".join(lines[1:]).strip()
        # プレースホルダ掃除
        content = re.sub(r'〇〇|××|△△', '', content)
        return {"title": title, "content": content, "theme": theme}
    except Exception as e:
        st.error(f"記事生成エラー: {e}")
        return None

# ------------------------------------------------------------
# WordPress 予約投稿（XML-RPC）：先に定義
# ------------------------------------------------------------
def _get_wp_config_by_site_key(site_key: str) -> Dict:
    # secrets には wp_{site_key} というキーで {url,user,password} を持つことを想定
    secret_key = f"wp_{site_key}"
    if secret_key not in st.secrets:
        raise KeyError(f"secrets に {secret_key} がありません")
    conf = st.secrets[secret_key]
    # conf["url"] は末尾スラッシュあり/なし両対応にする
    base = conf["url"]
    if not base.endswith('/'):
        base += '/'
    return {"url": base, "user": conf["user"], "password": conf["password"]}

def post_to_wordpress_scheduled(article: Dict, site_key: str, schedule_dt: datetime) -> Tuple[bool, str]:
    try:
        conf = _get_wp_config_by_site_key(site_key)
        endpoint = f"{conf['url']}xmlrpc.php"
        server = xmlrpc.client.ServerProxy(endpoint)
        post_data = {
            'post_title': article['title'],
            'post_content': article['content'],
            'post_type': 'post',
            'post_status': 'future',
            'post_date': xmlrpc.client.DateTime(schedule_dt)  # サーバー側TZに依存、必要ならUTC指定へ拡張
        }
        post_id = server.wp.newPost(0, conf['user'], conf['password'], post_data)
        return True, f"{conf['url']}?p={post_id} (予約: {schedule_dt.strftime('%Y/%m/%d %H:%M')})"
    except Exception as e:
        return False, f"WordPress予約投稿エラー({site_key}): {e}"

# Blogger（予約投稿は未サポート→GitHub Actionsへ回す）
def post_to_blogger_scheduled(article: Dict, schedule_dt: datetime) -> Tuple[bool, str]:
    return False, "Blogger予約投稿は未実装（GitHub Actionsで実行）"

# ------------------------------------------------------------
# 予約一括処理：**投稿先は1サイトに限定**
# ------------------------------------------------------------
def process_scheduled_posts(row_data: Dict, project_name: str, project_config: Dict, schedule_times: List[datetime]) -> Dict:
    results = {'success': [], 'failed': [], 'github_actions_needed': []}

    # 投稿先（WordPressのみ）
    wp_sites = [s.lower() for s in project_config.get('sites', [])]
    target_sites: List[str] = []
    if 'WordPress' in project_config.get('platforms', []):
        wp_target = (row_data.get('投稿先') or '').strip().lower()
        if wp_target and wp_target in wp_sites:
            target_sites = [wp_target]
        else:
            target_sites = wp_sites[:1]  # デフォルトは先頭だけ

    # カウンターと上限
    counter = 0
    if 'カウンター' in row_data and str(row_data['カウンター']).strip():
        try:
            counter = int(str(row_data['カウンター']).strip())
        except:
            counter = 0

    max_posts = project_config.get('max_posts', 20)
    if isinstance(max_posts, dict):
        # 複数プラットフォームのときでも、行単位の実運用は1つなので最初の値を採用
        max_posts = list(max_posts.values())[0]

    # 競合・その他リンク
    other_links = get_other_links()
    competitor_domains = get_competitor_domains()
    def pick_other_link():
        cand = []
        for link in other_links:
            domain = urlparse(link['url']).netloc.lower()
            if not any(comp in domain for comp in competitor_domains):
                cand.append(link)
        return random.choice(cand) if cand else None

    for schedule_dt in schedule_times:
        if counter >= max_posts:
            results['failed'].append(f"最大投稿数({max_posts})に達しました")
            break

        # 1~(max-1) はその他リンク、max記事目だけ宣伝URL
        if counter == max_posts - 1:
            use_url = row_data.get('宣伝URL', '').strip()
            use_anchor = (row_data.get('アンカーテキスト') or project_name).strip()
            if not use_url:
                results['failed'].append("宣伝URLが空です")
                counter += 1
                continue
        else:
            chosen = pick_other_link()
            if not chosen:
                results['failed'].append("その他リンクが見つかりません")
                counter += 1
                continue
            use_url, use_anchor = chosen['url'], chosen['anchor']

        theme = (row_data.get('テーマ') or '').strip()
        article = generate_article(theme, use_url, use_anchor)
        if not article:
            results['failed'].append(f"{schedule_dt.strftime('%H:%M')} 記事生成失敗")
            counter += 1
            continue

        # プラットフォームごと
        if 'WordPress' in project_config.get('platforms', []):
            # **1サイト限定** で実行
            for site in target_sites:
                ok, msg = post_to_wordpress_scheduled(article, site, schedule_dt)
                if ok:
                    results['success'].append(f"{site}: {msg}")
                else:
                    results['failed'].append(f"{site}: {msg}")

        # 非WP系は GitHub Actions 側へ
        if 'Blogger' in project_config.get('platforms', []):
            results['github_actions_needed'].append({'platform': 'Blogger', 'schedule': schedule_dt, 'article': article})
        if 'livedoor' in project_config.get('platforms', []):
            results['github_actions_needed'].append({'platform': 'livedoor', 'schedule': schedule_dt, 'article': article})
        if 'Seesaa' in project_config.get('platforms', []):
            results['github_actions_needed'].append({'platform': 'Seesaa', 'schedule': schedule_dt, 'article': article})
        if 'FC2' in project_config.get('platforms', []):
            results['github_actions_needed'].append({'platform': 'FC2', 'schedule': schedule_dt, 'article': article})

        counter += 1

    return results

# ------------------------------------------------------------
# UI
# ------------------------------------------------------------
def main():
    if not check_authentication():
        return

    st.markdown("""
    <style>
    .main-header{background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);padding:1.2rem;border-radius:10px;margin-bottom:1rem;color:white;text-align:center}
    .warning-box{background:#fff3cd;border-left:4px solid #ffc107;padding:1rem;margin:1rem 0}
    .success-box{background:#d4edda;border-left:4px solid #28a745;padding:1rem;margin:1rem 0}
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="main-header"><h1>📝 統合ブログ投稿管理システム</h1><p>完全予約投稿対応版 - PCシャットダウンOK</p></div>', unsafe_allow_html=True)

    with st.sidebar:
        st.markdown(f"### 👤 {st.session_state.username}")
        if st.button("🚪 ログアウト", use_container_width=True):
            st.session_state.authenticated = False
            st.session_state.username = None
            st.rerun()
        st.divider()
        st.markdown("### 🎯 プロジェクト選択")
        project_names = list(PROJECTS.keys())
        selected_project = st.selectbox("プロジェクトを選択", project_names, key="project_selector")
        project_info = PROJECTS[selected_project]

        supports_schedule = project_info.get('supports_schedule', False)
        if isinstance(supports_schedule, dict):
            schedule_status, schedule_color = "一部対応", "#ff9800"
        elif supports_schedule:
            schedule_status, schedule_color = "完全対応", "#4caf50"
        else:
            schedule_status, schedule_color = "GitHub Actions必要", "#f44336"

        st.markdown(
            f"""
            <div style="background:{project_info['color']}20;padding:1rem;border-radius:8px;border-left:4px solid {project_info['color']}">
            <h4>{project_info['icon']} {selected_project}</h4>
            <p>プラットフォーム: {', '.join(project_info['platforms'])}</p>
            <p style="color:{schedule_color}">予約投稿: {schedule_status}</p>
            </div>
            """,
            unsafe_allow_html=True
        )

    tabs = st.tabs(["⏰ 予約投稿", "📝 即時投稿", "📊 ダッシュボード", "⚙️ 設定"])

    # 予約投稿
    with tabs[0]:
        st.markdown("### ⏰ 完全予約投稿システム")

        if project_info.get('supports_schedule') is True or \
           (isinstance(project_info.get('supports_schedule'), dict) and 'WordPress' in str(project_info.get('supports_schedule'))):
            st.markdown('<div class="success-box">✅ <b>このプロジェクトは完全予約投稿対応</b><br>PCをシャットダウンしても、指定時刻に自動投稿されます。</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="warning-box">⚠️ <b>GitHub Actions設定が必要</b><br>予約はシートに記録され、定期実行で投稿します。</div>', unsafe_allow_html=True)

        df = load_sheet_data(project_info['worksheet'])
        if df.empty:
            st.info("データがありません")
        else:
            df.columns = [str(c).strip() if c else f"列{i+1}" for i, c in enumerate(df.columns)]
            if '選択' not in df.columns:
                df.insert(0, '選択', False)

            st.markdown("#### 📋 投稿対象を選択")
            edited_df = st.data_editor(
                df, use_container_width=True, hide_index=True, key="schedule_data_editor",
                column_config={
                    "選択": st.column_config.CheckboxColumn("選択", help="予約投稿する行を選択", default=False),
                }
            )

            st.markdown("#### 🕐 予約スケジュール設定")
            col1, col2 = st.columns([3, 2])
            with col1:
                defaults = []
                now = datetime.now()
                for h in [9, 12, 15, 18]:
                    dt = now.replace(hour=h, minute=0, second=0, microsecond=0)
                    if dt > now:
                        defaults.append(dt.strftime('%Y/%m/%d %H:%M'))
                if not defaults:
                    tmr = now + timedelta(days=1)
                    for h in [9, 12, 15, 18]:
                        dt = tmr.replace(hour=h, minute=0, second=0, microsecond=0)
                        defaults.append(dt.strftime('%Y/%m/%d %H:%M'))

                schedule_input = st.text_area("予約日時（1行1件）", value="\n".join(defaults), height=180, help="形式: YYYY/MM/DD HH:MM")
                posts_per_time = st.number_input("各時刻での投稿数", min_value=1, max_value=5, value=1, step=1)

            with col2:
                st.markdown("#### 📊 予約サマリー")
                schedule_times: List[datetime] = []
                for line in schedule_input.strip().split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        dt = datetime.strptime(line, "%Y/%m/%d %H:%M")
                        if dt > datetime.now():
                            schedule_times.append(dt)
                    except:
                        pass

                if schedule_times:
                    st.success(f"✅ {len(schedule_times)}回の投稿を予約")
                    for dt in schedule_times[:5]:
                        st.write(f"• {dt.strftime('%m/%d %H:%M')}")
                    if len(schedule_times) > 5:
                        st.write(f"... 他 {len(schedule_times)-5}件")
                else:
                    st.warning("有効な予約時刻がありません")

                selected_count = len(edited_df[edited_df['選択'] == True]) if '選択' in edited_df.columns else 0
                st.info(f"選択行数: {selected_count}")
                if selected_count > 0 and schedule_times:
                    total_posts = selected_count * len(schedule_times) * posts_per_time
                    st.metric("総投稿数", total_posts)

            if st.button("🚀 予約投稿を実行", type="primary", use_container_width=True):
                selected_rows = edited_df[edited_df['選択'] == True] if '選択' in edited_df.columns else pd.DataFrame()
                if selected_rows.empty:
                    st.error("投稿する行を選択してください")
                elif not schedule_times:
                    st.error("有効な予約時刻を入力してください")
                else:
                    progress = st.progress(0)
                    status_text = st.empty()
                    total_tasks = len(selected_rows) * len(schedule_times)
                    done = 0
                    all_results = []

                    for idx, row in selected_rows.iterrows():
                        row_num = idx + 2  # シート行番号
                        status_text.text(f"処理中: {str(row.get('宣伝URL',''))[:30]} ...")

                        results = process_scheduled_posts(row.to_dict(), selected_project, project_info, schedule_times)
                        all_results.append(results)

                        # シートに予約刻印（K列以降）とステータス
                        if results['success'] or results['github_actions_needed']:
                            add_schedule_to_sheet(project_info['worksheet'], row_num, schedule_times)
                            update_sheet_cell(project_info['worksheet'], row_num, 5, '予約済み')

                        done += len(schedule_times)
                        progress.progress(min(1.0, done / max(1, total_tasks)))

                    # 結果まとめ
                    total_success = sum(len(r['success']) for r in all_results)
                    total_failed = sum(len(r['failed']) for r in all_results)
                    total_ga = sum(len(r['github_actions_needed']) for r in all_results)

                    st.markdown("### 📊 予約結果")
                    c1, c2, c3 = st.columns(3)
                    c1.metric("✅ 成功", total_success)
                    c2.metric("❌ 失敗", total_failed)
                    c3.metric("⏰ GitHub Actions待ち", total_ga)

                    if total_success > 0:
                        st.success("✅ 予約投稿の登録が完了しました（WordPressはサーバで自動公開）。")
                    if total_ga > 0:
                        st.warning("⚠️ Blogger / livedoor / Seesaa / FC2 は GitHub Actions 側で実行します。")

                    time.sleep(2)
                    st.rerun()

    # 即時投稿（簡易）
    with tabs[1]:
        st.markdown("### 📝 即時投稿")
        st.info("即時投稿は簡易版です。予約投稿の利用を推奨します。")

    # ダッシュボード
    with tabs[2]:
        st.markdown("### 📊 ダッシュボード")
        df2 = load_sheet_data(project_info['worksheet'])
        if df2.empty:
            st.info("データがありません")
        else:
            status_col = 'ステータス' if 'ステータス' in df2.columns else (df2.columns[4] if len(df2.columns) > 4 else None)
            total = len(df2)
            completed = len(df2[df2[status_col] == '処理済み']) if status_col else 0
            scheduled = len(df2[df2[status_col] == '予約済み']) if status_col else 0
            processing = total - completed - scheduled
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("総URL数", total)
            c2.metric("処理済み", completed)
            c3.metric("予約済み", scheduled)
            c4.metric("未処理", processing)

            st.markdown("### 📅 予約状況プレビュー（K列以降の一部）")
            # K列以降（列番号 >= 11）を探す（列名が明示されていない場合の保険）
            sched_cols = [c for c in df2.columns if (c.startswith('列') and c[1:].isdigit() and int(c[1:]) >= 11) or re.match(r'^\d{4}/\d{2}/\d{2}', str(c))]
            cols_to_show = []
            if '宣伝URL' in df2.columns: cols_to_show.append('宣伝URL')
            if status_col: cols_to_show.append(status_col)
            cols_to_show += sched_cols[:5]
            if cols_to_show:
                st.dataframe(df2[cols_to_show], use_container_width=True)
            else:
                st.info("予約刻印の列が検出できませんでした（K列以降が未入力の可能性）")

    # 設定
    with tabs[3]:
        st.markdown("### ⚙️ 設定")
        if st.session_state.is_admin:
            st.markdown("#### 🤖 GitHub Actions（30分ごと実行例）")
            st.code(
                """name: Auto Blog Post
on:
  schedule:
    - cron: '0,30 * * * *'
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
""",
                language="yaml"
            )
            st.info("Blogger/livedoor/Seesaa/FC2 は Actions 側で実行してください。")

# ------------------------------------------------------------
if __name__ == "__main__":
    main()

