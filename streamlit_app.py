# streamlit_app.py

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

# ========================
# 設定値（Secretsから取得）
# ========================
# Streamlit Secretsから設定値を取得
try:
    SHEET_ID = st.secrets["google"]["spreadsheet_id"]
    GEMINI_API_KEYS = [
        st.secrets["google"]["gemini_api_key_1"],
        st.secrets["google"]["gemini_api_key_2"],
    ]
except KeyError as e:
    st.error(f"Secretsの設定が不足しています: {e}")
    st.stop()

# プロジェクト設定（完全版）
PROJECT_CONFIGS = {
    'biggift': {
        'worksheet': 'ビックギフト向け',
        'platforms': ['blogger', 'livedoor'],
        'max_posts': {'blogger': 20, 'livedoor': 15},
        'needs_k_column': True  # 予約時刻をK列に記録（GitHub Actionsで処理）
    },
    'arigataya': {
        'worksheet': 'ありがた屋向け',
        'platforms': ['seesaa', 'fc2'],
        'max_posts': 20,
        'needs_k_column': True  # 予約時刻をK列に記録（GitHub Actionsで処理）
    },
    'kaitori_life': {
        'worksheet': '買取LIFE向け',
        'platforms': ['wordpress'],
        'wp_sites': ['selectad', 'thrones'],
        'max_posts': 20,
        'needs_k_column': False  # WordPressの予約機能を使用
    },
    'osaifu_rescue': {
        'worksheet': 'お財布レスキュー向け',
        'platforms': ['wordpress'],
        'wp_sites': ['ykikaku', 'efdlqjtz'],
        'max_posts': 20,
        'needs_k_column': False  # WordPressの予約機能を使用
    },
    'kure_kaeru': {
        'worksheet': 'クレかえる向け',
        'platforms': ['wordpress'],
        'wp_sites': ['selectadvance', 'welkenraedt'],
        'max_posts': 20,
        'needs_k_column': False  # WordPressの予約機能を使用
    },
    'red_site': {
        'worksheet': '赤いサイト向け',
        'platforms': ['wordpress'],
        'wp_sites': ['ncepqvub', 'kosagi'],
        'max_posts': 20,
        'needs_k_column': False  # WordPressの予約機能（kosagiは特殊）
    }
}

# ★★ 必ず提示どおりに統一（ローカルで成功した設定）★★
WP_CONFIGS = {
    'ykikaku': {
        'url': 'https://ykikaku.xsrv.jp/',
        'user': 'ykikaku',
        'password': 'lfXp BJNx Rvy8 rBlt Yjug ADRn'
    },
    'efdlqjtz': {
        'url': 'https://www.efdlqjtz.v2010.coreserver.jp/',
        'user': 'efdlqjtz',
        'password': 'KCIA cTyz TcdG U1Qs M4pd eezb'
    },
    'selectadvance': {
        'url': 'https://selectadvance.v2006.coreserver.jp/',
        'user': 'selectadvance',
        'password': '6HUY g7oZ Gow8 LBCu yzL8 cR3S'
    },
    'welkenraedt': {
        'url': 'https://www.welkenraedt-online.com/',
        'user': 'welkenraedtonline',
        'password': 'yzn4 6nlm vtrh 8N4v oxHl KUvf'
    },
    'ncepqvub': {
        'url': 'https://ncepqvub.v2009.coreserver.jp/',
        'user': 'ncepqvub',
        'password': 'DIZy ky10 UAhO NJ47 6Jww ImdE'
    },
    'kosagi': {
        'url': 'https://www.kosagi.jp/',
        'user': 'kosagi',
        'password': 'K2DZ ERIy aTLb K2Z0 gHi6 XdIN'
    },
    'selectad': {
        'url': 'https://selectad01.xsrv.jp/',
        'user': 'selectad01',
        'password': '8LhM laXm pDUx gkjV cg1f EXYr'
    },
    'thrones': {
        'url': 'https://thrones.v2009.coreserver.jp/',
        'user': 'thrones',
        'password': 'ETvJ VP2F jugd mxXU xJX0 wHVr'
    }
}

# 各プラットフォーム設定
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

# 投稿間隔（スパム回避） - Streamlit用に短縮
MIN_INTERVAL = 30
MAX_INTERVAL = 60

# ========================
# Streamlit設定
# ========================
st.set_page_config(
    page_title="統合ブログ投稿ツール",
    page_icon="🚀",
    layout="wide"
)

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
    .warning-box {
        background: #fff3cd;
        border: 1px solid #ffc107;
        color: #856404;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .success-box {
        background: #d4edda;
        border: 1px solid #28a745;
        color: #155724;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# ========================
# セッションステート初期化
# ========================
if 'gemini_key_index' not in st.session_state:
    st.session_state.gemini_key_index = 0

# 投稿処理中フラグ（プロジェクト別）
if 'posting_projects' not in st.session_state:
    st.session_state.posting_projects = set()

if 'current_project' not in st.session_state:
    st.session_state.current_project = None

# リアルタイムログ用
if 'realtime_logs' not in st.session_state:
    st.session_state.realtime_logs = []

# ========================
# 認証 & シート取得
# ========================
@st.cache_resource
def get_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    try:
        # Streamlit Secretsから認証情報を取得（TOML形式）
        if "gcp" in st.secrets:
            gcp_info = st.secrets["gcp"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(dict(gcp_info), scope)
            return gspread.authorize(creds)
    except Exception:
        # GitHub Actions用フォールバック
        creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
        if creds_json:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                f.write(creds_json)
                temp_path = f.name
            creds = ServiceAccountCredentials.from_json_keyfile_name(temp_path, scope)
            os.unlink(temp_path)
            return gspread.authorize(creds)
    
    st.error("Google認証情報が設定されていません。Secretsの[gcp]セクションを確認してください。")
    st.stop()

# ========================
# 競合他社・その他リンク管理
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
                {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
                {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"},
            ]
        return other_sites
    except Exception:
        return [
            {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
            {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"},
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
# アイキャッチ画像自動生成
# ========================
def _load_font_candidates():
    # リポジトリ同梱フォントを最優先
    candidates = [
        "fonts/NotoSansJP-Bold.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansJP-Bold.otf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "C:/Windows/Fonts/meiryob.ttc",
        "C:/Windows/Fonts/meiryo.ttc",
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return None

def create_eyecatch_image(title: str, site_key: str) -> bytes:
    """タイトルからアイキャッチ画像を自動生成(サイト別カラー)"""
    width, height = 600, 400
    
    site_color_schemes = {
        'selectadvance': [
            {'bg': '#2E7D32', 'accent': '#66BB6A', 'text': '#FFFFFF'},
            {'bg': '#388E3C', 'accent': '#81C784', 'text': '#FFFFFF'},
        ],
        'welkenraedt': [
            {'bg': '#1976D2', 'accent': '#64B5F6', 'text': '#FFFFFF'},
            {'bg': '#303F9F', 'accent': '#7986CB', 'text': '#FFFFFF'},
        ],
        'ykikaku': [
            {'bg': '#E91E63', 'accent': '#F48FB1', 'text': '#FFFFFF'},
            {'bg': '#C2185B', 'accent': '#F8BBD9', 'text': '#FFFFFF'},
        ],
        'efdlqjtz': [
            {'bg': '#FF5722', 'accent': '#FF8A65', 'text': '#FFFFFF'},
            {'bg': '#D84315', 'accent': '#FFAB91', 'text': '#FFFFFF'},
        ],
        'ncepqvub': [
            {'bg': '#B71C1C', 'accent': '#EF5350', 'text': '#FFFFFF'},
            {'bg': '#C62828', 'accent': '#E57373', 'text': '#FFFFFF'},
        ],
        'kosagi': [
            {'bg': '#B71C1C', 'accent': '#EF5350', 'text': '#FFFFFF'},
            {'bg': '#C62828', 'accent': '#E57373', 'text': '#FFFFFF'},
        ],
        'selectad': [
            {'bg': '#4A148C', 'accent': '#AB47BC', 'text': '#FFFFFF'},
            {'bg': '#6A1B9A', 'accent': '#CE93D8', 'text': '#FFFFFF'},
        ],
        'thrones': [
            {'bg': '#004D40', 'accent': '#26A69A', 'text': '#FFFFFF'},
            {'bg': '#00695C', 'accent': '#4DB6AC', 'text': '#FFFFFF'},
        ],
        'default': [
            {'bg': '#4CAF50', 'accent': '#8BC34A', 'text': '#FFFFFF'},
            {'bg': '#689F38', 'accent': '#AED581', 'text': '#FFFFFF'},
        ]
    }
    scheme = random.choice(site_color_schemes.get(site_key, site_color_schemes['default']))
    
    img = Image.new('RGB', (width, height), color=scheme['bg'])
    draw = ImageDraw.Draw(img)
    # グラデーション
    for i in range(height):
        alpha = i / height
        r = int(int(scheme['bg'][1:3], 16) * (1 - alpha * 0.3))
        g = int(int(scheme['bg'][3:5], 16) * (1 - alpha * 0.3))
        b = int(int(scheme['bg'][5:7], 16) * (1 - alpha * 0.3))
        draw.rectangle([(0, i), (width, i + 1)], fill=(r, g, b))
    # 装飾
    draw.ellipse([-50, -50, 150, 150], fill=scheme['accent'])
    draw.ellipse([width-100, height-100, width+50, height+50], fill=scheme['accent'])
    
    # フォント
    font_path = _load_font_candidates()
    try:
        title_font = ImageFont.truetype(font_path if font_path else "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 28)
        subtitle_font = ImageFont.truetype(font_path if font_path else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 20)
    except Exception:
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
    
    # タイトルの改行
    lines = []
    if len(title) > 12:
        for sep in ['！', '？', '…', '!', '?', '、', '。', '・', '｜']:
            if sep in title:
                idx = title.find(sep)
                if 5 < idx < len(title) - 5:
                    lines = [title[:idx+1], title[idx+1:].strip()]
                    break
        if not lines:
            for sep in ['と', 'の', 'は', 'が', 'を', 'に', 'で']:
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
        except AttributeError:
            text_width, _ = draw.textsize(line, font=title_font)
        x = (width - text_width) // 2
        y = y_start + i * 50
        draw.text((x + 2, y + 2), line, font=title_font, fill=(0, 0, 0))
        draw.text((x, y), line, font=title_font, fill=scheme['text'])
    
    site_names = {
        'selectadvance': '後払いアプリ現金化攻略ブログ',
        'welkenraedt': 'マネーハック365',
        'ykikaku': 'お財布レスキュー',
        'efdlqjtz': 'キャッシュアドバイザー',
        'ncepqvub': 'あと払いスマートライフ',
        'kosagi': '金欠ブロガーの裏金策帖',
        'selectad': '買取LIFEサポート',
        'thrones': 'アセットマネジメント'
    }
    site_name = site_names.get(site_key, 'Financial Blog')
    try:
        bbox = draw.textbbox((0, 0), site_name, font=subtitle_font)
        text_width = bbox[2] - bbox[0]
    except AttributeError:
        text_width, _ = draw.textsize(site_name, font=subtitle_font)
    x = (width - text_width) // 2
    draw.text((x, height - 50), site_name, font=subtitle_font, fill=scheme['text'])
    draw.rectangle([50, 40, width-50, 42], fill=scheme['text'])
    
    bio = io.BytesIO()
    img.save(bio, format='JPEG', quality=90)
    bio.seek(0)
    return bio.getvalue()

def upload_image_to_wordpress(image_data: bytes, filename: str, site_config: dict) -> int:
    """画像をWordPressにアップロードしてIDを返す"""
    media_endpoint = f'{site_config["url"]}wp-json/wp/v2/media'
    import string
    safe_filename = ''.join(c for c in filename if c in string.ascii_letters + string.digits + '-_.')
    if not safe_filename or safe_filename == '.jpg':
        safe_filename = f"eyecatch_{int(time.time())}.jpg"
    if not safe_filename.endswith('.jpg'):
        safe_filename += '.jpg'
    headers = {
        'Content-Disposition': f'attachment; filename="{safe_filename}"',
        'Content-Type': 'image/jpeg'
    }
    try:
        response = requests.post(
            media_endpoint,
            data=image_data,
            headers=headers,
            auth=HTTPBasicAuth(site_config['user'], site_config['password']),
            timeout=60,
            verify=False
        )
        if response.status_code == 201:
            return response.json()['id']
        return None
    except Exception:
        return None

def generate_slug_from_title(title):
    """タイトルから英数字のスラッグを生成"""
    keyword_map = {
        '投資': 'investment','資産': 'asset','運用': 'management','増やす': 'increase',
        '貯金': 'savings','節約': 'saving','クレジット': 'credit','カード': 'card',
        'ローン': 'loan','金融': 'finance','銀行': 'bank','保険': 'insurance','実践': 'practice',
        '方法': 'method','戦略': 'strategy','ガイド': 'guide','初心者': 'beginner','完全': 'complete',
        '効果': 'effect','成功': 'success','選び方': 'selection','比較': 'comparison','活用': 'utilization',
        'おすすめ': 'recommend','基礎': 'basic','知識': 'knowledge'
    }
    slug_parts = ['money']
    for jp_word, en_word in keyword_map.items():
        if jp_word in title:
            slug_parts.append(en_word); break
    if len(slug_parts) == 1:
        slug_parts.append('tips')
    date_str = datetime.now().strftime('%m%d')
    random_num = random.randint(100, 999)
    return ('-'.join(slug_parts) + f'-{date_str}-{random_num}').lower()

def _get_gemini_key():
    key = GEMINI_API_KEYS[st.session_state.gemini_key_index % len(GEMINI_API_KEYS)]
    st.session_state.gemini_key_index += 1
    return key

def call_gemini(prompt: str) -> str:
    api_key = _get_gemini_key()
    endpoint = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent?key={api_key}'
    payload = {"contents": [{"parts": [{"text": prompt}]}]], "generationConfig": {"temperature": 0.7}}
    response = requests.post(endpoint, json=payload, timeout=60)
    if response.status_code != 200:
        raise Exception(f"Gemini API エラー: {response.status_code} / {response.text[:200]}")
    result = response.json()
    return result['candidates'][0]['content']['parts'][0]['text']

def generate_article_with_link(theme: str, url: str, anchor_text: str) -> dict:
    auto_theme = False
    if not theme or theme.strip() == "":
        theme = "金融・投資・資産運用"; auto_theme = True
    theme_instruction = f"「{theme}」をテーマに" if not auto_theme else "金融系（投資、クレジットカード、ローン、資産運用など）から自由にテーマを選んで"
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
        content = re.sub(r'〇〇|××|△△', '', content)
        content = re.sub(r'（ここで.*?）', '', content)
        content = re.sub(r'<p>\s*</p>', '', content)
        content = content.strip()
        return {"title": title, "content": content, "theme": theme if not auto_theme else "金融"}
    except Exception as e:
        st.error(f"記事生成エラー: {e}")
        raise

# ========================
# WordPress 周り
# ========================
def get_category_id(site_config, category_name):
    """カテゴリー名からIDを取得"""
    if not category_name:
        return None
    try:
        endpoint = f"{site_config['url']}wp-json/wp/v2/categories"
        response = requests.get(endpoint, timeout=30, verify=False)
        if response.status_code == 200:
            for cat in response.json():
                if cat['name'] == category_name:
                    return cat['id']
        return None
    except Exception:
        return None

def post_to_wordpress(article_data: dict, site_key: str, category_name: str = None, 
                      schedule_dt: datetime = None, enable_eyecatch: bool = True) -> str:
    """WordPressに投稿（kosagiはXML-RPC）"""
    if site_key not in WP_CONFIGS:
        st.error(f"不明なサイト: {site_key}")
        return ""
    site_config = WP_CONFIGS[site_key]

    # kosagi: XML-RPC (予約はPython側で待機)
    if site_key == 'kosagi':
        if schedule_dt and schedule_dt > datetime.now():
            wait_seconds = int((schedule_dt - datetime.now()).total_seconds())
            st.info(f"kosagi用: {schedule_dt.strftime('%H:%M')}まで待機します（{wait_seconds}秒）")
            progress_bar = st.progress(0)
            for i in range(wait_seconds):
                progress_bar.progress((i + 1) / max(1, wait_seconds))
                time.sleep(1)
            st.success("予約時刻になりました。kosagiに投稿を開始します")
        try:
            server = xmlrpc.client.ServerProxy(f"{site_config['url']}xmlrpc.php")
            content = {
                "title": article_data['title'],
                "description": article_data['content'],
                "mt_allow_comments": 1,
                "mt_allow_pings": 1,
                "post_type": "post"
            }
            post_id = server.metaWeblog.newPost(0, site_config['user'], site_config['password'], content, True)
            post_url = f"{site_config['url']}?p={post_id}"
            st.success(f"kosagi投稿成功 (XMLRPC): {post_url}")
            return post_url
        except Exception as e:
            st.error(f"kosagi投稿エラー: {e}")
            return ""

    # 通常WP: REST API
    featured_media_id = None
    if enable_eyecatch:
        try:
            with st.spinner("アイキャッチ画像を生成中..."):
                image_data = create_eyecatch_image(article_data['title'], site_key)
                image_filename = f"{generate_slug_from_title(article_data['title'])}.jpg"
                featured_media_id = upload_image_to_wordpress(image_data, image_filename, site_config)
                if featured_media_id:
                    st.success("アイキャッチ画像設定完了")
                else:
                    st.warning("アイキャッチ画像の設定をスキップして記事投稿を続行")
        except Exception as e:
            st.warning(f"アイキャッチ画像生成エラー: {e}")
    
    endpoint = f"{site_config['url']}wp-json/wp/v2/posts"
    category_id = get_category_id(site_config, category_name) if category_name else None
    slug = generate_slug_from_title(article_data['title'])

    post_data = {
        'title': article_data['title'],
        'content': article_data['content'],
        'slug': slug,
        'categories': [category_id] if category_id else []
    }
    if featured_media_id:
        post_data['featured_media'] = featured_media_id
    if schedule_dt and schedule_dt > datetime.now():
        post_data['status'] = 'future'
        post_data['date'] = schedule_dt.strftime('%Y-%m-%dT%H:%M:%S')
        st.info(f"予約投稿設定: {schedule_dt.strftime('%Y/%m/%d %H:%M')}")
    else:
        post_data['status'] = 'publish'

    try:
        response = requests.post(
            endpoint,
            auth=HTTPBasicAuth(site_config['user'], site_config['password']),
            headers={'Content-Type': 'application/json', 'User-Agent': 'WordPress/6.0; streamlit-app'},
            data=json.dumps(post_data),
            timeout=60,
            verify=False
        )
        if response.status_code in (201, 200):
            post_url = response.json().get('link', '')
            if schedule_dt and schedule_dt > datetime.now():
                st.success(f"予約投稿成功 ({site_key}): {schedule_dt.strftime('%Y/%m/%d %H:%M')}に公開予定")
            else:
                st.success(f"投稿成功 ({site_key}): {post_url}")
            return post_url
        else:
            st.error(f"WordPress投稿失敗 ({site_key}): {response.status_code}\n{response.text[:300]}...")
            return ""
    except Exception as e:
        st.error(f"WordPress投稿エラー ({site_key}): {e}")
        return ""

def test_wordpress_connection(site_key):
    """WordPress接続テスト関数（REST: /users/me、kosagiはXML-RPC ping）"""
    if site_key not in WP_CONFIGS:
        st.error(f"設定が見つかりません: {site_key}")
        return False
    config = WP_CONFIGS[site_key]
    try:
        if site_key == 'kosagi':
            server = xmlrpc.client.ServerProxy(f"{config['url']}xmlrpc.php")
            blogs = server.blogger.getUsersBlogs("", config['user'], config['password'])
            st.success("kosagi 接続成功 (XML-RPC)")
            st.write(blogs)
            return True
        test_url = f"{config['url']}wp-json/wp/v2/users/me"
        st.info(f"🔍 {site_key} 接続テスト中...")
        response = requests.get(
            test_url,
            auth=HTTPBasicAuth(config['user'], config['password']),
            timeout=15,
            verify=False
        )
        st.write(f"**ステータスコード**: {response.status_code}")
        if response.status_code == 200:
            st.success(f"✅ {site_key} 接続成功")
            user_data = response.json()
            st.write(f"**ユーザー名**: {user_data.get('name', 'N/A')}")
            st.write(f"**権限**: {user_data.get('roles', 'N/A')}")
            return True
        elif response.status_code == 403:
            st.error(f"❌ {site_key} アクセス拒否 (403)")
            st.write("- セキュリティプラグイン/WAF/IP制限/REST API無効の可能性")
        elif response.status_code == 401:
            st.error(f"❌ {site_key} 認証失敗 (401) → ユーザー/パス確認")
        else:
            st.error(f"❌ {site_key} 接続失敗 ({response.status_code})")
        st.write(f"**レスポンス内容**: {response.text[:500]}...")
        return False
    except Exception as e:
        st.error(f"❌ {site_key} 接続エラー: {e}")
        return False

# ========================
# 各プラットフォーム投稿関数
# ========================
def post_to_seesaa(article: dict, category_name: str = None) -> str:
    """Seesaa投稿"""
    config = PLATFORM_CONFIGS['seesaa']
    server = xmlrpc.client.ServerProxy(config['endpoint'], allow_none=True)
    content = {"title": article["title"], "description": article["content"]}
    try:
        post_id = server.metaWeblog.newPost(config['blogid'], config['username'], config['password'], content, True)
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
        st.error(f"Seesaa投稿エラー: {e}")
        return ""

def post_to_fc2(article: dict, category_name: str = None) -> str:
    """FC2投稿"""
    config = PLATFORM_CONFIGS['fc2']
    server = xmlrpc.client.ServerProxy(config['endpoint'])
    content = {'title': article['title'], 'description': article['content']}
    try:
        post_id = server.metaWeblog.newPost(config['blog_id'], config['username'], config['password'], content, True)
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
        st.error(f"FC2投稿エラー: {e}")
        return ""

def post_to_livedoor(article: dict, category_name: str = None) -> str:
    """livedoor投稿"""
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
            st.error(f"livedoor投稿失敗: {response.status_code}")
            return ""
    except Exception as e:
        st.error(f"livedoor投稿エラー: {e}")
        return ""

def post_to_blogger(article: dict) -> str:
    """Blogger投稿(簡易実装)"""
    st.warning("Blogger投稿は未実装（認証が複雑なため）")
    return ""

# ========================
# スプレッドシート操作
# ========================
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
        clean_headers = []
        for i, header in enumerate(headers):
            clean_headers.append(f"{header}_{i}" if header in clean_headers else header)
        filtered_rows = []
        for row in data_rows:
            if len(row) >= 5 and row[1] and row[1].strip():
                status = row[4].strip().lower() if len(row) > 4 else ''
                if status in ['', '未処理']:
                    adjusted_row = row + [''] * (len(clean_headers) - len(row))
                    filtered_rows.append(adjusted_row[:len(clean_headers)])
        if not filtered_rows:
            return pd.DataFrame()
        df = pd.DataFrame(filtered_rows, columns=clean_headers)
        if '選択' not in df.columns:
            df.insert(0, '選択', False)
        return df
    except Exception as e:
        st.error(f"データ読み込みエラー: {e}")
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
                return True
        return False
    except Exception as e:
        st.error(f"スプレッドシート更新エラー: {e}")
        return False

def add_schedule_to_k_column(project_key, row_data, schedule_times):
    """K列以降に予約時刻を追加(非WordPressプロジェクト用)"""
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
                    while col_num <= len(row) + 10:
                        try:
                            current_value = sheet.cell(i, col_num).value
                            if not current_value:
                                break
                        except Exception:
                            break
                        col_num += 1
                    sheet.update_cell(i, col_num, schedule_dt.strftime('%Y/%m/%d %H:%M'))
                    col_num += 1
                return True
        return False
    except Exception as e:
        st.error(f"K列記録エラー: {e}")
        return False

# ========================
# 投稿処理（プラットフォーム判定）
# ========================
def get_max_posts_for_project(project_key, post_target=""):
    """プロジェクトと投稿先に応じた最大投稿数を取得"""
    config = PROJECT_CONFIGS[project_key]
    max_posts = config['max_posts']
    if isinstance(max_posts, dict):
        if post_target.lower() == 'livedoor':
            return 15
        elif post_target.lower() == 'blogger':
            return 20
        else:
            return 20
    else:
        return max_posts

def add_realtime_log(message):
    """リアルタイムログを追加"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.realtime_logs.append(f"[{timestamp}] {message}")

def execute_post(row_data, project_key, post_count=1, schedule_times=None, enable_eyecatch=True):
    """投稿実行（Tkinter版のロジック準拠）"""
    try:
        st.session_state.posting_projects.add(project_key)
        add_realtime_log(f"📋 {PROJECT_CONFIGS[project_key]['worksheet']} の投稿開始")
        config = PROJECT_CONFIGS[project_key]
        schedule_times = schedule_times or []
        current_counter = 0
        if 'カウンター' in row_data and row_data['カウンター']:
            try:
                current_counter = int(row_data['カウンター'])
            except Exception:
                current_counter = 0
        add_realtime_log(f"📊 現在のカウンター: {current_counter}")

        post_target = row_data.get('投稿先', '').strip() or (config.get('wp_sites', ['ykikaku'])[0] if 'wordpress' in config['platforms'] else '')
        add_realtime_log(f"🎯 投稿先: '{post_target}'")

        max_posts = get_max_posts_for_project(project_key, post_target)
        if current_counter >= max_posts:
            add_realtime_log(f"⚠️ 既に{max_posts}記事完了済み")
            st.warning(f"既に{max_posts}記事完了しています")
            return False

        posts_completed = 0
        progress_bar = st.progress(0)

        for i in range(post_count):
            if current_counter >= max_posts:
                add_realtime_log(f"⚠️ カウンター{current_counter}: 既に{max_posts}記事完了済み")
                break
            schedule_dt = schedule_times[i] if i < len(schedule_times) else None
            add_realtime_log(f"📝 記事{i+1}/{post_count}の処理開始")

            with st.expander(f"記事{i+1}/{post_count}の投稿", expanded=True):
                try:
                    # 記事に使うリンク
                    if current_counter == max_posts - 1:
                        add_realtime_log(f"🎯 {max_posts}記事目 → 宣伝URL使用")
                        url = row_data.get('宣伝URL', '')
                        anchor = row_data.get('アンカーテキスト', 'お財布レスキュー')
                        category = row_data.get('カテゴリー', 'お金のマメ知識')
                    else:
                        add_realtime_log(f"🔗 {current_counter + 1}記事目 → その他リンク使用")
                        url, anchor = get_other_link()
                        if not url:
                            add_realtime_log("❌ その他リンクが取得できません")
                            st.error("その他リンクが取得できません")
                            break
                        category = 'お金のマメ知識'

                    # 記事生成
                    add_realtime_log("🧠 記事を生成中...")
                    with st.spinner("記事を生成中..."):
                        theme = row_data.get('テーマ', '') or '金融・投資・資産運用'
                        article = generate_article_with_link(theme, url, anchor)
                    st.success(f"タイトル: {article['title']}")
                    st.info(f"使用リンク: {anchor}")

                    posted_urls = []
                    platforms = config['platforms']

                    if 'wordpress' in platforms:
                        add_realtime_log(f"🔍 WordPress投稿開始 - 投稿先: '{post_target}'")
                        # 固定2サイト
                        if post_target in ['ykikaku', '両方']:
                            add_realtime_log("📤 ykikakuに投稿中...")
                            post_url = post_to_wordpress(article, 'ykikaku', category, schedule_dt, enable_eyecatch)
                            if post_url: posted_urls.append(post_url); add_realtime_log("✅ ykikaku投稿成功")
                            else: add_realtime_log("❌ ykikaku投稿失敗")
                        if post_target in ['efdlqjtz', '両方']:
                            add_realtime_log("📤 efdlqjtzに投稿中...")
                            post_url = post_to_wordpress(article, 'efdlqjtz', category, schedule_dt, enable_eyecatch)
                            if post_url: posted_urls.append(post_url); add_realtime_log("✅ efdlqjtz投稿成功")
                            else: add_realtime_log("❌ efdlqjtz投稿失敗")
                        # 任意サイト（プロジェクトごとの wp_sites）
                        available_sites = config.get('wp_sites', [])
                        if post_target not in ['ykikaku', 'efdlqjtz', '両方'] and post_target in available_sites:
                            add_realtime_log(f"📤 {post_target}に投稿中...")
                            post_url = post_to_wordpress(article, post_target, category, schedule_dt, enable_eyecatch)
                            if post_url: posted_urls.append(post_url); add_realtime_log(f"✅ {post_target}投稿成功")
                            else: add_realtime_log(f"❌ {post_target}投稿失敗")

                    if 'seesaa' in platforms:
                        add_realtime_log("📤 Seesaaに投稿中...")
                        post_url = post_to_seesaa(article, category)
                        if post_url: posted_urls.append(post_url); add_realtime_log("✅ Seesaa投稿成功")

                    if 'fc2' in platforms:
                        add_realtime_log("📤 FC2に投稿中...")
                        post_url = post_to_fc2(article, category)
                        if post_url: posted_urls.append(post_url); add_realtime_log("✅ FC2投稿成功")

                    if 'livedoor' in platforms:
                        add_realtime_log("📤 livedoorに投稿中...")
                        post_url = post_to_livedoor(article, category)
                        if post_url: posted_urls.append(post_url); add_realtime_log("✅ livedoor投稿成功")

                    if 'blogger' in platforms:
                        add_realtime_log("📤 Bloggerに投稿中...")
                        post_url = post_to_blogger(article)
                        if post_url: posted_urls.append(post_url); add_realtime_log("✅ Blogger投稿成功")

                    if not posted_urls:
                        add_realtime_log("❌ 投稿に失敗しました")
                        st.error("投稿に失敗しました")
                        break

                    current_counter += 1
                    posts_completed += 1

                    # スプレッドシート更新
                    updates = {'カウンター': str(current_counter)}
                    if current_counter >= max_posts:
                        updates['ステータス'] = '処理済み'
                    # 投稿URL列がある場合のみ更新
                    updates['投稿URL'] = ', '.join(posted_urls)
                    update_sheet_row(project_key, row_data, updates)

                    st.success(f"投稿完了 {posts_completed}/{post_count}")
                except Exception as e:
                    add_realtime_log(f"❌ 記事{i+1}の投稿エラー: {e}")
                    st.error(f"記事{i+1}の投稿エラー: {e}")
                    break

            progress_bar.progress(posts_completed / max(1, post_count))

            if posts_completed < post_count:
                wait_time = random.randint(MIN_INTERVAL, MAX_INTERVAL)
                add_realtime_log(f"⏳ 次の投稿まで {wait_time} 秒待機")
                for s in range(wait_time):
                    time.sleep(1)

        add_realtime_log(f"✅ {posts_completed}件の投稿を完了")
        return True
    except Exception as e:
        add_realtime_log(f"❌ 投稿処理エラー: {e}")
        st.error(f"投稿処理エラー: {e}")
        return False
    finally:
        st.session_state.posting_projects.discard(project_key)

# ========================
# UI構築
# ========================
def main():
    st.title("SEOブログ自動化ツール")
    st.write("記事を自動生成して複数のプラットフォームに投稿します")
    
    project_options = {
        'biggift': 'ビックギフト（非WordPress・K列予約）',
        'arigataya': 'ありがた屋（非WordPress・K列予約）',
        'kaitori_life': '買取LIFE（WordPress・予約投稿）',
        'osaifu_rescue': 'お財布レスキュー（WordPress・予約投稿）',
        'kure_kaeru': 'クレかえる（WordPress・予約投稿）',
        'red_site': '赤いサイト（WordPress・kosagi特殊）'
    }
    project_key = st.selectbox(
        "プロジェクト選択:",
        options=list(project_options.keys()),
        format_func=lambda x: project_options[x],
        disabled=st.session_state.get("project_selector", "biggift") in st.session_state.posting_projects,
        key="project_selector"
    )
    
    if project_key in st.session_state.posting_projects:
        st.warning(f"🚀 {PROJECT_CONFIGS[project_key]['worksheet']} 投稿処理中です。完了まで設定を変更しないでください。")
        if st.session_state.realtime_logs:
            with st.expander("進行状況ログ", expanded=True):
                for log in st.session_state.realtime_logs:
                    st.text(log)
                if st.button("ログクリア"):
                    st.session_state.realtime_logs = []
                    st.rerun()
    
    current_project = st.session_state.get('current_project')
    if current_project != project_key and project_key not in st.session_state.posting_projects:
        st.session_state.current_project = project_key
        st.cache_data.clear()
    
    config = PROJECT_CONFIGS[project_key]
    
    col1, col2 = st.columns(2)
    with col1:
        st.info(f"**プロジェクト**: {config['worksheet']}")
        st.info(f"**プラットフォーム**: {', '.join(config['platforms'])}")
    with col2:
        if config['needs_k_column']:
            st.warning("**予約方式**: K列記録 → GitHub Actions実行")
        else:
            st.success("**予約方式**: WordPress予約投稿機能")
    
    # WordPress接続テスト（WordPressプロジェクトのみ）
    if not config['needs_k_column']:
        with st.expander("🔧 WordPress接続テスト", expanded=False):
            st.info("投稿前にWordPressサイトへの接続をテストできます")
            available_sites = config.get('wp_sites', [])
            if not available_sites:
                st.write("このプロジェクトにはWordPressサイトの指定がありません。")
            else:
                test_col1, test_col2 = st.columns(2)
                with test_col1:
                    selected_site = st.selectbox(
                        "テスト対象サイト:",
                        options=available_sites,
                        help="接続テストを行うサイトを選択"
                    )
                with test_col2:
                    if st.button(f"🔍 {selected_site} 接続テスト", type="secondary"):
                        test_wordpress_connection(selected_site)
                if len(available_sites) > 1 and st.button("🔍 全サイト一括テスト", type="secondary"):
                    for site in available_sites:
                        st.write(f"## {site} のテスト結果")
                        test_wordpress_connection(site)
                        st.write("---")
    
    df = load_sheet_data(project_key)
    if df.empty:
        st.info("未処理のデータがありません")
        return
    
    st.header("データ一覧")
    edited_df = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
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
        post_count = st.selectbox("投稿数", options=[1, 2, 3, 4, 5], help="一度に投稿する記事数を選択")
    with col2:
        enable_eyecatch = st.checkbox("アイキャッチ画像を自動生成", value=True)
    
    if config['needs_k_column']:
        st.markdown("""
        <div class="warning-box">
        <strong>非WordPressプロジェクト</strong><br>
        予約時刻はK列に記録され、GitHub Actionsで定期実行されます。
        </div>
        """, unsafe_allow_html=True)
        enable_schedule = st.checkbox("予約投稿を使用する（K列記録）")
        schedule_times = []
        if enable_schedule:
            st.subheader("予約時刻設定")
            schedule_input = st.text_area(
                "予約時刻（1行につき1件）",
                placeholder="10:30\n12:15\n14:00",
                help="HH:MM形式で入力。今日の未来時刻のみ有効。投稿数分の時刻を入力してください。"
            )
            if schedule_input:
                lines = [line.strip() for line in schedule_input.split('\n') if line.strip()]
                now = datetime.now()
                for line in lines:
                    try:
                        if ':' in line and len(line) <= 5:
                            t = datetime.strptime(line, '%H:%M')
                            dt = now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
                            if dt > now:
                                schedule_times.append(dt)
                            else:
                                st.error(f"過去時刻: {line}")
                        else:
                            st.error(f"無効な時刻形式: {line}")
                    except ValueError:
                        st.error(f"無効な時刻形式: {line}")
                if schedule_times:
                    st.success(f"予約時刻 {len(schedule_times)}件を設定")
                    for dt in schedule_times:
                        st.write(f"• {dt.strftime('%H:%M')}")
                if len(schedule_times) < post_count and enable_schedule:
                    st.warning(f"投稿数{post_count}に対して予約時刻が{len(schedule_times)}件")
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
            st.subheader("予約時刻設定")
            schedule_input = st.text_area(
                "予約時刻（1行につき1件）",
                placeholder="2025-08-20 10:30\n2025-08-20 12:15\n2025-08-20 14:00",
                help="YYYY-MM-DD HH:MM または HH:MM 形式。投稿数分の時刻を入力してください。"
            )
            if schedule_input:
                lines = [line.strip() for line in schedule_input.split('\n') if line.strip()]
                now = datetime.now()
                for line in lines:
                    try:
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
                            st.error(f"過去の時刻は指定不可: {line}")
                        else:
                            st.error(f"無効な時刻形式: {line}")
                    except Exception:
                        st.error(f"解析エラー: {line}")
                if schedule_times:
                    st.success(f"予約時刻 {len(schedule_times)}件を設定")
                    for dt in schedule_times:
                        st.write(f"• {dt.strftime('%Y/%m/%d %H:%M')}")
                if len(schedule_times) < post_count and enable_schedule:
                    st.warning(f"投稿数{post_count}に対して予約時刻が{len(schedule_times)}件")

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
                st.error("投稿する行を選択してください")
            elif len(selected_rows) > 1:
                st.error("1行のみ選択してください")
            else:
                row = selected_rows.iloc[0]
                if config['needs_k_column'] and enable_schedule:
                    if not schedule_times:
                        st.error("予約時刻を入力してください")
                    else:
                        success = add_schedule_to_k_column(project_key, row.to_dict(), schedule_times)
                        if success:
                            st.success("K列に予約時刻を記録しました。GitHub Actionsで実行されます。")
                            time.sleep(1.5); st.cache_data.clear(); st.rerun()
                else:
                    success = execute_post(
                        row.to_dict(), 
                        project_key, 
                        post_count=post_count, 
                        schedule_times=schedule_times if enable_schedule else [],
                        enable_eyecatch=enable_eyecatch
                    )
                    if success:
                        st.cache_data.clear()
    with col_b:
        if st.button("データ更新", use_container_width=True):
            st.cache_data.clear()
            st.success("データを更新しました")
            st.rerun()
    
    if config['needs_k_column']:
        with st.expander("GitHub Actions設定"):
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
          GOOGLE_APPLICATION_CREDENTIALS_JSON: ${{ secrets.GOOGLE_APPLICATION_CREDENTIALS_JSON }}
        run: python scripts/post_executor.py
            """, language="yaml")
    
    with st.expander("使い方"):
        st.markdown(f"""
        **プロジェクト: {config['worksheet']}**
        
        **投稿ロジック：**
        - 1〜{get_max_posts_for_project(project_key)-1}記事目：その他リンク先を使用
        - {get_max_posts_for_project(project_key)}記事目：宣伝URLを使用（完了時に「処理済み」）
        - 投稿ごとに「カウンター」を自動更新（シートの列名ベース）
        
        **予約投稿について：**
        {'- K列に予約時刻を記録し、GitHub Actionsで定期実行' if config['needs_k_column'] else '- WordPressの予約投稿機能を使用'}
        """)
    
    st.markdown("---")
    col_info1, col_info2, col_info3 = st.columns(3)
    with col_info1:
        st.metric("未処理件数", len(df))
    with col_info2:
        st.metric("プラットフォーム", len(config['platforms']))
    with col_info3:
        st.metric("最終更新", datetime.now().strftime("%H:%M:%S"))

if __name__ == "__main__":
    main()
