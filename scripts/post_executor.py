import streamlit as st
import requests
import gspread
import time
import random
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from requests.auth import HTTPBasicAuth
import json
import re
import pandas as pd
from urllib.parse import urlparse
import io
from PIL import Image, ImageDraw, ImageFont
import base64
import tempfile
import os
import threading
import traceback

# ========================
# 設定値
# ========================
SHEET_ID = '1sV0r6LavB4BgU7jGaa5C-GdyogUpWr_y42a-tNZXuFo'
PROJECT_CONFIGS = {
    'kaitori_life': {
        'worksheet': '買取LIFE向け',
        'sites': ['selectad', 'thrones'],
        'max_posts': 20
    },
    'osaifu_rescue': {
        'worksheet': 'お財布レスキュー向け',
        'sites': ['ykikaku', 'efdlqjtz'],
        'max_posts': 20
    },
    'kure_kaeru': {
        'worksheet': 'クレかえる向け',
        'sites': ['selectadvance', 'welkenraedt'],
        'max_posts': 20
    },
    'red_site': {
        'worksheet': '赤いサイト向け',
        'sites': ['ncepqvub', 'kosagi'],
        'max_posts': 20
    }
}

# WordPress設定
WP_CONFIGS = {
    'ykikaku': {
        'url': 'https://ykikaku.v2006.coreserver.jp/',
        'user': 'ykikaku',
        'password': 'QnV8 5VlW RwZN YV4P zAcl Gfce'
    },
    'efdlqjtz': {
        'url': 'https://www.efdlqjtz.com/',
        'user': 'efdlqjtz',
        'password': 'nJh6 Gqm6 qfPn T6Zu WQGV Aymx'
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
        'url': 'https://www.ncepqvub.com/',
        'user': 'ncepqvub',
        'password': 'ZNdJ IGoK Wdj3 mNz4 Xevp KGFj'
    },
    'kosagi': {
        'url': 'https://www.kosagi.biz/',
        'user': 'kosagi',
        'password': 'VsGS VU5J cKx8 HM6p oLEb VdNH'
    },
    'selectad': {
        'url': 'https://selectad.v2006.coreserver.jp/',
        'user': 'selectad',
        'password': 'xVA8 6yxD TdkP CJE4 yoQN qAHn'
    },
    'thrones': {
        'url': 'https://www.thrones.jp/',
        'user': 'thrones',
        'password': 'Fz9k fB3y wJuN tL8m zPqX vR4s'
    }
}

# Gemini設定
GEMINI_API_KEYS = [
    'AIzaSyBCxQruA6WrmfZHoZ6pTBPRVqkALKvdsT0',
    'AIzaSyAiCODJAE32JYGCTzSKqO2zSp8y7qR0ABC',
    'AIzaSyDEF456HIJKLMNOPQRSTUVWXYZabcdefgh'
]

# 安全設定
MIN_INTERVAL = 60
MAX_INTERVAL = 120

# ========================
# Streamlit設定
# ========================
st.set_page_config(
    page_title="🐸 ブログ自動投稿ツール",
    page_icon="🐸",
    layout="wide",
    initial_sidebar_state="expanded"
)

# カスタムCSS
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #2E7D32, #4CAF50);
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
        padding: 0.5rem 1rem;
        font-weight: bold;
    }
    .stButton > button:hover {
        background: linear-gradient(135deg, #66BB6A, #4CAF50);
        transform: translateY(-2px);
    }
    .success-box {
        background: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .error-box {
        background: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .info-box {
        background: #d1ecf1;
        border: 1px solid #bee5eb;
        color: #0c5460;
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
if 'is_posting' not in st.session_state:
    st.session_state.is_posting = False
if 'log_messages' not in st.session_state:
    st.session_state.log_messages = []
if 'sheet_data' not in st.session_state:
    st.session_state.sheet_data = None

# ========================
# 認証 & シート取得
# ========================
@st.cache_resource
def get_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    # 環境変数から認証情報を取得
    creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if creds_json:
        # 一時ファイルに書き込み
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write(creds_json)
            temp_path = f.name
        creds = ServiceAccountCredentials.from_json_keyfile_name(temp_path, scope)
        os.unlink(temp_path)
    else:
        # ローカルファイルを使用
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    
    return gspread.authorize(creds)

# ========================
# ログ機能
# ========================
def add_log(message, level="info"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_entry = {
        'timestamp': timestamp,
        'message': message,
        'level': level
    }
    st.session_state.log_messages.append(log_entry)
    
    # ログが多すぎる場合は古いものを削除
    if len(st.session_state.log_messages) > 100:
        st.session_state.log_messages = st.session_state.log_messages[-100:]

def display_logs():
    if st.session_state.log_messages:
        log_container = st.container()
        with log_container:
            for log in st.session_state.log_messages[-20:]:  # 最新20件を表示
                if log['level'] == 'success':
                    st.markdown(f"<div class='success-box'>[{log['timestamp']}] {log['message']}</div>", unsafe_allow_html=True)
                elif log['level'] == 'error':
                    st.markdown(f"<div class='error-box'>[{log['timestamp']}] {log['message']}</div>", unsafe_allow_html=True)
                else:
                    st.markdown(f"<div class='info-box'>[{log['timestamp']}] {log['message']}</div>", unsafe_allow_html=True)

# ========================
# アイキャッチ画像生成
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
    
    # グラデーション効果
    for i in range(height):
        alpha = i / height
        r = int(int(scheme['bg'][1:3], 16) * (1 - alpha * 0.3))
        g = int(int(scheme['bg'][3:5], 16) * (1 - alpha * 0.3))
        b = int(int(scheme['bg'][5:7], 16) * (1 - alpha * 0.3))
        draw.rectangle([(0, i), (width, i + 1)], fill=(r, g, b))
    
    # 装飾図形
    draw.ellipse([-50, -50, 150, 150], fill=scheme['accent'])
    draw.ellipse([width-100, height-100, width+50, height+50], fill=scheme['accent'])
    
    # フォント設定（デフォルトフォントを使用）
    try:
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
    except:
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
    
    # タイトルを描画
    lines = []
    if len(title) > 12:
        for sep in ['！', '？', '…', '!', '?']:
            if sep in title:
                idx = title.find(sep)
                if idx > 0:
                    lines = [title[:idx+1], title[idx+1:].strip()]
                    break
        
        if not lines:
            for sep in ['と', '、', 'の', 'は', 'が', 'を', 'に', '…', 'で']:
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
            text_width = len(line) * 10
        
        x = (width - text_width) // 2
        y = y_start + i * 50
        
        # 影
        draw.text((x + 2, y + 2), line, font=title_font, fill=(0, 0, 0))
        # 本体
        draw.text((x, y), line, font=title_font, fill=scheme['text'])
    
    # サイト名
    site_names = {
        'selectadvance': '後払いアプリ現金化攻略ブログ',
        'welkenraedt': 'マネーハック365',
        'ykikaku': 'お財布レスキュー',
        'efdlqjtz': 'マネーサポート',
        'selectad': '買取LIFE',
        'thrones': 'リサイクルマスター',
        'ncepqvub': '赤いサイト',
        'kosagi': 'うさぎファイナンス'
    }
    
    site_name = site_names.get(site_key, 'Financial Blog')
    
    try:
        bbox = draw.textbbox((0, 0), site_name, font=subtitle_font)
        text_width = bbox[2] - bbox[0]
    except AttributeError:
        text_width = len(site_name) * 8
    
    x = (width - text_width) // 2
    draw.text((x, height - 50), site_name, font=subtitle_font, fill=scheme['text'])
    
    # 上部ライン
    draw.rectangle([50, 40, width-50, 42], fill=scheme['text'])
    
    # バイトデータとして返す
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='JPEG', quality=90)
    img_byte_arr.seek(0)
    
    return img_byte_arr.getvalue()

def upload_image_to_wordpress(image_data: bytes, filename: str, site_config: dict) -> int:
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
            auth=HTTPBasicAuth(site_config['user'], site_config['password'])
        )
        
        if response.status_code == 201:
            media_id = response.json()['id']
            add_log(f"✅ アイキャッチ画像アップロード成功: {safe_filename} (ID: {media_id})", "success")
            return media_id
        else:
            add_log(f"❌ 画像アップロードエラー: {response.status_code}", "error")
            return None
            
    except Exception as e:
        add_log(f"❌ 画像アップロードエラー: {e}", "error")
        return None

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
        
        add_log(f"📋 競合他社ドメイン {len(domains)}件を読み込みました", "info")
        return domains
    except Exception as e:
        add_log(f"⚠️ 競合他社リスト取得エラー: {e}", "error")
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
                other_sites.append({
                    "url": row[0].strip(),
                    "anchor": row[1].strip()
                })
        
        add_log(f"📋 その他リンク先 {len(other_sites)}件を読み込みました", "info")
        
        if not other_sites:
            other_sites = [
                {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
                {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"},
            ]
        
        return other_sites
        
    except Exception as e:
        add_log(f"❌ その他リンク先の読み込みエラー: {e}", "error")
        return [
            {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
            {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"},
        ]

def get_other_link():
    other_sites = get_other_links()
    competitor_domains = get_competitor_domains()
    
    available_sites = []
    for site in other_sites:
        site_domain = urlparse(site['url']).netloc.lower()
        if not any(comp in site_domain for comp in competitor_domains):
            available_sites.append(site)
    
    if available_sites:
        site = random.choice(available_sites)
        return site['url'], site['anchor']
    
    return None, None

# ========================
# Gemini記事生成
# ========================
def _get_gemini_key():
    key = GEMINI_API_KEYS[st.session_state.gemini_key_index % len(GEMINI_API_KEYS)]
    st.session_state.gemini_key_index += 1
    return key

def call_gemini(prompt: str) -> str:
    api_key = _get_gemini_key()
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

def generate_article_with_link(theme: str, url: str, anchor_text: str) -> dict:
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
・**重要**: 各<p>タグの後に必ず空行を入れる
・リンク: <a href="URL" target="_blank" rel="noopener noreferrer">アンカーテキスト</a>
・リスト: <ul><li>

# 重要な禁止事項:
・<h1>タグは絶対に使用しない（タイトルはWordPressが自動設定するため）
・本文内にタイトルを重複させない

# 段落の書き方の例:
<p>これは最初の段落です。</p>

<p>これは次の段落です。段落間に空行があります。</p>

<p>このように各段落の後に空行を入れてください。</p>

# 記事の要件:
・2000-2500文字
・専門的でありながら分かりやすい
・具体的な数値や事例を含める
・読者の悩みを解決する内容
・各段落は2-3文程度でまとめる

# 重要:
・プレースホルダー（〇〇など）は使用禁止
・すべて具体的な内容で記述
・リンクは指定されたものを正確に使用
・必ず各<p>タグの後に空行を入れる
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
        add_log(f"❌ 記事生成エラー: {e}", "error")
        raise

# ========================
# WordPress投稿
# ========================
def get_category_id(site_config, category_name):
    if not category_name:
        return None
    
    try:
        endpoint = f"{site_config['url']}wp-json/wp/v2/categories"
        response = requests.get(endpoint)
        
        if response.status_code == 200:
            categories = response.json()
            for cat in categories:
                if cat['name'] == category_name:
                    return cat['id']
        return None
    except:
        return None

def generate_slug_from_title(title):
    keyword_map = {
        '投資': 'investment',
        '資産': 'asset',
        '運用': 'management',
        '増やす': 'increase',
        '貯金': 'savings',
        '節約': 'saving',
        'クレジット': 'credit',
        'カード': 'card',
        'ローン': 'loan',
        '金融': 'finance',
        '銀行': 'bank',
        '保険': 'insurance',
        '実践': 'practice',
        '方法': 'method',
        '戦略': 'strategy',
        'ガイド': 'guide',
        '初心者': 'beginner',
        '完全': 'complete',
        '効果': 'effect',
        '成功': 'success',
        '選び方': 'selection',
        '比較': 'comparison',
        '活用': 'utilization',
        'おすすめ': 'recommend',
        '基礎': 'basic',
        '知識': 'knowledge'
    }
    
    slug_parts = ['money']
    
    for jp_word, en_word in keyword_map.items():
        if jp_word in title:
            slug_parts.append(en_word)
            break
    
    if len(slug_parts) == 1:
        slug_parts.append('tips')
    
    date_str = datetime.now().strftime('%m%d')
    random_num = random.randint(100, 999)
    
    slug = '-'.join(slug_parts) + f'-{date_str}-{random_num}'
    
    return slug.lower()

def infer_slug_from_promo(promo_url: str, fallback_title: str) -> str:
    try:
        u = urlparse(promo_url)
        host = u.netloc.split(':')[0]
        host_parts = [p for p in host.split('.') if p and p != 'www']
        sld = host_parts[-2] if len(host_parts) >= 2 else (host_parts[0] if host_parts else '')
        last = ''
        if u.path:
            segs = [s for s in u.path.split('/') if s]
            if segs:
                last = segs[-1]
        base = last or sld or fallback_title
    except:
        base = fallback_title or 'money'
    base = re.sub(r'[^a-zA-Z0-9-]+', '-', base.lower()).strip('-')
    if not base:
        base = 'money'
    date_str = datetime.now().strftime('%m%d')
    rnd = random.randint(100, 999)
    return f"{base}-{date_str}-{rnd}"

def post_to_wordpress(article_data: dict, site_key: str, category_name: str = None, 
                      permalink: str = None, schedule_dt: datetime = None,
                      create_eyecatch: bool = True) -> str:
    
    if site_key not in WP_CONFIGS:
        add_log(f"❌ 不明なサイト: {site_key}", "error")
        return ""
    
    site_config = WP_CONFIGS[site_key]
    
    if not site_config['user']:
        add_log(f"⚠️ {site_key}の認証情報が設定されていません", "error")
        return ""
    
    # アイキャッチ画像を生成・アップロード
    featured_media_id = None
    if create_eyecatch:
        try:
            add_log("🖼️ アイキャッチ画像を生成中...", "info")
            image_data = create_eyecatch_image(article_data['title'], site_key)
            
            if permalink and permalink.strip():
                image_filename = f"{permalink}.jpg"
            else:
                image_filename = f"{generate_slug_from_title(article_data['title'])}.jpg"
            
            featured_media_id = upload_image_to_wordpress(image_data, image_filename, site_config)
            
            if featured_media_id:
                add_log(f"✅ アイキャッチ画像設定完了", "success")
            else:
                add_log("⚠️ アイキャッチ画像の設定をスキップして記事投稿を続行", "info")
                
        except Exception as e:
            add_log(f"⚠️ アイキャッチ画像生成エラー: {e}", "error")
            add_log("アイキャッチなしで記事投稿を続行", "info")
    
    endpoint = f"{site_config['url']}wp-json/wp/v2/posts"
    content = article_data['content']
    
    # カテゴリーIDを取得
    category_id = get_category_id(site_config, category_name) if category_name else None
    
    # スラッグの決定
    if permalink and permalink.strip():
        slug = permalink.strip()
    else:
        slug = generate_slug_from_title(article_data['title'])
    
    post_data = {
        'title': article_data['title'],
        'content': content,
        'slug': slug,
        'categories': [category_id] if category_id else []
    }
    
    # アイキャッチ画像を設定
    if featured_media_id:
        post_data['featured_media'] = featured_media_id
    
    # 予約投稿の設定
    if schedule_dt and schedule_dt > datetime.now():
        post_data['status'] = 'future'
        post_data['date'] = schedule_dt.strftime('%Y-%m-%dT%H:%M:%S')
    else:
        post_data['status'] = 'publish'
    
    try:
        response = requests.post(
            endpoint,
            auth=HTTPBasicAuth(site_config['user'], site_config['password']),
            headers={'Content-Type': 'application/json'},
            data=json.dumps(post_data)
        )
        
        if response.status_code in (201, 200):
            post_url = response.json().get('link', '')
            add_log(f"✅ WordPress投稿成功 ({site_key}): {post_url}", "success")
            return post_url
        else:
            add_log(f"❌ WordPress投稿失敗 ({site_key}): {response.status_code}", "error")
            add_log(f"エラー詳細: {response.text[:500]}...", "error")
            return ""
            
    except Exception as e:
        add_log(f"❌ WordPress投稿エラー ({site_key}): {e}", "error")
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
        
        # 未処理のみフィルタ
        filtered_rows = []
        for row in data_rows:
            if len(row) >= 5 and row[1] and row[1].strip():
                status = row[4].strip().lower() if len(row) > 4 else ''
                if status in ['', '未処理']:
                    # 行を適切な長さに調整
                    adjusted_row = row + [''] * (len(headers) - len(row))
                    filtered_rows.append(adjusted_row[:len(headers)])
        
        if not filtered_rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(filtered_rows, columns=headers)
        df['選択'] = False
        
        return df
        
    except Exception as e:
        add_log(f"❌ データ読み込みエラー: {e}", "error")
        return pd.DataFrame()

def update_sheet_row(project_key, row_data, updates):
    try:
        client = get_sheets_client()
        config = PROJECT_CONFIGS[project_key]
        sheet = client.open_by_key(SHEET_ID).worksheet(config['worksheet'])
        
        # 宣伝URLで行を特定
        all_rows = sheet.get_all_values()
        promo_url = row_data.get('宣伝URL', '')
        
        for i, row in enumerate(all_rows[1:], start=2):
            if len(row) > 1 and row[1] == promo_url:
                for col_name, value in updates.items():
                    if col_name in all_rows[0]:
                        col_idx = all_rows[0].index(col_name) + 1
                        sheet.update_cell(i, col_idx, value)
                add_log(f"✅ スプレッドシート更新完了: 行{i}", "success")
                return True
        
        add_log(f"⚠️ 対象行が見つかりませんでした", "error")
        return False
        
    except Exception as e:
        add_log(f"❌ スプレッドシート更新エラー: {e}", "error")
        return False

# ========================
# 投稿処理
# ========================
def process_single_post(row_data, project_key, schedule_times=None):
    try:
        config = PROJECT_CONFIGS[project_key]
        
        # 現在のカウンター取得
        current_counter = 0
        if 'カウンター' in row_data and row_data['カウンター']:
            try:
                current_counter = int(row_data['カウンター'])
            except:
                current_counter = 0
        
        max_posts = config['max_posts']
        
        if current_counter >= max_posts:
            add_log(f"⚠️ 既に{max_posts}記事完了しています", "info")
            return False
        
        # 予約時刻の取得
        schedule_dt = None
        if schedule_times and len(schedule_times) > 0:
            schedule_dt = schedule_times[0]
        
        # 記事内容の決定
        if current_counter == max_posts - 1:
            # 20記事目：宣伝URL
            add_log(f"📊 {max_posts}記事目 → 宣伝URL使用", "info")
            url = row_data.get('宣伝URL', '')
            anchor = row_data.get('アンカーテキスト', project_key)
            category_name = row_data.get('カテゴリー', 'お金のマメ知識')
            permalink = row_data.get('パーマリンク', '')
            
            if not permalink:
                permalink = infer_slug_from_promo(url, row_data.get('テーマ', ''))
        else:
            # 1-19記事目：その他リンク
            add_log(f"📊 {current_counter + 1}記事目 → その他リンク使用", "info")
            url, anchor = get_other_link()
            if not url:
                add_log("❌ その他リンクが取得できません", "error")
                return False
            category_name = 'お金のマメ知識'
            permalink = None
        
        # 記事生成
        add_log("🧠 記事を生成中...", "info")
        theme = row_data.get('テーマ', '') or '金融・投資・資産運用'
        article = generate_article_with_link(theme, url, anchor)
        
        add_log(f"📝 タイトル: {article['title']}", "info")
        add_log(f"🔗 使用リンク: {anchor}", "info")
        
        # 投稿先の決定
        post_target = row_data.get('投稿先', '').strip() or config['sites'][0]
        posted_urls = []
        
        # 投稿実行
        for site_key in config['sites']:
            if post_target in [site_key, '両方']:
                add_log(f"📤 {site_key}に投稿中...", "info")
                url_result = post_to_wordpress(
                    article, site_key, category_name, permalink, 
                    schedule_dt=schedule_dt, create_eyecatch=True
                )
                if url_result:
                    posted_urls.append(url_result)
        
        if not posted_urls:
            add_log("❌ 投稿に失敗しました", "error")
            return False
        
        # スプレッドシート更新
        new_counter = current_counter + 1
        updates = {'カウンター': str(new_counter)}
        
        if new_counter >= max_posts:
            # 20記事目完了
            updates['ステータス'] = '処理済み'
            updates['投稿URL'] = ', '.join(posted_urls)
            completion_time = (schedule_dt or datetime.now()).strftime("%Y/%m/%d %H:%M")
            if '完了日時' in row_data:
                updates['完了日時'] = completion_time
            add_log(f"✅ {max_posts}記事完了！", "success")
        else:
            add_log(f"📊 カウンター更新: {new_counter}", "success")
        
        # パーマリンク記録（20記事目で新規生成した場合）
        if current_counter == max_posts - 1 and permalink and 'パーマリンク' in row_data:
            updates['パーマリンク'] = permalink
        
        update_sheet_row(project_key, row_data, updates)
        
        return True
        
    except Exception as e:
        add_log(f"❌ 投稿処理エラー: {e}", "error")
        add_log(f"詳細: {traceback.format_exc()}", "error")
        return False

# ========================
# UI構築
# ========================
def main():
    # ヘッダー
    st.markdown("""
    <div class="main-header">
        <h1>🐸 ブログ自動投稿ツール</h1>
        <p>AI-Powered Financial Content Generation</p>
    </div>
    """, unsafe_allow_html=True)
    
    # サイドバー
    with st.sidebar:
        st.header("⚙️ 設定")
        
        # プロジェクト選択
        project_key = st.selectbox(
            "プロジェクト",
            options=list(PROJECT_CONFIGS.keys()),
            format_func=lambda x: PROJECT_CONFIGS[x]['worksheet']
        )
        
        # データ更新ボタン
        if st.button("🔄 データ更新", use_container_width=True):
            st.cache_data.clear()
            st.session_state.sheet_data = load_sheet_data(project_key)
            st.success("データを更新しました")
            st.rerun()
        
        # 投稿設定
        st.subheader("📤 投稿設定")
        post_count = st.selectbox("投稿数", [1, 2, 3, 4, 5], index=0)
        
        # 予約投稿設定
        st.subheader("⏰ 予約投稿")
        enable_schedule = st.checkbox("予約投稿を有効にする")
        
        schedule_times = []
        if enable_schedule:
            schedule_input = st.text_area(
                "予約日時（1行につき1件）",
                placeholder="2025-08-20 14:30\n15:00\n16:30",
                help="形式: YYYY-MM-DD HH:MM または HH:MM"
            )
            
            if schedule_input:
                lines = [line.strip() for line in schedule_input.split('\n') if line.strip()]
                now = datetime.now()
                
                for line in lines:
                    try:
                        if ':' in line and len(line) <= 5:  # HH:MM形式
                            time_obj = datetime.strptime(line, '%H:%M')
                            schedule_dt = now.replace(
                                hour=time_obj.hour, 
                                minute=time_obj.minute, 
                                second=0, 
                                microsecond=0
                            )
                        else:  # 完全な日時形式
                            schedule_dt = datetime.strptime(line, '%Y-%m-%d %H:%M')
                        
                        if schedule_dt > now:
                            schedule_times.append(schedule_dt)
                    except ValueError:
                        st.error(f"無効な日時形式: {line}")
                
                if schedule_times:
                    st.success(f"予約時刻 {len(schedule_times)}件を設定")
    
    # メインエリア
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📋 データ一覧")
        
        # データ読み込み
        if st.session_state.sheet_data is None:
            st.session_state.sheet_data = load_sheet_data(project_key)
        
        df = st.session_state.sheet_data
        
        if df.empty:
            st.info("未処理のデータがありません")
        else:
            # データエディタ
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
            
            # 投稿ボタン
            st.subheader("🚀 投稿実行")
            
            col_a, col_b = st.columns(2)
            
            with col_a:
                if st.button("📤 選択行を投稿", type="primary", use_container_width=True):
                    selected_rows = edited_df[edited_df['選択'] == True]
                    
                    if len(selected_rows) == 0:
                        st.error("投稿する行を選択してください")
                    elif len(selected_rows) > 1:
                        st.error("1行のみ選択してください")
                    else:
                        row = selected_rows.iloc[0]
                        
                        # 投稿処理実行
                        with st.spinner("投稿中..."):
                            success = process_single_post(
                                row.to_dict(), 
                                project_key, 
                                schedule_times if enable_schedule else None
                            )
                        
                        if success:
                            st.success("投稿が完了しました！")
                            # データ更新
                            time.sleep(2)
                            st.cache_data.clear()
                            st.session_state.sheet_data = load_sheet_data(project_key)
                            st.rerun()
                        else:
                            st.error("投稿に失敗しました")
            
            with col_b:
                if st.button("🔄 データ更新", use_container_width=True):
                    st.cache_data.clear()
                    st.session_state.sheet_data = load_sheet_data(project_key)
                    st.success("データを更新しました")
                    st.rerun()
    
    with col2:
        st.header("📝 実行ログ")
        
        # ログ表示
        log_container = st.container()
        with log_container:
            display_logs()
        
        # ログクリアボタン
        if st.button("🗑️ ログクリア", use_container_width=True):
            st.session_state.log_messages = []
            st.rerun()
    
    # フッター情報
    st.markdown("---")
    col_info1, col_info2, col_info3 = st.columns(3)
    
    with col_info1:
        st.metric("未処理件数", len(df) if not df.empty else 0)
    
    with col_info2:
        total_logs = len(st.session_state.log_messages)
        st.metric("ログ件数", total_logs)
    
    with col_info3:
        last_update = datetime.now().strftime("%H:%M:%S")
        st.metric("最終更新", last_update)

if __name__ == "__main__":
    main()
