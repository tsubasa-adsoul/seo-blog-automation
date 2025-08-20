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

# Blogger関連のimportは条件付きで行う
try:
    import pickle
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    BLOGGER_AVAILABLE = True
except ImportError:
    BLOGGER_AVAILABLE = False

# ========================
# 設定値（Secretsから取得）
# ========================
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
        'needs_k_column': True
    },
    'arigataya': {
        'worksheet': 'ありがた屋向け',
        'platforms': ['seesaa', 'fc2'],
        'max_posts': 20,
        'needs_k_column': True
    },
    'kaitori_life': {
        'worksheet': '買取LIFE向け',
        'platforms': ['wordpress'],
        'wp_sites': ['selectad', 'thrones'],
        'max_posts': 20,
        'needs_k_column': False
    },
    'osaifu_rescue': {
        'worksheet': 'お財布レスキュー向け',
        'platforms': ['wordpress'],
        'wp_sites': ['ykikaku', 'efdlqjtz'],
        'max_posts': 20,
        'needs_k_column': False
    },
    'kure_kaeru': {
        'worksheet': 'クレかえる向け',
        'platforms': ['wordpress'],
        'wp_sites': ['selectadvance', 'welkenraedt'],
        'max_posts': 20,
        'needs_k_column': False
    },
    'red_site': {
        'worksheet': '赤いサイト向け',
        'platforms': ['wordpress'],
        'wp_sites': ['ncepqvub', 'kosagi'],
        'max_posts': 20,
        'needs_k_column': False
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

# 投稿間隔（スパム回避）
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
    .completion-modal {
        background: #f8f9fa;
        border: 2px solid #28a745;
        border-radius: 10px;
        padding: 2rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# ========================
# セッションステート初期化（プロジェクト別対応）
# ========================
if 'gemini_key_index' not in st.session_state:
    st.session_state.gemini_key_index = 0

if 'posting_projects' not in st.session_state:
    st.session_state.posting_projects = set()

if 'current_project' not in st.session_state:
    st.session_state.current_project = None

if 'realtime_logs' not in st.session_state:
    st.session_state.realtime_logs = {}  # プロジェクト別辞書

if 'all_posted_urls' not in st.session_state:
    st.session_state.all_posted_urls = {}  # プロジェクト別辞書

if 'completion_results' not in st.session_state:
    st.session_state.completion_results = {}  # 完了結果保存

# ========================
# 認証 & シート取得
# ========================
@st.cache_resource
def get_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    try:
        if "gcp" in st.secrets:
            gcp_info = st.secrets["gcp"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(dict(gcp_info), scope)
            return gspread.authorize(creds)
    except Exception as e:
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
                other_sites.append({
                    "url": row[0].strip(),
                    "anchor": row[1].strip()
                })
        
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
        st.error(f"記事生成エラー: {e}")
        raise

# ========================
# 各プラットフォーム投稿関数
# ========================

# リンク属性強制付与関数（EXE版から移植）
def enforce_anchor_attrs(html: str) -> str:
    def add_attrs(m):
        tag = m.group(0)
        if re.search(r'\btarget\s*=', tag, flags=re.I) is None:
            tag = tag.replace('<a ', '<a target="_blank" ', 1)
        rel_m = re.search(r'\brel\s*=\s*"([^"]*)"', tag, flags=re.I)
        if rel_m:
            rel_val = rel_m.group(1)
            need = []
            for t in ('noopener', 'noreferrer'):
                if t not in rel_val.split():
                    need.append(t)
            if need:
                new_rel = rel_val + ' ' + ' '.join(need)
                tag = tag[:rel_m.start(1)] + new_rel + tag[rel_m.end(1):]
        else:
            tag = tag.replace('<a ', '<a rel="noopener noreferrer" ', 1)
        return tag
    return re.sub(r'<a\s+[^>]*>', add_attrs, html, flags=re.I)

def post_to_seesaa(article: dict, category_name: str = None) -> str:
    """Seesaa投稿"""
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
        st.error(f"Seesaa投稿エラー: {e}")
        return ""

def post_to_fc2(article: dict, category_name: str = None) -> str:
    """FC2投稿"""
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
        st.error(f"FC2投稿エラー: {e}")
        return ""

def post_to_livedoor(article: dict, category_name: str = None) -> str:
    """livedoor投稿（EXE版から移植）"""
    config = PLATFORM_CONFIGS['livedoor']
    root = f"https://livedoor.blogcms.jp/atompub/{config['blog_name']}"
    endpoint = f"{root}/article"
    
    title_xml = xml_escape(article["title"])
    safe_html = enforce_anchor_attrs(article["content"])
    content_xml = xml_escape(safe_html)
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
    """Blogger投稿（完全実装）"""
    if not BLOGGER_AVAILABLE:
        st.error("Blogger投稿に必要なライブラリがインストールされていません")
        return ""
    
    BLOG_ID = os.environ.get('BLOGGER_BLOG_ID', '3943718248369040188')
    SCOPES = ['https://www.googleapis.com/auth/blogger']
    
    try:
        st.info("📤 Blogger認証処理開始...")
        
        creds = None
        token_file = '/tmp/blogger_token.pickle'
        
        # 既存のトークンファイルを読み込み
        if os.path.exists(token_file):
            with open(token_file, 'rb') as token:
                creds = pickle.load(token)
        
        # 認証情報の検証・更新
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                st.info("🔄 Bloggerトークンを更新中...")
                creds.refresh(Request())
            else:
                st.error("🆕 Blogger初回認証が必要です。Streamlit環境では自動認証できません。")
                return ""
            
            # トークンを保存
            with open(token_file, 'wb') as token:
                pickle.dump(creds, token)
        
        st.success("✅ Blogger認証成功")
        
        # Blogger APIサービスを構築
        service = build('blogger', 'v3', credentials=creds)
        
        # 投稿データを作成
        post_data = {
            'title': article['title'],
            'content': article['content'],
            'labels': [article.get('theme', '金融')]
        }
        
        st.info(f"📝 Blogger投稿実行: {article['title']}")
        
        # 投稿を実行
        response = service.posts().insert(
            blogId=BLOG_ID,
            body=post_data,
            isDraft=False
        ).execute()
        
        if response and 'url' in response:
            post_url = response['url']
            st.success(f"✅ Blogger投稿成功: {post_url}")
            return post_url
        else:
            st.error("❌ Blogger投稿失敗: レスポンスにURLが含まれていません")
            return ""
            
    except Exception as e:
        st.error(f"❌ Blogger投稿エラー: {e}")
        return ""

def post_to_wordpress(article_data: dict, site_key: str, category_name: str = None, 
                      schedule_dt: datetime = None, enable_eyecatch: bool = True) -> str:
    """WordPressに投稿（予約投稿対応・kosagi特別処理）"""
    if site_key not in WP_CONFIGS:
        st.error(f"不明なサイト: {site_key}")
        return ""
    
    site_config = WP_CONFIGS[site_key]
    
    # kosagi特別処理（時間まで待機してから即時投稿）
    if site_key == 'kosagi':
        if schedule_dt and schedule_dt > datetime.now():
            wait_seconds = (schedule_dt - datetime.now()).total_seconds()
            st.info(f"kosagi用: {schedule_dt.strftime('%Y/%m/%d %H:%M')}まで待機します（{int(wait_seconds)}秒）")
            
            progress_bar = st.progress(0)
            total_seconds = int(wait_seconds)
            
            for i in range(total_seconds):
                progress_bar.progress((i + 1) / total_seconds)
                time.sleep(1)
            
            st.success("予約時刻になりました。kosagiに投稿を開始します")
        
        # XMLRPC方式で即時投稿
        endpoint = f"{site_config['url']}xmlrpc.php"
        
        import html
        escaped_title = html.escape(article_data['title'])
        
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
                        <value><string><![CDATA[{article_data['content']}]]></string></value>
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
                },
                timeout=60
            )
            
            if response.status_code == 200:
                if '<name>faultCode</name>' in response.text:
                    st.error("kosagi XMLRPC投稿エラー")
                    return ""
                
                match = re.search(r'<string>(\d+)</string>', response.text)
                if match:
                    post_id = match.group(1)
                    post_url = f"{site_config['url']}?p={post_id}"
                    st.success(f"kosagi投稿成功 (XMLRPC): {post_url}")
                    return post_url
                else:
                    st.success(f"kosagi投稿成功 (XMLRPC)")
                    return f"{site_config['url']}"
            else:
                st.error(f"kosagi投稿失敗: {response.status_code}")
                return ""
                
        except Exception as e:
            st.error(f"kosagi投稿エラー: {e}")
            return ""
    
    # 他のサイト（通常のWordPress REST API）
    else:
        endpoint = f"{site_config['url']}wp-json/wp/v2/posts"
        
        post_data = {
            'title': article_data['title'],
            'content': article_data['content'],
            'status': 'publish'
        }
        
        # 予約投稿の設定
        if schedule_dt and schedule_dt > datetime.now():
            post_data['status'] = 'future'
            post_data['date'] = schedule_dt.strftime('%Y-%m-%dT%H:%M:%S')
            st.info(f"予約投稿設定: {schedule_dt.strftime('%Y/%m/%d %H:%M')}に公開予定")
        
        try:
            # リトライ機能付きでリクエスト
            import urllib3
            from urllib3.util.retry import Retry
            from requests.adapters import HTTPAdapter
            
            session = requests.Session()
            retry_strategy = Retry(
                total=3,
                status_forcelist=[429, 500, 502, 503, 504],
                method_whitelist=["HEAD", "GET", "OPTIONS", "POST"]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            
            response = session.post(
                endpoint,
                auth=HTTPBasicAuth(site_config['user'], site_config['password']),
                headers={'Content-Type': 'application/json'},
                data=json.dumps(post_data),
                timeout=60
            )
            
            if response.status_code in (201, 200):
                post_url = response.json().get('link', '')
                if schedule_dt and schedule_dt > datetime.now():
                    st.success(f"予約投稿成功 ({site_key}): {schedule_dt.strftime('%Y/%m/%d %H:%M')}に公開予定")
                else:
                    st.success(f"投稿成功 ({site_key}): {post_url}")
                return post_url
            else:
                st.error(f"WordPress投稿失敗 ({site_key}): {response.status_code}")
                return ""
                
        except Exception as e:
            st.error(f"WordPress投稿エラー ({site_key}): {e}")
            return ""

# ========================
# ユーティリティ関数（プロジェクト別対応）
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

def add_realtime_log(message, project_key):
    """リアルタイムログを追加（プロジェクト別）"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_message = f"[{timestamp}] {message}"
    
    # プロジェクト別ログ管理
    if project_key not in st.session_state.realtime_logs:
        st.session_state.realtime_logs[project_key] = []
    
    st.session_state.realtime_logs[project_key].append(log_message)
    
    # ログが多すぎる場合は古いものを削除
    if len(st.session_state.realtime_logs[project_key]) > 50:
        st.session_state.realtime_logs[project_key] = st.session_state.realtime_logs[project_key][-30:]

def add_posted_url(counter, title, url, timestamp, project_key):
    """投稿URLを記録（プロジェクト別）"""
    if project_key not in st.session_state.all_posted_urls:
        st.session_state.all_posted_urls[project_key] = []
    
    st.session_state.all_posted_urls[project_key].append({
        'counter': counter,
        'title': title,
        'url': url,
        'timestamp': timestamp
    })

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
            if header in clean_headers:
                clean_headers.append(f"{header}_{i}")
            else:
                clean_headers.append(header)
        
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
    """スプレッドシート行更新（強化版）"""
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
                        # 更新確認のため少し待機
                        time.sleep(0.5)
                
                add_realtime_log(f"✅ スプレッドシート更新完了: 行{i}", project_key)
                return True
        
        add_realtime_log(f"❌ 対象行が見つかりませんでした", project_key)
        return False
        
    except Exception as e:
        add_realtime_log(f"❌ スプレッドシート更新エラー: {e}", project_key)
        return False

def add_schedule_to_k_column(project_key, row_data, schedule_times):
    """K列以降に予約時刻を追加（非WordPressプロジェクト用）"""
    try:
        client = get_sheets_client()
        config = PROJECT_CONFIGS[project_key]
        sheet = client.open_by_key(SHEET_ID).worksheet(config['worksheet'])
        
        all_rows = sheet.get_all_values()
        promo_url = row_data.get('宣伝URL', '')
        
        for i, row in enumerate(all_rows[1:], start=2):
            if len(row) > 1 and row[1] == promo_url:
                col_num = 11
                for schedule_dt in schedule_times:
                    while col_num <= len(row) + 10:
                        try:
                            current_value = sheet.cell(i, col_num).value
                            if not current_value:
                                break
                        except:
                            break
                        col_num += 1
                    
                    sheet.update_cell(i, col_num, schedule_dt.strftime('%Y/%m/%d %H:%M'))
                    col_num += 1
                
                st.success(f"K列以降に予約時刻を記録しました: 行{i}")
                return True
        
        st.error(f"対象行が見つかりませんでした")
        return False
        
    except Exception as e:
        st.error(f"K列記録エラー: {e}")
        return False

# ========================
# 投稿処理（完全版・プロジェクト別ログ・スプシ更新修正）
# ========================
def execute_post(row_data, project_key, post_count=1, schedule_times=None, enable_eyecatch=True):
    """投稿実行（完全版・プロジェクト別ログ・スプシ更新修正）"""
    try:
        st.session_state.posting_projects.add(project_key)
        
        # プロジェクト別ログ初期化（リセットしない）
        if project_key not in st.session_state.realtime_logs:
            st.session_state.realtime_logs[project_key] = []
        if project_key not in st.session_state.all_posted_urls:
            st.session_state.all_posted_urls[project_key] = []
        
        add_realtime_log(f"📋 {PROJECT_CONFIGS[project_key]['worksheet']} の投稿開始", project_key)
        
        config = PROJECT_CONFIGS[project_key]
        schedule_times = schedule_times or []
        
        current_counter = 0
        if 'カウンター' in row_data and row_data['カウンター']:
            try:
                current_counter = int(row_data['カウンター'])
            except:
                current_counter = 0
        
        add_realtime_log(f"📊 現在のカウンター: {current_counter}", project_key)
        
        post_target = row_data.get('投稿先', '').strip()
        max_posts = get_max_posts_for_project(project_key, post_target)
        
        if current_counter >= max_posts:
            add_realtime_log(f"⚠️ 既に{max_posts}記事完了済み", project_key)
            st.warning(f"既に{max_posts}記事完了しています")
            return False
        
        posts_completed = 0
        add_realtime_log(f"🚀 {post_count}記事の投稿を開始", project_key)
        
        progress_bar = st.progress(0)
        
        for i in range(post_count):
            if current_counter >= max_posts:
                add_realtime_log(f"⚠️ カウンター{current_counter}: 既に{max_posts}記事完了済み", project_key)
                st.warning(f"カウンター{current_counter}: 既に{max_posts}記事完了済み")
                break
            
            schedule_dt = schedule_times[i] if i < len(schedule_times) else None
            
            add_realtime_log(f"📝 記事{i+1}/{post_count}の処理開始", project_key)
            
            with st.expander(f"記事{i+1}/{post_count}の投稿", expanded=True):
                try:
                    # 記事内容の決定
                    if current_counter == max_posts - 1:
                        add_realtime_log(f"🎯 {max_posts}記事目 → 宣伝URL使用", project_key)
                        st.info(f"{max_posts}記事目 → 宣伝URL使用")
                        url = row_data.get('宣伝URL', '')
                        anchor = row_data.get('アンカーテキスト', project_key)
                        category = row_data.get('カテゴリー', 'お金のマメ知識')
                    else:
                        add_realtime_log(f"🔗 {current_counter + 1}記事目 → その他リンク使用", project_key)
                        st.info(f"{current_counter + 1}記事目 → その他リンク使用")
                        url, anchor = get_other_link()
                        if not url:
                            add_realtime_log("❌ その他リンクが取得できません", project_key)
                            st.error("その他リンクが取得できません")
                            break
                        category = 'お金のマメ知識'
                    
                    # 記事生成
                    add_realtime_log("🧠 記事を生成中...", project_key)
                    with st.spinner("記事を生成中..."):
                        theme = row_data.get('テーマ', '') or '金融・投資・資産運用'
                        article = generate_article_with_link(theme, url, anchor)
                    
                    add_realtime_log(f"✅ 記事生成完了: {article['title'][:30]}...", project_key)
                    st.success(f"タイトル: {article['title']}")
                    st.info(f"使用リンク: {anchor}")
                    
                    # プラットフォーム別投稿
                    posted_urls = []
                    platforms = config['platforms']
                    
                    if 'wordpress' in platforms:
                        for site_key in config.get('wp_sites', []):
                            if not post_target or post_target in [site_key, '両方']:
                                add_realtime_log(f"📤 {site_key}に投稿中...", project_key)
                                post_url = post_to_wordpress(
                                    article, 
                                    site_key, 
                                    category, 
                                    schedule_dt, 
                                    enable_eyecatch
                                )
                                if post_url:
                                    posted_urls.append(post_url)
                                    add_realtime_log(f"✅ {site_key}投稿成功: {post_url}", project_key)
                    
                    elif 'seesaa' in platforms:
                        add_realtime_log("📤 Seesaaに投稿中...", project_key)
                        post_url = post_to_seesaa(article, category)
                        if post_url:
                            posted_urls.append(post_url)
                            add_realtime_log(f"✅ Seesaa投稿成功: {post_url}", project_key)
                    
                    elif 'fc2' in platforms:
                        add_realtime_log("📤 FC2に投稿中...", project_key)
                        post_url = post_to_fc2(article, category)
                        if post_url:
                            posted_urls.append(post_url)
                            add_realtime_log(f"✅ FC2投稿成功: {post_url}", project_key)
                    
                    elif 'livedoor' in platforms:
                        add_realtime_log("📤 livedoorに投稿中...", project_key)
                        post_url = post_to_livedoor(article, category)
                        if post_url:
                            posted_urls.append(post_url)
                            add_realtime_log(f"✅ livedoor投稿成功: {post_url}", project_key)
                    
                    elif 'blogger' in platforms:
                        add_realtime_log("📤 Bloggerに投稿中...", project_key)
                        post_url = post_to_blogger(article)
                        if post_url:
                            posted_urls.append(post_url)
                            add_realtime_log(f"✅ Blogger投稿成功: {post_url}", project_key)
                    
                    if not posted_urls:
                        add_realtime_log("❌ 投稿に失敗しました", project_key)
                        st.error("投稿に失敗しました")
                        break
                    
                    # 全投稿URLを記録
                    timestamp = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                    for url_item in posted_urls:
                        add_posted_url(current_counter + 1, article['title'], url_item, timestamp, project_key)
                        add_realtime_log(f"📋 記事{current_counter + 1}記録: {article['title'][:30]}... → {url_item}", project_key)
                    
                    # カウンター更新
                    current_counter += 1
                    posts_completed += 1
                    
                    add_realtime_log(f"📊 スプレッドシート更新中... (カウンター: {current_counter})", project_key)
                    
                    # スプレッドシート更新（強化版）
                    client = get_sheets_client()
                    config_sheet = PROJECT_CONFIGS[project_key]
                    sheet = client.open_by_key(SHEET_ID).worksheet(config_sheet['worksheet'])
                    
                    all_rows = sheet.get_all_values()
                    promo_url = row_data.get('宣伝URL', '')
                    
                    for row_idx, row in enumerate(all_rows[1:], start=2):
                        if len(row) > 1 and row[1] == promo_url:
                            # カウンター更新
                            sheet.update_cell(row_idx, 7, str(current_counter))
                            time.sleep(0.5)  # 更新確認のため待機
                            
                            if current_counter >= max_posts:
                                # 最終記事完了処理
                                final_urls = [item['url'] for item in st.session_state.all_posted_urls[project_key] if item['counter'] == max_posts]
                                
                                # ステータス更新
                                sheet.update_cell(row_idx, 5, "処理済み")
                                time.sleep(0.5)
                                
                                # 投稿URL記録
                                sheet.update_cell(row_idx, 6, ', '.join(final_urls))
                                time.sleep(0.5)
                                
                                # 完了日時記録
                                completion_time = datetime.now().strftime("%Y/%m/%d %H:%M")
                                sheet.update_cell(row_idx, 9, completion_time)
                                time.sleep(0.5)
                                
                                add_realtime_log(f"🎉 {max_posts}記事完了！スプレッドシート更新完了", project_key)
                                
                                # 完了結果を保存（消えないように）
                                st.session_state.completion_results[project_key] = {
                                    'project_name': PROJECT_CONFIGS[project_key]['worksheet'],
                                    'completed_at': completion_time,
                                    'total_posts': max_posts,
                                    'all_urls': st.session_state.all_posted_urls[project_key].copy()
                                }
                                
                                st.balloons()
                                st.success(f"{max_posts}記事完了!")
                                
                                st.session_state.posting_projects.discard(project_key)
                                return True
                            else:
                                add_realtime_log(f"✅ カウンター更新: {current_counter}", project_key)
                                st.success(f"カウンター更新: {current_counter}")
                            break
                    
                    progress_bar.progress(posts_completed / post_count)
                    
                    if current_counter < max_posts and i < post_count - 1:
                        wait_time = random.randint(MIN_INTERVAL, MAX_INTERVAL)
                        add_realtime_log(f"⏳ 次の記事まで{wait_time}秒待機中...", project_key)
                        st.info(f"次の記事まで{wait_time}秒待機中...")
                        time.sleep(wait_time)
                    
                except Exception as e:
                    add_realtime_log(f"❌ 記事{i+1}の投稿エラー: {e}", project_key)
                    st.error(f"記事{i+1}の投稿エラー: {e}")
                    st.session_state.posting_projects.discard(project_key)
                    break
        
        st.session_state.posting_projects.discard(project_key)
        add_realtime_log(f"✅ {posts_completed}記事の投稿が完了しました", project_key)
        st.success(f"{posts_completed}記事の投稿が完了しました")
        return True
        
    except Exception as e:
        st.session_state.posting_projects.discard(project_key)
        add_realtime_log(f"❌ 投稿処理エラー: {e}", project_key)
        st.error(f"投稿処理エラー: {e}")
        return False

# ========================
# UI構築（完全版・プロジェクト別表示）
# ========================
def main():
    # ヘッダー
    st.markdown("""
    <div class="main-header">
        <h1>統合ブログ投稿管理システム</h1>
        <p>全プラットフォーム対応 - WordPress/Seesaa/FC2/livedoor/Blogger</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Blogger可用性チェック
    if not BLOGGER_AVAILABLE:
        st.warning("⚠️ Blogger投稿機能を使用するには、requirements.txtに追加ライブラリが必要です")
    
    # 完了結果の表示（消えないように）
    if st.session_state.completion_results:
        st.markdown("## 🎉 投稿完了プロジェクト")
        
        for proj_key, result in st.session_state.completion_results.items():
            with st.expander(f"✅ {result['project_name']} - 完了時刻: {result['completed_at']}", expanded=False):
                st.markdown(f"**投稿完了**: {result['total_posts']}記事")
                st.markdown("### 📋 投稿完了記事一覧")
                
                for item in result['all_urls']:
                    st.write(f"**{item['counter']}記事目**: {item['title']}")
                    st.write(f"🔗 [{item['url']}]({item['url']})")
                    st.write(f"⏰ {item['timestamp']}")
                    st.markdown("---")
                
                # OKボタンで結果を削除
                if st.button(f"OK（{result['project_name']}の結果を閉じる）", key=f"close_{proj_key}"):
                    del st.session_state.completion_results[proj_key]
                    st.rerun()
    
    # 投稿中の全プロジェクトの進行状況を表示
    posting_projects = st.session_state.get('posting_projects', set())
    
    if posting_projects:
        st.markdown("## 🚀 投稿中プロジェクト")
        
        # タブで各プロジェクトの進行状況を表示
        if len(posting_projects) > 1:
            tabs = st.tabs([f"{PROJECT_CONFIGS[pk]['worksheet']}" for pk in posting_projects])
            
            for i, proj_key in enumerate(posting_projects):
                with tabs[i]:
                    if proj_key in st.session_state.get('realtime_logs', {}):
                        st.markdown(f"### 📋 {PROJECT_CONFIGS[proj_key]['worksheet']} 進行状況")
                        
                        # 進捗率表示
                        if proj_key in st.session_state.get('all_posted_urls', {}):
                            posted_count = len(st.session_state.all_posted_urls[proj_key])
                            max_posts = get_max_posts_for_project(proj_key)
                            if max_posts > 0:
                                progress = min(posted_count / max_posts, 1.0)
                                st.progress(progress, f"{posted_count}/{max_posts} 記事完了")
                        
                        # 最新10件のログを表示
                        logs = st.session_state.realtime_logs[proj_key][-10:]
                        for log in logs:
                            st.text(log)
        else:
            # 単一プロジェクトの場合
            proj_key = list(posting_projects)[0]
            st.warning(f"🚀 {PROJECT_CONFIGS[proj_key]['worksheet']} 投稿処理中です。")
            
            if proj_key in st.session_state.get('realtime_logs', {}):
                # 進捗率表示
                if proj_key in st.session_state.get('all_posted_urls', {}):
                    posted_count = len(st.session_state.all_posted_urls[proj_key])
                    max_posts = get_max_posts_for_project(proj_key)
                    if max_posts > 0:
                        progress = min(posted_count / max_posts, 1.0)
                        st.progress(progress, f"{posted_count}/{max_posts} 記事完了")
                
                with st.expander("📋 リアルタイム進行状況", expanded=True):
                    logs = st.session_state.realtime_logs[proj_key][-10:]
                    for log in logs:
                        st.text(log)
    
    # プロジェクト選択
    project_key = st.selectbox(
        "プロジェクト選択",
        options=list(PROJECT_CONFIGS.keys()),
        format_func=lambda x: f"{PROJECT_CONFIGS[x]['worksheet']} ({', '.join(PROJECT_CONFIGS[x]['platforms'])})",
        key="project_selector"
    )
    
    # プロジェクト変更検知
    if st.session_state.current_project != project_key and project_key not in st.session_state.get('posting_projects', set()):
        st.session_state.current_project = project_key
        st.cache_data.clear()
    
    config = PROJECT_CONFIGS[project_key]
    
    # プロジェクト情報表示
    col1, col2 = st.columns(2)
    with col1:
        st.info(f"**プロジェクト**: {config['worksheet']}")
        st.info(f"**プラットフォーム**: {', '.join(config['platforms'])}")
    with col2:
        if config['needs_k_column']:
            st.warning("**予約方式**: K列記録 → GitHub Actions実行")
        else:
            st.success("**予約方式**: WordPress予約投稿機能")
    
    # データ読み込み
    df = load_sheet_data(project_key)
    
    if df.empty:
        st.info("未処理のデータがありません")
        return
    
    st.header("データ一覧")
    
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
    
    # 投稿設定
    st.header("投稿設定")
    
    col1, col2 = st.columns(2)
    with col1:
        post_count = st.selectbox(
            "投稿数",
            options=[1, 2, 3, 4, 5],
            help="一度に投稿する記事数を選択"
        )
    
    with col2:
        enable_eyecatch = st.checkbox("アイキャッチ画像を自動生成", value=True)
    
    # 予約設定
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
                help="HH:MM形式で入力。今日の未来時刻のみ有効。"
            )
            
            if schedule_input:
                lines = [line.strip() for line in schedule_input.split('\n') if line.strip()]
                now = datetime.now()
                
                for line in lines:
                    try:
                        if ':' in line and len(line) <= 5:
                            time_obj = datetime.strptime(line, '%H:%M')
                            schedule_dt = now.replace(
                                hour=time_obj.hour, 
                                minute=time_obj.minute, 
                                second=0, 
                                microsecond=0
                            )
                            if schedule_dt > now:
                                schedule_times.append(schedule_dt)
                    except ValueError:
                        st.error(f"無効な時刻形式: {line}")
                
                if schedule_times:
                    st.success(f"予約時刻 {len(schedule_times)}件を設定")
                    for dt in schedule_times:
                        st.write(f"• {dt.strftime('%H:%M')}")
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
                help="YYYY-MM-DD HH:MM形式またはHH:MM形式で入力。"
            )
            
            if schedule_input:
                lines = [line.strip() for line in schedule_input.split('\n') if line.strip()]
                now = datetime.now()
                
                for line in lines:
                    try:
                        formats = ['%Y-%m-%d %H:%M', '%Y/%m/%d %H:%M', '%H:%M']
                        dt = None
                        
                        for fmt in formats:
                            try:
                                if fmt == '%H:%M':
                                    time_obj = datetime.strptime(line, fmt)
                                    dt = now.replace(
                                        hour=time_obj.hour, 
                                        minute=time_obj.minute, 
                                        second=0, 
                                        microsecond=0
                                    )
                                else:
                                    dt = datetime.strptime(line, fmt)
                                break
                            except ValueError:
                                continue
                        
                        if dt and dt > now:
                            schedule_times.append(dt)
                        elif dt:
                            st.error(f"過去の時刻は指定できません: {line}")
                            
                    except Exception:
                        st.error(f"無効な時刻形式: {line}")
                
                if schedule_times:
                    st.success(f"予約時刻 {len(schedule_times)}件を設定")
                    for dt in schedule_times:
                        st.write(f"• {dt.strftime('%Y/%m/%d %H:%M')}")
    
    # 投稿ボタン
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
                            time.sleep(2)
                            st.cache_data.clear()
                            st.rerun()
                else:
                    success = execute_post(
                        row.to_dict(), 
                        project_key, 
                        post_count=post_count, 
                        schedule_times=schedule_times,
                        enable_eyecatch=enable_eyecatch
                    )
                    
                    if success:
                        time.sleep(2)
                        st.cache_data.clear()
                        st.rerun()
    
    with col_b:
        if st.button("データ更新", use_container_width=True):
            st.cache_data.clear()
            st.success("データを更新しました")
            st.rerun()
    
    # 情報表示
    st.markdown("---")
    col_info1, col_info2, col_info3 = st.columns(3)
    
    with col_info1:
        st.metric("未処理件数", len(df))
    
    with col_info2:
        st.metric("プラットフォーム", len(config['platforms']))
    
    with col_info3:
        last_update = datetime.now().strftime("%H:%M:%S")
        st.metric("最終更新", last_update)

if __name__ == "__main__":
    main()
