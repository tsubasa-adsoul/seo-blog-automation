import os
import io
import re
import json
import time
import base64
import random
import string
import logging
import traceback
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple

import requests
import streamlit as st

# ===== Optional clients (installed via requirements.txt) =====
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
except Exception:
    gspread = None
    ServiceAccountCredentials = None

try:
    import xmlrpc.client as xmlrpclib
except Exception:
    xmlrpclib = None

try:
    from google.oauth2.credentials import Credentials as GoogleCredentials
    from googleapiclient.discovery import build as gapi_build
    from googleapiclient.errors import HttpError as GoogleHttpError
except Exception:
    GoogleCredentials = None
    gapi_build = None
    GoogleHttpError = None

try:
    from PIL import Image, ImageDraw, ImageFont
except Exception:
    Image = None
    ImageDraw = None
    ImageFont = None

# =============================================================
# Streamlit page conf
# =============================================================
st.set_page_config(page_title="統合ブログ自動投稿 (WP/Seesaa/FC2/Livedoor/Blogger/+非WP K列)", layout="wide")
st.title("📰 統合ブログ自動投稿ツール — 完全版")

# =============================================================
# Secrets / Config
# =============================================================
# 必須シークレット例 (Streamlit Cloud > Secrets):
#
# [gcp]
# service_account_json = "{...}"   # GSpread用サービスアカウント JSON 文字列
#
# [sheets]
# sheet_id = "1sV0r6LavB4BgU7jGaa5C-GdyogUpWr_y42a-tNZXuFo"
# ws_names = ["クレかえる向け","買取LIFE向け","お財布レスキュー向け"]
# competitor_ws = "競合他社"
# other_links_ws = "その他リンク先"
#
# [gemini]
# api_key = "..."
# model = "gemini-1.5-pro"
#
# [wp]  # Basic Auth (Application Password 推奨)
# sites = {
#   "selectadvance": {"url":"https://selectadvance.v2006.coreserver.jp/", "user":"selectadvance", "password":"..."},
#   "welkenraedt": {"url":"https://www.welkenraedt-online.com/", "user":"welkenraedtonline", "password":"..."},
#   "selectad": {"url":"https://selectad01.xsrv.jp/", "user":"selectad01", "password":"..."},
#   "thrones": {"url":"https://thrones.v2009.coreserver.jp/", "user":"thrones", "password":"..."},
#   "ykikaku": {"url":"https://ykikaku.xsrv.jp/", "user":"ykikaku", "password":"..."},
#   "efdlqjtz": {"url":"https://www.efdlqjtz.v2010.coreserver.jp/", "user":"efdlqjtz", "password":"..."}
# }
#
# [fc2]
# endpoint = "https://blog.fc2.com/xmlrpc.php"
# accounts = {"genkinka1313": {"user":"esciresearch.com@gmail.com","password":"..."}}
# blog_id = "genkinka1313"  # 一意であれば1個でOK
#
# [livedoor]
# blog_name = "radiochildcare"
# user = "radiochildcare"
# api_key = "..."  # マイページ発行のAPIKey
#
# [seesaa]
# blog_id = "xxxxx"  # アカウントのブログID
# user = "..."
# api_key = "..."  # Seesaa API Key (AtomPub)
# endpoint = "https://api.seesaa.net/blog/atom"
#
# [blogger]
# client_id = "..."
# client_secret = "..."
# refresh_token = "..."
# blog_id = "1234567890123456789"
#
# [ui]
# brand_color = "#7ed321"   # ロゴ・強調色
#

SECRETS = st.secrets

# ===== Logging helper =====
class UILog:
    def __init__(self):
        self.lines: List[str] = []
    def write(self, msg: str):
        ts = dt.datetime.now().strftime('%H:%M:%S')
        line = f"[{ts}] {msg}"
        self.lines.append(line)
        st.session_state.setdefault("_log", []).append(line)
    def dump(self):
        st.code("\n".join(self.lines))

ui_log = UILog()

# =============================================================
# Google Sheets
# =============================================================
def get_gspread_client():
    if gspread is None:
        raise RuntimeError("gspread が未インストールです。requirements.txt を確認してください。")
    if "gcp" in SECRETS and "service_account_json" in SECRETS["gcp"]:
        key_json = json.loads(SECRETS["gcp"]["service_account_json"])  # dict
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(key_json, scope)
        ui_log.write("✅ GSpread: サービスアカウント読み込み成功")
        return gspread.authorize(creds)
    else:
        raise RuntimeError("Secrets[gcp][service_account_json] が未設定です。")

@st.cache_data(ttl=300)
def load_sheet(ws_name: str) -> List[List[str]]:
    sheet_id = SECRETS["sheets"]["sheet_id"]
    gc = get_gspread_client()
    ws = gc.open_by_key(sheet_id).worksheet(ws_name)
    rows = ws.get_all_values()
    return rows

@st.cache_data(ttl=300)
def load_competitors() -> List[str]:
    sheet_id = SECRETS["sheets"]["sheet_id"]
    comp_ws = SECRETS["sheets"].get("competitor_ws", "競合他社")
    gc = get_gspread_client()
    ws = gc.open_by_key(sheet_id).worksheet(comp_ws)
    values = ws.get_all_values()[1:]
    doms: List[str] = []
    for r in values:
        if not r or not r[0]:
            continue
        url = r[0].strip()
        if url.startswith("http"):
            try:
                from urllib.parse import urlparse
                doms.append(urlparse(url).netloc.lower())
            except Exception:
                pass
        else:
            doms.append(url.lower())
    ui_log.write(f"📋 競合 {len(doms)} 件読込")
    return doms

@st.cache_data(ttl=300)
def load_other_links() -> List[Tuple[str, str]]:
    sheet_id = SECRETS["sheets"]["sheet_id"]
    ws_name = SECRETS["sheets"].get("other_links_ws", "その他リンク先")
    gc = get_gspread_client()
    ws = gc.open_by_key(sheet_id).worksheet(ws_name)
    rows = ws.get_all_values()[1:]
    out: List[Tuple[str, str]] = []
    for r in rows:
        if len(r) >= 2 and r[0] and r[1]:
            out.append((r[0].strip(), r[1].strip()))
    ui_log.write(f"🔗 その他リンク {len(out)} 件読込")
    return out

# =============================================================
# Gemini (Generative Language API)
# =============================================================
GEMINI_KEY = SECRETS.get("gemini", {}).get("api_key", "")
GEMINI_MODEL = SECRETS.get("gemini", {}).get("model", "gemini-1.5-pro")
GEMINI_ENDPOINT = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_KEY}"

def call_gemini(prompt: str) -> str:
    if not GEMINI_KEY:
        raise RuntimeError("Secrets[gemini][api_key] が未設定です。")
    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.7}}
    r = requests.post(GEMINI_ENDPOINT, json=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Gemini API エラー: {r.status_code} {r.text[:200]}")
    data = r.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]

ARTICLE_SYSTEM_PROMPT = """
# 命令書:
{theme_instruction}、読者に価値のある記事を作成してください。

# 記事に含めるリンク（1つのみ）:
URL: {url}
アンカーテキスト: {anchor}

# 出力形式:
・最初の行に魅力的なタイトルを出力（タグなし）
・その後、HTML形式で本文作成
・リンクを自然に挿入（1回のみ）

# HTML記法:
・見出し: <h2>, <h3>のみ（H1不可）
・段落: <p>タグで囲む（各<p>の後に空行を入れる）
・リンク: <a href="URL" target="_blank" rel="noopener noreferrer">アンカーテキスト</a>
・箇条書き: <ul><li>

# 禁止:
・<h1>の使用、タイトルの本文重複、プレースホルダー（〇〇等）

# 要件:
・2000-2500文字 / 具体例・数値 / 各段落2-3文
""".strip()


def generate_article(theme: str, url: str, anchor: str) -> Dict[str, str]:
    auto = False
    if not theme or not theme.strip():
        theme = "金融・投資・資産運用"
        auto = True
    theme_instruction = ("金融系（投資、クレジットカード、ローン、資産運用など）から自由にテーマを選んで"
                         if auto else f"「{theme}」をテーマに")
    prompt = ARTICLE_SYSTEM_PROMPT.format(theme_instruction=theme_instruction, url=url, anchor=anchor)
    text = call_gemini(prompt).strip()
    lines = text.splitlines()
    title = lines[0].strip()
    content = "\n".join(lines[1:]).strip()
    # cleanup
    content = re.sub(r"<p>\s*</p>", "", content)
    content = re.sub(r"[〇×△]{2,}", "", content)
    return {"title": title, "content": content, "theme": ("金融" if auto else theme)}

# =============================================================
# Eyecatch image
# =============================================================
FONT_PATH = os.path.join(os.path.dirname(__file__), "fonts", "NotoSansJP-Bold.ttf")

def create_eyecatch(title: str, brand: str = "generic", theme_color: str = "#4CAF50") -> bytes:
    if Image is None:
        raise RuntimeError("Pillow 未インストール。requirements.txt を確認してください。")
    W, H = 1200, 630
    bg = theme_color
    img = Image.new("RGB", (W, H), color=bg)
    draw = ImageDraw.Draw(img)

    # simple gradient overlay
    for y in range(H):
        a = y / H
        # darken
        r = int(int(bg[1:3], 16) * (1 - 0.25 * a))
        g = int(int(bg[3:5], 16) * (1 - 0.25 * a))
        b = int(int(bg[5:7], 16) * (1 - 0.25 * a))
        draw.line([(0, y), (W, y)], fill=(r, g, b))

    # accent circles
    draw.ellipse([-80, -80, 260, 260], fill="white")
    draw.ellipse([W-260, H-260, W+80, H+80], fill=(255, 255, 255,))

    # fonts
    try:
        title_font = ImageFont.truetype(FONT_PATH, 64)
        sub_font = ImageFont.truetype(FONT_PATH, 28)
    except Exception:
        title_font = ImageFont.load_default()
        sub_font = ImageFont.load_default()

    # wrap title in 2 lines max
    def wrap(text: str, max_chars: int = 18) -> List[str]:
        if len(text) <= max_chars:
            return [text]
        # split by punctuation or middle
        for sep in ["・", "、", "。", "-", "—", ":", "！", "？", " "]:
            if sep in text and 6 < text.find(sep) < len(text)-6:
                i = text.find(sep) + 1
                return [text[:i].strip(), text[i:].strip()]
        mid = len(text)//2
        return [text[:mid].strip(), text[mid:].strip()]

    lines = wrap(title)
    total_h = sum(draw.textbbox((0,0), ln, font=title_font)[3] for ln in lines) + (len(lines)-1)*12
    y0 = (H - total_h)//2
    for i, ln in enumerate(lines):
        bbox = draw.textbbox((0,0), ln, font=title_font)
        tw = bbox[2]-bbox[0]
        x = (W - tw)//2
        y = y0 + i*(bbox[3] + 12)
        # shadow
        draw.text((x+3, y+3), ln, font=title_font, fill=(0,0,0))
        draw.text((x, y), ln, font=title_font, fill=(255,255,255))

    sub = {
        "selectadvance":"後払いアプリ現金化攻略ブログ",
        "welkenraedt":"マネーハック365",
        "selectad":"買取LIFE攻略ブログ",
        "thrones":"買取LIFE完全ガイド",
        "ykikaku":"お財布レスキュー完全ガイド",
        "efdlqjtz":"お財布レスキュー攻略ブログ",
    }.get(brand, "Financial Blog")
    sb = draw.textbbox((0,0), sub, font=sub_font)
    sx = (W - (sb[2]-sb[0]))//2
    draw.text((sx, H-80), sub, font=sub_font, fill=(255,255,255))

    bio = io.BytesIO()
    img.save(bio, format="JPEG", quality=92)
    bio.seek(0)
    return bio.read()

# =============================================================
# Slug & util
# =============================================================
KEYWORD_MAP = {
    '投資': 'investment','資産': 'asset','運用': 'management','増やす': 'increase','貯金': 'savings','節約': 'saving',
    'クレジット': 'credit','カード': 'card','ローン': 'loan','金融': 'finance','銀行': 'bank','保険': 'insurance',
    '実践': 'practice','方法': 'method','戦略': 'strategy','ガイド': 'guide','初心者': 'beginner','完全': 'complete',
    '効果': 'effect','成功': 'success','選び方': 'selection','比較': 'comparison','活用': 'utilization','おすすめ': 'recommend',
    '基礎': 'basic','知識': 'knowledge'
}

def slug_from_title(title: str) -> str:
    parts = ["money"]
    for jp, en in KEYWORD_MAP.items():
        if jp in title:
            parts.append(en)
            break
    if len(parts) == 1:
        parts.append("tips")
    date_str = dt.datetime.now().strftime('%m%d')
    rnd = random.randint(100, 999)
    return ("-".join(parts) + f"-{date_str}-{rnd}").lower()

# =============================================================
# Poster: WordPress REST
# =============================================================
from requests.auth import HTTPBasicAuth

def wp_get_category_id(site: Dict[str, str], name: Optional[str]) -> Optional[int]:
    if not name:
        return None
    try:
        r = requests.get(f"{site['url']}wp-json/wp/v2/categories", timeout=30)
        if r.ok:
            for c in r.json():
                if c.get('name') == name:
                    return int(c['id'])
    except Exception:
        pass
    return None

def wp_upload_media(site: Dict[str, str], image_bytes: bytes, filename: str, log: UILog) -> Optional[int]:
    safe = ''.join(c for c in filename if c in string.ascii_letters+string.digits+'-_.') or f"eyecatch_{int(time.time())}.jpg"
    if not safe.endswith('.jpg'):
        safe += '.jpg'
    hdr = {"Content-Disposition": f"attachment; filename=\"{safe}\"", "Content-Type": "image/jpeg"}
    try:
        r = requests.post(f"{site['url']}wp-json/wp/v2/media", data=image_bytes, headers=hdr,
                          auth=HTTPBasicAuth(site['user'], site['password']), timeout=60)
        if r.status_code == 201:
            mid = int(r.json()['id'])
            log.write(f"🖼️ WPメディアアップロード成功: {safe} (ID {mid})")
            return mid
        else:
            log.write(f"⚠️ WPメディア失敗: {r.status_code} {r.text[:180]}")
    except Exception as e:
        log.write(f"❌ WPメディア例外: {e}")
    return None

def post_wordpress(article: Dict[str, str], site_key: str, category: Optional[str], when: Optional[dt.datetime], create_eye: bool, brand_color: str, log: UILog) -> str:
    sites = SECRETS.get("wp", {}).get("sites", {})
    if site_key not in sites:
        log.write(f"❌ WP 投稿先未定義: {site_key}")
        return ""
    site = sites[site_key]
    slug = slug_from_title(article['title'])
    cat_id = wp_get_category_id(site, category) if category else None

    media_id = None
    if create_eye:
        img = create_eyecatch(article['title'], brand=site_key, theme_color=brand_color)
        media_id = wp_upload_media(site, img, f"{slug}.jpg", log)

    payload = {
        'title': article['title'],
        'content': article['content'],
        'slug': slug,
        'status': 'publish',
    }
    if cat_id:
        payload['categories'] = [cat_id]
    if media_id:
        payload['featured_media'] = media_id
    if when and when > dt.datetime.now():
        payload['status'] = 'future'
        payload['date'] = when.strftime('%Y-%m-%dT%H:%M:%S')

    try:
        r = requests.post(f"{site['url']}wp-json/wp/v2/posts",
                          auth=HTTPBasicAuth(site['user'], site['password']),
                          headers={'Content-Type': 'application/json'},
                          data=json.dumps(payload), timeout=60)
        if r.status_code in (200, 201):
            link = r.json().get('link', '')
            log.write(f"✅ WP投稿成功({site_key}): {link}")
            return link
        else:
            log.write(f"❌ WP投稿失敗({site_key}): {r.status_code} {r.text[:220]}")
    except Exception as e:
        log.write(f"❌ WP投稿例外({site_key}): {e}")
    return ""

# =============================================================
# Poster: FC2 (XML-RPC MetaWeblog)
# =============================================================
FC2_ENDPOINT = SECRETS.get("fc2", {}).get("endpoint", "https://blog.fc2.com/xmlrpc.php")

def post_fc2(article: Dict[str, str], blog_id: str, username: str, password: str, when: Optional[dt.datetime], log: UILog) -> str:
    if xmlrpclib is None:
        log.write("❌ XML-RPC 未インストール")
        return ""
    try:
        server = xmlrpclib.ServerProxy(FC2_ENDPOINT)
        content = {
            'title': article['title'],
            'description': article['content'],
            'mt_excerpt': '', 'mt_text_more': '',
            'mt_allow_comments': 1, 'mt_allow_pings': 0, 'mt_convert_breaks': 0,
            'categories': []
        }
        publish = True
        if when and when > dt.datetime.now():
            # FC2は予約APIが限定的。publish=Trueでも指定日時公開は不可のことが多い。
            publish = False  # 下書き保存 → 人手公開想定
        post_id = server.metaWeblog.newPost(blog_id, username, password, content, publish)
        log.write(f"✅ FC2投稿成功: post_id={post_id} (publish={publish})")
        return f"fc2://{post_id}"
    except Exception as e:
        log.write(f"❌ FC2投稿失敗: {e}")
        return ""

# =============================================================
# Poster: Livedoor (AtomPub)
# =============================================================
LIVEDOOR_CONF = SECRETS.get("livedoor", {})

def post_livedoor(article: Dict[str, str], when: Optional[dt.datetime], log: UILog) -> str:
    # 公式API: AtomPub (application/x.atom+xml)
    blog = LIVEDOOR_CONF.get("blog_name")
    user = LIVEDOOR_CONF.get("user")
    api_key = LIVEDOOR_CONF.get("api_key")
    if not (blog and user and api_key):
        log.write("⚠️ Livedoor設定不足")
        return ""
    url = f"https://livedoor.blogcms.jp/atompub/entry/{blog}"
    # 予約はdraft + publish_at相当フィールド非公開のため、基本は下書き化
    draft = (when and when > dt.datetime.now())
    # Atom entry
    title_xml = article['title']
    content_xml = article['content']
    entry = f"""
    <entry xmlns='http://www.w3.org/2005/Atom'>
      <title>{title_xml}</title>
      <content type='xhtml'><div xmlns='http://www.w3.org/1999/xhtml'>{content_xml}</div></content>
      <updated>{dt.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}</updated>
      {"<draft xmlns='http://purl.org/atom-blog/ns#'>yes</draft>" if draft else ''}
    </entry>
    """.strip()
    try:
        auth = (user, api_key)
        r = requests.post(url, data=entry.encode('utf-8'), headers={'Content-Type':'application/atom+xml; charset=utf-8'}, auth=auth, timeout=60)
        if r.status_code in (200,201):
            log.write("✅ Livedoor投稿成功" + (" (下書き)" if draft else ""))
            return "livedoor://ok"
        else:
            log.write(f"❌ Livedoor投稿失敗: {r.status_code} {r.text[:200]}")
    except Exception as e:
        log.write(f"❌ Livedoor投稿例外: {e}")
    return ""

# =============================================================
# Poster: Seesaa (AtomPub)
# =============================================================
SEESAA_CONF = SECRETS.get("seesaa", {})

def post_seesaa(article: Dict[str, str], when: Optional[dt.datetime], log: UILog) -> str:
    endpoint = SEESAA_CONF.get("endpoint", "https://api.seesaa.net/blog/atom")
    blog_id = SEESAA_CONF.get("blog_id")
    user = SEESAA_CONF.get("user")
    api_key = SEESAA_CONF.get("api_key")
    if not (blog_id and user and api_key):
        log.write("⚠️ Seesaa設定不足")
        return ""
    # draft when future
    draft_tag = ("<app:control xmlns:app='http://www.w3.org/2007/app'><app:draft>yes</app:draft></app:control>"
                 if (when and when > dt.datetime.now()) else "")
    entry = f"""
    <entry xmlns='http://www.w3.org/2005/Atom'>
      <title>{article['title']}</title>
      <content type='html'>{article['content']}</content>
      {draft_tag}
    </entry>
    """.strip()
    try:
        # SeesaaはBasic認証: user + APIKey
        r = requests.post(f"{endpoint}/{blog_id}/entry", data=entry.encode('utf-8'),
                          headers={'Content-Type': 'application/atom+xml; charset=utf-8'},
                          auth=(user, api_key), timeout=60)
        if r.status_code in (200,201):
            log.write("✅ Seesaa投稿成功" + (" (下書き)" if draft_tag else ""))
            return "seesaa://ok"
        else:
            log.write(f"❌ Seesaa投稿失敗: {r.status_code} {r.text[:200]}")
    except Exception as e:
        log.write(f"❌ Seesaa投稿例外: {e}")
    return ""

# =============================================================
# Poster: Blogger (Google API)
# =============================================================
BLOGGER_CONF = SECRETS.get("blogger", {})
SCOPES = ["https://www.googleapis.com/auth/blogger"]

def get_blogger_service():
    if gapi_build is None or GoogleCredentials is None:
        raise RuntimeError("google-api-python-client 未インストール")
    cid = BLOGGER_CONF.get("client_id"); cs = BLOGGER_CONF.get("client_secret"); rt = BLOGGER_CONF.get("refresh_token")
    if not (cid and cs and rt):
        raise RuntimeError("Blogger用 OAuth クレデンシャル (client_id/client_secret/refresh_token) が未設定")
    creds = GoogleCredentials(None, cid, cs, rt, None, None, None)
    creds.refresh(requests.Request())  # type: ignore
    return gapi_build('blogger', 'v3', credentials=creds, cache_discovery=False)

def post_blogger(article: Dict[str, str], when: Optional[dt.datetime], log: UILog) -> str:
    try:
        blog_id = BLOGGER_CONF.get("blog_id")
        if not blog_id:
            log.write("⚠️ Blogger blog_id 未設定")
            return ""
        svc = get_blogger_service()
        body = {
            'kind': 'blogger#post',
            'title': article['title'],
            'content': article['content'],
        }
        if when and when > dt.datetime.now():
            body['published'] = when.isoformat()
        post = svc.posts().insert(blogId=blog_id, body=body, isDraft=bool(when and when > dt.datetime.now())).execute()
        url = post.get('url', '')
        log.write(f"✅ Blogger投稿成功: {url}")
        return url
    except Exception as e:
        log.write(f"❌ Blogger投稿失敗: {e}")
        return ""

# =============================================================
# UI Helpers
# =============================================================
BRAND_COLOR = SECRETS.get("ui", {}).get("brand_color", "#22c55e")

st.sidebar.header("設定")
ws_options = SECRETS.get("sheets", {}).get("ws_names", ["クレかえる向け","買取LIFE向け","お財布レスキュー向け"])  # 既定
ws_name = st.sidebar.selectbox("対象ワークシート", ws_options)
platforms = ["WordPress","Seesaa","FC2","Livedoor","Blogger"]
platform = st.sidebar.selectbox("投稿プラットフォーム", platforms)
wp_site_key = st.sidebar.selectbox("(WPのみ) 投稿サイト", list(SECRETS.get("wp", {}).get("sites", {}).keys()) or ["未設定"])
category = st.sidebar.text_input("カテゴリ名 (WPのみ) 省略可", "")
create_eye = st.sidebar.checkbox("アイキャッチ自動生成", True)

st.sidebar.subheader("予約設定 (任意)")
schedule_mode = st.sidebar.selectbox("スケジュール入力方式", ["なし","単一日時","複数行（テキストエリア）"]) 
when_single_str = None
when_multi_str = None
if schedule_mode == "単一日時":
    when_single_str = st.sidebar.text_input("ISO日時 (例: 2025-08-20T14:30)")
elif schedule_mode == "複数行（テキストエリア）":
    when_multi_str = st.sidebar.text_area("1行につき1日時 (ISO または HH:MM) — 行数分だけ順に消費")

st.sidebar.subheader("リンク挿入")
use_other_link = st.sidebar.checkbox("スプレッドシート『その他リンク先』から自動で1件選ぶ", True)

st.sidebar.markdown("---")
go_btn = st.sidebar.button("▶️ データ読込 / 更新")

# =============================================================
# Main table
# =============================================================
if go_btn:
    try:
        rows = load_sheet(ws_name)
        st.session_state["rows"] = rows
        ui_log.write(f"✅ ワークシート '{ws_name}' 読み込み {len(rows)} 行")
    except Exception as e:
        ui_log.write(f"❌ ワークシート読込失敗: {e}")

rows = st.session_state.get("rows")
if not rows:
    st.info("左の『▶️ データ読込 / 更新』を押してください。")
    ui_log.dump()
    st.stop()

headers = rows[0] if rows else []
data = rows[1:]

# 代表的な列想定: A:テーマ / B:宣伝URL / C:投稿先(サイトキー) / D:アンカー / K:予約日時
col_theme = next((i for i,h in enumerate(headers) if 'テーマ' in h), 0)
col_url   = next((i for i,h in enumerate(headers) if '宣伝' in h or 'URL' in h), 1)
col_site  = next((i for i,h in enumerate(headers) if '投稿先' in h or 'サイト' in h), 2)
col_anchor= next((i for i,h in enumerate(headers) if 'アンカー' in h or 'anchor' in h.lower()), 3)
col_sched = next((i for i,h in enumerate(headers) if h.strip().startswith('K') or '予約' in h), 10)

st.subheader(f"{ws_name} — データ一覧")
sel = []

# 表示と選択
for idx, row in enumerate(data):
    with st.expander(f"#{idx+2} | {row[col_theme] if len(row)>col_theme else ''}"):
        cols = st.columns([2,3,3,2,2])
        with cols[0]:
            picked = st.checkbox("選択", key=f"pick_{idx}")
        with cols[1]:
            st.write("**宣伝URL**", row[col_url] if len(row)>col_url else "")
        with cols[2]:
            st.write("**投稿先**", row[col_site] if len(row)>col_site else "")
        with cols[3]:
            st.write("**アンカー**", row[col_anchor] if len(row)>col_anchor else "")
        with cols[4]:
            st.write("**K列(予約)**", row[col_sched] if len(row)>col_sched else "")
        if picked:
            sel.append(idx)

st.success(f"選択行: {len(sel)} 件")

# =============================================================
# Posting execution
# =============================================================
post_count = st.number_input("この実行で投稿する上限数", min_value=1, max_value=50, value=1, step=1)
exec_btn = st.button("🚀 投稿実行")

competitors = load_competitors() if use_other_link else []
other_links = load_other_links() if use_other_link else []


def pick_other_link() -> Tuple[str,str]:
    if not other_links:
        return ("https://www.fsa.go.jp/", "金融庁")
    import random as _r
    # 競合ドメインを含まないもの
    from urllib.parse import urlparse
    pool = []
    for url, anc in other_links:
        dom = urlparse(url).netloc.lower()
        if not any(c in dom for c in competitors):
            pool.append((url, anc))
    return _r.choice(pool or other_links)


def parse_when_from_text(s: str) -> Optional[dt.datetime]:
    s = s.strip()
    if not s:
        return None
    # ISO like
    try:
        return dt.datetime.fromisoformat(s)
    except Exception:
        pass
    # HH:MM today
    m = re.match(r"^(\d{1,2}):(\d{2})$", s)
    if m:
        h = int(m.group(1)); M = int(m.group(2))
        today = dt.datetime.now().replace(hour=h, minute=M, second=0, microsecond=0)
        if today < dt.datetime.now():
            # tomorrow
            today = today + dt.timedelta(days=1)
        return today
    return None


if exec_btn and sel:
    used_multi_times: List[dt.datetime] = []
    if when_multi_str:
        used_multi_times = [t for t in (parse_when_from_text(x) for x in when_multi_str.splitlines()) if t]

    posted = 0
    for idx in sel:
        if posted >= post_count:
            break
        row = data[idx]
        theme = row[col_theme] if len(row)>col_theme else ""
        url = row[col_url] if len(row)>col_url and row[col_url] else None
        anchor = row[col_anchor] if len(row)>col_anchor and row[col_anchor] else None
        # URL/アンカーが無ければその他リンクから補完
        if (not url or not anchor) and use_other_link:
            ou, oa = pick_other_link()
            url = url or ou
            anchor = anchor or oa

        if not (url and anchor):
            ui_log.write(f"⚠️ URL/アンカー不足のためスキップ: 行{idx+2}")
            continue

        ui_log.write(f"🧠 記事生成中... (行{idx+2})")
        try:
            article = generate_article(theme, url, anchor)
        except Exception as e:
            ui_log.write(f"❌ 記事生成失敗: {e}")
            continue

        # 予約決定: 優先順 1) 複数行入力の先頭→消費 2) 単一入力 3) シートK列 4) なし
        when: Optional[dt.datetime] = None
        if used_multi_times:
            when = used_multi_times.pop(0)
        elif when_single_str:
            when = parse_when_from_text(when_single_str)
        elif len(row)>col_sched and row[col_sched].strip():
            when = parse_when_from_text(row[col_sched].strip())

        brand_color = BRAND_COLOR

        # Dispatch by platform
        link = ""
        try:
            if platform == "WordPress":
                link = post_wordpress(article, site_key=wp_site_key, category=category or None, when=when,
                                      create_eye=create_eye, brand_color=brand_color, log=ui_log)
            elif platform == "FC2":
                fc2_accs = SECRETS.get("fc2", {}).get("accounts", {})
                blog_id = SECRETS.get("fc2", {}).get("blog_id")
                if not blog_id:
                    # fallback: rowの投稿先をblog_idとして使う
                    blog_id = row[col_site] if len(row)>col_site and row[col_site] else next(iter(fc2_accs.keys()), "")
                acc = fc2_accs.get(blog_id) or {}
                link = post_fc2(article, blog_id, acc.get('user',''), acc.get('password',''), when, ui_log)
            elif platform == "Livedoor":
                link = post_livedoor(article, when, ui_log)
            elif platform == "Seesaa":
                link = post_seesaa(article, when, ui_log)
            elif platform == "Blogger":
                link = post_blogger(article, when, ui_log)
            else:
                ui_log.write(f"⚠️ 未対応プラットフォーム: {platform}")
        except Exception as e:
            ui_log.write(f"❌ 投稿処理エラー: {e}\n{traceback.format_exc()}")
            link = ""

        if link:
            posted += 1
        else:
            ui_log.write(f"⚠️ 投稿失敗 (行{idx+2})")

    ui_log.write(f"📦 実行完了: 投稿 {posted} 件 / 指定 {post_count} 件")

# =============================================================
# Log area
# =============================================================
st.subheader("実行ログ")
logs = st.session_state.get("_log", [])
st.code("\n".join(logs[-400:]) or "(ログなし)")
