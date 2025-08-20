# -*- coding: utf-8 -*-
import streamlit as st
import requests
import gspread
import time
import random
import xmlrpc.client
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from requests.auth import HTTPBasicAuth
import json
import re
import pandas as pd
from urllib.parse import urlparse, quote
import tempfile
import os
import base64
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape as xml_escape
import io
from PIL import Image, ImageDraw, ImageFont

# ====== Blogger関連（存在すれば使う） ======
try:
    import pickle
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    BLOGGER_AVAILABLE = True
except Exception:
    BLOGGER_AVAILABLE = False

# ====== PAS（あなたのキー名）対応 ======
def _get_secret(name, default=None):
    try:
        return st.secrets[name]
    except Exception:
        return os.environ.get(name, default)

# ---- Google / Gemini ----
SHEET_ID = _get_secret("SPREADSHEET_ID")
GEMINI_API_KEYS = [k for k in [
    _get_secret("GEMINI_API_KEY_1"),
    _get_secret("GEMINI_API_KEY_2"),
] if k]

if not SHEET_ID or not GEMINI_API_KEYS:
    st.error("必須Secrets不足: SPREADSHEET_ID / GEMINI_API_KEY_* を確認して下さい。")
    st.stop()

# ---- GCP サービスアカウント（credentials.jsonの内容そのまま）----
GCP_SA_RAW = _get_secret("GCP_SERVICE_ACCOUNT")
if not GCP_SA_RAW:
    st.error("GCP_SERVICE_ACCOUNT が未設定です（credentials.jsonの中身をそのまま指定）。")
    st.stop()

try:
    # st.secrets に dict として入っている場合/文字列の場合 両対応
    if isinstance(GCP_SA_RAW, dict):
        GCP_SA = dict(GCP_SA_RAW)
    else:
        GCP_SA = json.loads(GCP_SA_RAW)
except Exception as e:
    st.error(f"GCP_SERVICE_ACCOUNT のJSON解釈に失敗: {e}")
    st.stop()

# ---- Blogger PAS ----
BLOGGER_BLOG_ID = _get_secret("BLOG_ID")
BLOGGER_CREDENTIALS_JSON = _get_secret("BLOGGER_CREDENTIALS")  # client_secret.jsonの中身
BLOGGER_TOKEN_B64 = _get_secret("BLOGGER_TOKEN")               # blogger_token.pickle をbase64

# ---- livedoor PAS ----
LIVEDOOR_BLOG_NAME = _get_secret("LIVEDOOR_BLOG_NAME")
LIVEDOOR_ID = _get_secret("LIVEDOOR_ID")
LIVEDOOR_API_KEY = _get_secret("LIVEDOOR_API_KEY")

# ---- Seesaa PAS ----
SEESAA_USERNAME = _get_secret("SEESAA_USERNAME")
SEESAA_PASSWORD = _get_secret("SEESAA_PASSWORD")
SEESAA_BLOGID = _get_secret("SEESAA_BLOGID")

# ---- FC2 PAS ----
FC2_BLOG_ID = _get_secret("FC2_BLOG_ID")
FC2_USERNAME = _get_secret("FC2_USERNAME")
FC2_PASSWORD = _get_secret("FC2_PASSWORD")

# ---- WordPress PAS（環境変数/Secretsのフラット名）----
def _wp(site):
    return {
        'url': _get_secret(f"WP_{site.upper()}_URL"),
        'user': _get_secret(f"WP_{site.upper()}_USER"),
        'password': _get_secret(f"WP_{site.upper()}_PASSWORD"),
    }

# あなたのPAS名に合わせたsite_key
WP_CONFIGS = {
    'ykikaku': _wp('ykikaku'),
    'efdlqjtz': _wp('efdlqjtz'),
    'selectadvance': _wp('selectadvance'),
    'welkenraedt': _wp('welkenraedt'),
    'ncepqvub': _wp('ncepqvub'),
    'kosagi': _wp('kosagi'),
    'selectad01': _wp('selectad'),   # ← PASが selectad01.* のため site_key は selectad01 にする
    'thrones': _wp('thrones'),
}

# 値検証（URLが未設定のサイトは除去）
WP_CONFIGS = {k:v for k,v in WP_CONFIGS.items() if v['url']}

# ====== プロジェクト構成（C列の値を最優先） ======
PROJECT_CONFIGS = {
    'biggift': {
        'worksheet': 'ビックギフト向け',
        'platforms': ['blogger', 'livedoor'],      # C列に 'blogger' or 'livedoor'
        'max_posts': {'blogger': 20, 'livedoor': 15},
        'needs_k_column': True
    },
    'arigataya': {
        'worksheet': 'ありがた屋向け',
        'platforms': ['seesaa', 'fc2'],            # C列に 'seesaa' or 'fc2'
        'max_posts': 20,
        'needs_k_column': True
    },
    'kaitori_life': {
        'worksheet': '買取LIFE向け',
        'platforms': ['wordpress'],                # C列に WPサイトキー（例: 'selectad01' / 'thrones'）
        'wp_sites': ['selectad01', 'thrones'],
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

MIN_INTERVAL = 30
MAX_INTERVAL = 60

# ====== Streamlit UI ======
st.set_page_config(page_title="統合ブログ投稿ツール", page_icon="🚀", layout="wide")
st.markdown("""
<style>
.main-header{background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);color:#fff;padding:2rem;border-radius:10px;text-align:center;margin-bottom:2rem;}
.stButton>button{background:linear-gradient(135deg,#4CAF50,#66BB6A);color:white;border:none;border-radius:8px;padding:.8rem 2rem;font-weight:bold;font-size:16px;}
.stButton>button:hover{background:linear-gradient(135deg,#66BB6A,#4CAF50);}
.warning-box{background:#fff3cd;border:1px solid #ffc107;color:#856404;padding:1rem;border-radius:8px;margin:1rem 0;}
.success-box{background:#d4edda;border:1px solid #28a745;color:#155724;padding:1rem;border-radius:8px;margin:1rem 0;}
.error-box{background:#f8d7da;border:1px solid #dc3545;color:#721c24;padding:1rem;border-radius:8px;margin:1rem 0;}
.notification-container{position:sticky;top:0;z-index:1000;background:white;padding:1rem;border-bottom:1px solid #ddd;margin-bottom:1rem;}
</style>
""", unsafe_allow_html=True)

# ====== セッション ======
for k, v in [
    ('gemini_key_index', 0), ('posting_projects', set()), ('current_project', None),
    ('realtime_logs', {}), ('all_posted_urls', {}), ('completion_results', {}),
    ('persistent_notifications', []), ('notification_counter', 0)
]:
    if k not in st.session_state: st.session_state[k] = v

def add_notification(message, notification_type="info", project_key=None):
    st.session_state.notification_counter += 1
    n = {'id': st.session_state.notification_counter,
         'timestamp': datetime.now().strftime("%H:%M:%S"),
         'message': message, 'type': notification_type,
         'project_key': project_key, 'created_at': datetime.now()}
    st.session_state.persistent_notifications.append(n)
    if len(st.session_state.persistent_notifications) > 30:
        st.session_state.persistent_notifications = st.session_state.persistent_notifications[-25:]

def show_notifications():
    if not st.session_state.persistent_notifications: return
    st.markdown('<div class="notification-container">', unsafe_allow_html=True)
    st.markdown("### 📢 通知一覧")
    for n in reversed(st.session_state.persistent_notifications[-5:]):
        icon = "✅" if n['type']=="success" else "❌" if n['type']=="error" else "⚠️" if n['type']=="warning" else "ℹ️"
        css  = "success-box" if n['type']=="success" else "error-box" if n['type']=="error" else "warning-box" if n['type']=="warning" else "success-box"
        pj = f"[{n.get('project_key','')}] " if n.get('project_key') else ""
        st.markdown(f'<div class="{css}"><strong>{icon} {n["timestamp"]}</strong> {pj}{n["message"]}</div>', unsafe_allow_html=True)
    if len(st.session_state.persistent_notifications) > 5:
        with st.expander(f"全通知を表示 ({len(st.session_state.persistent_notifications)}件)"):
            for n in reversed(st.session_state.persistent_notifications):
                icon = "✅" if n['type']=="success" else "❌" if n['type']=="error" else "⚠️" if n['type']=="warning" else "ℹ️"
                pj = f"[{n.get('project_key','')}] " if n.get('project_key') else ""
                st.write(f"{icon} **{n['timestamp']}** {pj}{n['message']}")
    if st.button("🗑️ 通知クリア", key="clear_notifications"): 
        st.session_state.persistent_notifications = []
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

def add_realtime_log(message, project_key):
    ts = datetime.now().strftime("%H:%M:%S")
    st.session_state.realtime_logs.setdefault(project_key, []).append(f"[{ts}] {message}")
    if len(st.session_state.realtime_logs[project_key]) > 50:
        st.session_state.realtime_logs[project_key] = st.session_state.realtime_logs[project_key][-30:]

def add_posted_url(counter, title, url, timestamp, project_key):
    st.session_state.all_posted_urls.setdefault(project_key, []).append(
        {'counter': counter, 'title': title, 'url': url, 'timestamp': timestamp}
    )

# ====== 画像 ======
def create_eyecatch_image(title: str, site_key: str) -> bytes:
    width, height = 600, 400
    schemes = [
        {'bg': '#2E7D32', 'accent': '#66BB6A', 'text': '#FFFFFF'},
        {'bg': '#388E3C', 'accent': '#81C784', 'text': '#FFFFFF'},
        {'bg': '#4CAF50', 'accent': '#8BC34A', 'text': '#FFFFFF'},
        {'bg': '#689F38', 'accent': '#AED581', 'text': '#FFFFFF'},
        {'bg': '#7CB342', 'accent': '#C5E1A5', 'text': '#2E7D32'},
    ]
    scheme = random.choice(schemes)
    img = Image.new('RGB', (width, height), color=scheme['bg'])
    draw = ImageDraw.Draw(img)
    for i in range(height):
        alpha = i/height
        def c(x): return int(int(x,16)*(1-alpha*0.3))
        r,g,b = c(scheme['bg'][1:3]), c(scheme['bg'][3:5]), c(scheme['bg'][5:7])
        draw.rectangle([(0,i),(width,i+1)], fill=(r,g,b))
    draw.ellipse([-50,-50,150,150], fill=scheme['accent'])
    draw.ellipse([width-100,height-100,width+50,height+50], fill=scheme['accent'])
    try:
        f1 = ImageFont.truetype("C:/Windows/Fonts/meiryob.ttc", 28)
        f2 = ImageFont.truetype("C:/Windows/Fonts/meiryob.ttc", 20)
    except:
        try:
            f1 = ImageFont.truetype("C:/Windows/Fonts/meiryo.ttc", 28)
            f2 = ImageFont.truetype("C:/Windows/Fonts/meiryo.ttc", 20)
        except:
            f1 = ImageFont.load_default(); f2 = ImageFont.load_default()
    lines=[]
    if len(title)>12:
        for sep in ['！','？','…','!','?']:
            if sep in title:
                i=title.find(sep); 
                if i>0: lines=[title[:i+1],title[i+1:].strip()]; break
        if not lines:
            for sep in ['と','、','の','は','が','を','に','…','で']:
                if sep in title:
                    i=title.find(sep)
                    if 5<i<len(title)-5: lines=[title[:i],title[i:]]; break
        if not lines:
            mid=len(title)//2; lines=[title[:mid],title[mid:]]
    else:
        lines=[title]
    y=(height-len(lines)*50)//2
    for j,t in enumerate(lines):
        try: bbox=draw.textbbox((0,0),t,font=f1); w=bbox[2]-bbox[0]
        except: w,_=draw.textsize(t,font=f1)
        x=(width-w)//2; yy=y+j*50
        draw.text((x+2,yy+2),t,font=f1,fill=(0,0,0))
        draw.text((x,yy),t,font=f1,fill=scheme['text'])
    try: bbox=draw.textbbox((0,0),site_key,font=f2); w=bbox[2]-bbox[0]
    except: w,_=draw.textsize(site_key,font=f2)
    x=(width-w)//2
    draw.text((x,height-50),site_key,font=f2,fill=scheme['text'])
    draw.rectangle([50,40,width-50,42], fill=scheme['text'])
    b=io.BytesIO(); img.save(b,format='JPEG',quality=90); b.seek(0); return b.getvalue()

# ====== Sheets ======
@st.cache_resource
def get_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(GCP_SA, scope)
    return gspread.authorize(creds)

@st.cache_data(ttl=60)
def load_sheet_data(project_key):
    try:
        client = get_sheets_client()
        ws_name = PROJECT_CONFIGS[project_key]['worksheet']
        sheet = client.open_by_key(SHEET_ID).worksheet(ws_name)
        rows = sheet.get_all_values()
        if len(rows)<=1: return pd.DataFrame()
        headers = rows[0]; data = rows[1:]
        # 未処理のみ
        filtered=[]
        for r in data:
            if len(r)>=5 and r[1] and r[1].strip():
                status = r[4].strip().lower() if len(r)>4 else ''
                if status in ('','未処理'):
                    filtered.append(r + ['']*(len(headers)-len(r)))
        if not filtered: return pd.DataFrame()
        df = pd.DataFrame(filtered, columns=headers)
        if '選択' not in df.columns:
            df.insert(0,'選択',False)
        return df
    except Exception as e:
        add_notification(f"データ読み込みエラー: {e}","error"); return pd.DataFrame()

# ====== 競合/その他リンク（省略せず動く簡易版） ======
@st.cache_data(ttl=300)
def get_other_links():
    try:
        client = get_sheets_client()
        sh = client.open_by_key(SHEET_ID).worksheet('その他リンク先')
        rows = sh.get_all_values()[1:]
        out=[]
        for r in rows:
            if len(r)>=2 and r[0] and r[1]:
                out.append({"url":r[0].strip(), "anchor":r[1].strip()})
        if out: return out
    except Exception:
        pass
    return [{"url":"https://www.fsa.go.jp/","anchor":"金融庁"},
            {"url":"https://www.boj.or.jp/","anchor":"日本銀行"}]

def get_other_link():
    sites=get_other_links()
    return (sites and (sites[random.randrange(len(sites))]['url'], sites[random.randrange(len(sites))]['anchor'])) or (None,None)

# ====== Gemini ======
def _get_gemini_key():
    key = GEMINI_API_KEYS[ st.session_state.gemini_key_index % len(GEMINI_API_KEYS) ]
    st.session_state.gemini_key_index += 1
    return key

def call_gemini(prompt:str)->str:
    api_key = _get_gemini_key()
    url=f'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent?key={api_key}'
    payload={"contents":[{"parts":[{"text":prompt}]}],"generationConfig":{"temperature":0.7}}
    r=requests.post(url,json=payload,timeout=60)
    if r.status_code!=200:
        raise Exception(f"Gemini API {r.status_code}: {r.text[:200]}")
    j=r.json()
    return j['candidates'][0]['content']['parts'][0]['text']

def generate_article_with_link(theme:str, url:str, anchor_text:str)->dict:
    theme = theme.strip() or "金融・投資・資産運用"
    prompt=f"""
# 命令書:
「{theme}」をテーマに、読者に価値のある記事を作成。

# 記事に含めるリンク（1つのみ）
URL: {url}
アンカーテキスト: {anchor_text}

# 出力:
・最初の行にタイトル（タグなし）
・以降HTML本文
・リンクは1回だけ自然に挿入

# HTML:
・h2/h3のみ
・段落は<p>
・リンクは <a href="{url}" target="_blank" rel="noopener noreferrer">{anchor_text}</a>
・箇条書きOK

# 要件:
・2000-2500文字
・専門的で平易
・数値/事例
・具体的記述（〇〇等禁止）
"""
    txt = call_gemini(prompt)
    lines = txt.strip().split('\n')
    title = lines[0].strip()
    content = '\n'.join(lines[1:]).strip()
    content = re.sub(r'〇〇|××|△△','',content)
    content = re.sub(r'<p>\s*</p>','',content)
    return {"title":title,"content":content,"theme":theme}

# ====== HTML整形 ======
def enforce_anchor_attrs(html:str)->str:
    def add_attrs(m):
        tag=m.group(0)
        if re.search(r'\btarget\s*=',tag,flags=re.I) is None:
            tag=tag.replace('<a ','<a target="_blank" ',1)
        rel_m=re.search(r'\brel\s*=\s*"([^"]*)"',tag,flags=re.I)
        if rel_m:
            rel_val=rel_m.group(1); need=[]
            for t in ('noopener','noreferrer'):
                if t not in rel_val.split(): need.append(t)
            if need:
                new_rel=rel_val+' '+' '.join(need)
                tag=tag[:rel_m.start(1)]+new_rel+tag[rel_m.end(1):]
        else:
            tag=tag.replace('<a ','<a rel="noopener noreferrer" ',1)
        return tag
    return re.sub(r'<a\s+[^>]*>', add_attrs, html, flags=re.I)

# ====== 投稿先 各実装 ======
def post_to_seesaa(article, category=None, project_key=None):
    endpoint="http://blog.seesaa.jp/rpc"
    server=xmlrpc.client.ServerProxy(endpoint, allow_none=True)
    content={"title":article["title"],"description":article["content"]}
    try:
        add_notification("Seesaa投稿開始","info",project_key)
        post_id=server.metaWeblog.newPost(SEESAA_BLOGID, SEESAA_USERNAME, SEESAA_PASSWORD, content, True)
        add_notification(f"Seesaa投稿成功: post_id={post_id}","success",project_key)
        return f"post_id:{post_id}"
    except Exception as e:
        add_notification(f"Seesaa投稿エラー: {e}","error",project_key); return ""

def post_to_fc2(article, category=None, project_key=None):
    server=xmlrpc.client.ServerProxy('https://blog.fc2.com/xmlrpc.php')
    content={'title':article['title'],'description':article['content']}
    try:
        add_notification("FC2投稿開始","info",project_key)
        post_id=server.metaWeblog.newPost(FC2_BLOG_ID, FC2_USERNAME, FC2_PASSWORD, content, True)
        url=f"https://{FC2_BLOG_ID}.blog.fc2.com/blog-entry-{post_id}.html"
        add_notification(f"FC2投稿成功: {url}","success",project_key)
        return url
    except Exception as e:
        add_notification(f"FC2投稿エラー: {e}","error",project_key); return ""

def post_to_livedoor(article, category=None, project_key=None):
    root=f"https://livedoor.blogcms.jp/atompub/{LIVEDOOR_BLOG_NAME}"
    endpoint=f"{root}/article"
    add_notification("livedoor投稿開始","info",project_key)
    title_xml=xml_escape(article["title"])
    safe_html=enforce_anchor_attrs(article["content"])
    content_xml=xml_escape(safe_html)
    cat_xml=f'<category term="{xml_escape(category)}"/>' if category else ""
    entry_xml=f'''<?xml version="1.0" encoding="utf-8"?>
<entry xmlns="http://www.w3.org/2005/Atom">
  <title>{title_xml}</title>
  <content type="html">{content_xml}</content>
  {cat_xml}
</entry>'''.encode("utf-8")
    try:
        r=requests.post(endpoint, data=entry_xml,
                        headers={"Content-Type":"application/atom+xml;type=entry"},
                        auth=HTTPBasicAuth(LIVEDOOR_ID, LIVEDOOR_API_KEY), timeout=30)
        if r.status_code in (200,201):
            try:
                root_xml=ET.fromstring(r.text); ns={"atom":"http://www.w3.org/2005/Atom"}
                alt=root_xml.find(".//atom:link[@rel='alternate']", ns)
                url= alt.get("href") if alt is not None else ""
                if url: add_notification(f"livedoor投稿成功: {url}","success",project_key)
                else: add_notification("livedoor投稿成功（URL取得不可）","warning",project_key)
                return url
            except Exception as pe:
                add_notification(f"livedoor解析エラー: {pe}","warning",project_key); return ""
        else:
            add_notification(f"livedoor失敗: HTTP {r.status_code} {r.text[:200]}","error",project_key); return ""
    except Exception as e:
        add_notification(f"livedoor投稿エラー: {e}","error",project_key); return ""

def post_to_blogger(article, project_key=None):
    if not BLOGGER_AVAILABLE:
        add_notification("Bloggerライブラリ未導入","error",project_key); return ""
    if not BLOGGER_BLOG_ID:
        add_notification("BLOG_ID が未設定です","error",project_key); return ""
    try:
        # token_base64 があれば /tmp に展開
        token_path="/tmp/blogger_token.pickle"
        if BLOGGER_TOKEN_B64:
            try:
                with open(token_path,"wb") as f:
                    f.write(base64.b64decode(BLOGGER_TOKEN_B64))
                add_notification("Bloggerトークン展開OK","success",project_key)
            except Exception as e:
                add_notification(f"Bloggerトークン展開失敗: {e}","error",project_key); return ""
        # 認証
        creds=None
        if os.path.exists(token_path):
            with open(token_path,"rb") as f:
                creds=pickle.load(f)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                add_notification("Bloggerトークン更新中...","info",project_key)
                creds.refresh(Request())
            else:
                add_notification("Blogger初回認証はUI不可のため、BLOGGER_TOKENを必ず設定してください","error",project_key)
                return ""
            with open(token_path,"wb") as f:
                pickle.dump(creds,f)
        service=build('blogger','v3',credentials=creds)
        post={'title':article['title'],'content':article['content'],'labels':[article.get('theme','金融')]}
        add_notification("Blogger投稿実行","info",project_key)
        res=service.posts().insert(blogId=BLOGGER_BLOG_ID, body=post, isDraft=False).execute()
        url = res.get('url',"")
        if url: add_notification(f"Blogger投稿成功: {url}","success",project_key)
        else: add_notification("Blogger投稿成功（URLなし）","warning",project_key)
        return url
    except Exception as e:
        add_notification(f"Blogger投稿エラー: {e}","error",project_key); return ""

# ---- WordPress（REST / kosagiはXMLRPCなし。全サイトREST統一） ----
def get_wp_category_id_rest(site_config:dict, category_name:str)->int|None:
    try:
        q=quote(category_name)
        url=f"{site_config['url']}wp-json/wp/v2/categories?search={q}&per_page=50"
        r=requests.get(url,auth=HTTPBasicAuth(site_config['user'],site_config['password']),timeout=30)
        if r.status_code==200:
            for it in r.json():
                if it.get('name')==category_name: return it.get('id')
        # create
        create_url=f"{site_config['url']}wp-json/wp/v2/categories"
        r2=requests.post(create_url,auth=HTTPBasicAuth(site_config['user'],site_config['password']),
                         headers={'Content-Type':'application/json'},
                         data=json.dumps({"name":category_name}),timeout=30)
        if r2.status_code in (200,201): return r2.json().get('id')
        add_notification(f"カテゴリ作成失敗({site_config['url']}): {r2.status_code}","warning")
    except Exception as e:
        add_notification(f"カテゴリ処理エラー({site_config['url']}): {e}","warning")
    return None

def post_to_wordpress(article, site_key:str, category_name=None, schedule_dt:datetime=None,
                      enable_eyecatch=True, project_key=None)->str:
    if site_key not in WP_CONFIGS:
        add_notification(f"未知のWPサイト: {site_key}","error",project_key); return ""
    sc=WP_CONFIGS[site_key]
    endpoint=f"{sc['url']}wp-json/wp/v2/posts"
    data={'title':article['title'],'content':article['content'],'status':'publish'}
    # カテゴリ
    if category_name:
        cat_id = get_wp_category_id_rest(sc, category_name)
        if cat_id: data['categories']=[cat_id]
    # 予約
    if schedule_dt and schedule_dt>datetime.now():
        data['status']='future'
        data['date']=schedule_dt.strftime('%Y-%m-%dT%H:%M:%S')
    # アイキャッチ
    if enable_eyecatch:
        try:
            img=create_eyecatch_image(article['title'], site_key)
            media=f"{sc['url']}wp-json/wp/v2/media"
            files={'file':('eyecatch.jpg', img, 'image/jpeg')}
            mres=requests.post(media,auth=HTTPBasicAuth(sc['user'],sc['password']),
                               files=files, data={'title':f"eyecatch: {article['title'][:30]}"},
                               timeout=60)
            if mres.status_code==201: data['featured_media']=mres.json().get('id')
            else: add_notification(f"アイキャッチ失敗({site_key}): {mres.status_code}","warning",project_key)
        except Exception as e:
            add_notification(f"アイキャッチ処理エラー({site_key}): {e}","warning",project_key)
    try:
        r=requests.post(endpoint,auth=HTTPBasicAuth(sc['user'],sc['password']),
                        headers={'Content-Type':'application/json'},data=json.dumps(data),timeout=60)
        if r.status_code in (200,201):
            j=r.json(); url=j.get('link','')
            add_notification(f"WP投稿成功({site_key}): {url or 'URL不明'}","success",project_key)
            return url
        else:
            try: msg=r.json().get('message','Unknown')
            except: msg=r.text[:200]
            add_notification(f"WP投稿失敗({site_key}): {r.status_code} {msg}","error",project_key)
            return ""
    except Exception as e:
        add_notification(f"WP投稿エラー({site_key}): {e}","error",project_key); return ""

# ====== 補助 ======
def get_max_posts_for_project(project_key, post_target=""):
    cfg=PROJECT_CONFIGS[project_key]['max_posts']
    if isinstance(cfg, dict):
        t=(post_target or '').lower()
        return cfg.get(t, max(cfg.values()) if cfg else 20)
    return cfg

# ====== 実行（C列=投稿先 最優先） ======
def execute_post(row_data, project_key, post_count=1, schedule_times=None, enable_eyecatch=True):
    try:
        st.session_state.posting_projects.add(project_key)
        add_realtime_log(f"📋 {PROJECT_CONFIGS[project_key]['worksheet']} 投稿開始", project_key)
        add_notification(f"{PROJECT_CONFIGS[project_key]['worksheet']} の投稿を開始","info",project_key)
        cfg = PROJECT_CONFIGS[project_key]
        schedule_times = schedule_times or []
        try:
            current_counter = int(row_data.get('カウンター','') or 0)
        except: current_counter=0
        post_target = (row_data.get('投稿先','') or '').strip()  # ← C列（**最優先**）
        add_notification(f"投稿先指定(C列): '{post_target}'","info",project_key)

        max_posts = get_max_posts_for_project(project_key, post_target)
        if current_counter >= max_posts:
            add_notification(f"既に{max_posts}記事完了","warning",project_key)
            st.session_state.posting_projects.discard(project_key); return False

        progress_bar = st.progress(0.0)
        posts_completed=0

        for i in range(post_count):
            if current_counter >= max_posts: break
            schedule_dt = schedule_times[i] if i < len(schedule_times) else None
            with st.expander(f"記事{i+1}/{post_count}", expanded=True):
                # 記事リンク決定：最終回だけ宣伝URL
                if current_counter == max_posts - 1:
                    url = row_data.get('宣伝URL','')
                    anchor = row_data.get('アンカーテキスト', project_key)
                    category = row_data.get('カテゴリー','お金のマメ知識')
                    add_realtime_log(f"🎯 {max_posts}記事目 → 宣伝URL使用", project_key)
                else:
                    url, anchor = get_other_link()
                    if not url:
                        add_notification("その他リンクが取得できません","error",project_key); break
                    category = 'お金のマメ知識'
                    add_realtime_log(f"🔗 {current_counter+1}記事目 → その他リンク使用", project_key)

                # 記事作成
                add_realtime_log("🧠 記事生成中...", project_key)
                theme = row_data.get('テーマ','') or '金融・投資・資産運用'
                article = generate_article_with_link(theme, url, anchor)
                st.success(f"タイトル: {article['title']}")
                st.info(f"使用リンク: {anchor}")

                # ===== ディスパッチ（C列最優先） =====
                posted_urls=[]
                pt = post_target.lower()

                if 'wordpress' in cfg['platforms']:
                    # WordPress系は「C列=site_key」必須
                    site_key = pt
                    if not site_key:
                        add_notification("C列にWPサイトキーを指定してください","error",project_key); break
                    if site_key not in WP_CONFIGS:
                        add_notification(f"未知のWPサイトキー: {site_key}","error",project_key); break
                    add_notification(f"WP({site_key})に投稿","info",project_key)
                    u = post_to_wordpress(article, site_key, category, schedule_dt, enable_eyecatch, project_key)
                    if u: posted_urls.append(u)

                elif pt == 'blogger' and 'blogger' in cfg['platforms']:
                    add_notification("Bloggerに投稿","info",project_key)
                    u = post_to_blogger(article, project_key)
                    if u: posted_urls.append(u)

                elif pt == 'livedoor' and 'livedoor' in cfg['platforms']:
                    add_notification("livedoorに投稿","info",project_key)
                    u = post_to_livedoor(article, category, project_key)
                    if u: posted_urls.append(u)

                elif pt == 'seesaa' and 'seesaa' in cfg['platforms']:
                    add_notification("Seesaaに投稿","info",project_key)
                    u = post_to_seesaa(article, category, project_key)
                    if u: posted_urls.append(u)

                elif pt == 'fc2' and 'fc2' in cfg['platforms']:
                    add_notification("FC2に投稿","info",project_key)
                    u = post_to_fc2(article, category, project_key)
                    if u: posted_urls.append(u)

                else:
                    # C列不正
                    add_notification(f"C列の投稿先指定が不正: '{post_target}'", "error", project_key)
                    break

                if not posted_urls:
                    add_notification("投稿に失敗しました","error",project_key); break

                # 記録/カウンター更新
                ts = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                for u in posted_urls:
                    add_posted_url(current_counter+1, article['title'], u, ts, project_key)
                current_counter += 1; posts_completed += 1

                # スプレッドシート更新（カウンター／完了時処理）
                client=get_sheets_client()
                ws_name=PROJECT_CONFIGS[project_key]['worksheet']
                sheet=client.open_by_key(SHEET_ID).worksheet(ws_name)
                all_rows=sheet.get_all_values()
                promo=row_data.get('宣伝URL','')
                for ridx, r in enumerate(all_rows[1:], start=2):
                    if len(r)>1 and r[1]==promo:
                        sheet.update_cell(ridx, 7, str(current_counter))  # G列=カウンター
                        time.sleep(0.3)
                        if current_counter >= max_posts:
                            # 完了処理
                            sheet.update_cell(ridx, 5, "処理済み")  # E列=ステータス
                            time.sleep(0.3)
                            final_urls=[it['url'] for it in st.session_state.all_posted_urls[project_key] if it['counter']==max_posts]
                            sheet.update_cell(ridx, 6, ', '.join(final_urls))  # F列=投稿URL
                            time.sleep(0.3)
                            comp=datetime.now().strftime("%Y/%m/%d %H:%M")
                            sheet.update_cell(ridx, 9, comp)  # I列=完了日時
                            time.sleep(0.3)
                            add_notification(f"{max_posts}記事完了","success",project_key)
                            st.session_state.completion_results[project_key]={
                                'project_name': ws_name,'completed_at': comp,'total_posts': max_posts,
                                'all_urls': st.session_state.all_posted_urls[project_key].copy()
                            }
                            st.balloons()
                        break

                progress_bar.progress(posts_completed/max(1,post_count))
                if current_counter < max_posts and i < post_count-1:
                    wait=random.randint(MIN_INTERVAL, MAX_INTERVAL)
                    st.info(f"次の記事まで{wait}秒待機..."); time.sleep(wait)

        st.session_state.posting_projects.discard(project_key)
        add_notification(f"{posts_completed}記事 投稿完了","success",project_key)
        return posts_completed>0
    except Exception as e:
        st.session_state.posting_projects.discard(project_key)
        add_notification(f"投稿処理エラー: {e}","error",project_key)
        return False

# ====== UI ======
def main():
    show_notifications()
    st.markdown('<div class="main-header"><h1>統合ブログ投稿管理システム</h1><p>WordPress / Seesaa / FC2 / livedoor / Blogger</p></div>', unsafe_allow_html=True)

    # 完了結果表示
    if st.session_state.completion_results:
        st.markdown("## 🎉 投稿完了プロジェクト")
        for k,res in st.session_state.completion_results.items():
            with st.expander(f"✅ {res['project_name']} - {res['completed_at']}"):
                st.write(f"合計: {res['total_posts']}件")
                for it in res['all_urls']:
                    st.write(f"**{it['counter']}件目** {it['title']}")
                    st.write(f"🔗 {it['url']}")
                    st.write(f"⏰ {it['timestamp']}")
                if st.button(f"OK（{res['project_name']}を閉じる）", key=f"close_{k}"):
                    del st.session_state.completion_results[k]; st.rerun()

    # プロジェクト選択
    project_key = st.selectbox(
        "プロジェクト選択",
        options=list(PROJECT_CONFIGS.keys()),
        format_func=lambda x: f"{PROJECT_CONFIGS[x]['worksheet']} ({', '.join(PROJECT_CONFIGS[x]['platforms'])})"
    )
    if st.session_state.current_project != project_key and project_key not in st.session_state.posting_projects:
        st.session_state.current_project = project_key
        st.cache_data.clear()

    cfg=PROJECT_CONFIGS[project_key]
    col1,col2=st.columns(2)
    with col1:
        st.info(f"**プロジェクト**: {cfg['worksheet']}")
        st.info(f"**プラットフォーム**: {', '.join(cfg['platforms'])}")
    with col2:
        if cfg['needs_k_column']: st.warning("**予約方式**: K列記録 → 別ジョブ実行")
        else: st.success("**予約方式**: WordPress予約投稿を使用")

    df=load_sheet_data(project_key)
    if df.empty: st.info("未処理データなし"); return

    st.header("データ一覧（C列=投稿先は最優先で使用）")
    edited = st.data_editor(
        df, use_container_width=True, hide_index=True,
        column_config={
            "選択": st.column_config.CheckboxColumn("選択", width="small"),
            "テーマ": st.column_config.TextColumn("テーマ", width="medium"),
            "宣伝URL": st.column_config.TextColumn("宣伝URL", width="large"),
            "投稿先": st.column_config.TextColumn("投稿先", width="medium"),
            "アンカーテキスト": st.column_config.TextColumn("アンカー", width="medium"),
            "ステータス": st.column_config.TextColumn("ステータス", width="small"),
            "カウンター": st.column_config.TextColumn("カウンター", width="small")
        }
    )

    st.header("投稿設定")
    c1,c2 = st.columns(2)
    with c1:
        post_count = st.selectbox("投稿数", options=[1,2,3,4,5])
    with c2:
        enable_eyecatch = st.checkbox("アイキャッチ自動生成（WP）", value=True)

    # 予約（WPのみ、K列方式はここでは扱わず）
    schedule_times=[]
    if not cfg['needs_k_column']:
        enable_schedule = st.checkbox("WP予約投稿を使う")
        if enable_schedule:
            st.subheader("予約時刻（1行1件）")
            txt = st.text_area("YYYY-MM-DD HH:MM もしくは HH:MM（本日）", placeholder="2025-08-20 10:30\n14:05")
            if txt:
                now=datetime.now()
                for line in [x.strip() for x in txt.splitlines() if x.strip()]:
                    ok=False; dt=None
                    for fmt in ('%Y-%m-%d %H:%M','%Y/%m/%d %H:%M','%H:%M'):
                        try:
                            if fmt=='%H:%M':
                                t=datetime.strptime(line,fmt); dt=now.replace(hour=t.hour,minute=t.minute,second=0,microsecond=0)
                            else:
                                dt=datetime.strptime(line,fmt)
                            ok=True; break
                        except: pass
                    if ok and dt>now: schedule_times.append(dt)
        if schedule_times:
            st.success(f"予約 {len(schedule_times)}件 設定")
            for d in schedule_times: st.write("• "+d.strftime("%Y/%m/%d %H:%M"))

    # 実行
    b1,b2=st.columns(2)
    with b1:
        if st.button("投稿実行", type="primary", use_container_width=True):
            sel=edited[edited['選択']==True]
            if len(sel)==0: add_notification("投稿する行を選択してください","error")
            elif len(sel)>1: add_notification("1行のみ選択してください","error")
            else:
                row=sel.iloc[0]
                ok=execute_post(row.to_dict(), project_key, post_count=post_count,
                                schedule_times=schedule_times, enable_eyecatch=enable_eyecatch)
                if ok: time.sleep(1); st.cache_data.clear(); st.rerun()
    with b2:
        if st.button("データ更新", use_container_width=True):
            st.cache_data.clear(); add_notification("データ更新しました","success"); st.rerun()

    st.markdown("---")
    i1,i2,i3=st.columns(3)
    with i1: st.metric("未処理件数", len(df))
    with i2: st.metric("プラットフォーム", len(cfg['platforms']))
    with i3: st.metric("最終更新", datetime.now().strftime("%H:%M:%S"))

if __name__ == "__main__":
    main()
