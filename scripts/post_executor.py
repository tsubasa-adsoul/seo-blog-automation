#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
自動投稿実行スクリプト（GitHub Actions用）
- K列以降は使用しない（予約時刻記録なし）
- 1〜19本目：その他リンク、20本目：宣伝URL
- 20本目完了時のみ「処理済み」
- WordPress直接投稿（即時または予約）
"""

import os
import re
import io
import json
import time
import random
import logging
import argparse
import requests
import gspread
from typing import Dict, List, Optional
from urllib.parse import urlparse
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from requests.auth import HTTPBasicAuth
from PIL import Image, ImageDraw, ImageFont
import tempfile

# ----------------------------
# ログ設定
# ----------------------------
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/post_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
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
    GEMINI_KEYS = ['AIzaSyBCxQruA6WrmfZHoZ6pTBPRVqkALKvdsT0']  # デフォルト

_gemini_idx = 0

# 投稿間隔（スパム回避）
MIN_INTERVAL = int(os.environ.get('POST_MIN_INTERVAL', '60'))
MAX_INTERVAL = int(os.environ.get('POST_MAX_INTERVAL', '120'))

# ----------------------------
# プロジェクト設定
# ----------------------------
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
        'url': os.environ.get('WP_YKIKAKU_URL', 'https://ykikaku.v2006.coreserver.jp/'),
        'user': os.environ.get('WP_YKIKAKU_USER', 'ykikaku'),
        'password': os.environ.get('WP_YKIKAKU_PASSWORD', 'QnV8 5VlW RwZN YV4P zAcl Gfce'),
    },
    'efdlqjtz': {
        'url': os.environ.get('WP_EFDLQJTZ_URL', 'https://www.efdlqjtz.com/'),
        'user': os.environ.get('WP_EFDLQJTZ_USER', 'efdlqjtz'),
        'password': os.environ.get('WP_EFDLQJTZ_PASSWORD', 'nJh6 Gqm6 qfPn T6Zu WQGV Aymx'),
    },
    'selectadvance': {
        'url': os.environ.get('WP_SELECTADVANCE_URL', 'https://selectadvance.v2006.coreserver.jp/'),
        'user': os.environ.get('WP_SELECTADVANCE_USER', 'selectadvance'),
        'password': os.environ.get('WP_SELECTADVANCE_PASSWORD', '6HUY g7oZ Gow8 LBCu yzL8 cR3S'),
    },
    'welkenraedt': {
        'url': os.environ.get('WP_WELKENRAEDT_URL', 'https://www.welkenraedt-online.com/'),
        'user': os.environ.get('WP_WELKENRAEDT_USER', 'welkenraedtonline'),
        'password': os.environ.get('WP_WELKENRAEDT_PASSWORD', 'yzn4 6nlm vtrh 8N4v oxHl KUvf'),
    },
    'ncepqvub': {
        'url': os.environ.get('WP_NCEPQVUB_URL', 'https://www.ncepqvub.com/'),
        'user': os.environ.get('WP_NCEPQVUB_USER', 'ncepqvub'),
        'password': os.environ.get('WP_NCEPQVUB_PASSWORD', 'ZNdJ IGoK Wdj3 mNz4 Xevp KGFj'),
    },
    'kosagi': {
        'url': os.environ.get('WP_KOSAGI_URL', 'https://www.kosagi.biz/'),
        'user': os.environ.get('WP_KOSAGI_USER', 'kosagi'),
        'password': os.environ.get('WP_KOSAGI_PASSWORD', 'VsGS VU5J cKx8 HM6p oLEb VdNH'),
    },
    'selectad': {
        'url': os.environ.get('WP_SELECTAD_URL', 'https://selectad.v2006.coreserver.jp/'),
        'user': os.environ.get('WP_SELECTAD_USER', 'selectad'),
        'password': os.environ.get('WP_SELECTAD_PASSWORD', 'xVA8 6yxD TdkP CJE4 yoQN qAHn'),
    },
    'thrones': {
        'url': os.environ.get('WP_THRONES_URL', 'https://www.thrones.jp/'),
        'user': os.environ.get('WP_THRONES_USER', 'thrones'),
        'password': os.environ.get('WP_THRONES_PASSWORD', 'Fz9k fB3y wJuN tL8m zPqX vR4s'),
    }
}

# ----------------------------
# Google Sheets認証
# ----------------------------
def get_sheets_client():
    """GCP認証"""
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    if GOOGLE_APPLICATION_CREDENTIALS_JSON:
        # 環境変数からJSON認証
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
    else:
        # ローカルファイル認証
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        return gspread.authorize(creds)

# ----------------------------
# アイキャッチ画像生成
# ----------------------------
def create_eyecatch_image(title: str, site_key: str) -> bytes:
    """タイトルからアイキャッチ画像を自動生成"""
    
    width, height = 600, 400
    
    # カラーパレット
    color_schemes = [
        {'bg': '#2E7D32', 'accent': '#66BB6A', 'text': '#FFFFFF'},
        {'bg': '#388E3C', 'accent': '#81C784', 'text': '#FFFFFF'},
        {'bg': '#4CAF50', 'accent': '#8BC34A', 'text': '#FFFFFF'},
        {'bg': '#689F38', 'accent': '#AED581', 'text': '#FFFFFF'},
        {'bg': '#7CB342', 'accent': '#C5E1A5', 'text': '#2E7D32'},
    ]
    
    scheme = random.choice(color_schemes)
    
    # 画像作成
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
    
    # フォント設定
    try:
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
    except:
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
    
    # タイトルを描画（改行対応）
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

def upload_image_to_wordpress(image_data: bytes, filename: str, site_config: dict) -> Optional[int]:
    """画像をWordPressにアップロードしてIDを返す"""
    
    media_endpoint = f'{site_config["url"]}wp-json/wp/v2/media'
    
    # ファイル名をASCII文字のみに
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
            timeout=30
        )
        
        if response.status_code == 201:
            media_id = response.json()['id']
            logger.info(f"✅ アイキャッチ画像アップロード成功: {safe_filename} (ID: {media_id})")
            return media_id
        else:
            logger.error(f"❌ 画像アップロードエラー: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"❌ 画像アップロードエラー: {e}")
        return None

# ----------------------------
# 競合他社・その他リンク管理
# ----------------------------
def get_competitor_domains():
    """競合他社シートからドメインリストを取得"""
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
    """その他リンク先候補を取得"""
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
            logger.warning("⚠️ スプレッドシートから読み込めないため、デフォルトリストを使用")
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
    """競合を除外してその他リンクを選択"""
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
    """Gemini APIキーをローテーション取得"""
    global _gemini_idx
    if not GEMINI_KEYS:
        return None
    key = GEMINI_KEYS[_gemini_idx % len(GEMINI_KEYS)]
    _gemini_idx += 1
    return key

def call_gemini(prompt: str) -> str:
    """Gemini APIを呼び出し"""
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
    """記事を生成"""
    
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
        logger.error(f"❌ 記事生成エラー: {e}")
        raise

# ----------------------------
# WordPress投稿
# ----------------------------
def get_category_id(site_config, category_name):
    """カテゴリー名からIDを取得"""
    if not category_name:
        return None
    
    try:
        endpoint = f"{site_config['url']}wp-json/wp/v2/categories"
        response = requests.get(endpoint, timeout=30)
        
        if response.status_code == 200:
            categories = response.json()
            for cat in categories:
                if cat['name'] == category_name:
                    return cat['id']
        return None
    except:
        return None

def generate_slug_from_title(title):
    """タイトルから英数字のスラッグを生成"""
    
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
    """宣伝URLベースでスラッグを推測"""
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
    """WordPressに投稿（アイキャッチ・予約投稿対応）"""
    
    if site_key not in WP_CONFIGS:
        logger.error(f"❌ 不明なサイト: {site_key}")
        return ""
    
    site_config = WP_CONFIGS[site_key]
    
    if not site_config['user']:
        logger.warning(f"⚠️ {site_key}の認証情報が設定されていません")
        return ""
    
    # アイキャッチ画像を生成・アップロード
    featured_media_id = None
    if create_eyecatch:
        try:
            logger.info("🖼️ アイキャッチ画像を生成中...")
            image_data = create_eyecatch_image(article_data['title'], site_key)
            
            if permalink and permalink.strip():
                image_filename = f"{permalink}.jpg"
            else:
                image_filename = f"{generate_slug_from_title(article_data['title'])}.jpg"
            
            featured_media_id = upload_image_to_wordpress(image_data, image_filename, site_config)
            
            if featured_media_id:
                logger.info(f"✅ アイキャッチ画像設定完了")
            else:
                logger.warning("⚠️ アイキャッチ画像の設定をスキップして記事投稿を続行")
                
        except Exception as e:
            logger.warning(f"⚠️ アイキャッチ画像生成エラー: {e}")
            logger.info("アイキャッチなしで記事投稿を続行")
    
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
        logger.info(f"⏰ 予約投稿設定: {schedule_dt}")
    else:
        post_data['status'] = 'publish'
    
    try:
        response = requests.post(
            endpoint,
            auth=HTTPBasicAuth(site_config['user'], site_config['password']),
            headers={'Content-Type': 'application/json'},
            data=json.dumps(post_data),
            timeout=60
        )
        
        if response.status_code in (201, 200):
            post_url = response.json().get('link', '')
            logger.info(f"✅ WordPress投稿成功 ({site_key}): {post_url}")
            return post_url
        else:
            logger.error(f"❌ WordPress投稿失敗 ({site_key}): {response.status_code}")
            logger.error(f"エラー詳細: {response.text[:500]}...")
            return ""
            
    except Exception as e:
        logger.error(f"❌ WordPress投稿エラー ({site_key}): {e}")
        return ""

# ----------------------------
# スプレッドシート操作
# ----------------------------
def get_value_safe(row: List[str], idx: int) -> str:
    """配列から安全に値を取得"""
    return row[idx].strip() if len(row) > idx and row[idx] else ""

def to_int_safe(s: str, default: int = 0) -> int:
    """文字列を安全に整数に変換"""
    try:
        return int(s)
    except Exception:
        return default

def find_row_by_promo_url(sheet, promo_url: str) -> Optional[int]:
    """宣伝URLで行番号を検索"""
    try:
        all_rows = sheet.get_all_values()
        
        for i, row in enumerate(all_rows[1:], start=2):
            if len(row) > 1 and row[1] == promo_url:
                return i
        
        return None
    except Exception as e:
        logger.error(f"❌ 行検索エラー: {e}")
        return None

def update_sheet_cell(sheet, row_num: int, col_num: int, value: str):
    """スプレッドシートのセルを更新"""
    try:
        sheet.update_cell(row_num, col_num, value)
        logger.info(f"✅ セル更新成功: 行{row_num} 列{col_num} = {value}")
    except Exception as e:
        logger.error(f"❌ セル更新エラー: {e}")

# ----------------------------
# メイン投稿処理
# ----------------------------
def process_project_posts(project_key: str, target_count: int = 1):
    """プロジェクトの未処理行を処理"""
    
    if project_key not in PROJECT_CONFIGS:
        logger.error(f"❌ 未知のプロジェクト: {project_key}")
        return
    
    config = PROJECT_CONFIGS[project_key]
    client = get_sheets_client()
    
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(config['worksheet'])
        all_rows = sheet.get_all_values()
        
        if len(all_rows) <= 1:
            logger.warning(f"⚠️ {config['worksheet']} にデータがありません")
            return
        
        headers = all_rows[0]
        data_rows = all_rows[1:]
        
        # 競合・その他リンク取得
        competitor_domains = get_competitor_domains()
        other_links = get_other_links()
        
        posts_completed = 0
        
        for row_idx, row in enumerate(data_rows):
            if posts_completed >= target_count:
                break
            
            # 基本チェック
            if len(row) < 5 or not row[1].strip():
                continue
            
            status = get_value_safe(row, 4).lower()
            if status == '処理済み':
                continue
            
            # カウンター取得
            counter = to_int_safe(get_value_safe(row, 6), 0)
            max_posts = config['max_posts']
            
            if counter >= max_posts:
                continue
            
            row_num = row_idx + 2
            
            try:
                logger.info(f"🚀 処理開始: 行{row_num} (カウンター: {counter})")
                
                # 記事内容決定
                if counter == max_posts - 1:
                    # 20記事目：宣伝URL
                    logger.info(f"📊 {max_posts}記事目 → 宣伝URL使用")
                    url = get_value_safe(row, 1)
                    anchor = get_value_safe(row, 3) or project_key
                    category_name = get_value_safe(row, 7) or 'お金のマメ知識'
                    permalink = get_value_safe(row, 8)
                    
                    if not permalink:
                        permalink = infer_slug_from_promo(url, get_value_safe(row, 0))
                        # パーマリンク記録（I列=9列目）
                        update_sheet_cell(sheet, row_num, 9, permalink)
                else:
                    # 1-19記事目：その他リンク
                    logger.info(f"📊 {counter + 1}記事目 → その他リンク使用")
                    chosen_link = choose_other_link(other_links, competitor_domains)
                    if not chosen_link:
                        logger.error("❌ その他リンクが取得できません")
                        continue
                    
                    url = chosen_link['url']
                    anchor = chosen_link['anchor']
                    category_name = 'お金のマメ知識'
                    permalink = None
                
                # 記事生成
                logger.info("🧠 記事を生成中...")
                theme = get_value_safe(row, 0) or '金融・投資・資産運用'
                article = generate_article_with_link(theme, url, anchor)
                
                logger.info(f"📝 タイトル: {article['title']}")
                logger.info(f"🔗 使用リンク: {anchor}")
                
                # 投稿先決定
                post_target = get_value_safe(row, 2) or config['sites'][0]
                posted_urls = []
                
                # 投稿実行
                for site_key in config['sites']:
                    if post_target in [site_key, '両方']:
                        logger.info(f"📤 {site_key}に投稿中...")
                        post_url = post_to_wordpress(
                            article, site_key, category_name, permalink,
                            create_eyecatch=True
                        )
                        if post_url:
                            posted_urls.append(post_url)
                
                if not posted_urls:
                    logger.error("❌ 投稿に失敗しました")
                    update_sheet_cell(sheet, row_num, 5, "エラー")
                    continue
                
                # スプレッドシート更新
                new_counter = counter + 1
                update_sheet_cell(sheet, row_num, 7, str(new_counter))  # カウンター（G列）
                
                if new_counter >= max_posts:
                    # 20記事目完了
                    update_sheet_cell(sheet, row_num, 5, "処理済み")  # ステータス（E列）
                    update_sheet_cell(sheet, row_num, 6, ', '.join(posted_urls))  # 投稿URL（F列）
                    completion_time = datetime.now().strftime("%Y/%m/%d %H:%M")
                    update_sheet_cell(sheet, row_num, 10, completion_time)  # 完了日時（J列）
                    logger.info(f"✅ {max_posts}記事完了！")
                else:
                    logger.info(f"📊 カウンター更新: {new_counter}")
                
                posts_completed += 1
                
                # 間隔調整
                if posts_completed < target_count:
                    wait_time = random.randint(MIN_INTERVAL, MAX_INTERVAL)
                    logger.info(f"⏳ {wait_time}秒待機中...")
                    time.sleep(wait_time)
                
            except Exception as e:
                logger.error(f"❌ 行{row_num}の処理エラー: {e}")
                update_sheet_cell(sheet, row_num, 5, "エラー")
                continue
        
        logger.info(f"✅ 投稿完了: {posts_completed}記事")
        
    except Exception as e:
        logger.error(f"❌ プロジェクト処理エラー: {e}")

# ----------------------------
# CLI
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description='ブログ自動投稿スクリプト')
    parser.add_argument('--project', default='all', help='プロジェクト名（all/kaitori_life/osaifu_rescue/kure_kaeru/red_site）')
    parser.add_argument('--count', type=int, default=1, help='投稿数')
    parser.add_argument('--test', action='store_true', help='テストモード（実際の投稿は行わない）')
    
    args = parser.parse_args()
    
    if args.test:
        logger.info("🧪 テストモード - 実際の投稿は行いません")
        return
    
    logger.info(f"🚀 投稿開始: プロジェクト={args.project}, 投稿数={args.count}")
    
    if args.project == 'all':
        for project_key in PROJECT_CONFIGS.keys():
            logger.info(f"📋 {project_key} 処理開始")
            process_project_posts(project_key, args.count)
            
            # プロジェクト間の間隔
            if project_key != list(PROJECT_CONFIGS.keys())[-1]:
                wait_time = random.randint(30, 60)
                logger.info(f"⏳ 次のプロジェクトまで {wait_time}秒待機...")
                time.sleep(wait_time)
    else:
        if args.project in PROJECT_CONFIGS:
            process_project_posts(args.project, args.count)
        else:
            logger.error(f"❌ 不明なプロジェクト: {args.project}")
            logger.info(f"利用可能なプロジェクト: {', '.join(PROJECT_CONFIGS.keys())}")
    
    logger.info("🎉 全処理完了")

if __name__ == '__main__':
    main()
