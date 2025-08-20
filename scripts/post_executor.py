#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
K列予約投稿実行スクリプト（GitHub Actions用・非WordPress）
- K列以降の予約時刻をチェックして該当時刻の投稿を実行
- 対象プラットフォーム：Seesaa / FC2 / livedoor / Blogger
- 予約ウィンドウ: --window 分（デフォルト30分）
- プロジェクト選択: --project biggift | arigataya
- テスト: --test（接続検証のみ、投稿しない）
"""

import os
import re
import json
import time
import random
import logging
import requests
import gspread
import xmlrpc.client
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from requests.auth import HTTPBasicAuth
from xml.sax.saxutils import escape as xml_escape
import tempfile
import argparse
import traceback

# Google / Blogger
try:
    from google.oauth2.credentials import Credentials as GoogleCredentials
    from google.auth.transport.requests import Request as GoogleAuthRequest
    from googleapiclient.discovery import build as gapi_build
except Exception:
    GoogleCredentials = None
    GoogleAuthRequest = None
    gapi_build = None

# ----------------------------
# ログ設定
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ----------------------------
# 環境変数
# ----------------------------
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1sV0r6LavB4BgU7jGaa5C-GdyogUpWr_y42a-tNZXuFo")
GOOGLE_APPLICATION_CREDENTIALS_JSON = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")

# Gemini API設定（キーのローテーションに対応）
GEMINI_KEYS = [k for k in [
    os.environ.get("GEMINI_API_KEY_1"),
    os.environ.get("GEMINI_API_KEY_2"),
    os.environ.get("GEMINI_API_KEY_3"),
] if k]
if not GEMINI_KEYS:
    GEMINI_KEYS = ["AIzaSyBCxQruA6WrmfZHoZ6pTBPRVqkALKvdsT0"]
_gemini_idx = 0

# 投稿間隔（スパム回避）
MIN_INTERVAL = int(os.environ.get("POST_MIN_INTERVAL", "60"))
MAX_INTERVAL = int(os.environ.get("POST_MAX_INTERVAL", "120"))

# ----------------------------
# プロジェクト設定（非WordPressのみ）
# ----------------------------
NON_WP_PROJECTS = {
    "biggift": {
        "worksheet": "ビックギフト向け",
        "platforms": ["blogger", "livedoor"],  # デフォルトの対象
        "max_posts": {"blogger": 20, "livedoor": 15}
    },
    "arigataya": {
        "worksheet": "ありがた屋向け",
        "platforms": ["seesaa", "fc2"],
        "max_posts": 20
    }
}

# プラットフォーム設定（必要に応じてリポジトリシークレットから上書きを推奨）
PLATFORM_CONFIGS = {
    "seesaa": {
        "endpoint": os.environ.get("SEESAA_ENDPOINT", "http://blog.seesaa.jp/rpc"),
        "username": os.environ.get("SEESAA_USERNAME", "kyuuyo.fac@gmail.com"),
        "password": os.environ.get("SEESAA_PASSWORD", "st13131094pao"),
        "blogid":   os.environ.get("SEESAA_BLOGID", "7228801"),
        "timeout":  45,
    },
    "fc2": {
        "endpoint": "https://blog.fc2.com/xmlrpc.php",
        "blog_id":  os.environ.get("FC2_BLOG_ID", "genkinka1313"),
        "username": os.environ.get("FC2_USERNAME", "esciresearch.com@gmail.com"),
        "password": os.environ.get("FC2_PASSWORD", "st13131094pao"),
        "timeout":  45,
    },
    "livedoor": {
        "root":     "https://livedoor.blogcms.jp/atompub",
        "blog_name":os.environ.get("LIVEDOOR_BLOG_NAME", "radiochildcare"),
        "user_id":  os.environ.get("LIVEDOOR_ID", "radiochildcare"),
        "api_key":  os.environ.get("LIVEDOOR_API_KEY", "5WF0Akclk2"),
        "timeout":  45,
    },
    "blogger": {
        "blog_id":        os.environ.get("BLOGGER_BLOG_ID", ""),  # 必須
        "client_id":      os.environ.get("BLOGGER_CLIENT_ID", ""),
        "client_secret":  os.environ.get("BLOGGER_CLIENT_SECRET", ""),
        "refresh_token":  os.environ.get("BLOGGER_REFRESH_TOKEN", ""),
        "timeout":        45,
    },
}

# ----------------------------
# Google Sheets認証
# ----------------------------
def get_sheets_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    if not GOOGLE_APPLICATION_CREDENTIALS_JSON:
        logger.error("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON が未設定")
        raise RuntimeError("Google認証情報が設定されていません")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write(GOOGLE_APPLICATION_CREDENTIALS_JSON)
        temp_path = f.name

    creds = ServiceAccountCredentials.from_json_keyfile_name(temp_path, scope)
    client = gspread.authorize(creds)

    try:
        os.unlink(temp_path)
    except Exception:
        pass

    return client

# ----------------------------
# 競合他社・その他リンク管理
# ----------------------------
def get_competitor_domains() -> List[str]:
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet("競合他社")
        competitors = sheet.get_all_values()[1:]

        domains = []
        for row in competitors:
            if row and row[0]:
                domain = row[0].strip()
                if domain.startswith("http"):
                    parsed = urlparse(domain)
                    domain = parsed.netloc
                domains.append(domain.lower())

        logger.info(f"📋 競合ドメイン {len(domains)}件")
        return domains
    except Exception as e:
        logger.warning(f"⚠️ 競合他社リスト取得エラー: {e}")
        return []

def get_other_links() -> List[Dict]:
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet("その他リンク先")
        rows = sheet.get_all_values()[1:]

        other_sites = []
        for row in rows:
            if len(row) >= 2 and row[0] and row[1]:
                other_sites.append({"url": row[0].strip(), "anchor": row[1].strip()})

        logger.info(f"🔗 その他リンク {len(other_sites)}件")
        if not other_sites:
            other_sites = [
                {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
                {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"},
            ]
        return other_sites

    except Exception as e:
        logger.error(f"❌ その他リンク取得エラー: {e}")
        return [
            {"url": "https://www.fsa.go.jp/", "anchor": "金融庁"},
            {"url": "https://www.boj.or.jp/", "anchor": "日本銀行"},
        ]

def choose_other_link(other_links: List[Dict], competitor_domains: List[str]) -> Optional[Dict]:
    pool = []
    for site in other_links:
        site_domain = urlparse(site["url"]).netloc.lower()
        if not any(comp in site_domain for comp in competitor_domains):
            pool.append(site)
    if pool:
        return random.choice(pool)
    return None

# ----------------------------
# Gemini記事生成
# ----------------------------
def _get_gemini_key() -> Optional[str]:
    global _gemini_idx
    if not GEMINI_KEYS:
        return None
    key = GEMINI_KEYS[_gemini_idx % len(GEMINI_KEYS)]
    _gemini_idx += 1
    return key

def call_gemini(prompt: str) -> str:
    api_key = _get_gemini_key()
    if not api_key:
        raise RuntimeError("Gemini APIキーが設定されていません")

    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent?key={api_key}"
    payload = {"contents": [{"parts": [{"text": prompt}]}]], "generationConfig": {"temperature": 0.7}}
    r = requests.post(endpoint, json=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Gemini API エラー: {r.status_code} {r.text[:200]}")
    data = r.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]

def generate_article_with_link(theme: str, url: str, anchor_text: str) -> Dict:
    if not theme or theme.strip() == "":
        theme = "金融・投資・資産運用"
        auto_theme = True
    else:
        auto_theme = False

    theme_instruction = (
        "金融系（投資、クレジットカード、ローン、資産運用など）から自由にテーマを選んで"
        if auto_theme else f"「{theme}」をテーマに"
    )
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
・段落: <p>タグで囲む（空行は任意）
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
    text = call_gemini(prompt).strip()
    lines = text.splitlines()
    title = lines[0].strip() if lines else "タイトル"
    content = "\n".join(lines[1:]).strip() if len(lines) > 1 else ""
    # cleanup
    content = re.sub(r"〇〇|××|△△", "", content)
    content = re.sub(r"（ここで.*?）", "", content)
    content = re.sub(r"<p>\s*</p>", "", content)
    return {"title": title, "content": content, "theme": (theme if not auto_theme else "金融")}

# ----------------------------
# HTMLユーティリティ
# ----------------------------
def enforce_anchor_attrs(html: str) -> str:
    """<a> に target/_blank + rel を強制付与"""
    def add_attrs(m):
        tag = m.group(0)
        if re.search(r"\btarget\s*=", tag, flags=re.I) is None:
            tag = tag.replace("<a ", '<a target="_blank" ', 1)
        rel_m = re.search(r'\brel\s*=\s*"([^"]*)"', tag, flags=re.I)
        if rel_m:
            rel_val = rel_m.group(1)
            need = []
            for t in ("noopener", "noreferrer"):
                if t not in rel_val.split():
                    need.append(t)
            if need:
                new_rel = rel_val + " " + " ".join(need)
                tag = tag[:rel_m.start(1)] + new_rel + tag[rel_m.end(1):]
        else:
            tag = tag.replace("<a ", '<a rel="noopener noreferrer" ', 1)
        return tag
    return re.sub(r"<a\s+[^>]*>", add_attrs, html, flags=re.I)

# ----------------------------
# 各プラットフォーム投稿
# ----------------------------
def post_to_seesaa(article: dict, category_name: str = None) -> str:
    """Seesaa: XML-RPC MetaWeblog。将来日時はAPI制約により下書き化不可のため即時公開のみ"""
    cfg = PLATFORM_CONFIGS["seesaa"]
    server = xmlrpc.client.ServerProxy(cfg["endpoint"], allow_none=True)
    safe_html = enforce_anchor_attrs(article["content"])
    content = {"title": article["title"], "description": safe_html}
    try:
        post_id = server.metaWeblog.newPost(
            cfg["blogid"], cfg["username"], cfg["password"], content, True
        )
        # カテゴリ設定（可能な範囲で）
        if category_name:
            try:
                cats = server.mt.getCategoryList(cfg["blogid"], cfg["username"], cfg["password"])
                for c in cats:
                    if c.get("categoryName") == category_name:
                        server.mt.setPostCategories(
                            post_id, cfg["username"], cfg["password"],
                            [{"categoryId": c.get("categoryId"), "isPrimary": True}]
                        )
                        break
            except Exception:
                pass
        # パーマリンク取得
        try:
            post = server.metaWeblog.getPost(post_id, cfg["username"], cfg["password"])
            return post.get("permalink") or post.get("link") or f"seesaa://{post_id}"
        except Exception:
            return f"seesaa://{post_id}"
    except Exception as e:
        logger.error(f"❌ Seesaa投稿エラー: {e}")
        return ""

def post_to_fc2(article: dict, category_name: str = None) -> str:
    """FC2: XML-RPC MetaWeblog"""
    cfg = PLATFORM_CONFIGS["fc2"]
    server = xmlrpc.client.ServerProxy(cfg["endpoint"])
    safe_html = enforce_anchor_attrs(article["content"])
    content = {"title": article["title"], "description": safe_html}
    try:
        post_id = server.metaWeblog.newPost(
            cfg["blog_id"], cfg["username"], cfg["password"], content, True
        )
        # カテゴリ設定
        if category_name:
            try:
                cats = server.mt.getCategoryList(cfg["blog_id"], cfg["username"], cfg["password"])
                for c in cats:
                    if c.get("categoryName") == category_name:
                        server.mt.setPostCategories(post_id, cfg["username"], cfg["password"], [c])
                        break
            except Exception:
                pass
        # 代表URL（実URLはテンプレ依存のため目安）
        return f"https://{cfg['blog_id']}.blog.fc2.com/blog-entry-{post_id}.html"
    except Exception as e:
        logger.error(f"❌ FC2投稿エラー: {e}")
        return ""

def post_to_livedoor(article: dict, category_name: str = None) -> str:
    """livedoor: AtomPub"""
    cfg = PLATFORM_CONFIGS["livedoor"]
    endpoint = f"{cfg['root']}/{cfg['blog_name']}/article"
    title_xml = xml_escape(article["title"])
    safe_html = enforce_anchor_attrs(article["content"])
    content_xml = xml_escape(safe_html)
    cat_xml = f'<category term="{xml_escape(category_name)}"/>' if category_name else ""
    entry_xml = f"""<?xml version="1.0" encoding="utf-8"?>
<entry xmlns="http://www.w3.org/2005/Atom">
  <title>{title_xml}</title>
  <content type="html">{content_xml}</content>
  {cat_xml}
</entry>""".encode("utf-8")
    try:
        r = requests.post(
            endpoint,
            data=entry_xml,
            headers={"Content-Type": "application/atom+xml;type=entry"},
            auth=HTTPBasicAuth(cfg["user_id"], cfg["api_key"]),
            timeout=cfg["timeout"],
        )
        if r.status_code in (200, 201):
            try:
                root_xml = ET.fromstring(r.text)
                ns = {"atom": "http://www.w3.org/2005/Atom"}
                alt = root_xml.find(".//atom:link[@rel='alternate']", ns)
                return alt.get("href") if alt is not None else "livedoor://ok"
            except Exception:
                return "livedoor://ok"
        else:
            logger.error(f"❌ livedoor投稿失敗: {r.status_code} {r.text[:180]}")
            return ""
    except Exception as e:
        logger.error(f"❌ livedoor投稿エラー: {e}")
        return ""

# ---------- Blogger ----------
def get_blogger_service():
    """Blogger API v3（OAuth2 リフレッシュトークン）"""
    if not (GoogleCredentials and gapi_build and GoogleAuthRequest):
        raise RuntimeError("google-api-python-client が未インストールです。")
    cfg = PLATFORM_CONFIGS["blogger"]
    for k in ("client_id", "client_secret", "refresh_token"):
        if not cfg.get(k):
            raise RuntimeError("Blogger の OAuth2 環境変数（CLIENT_ID/CLIENT_SECRET/REFRESH_TOKEN）が未設定です。")
    creds = GoogleCredentials(
        token=None,
        refresh_token=cfg["refresh_token"],
        client_id=cfg["client_id"],
        client_secret=cfg["client_secret"],
        token_uri="https://oauth2.googleapis.com/token",
        scopes=["https://www.googleapis.com/auth/blogger"]
    )
    creds.refresh(GoogleAuthRequest())
    return gapi_build("blogger", "v3", credentials=creds, cache_discovery=False)

def post_to_blogger(article: dict, when: Optional[datetime] = None) -> str:
    """Blogger: 予約可。when が未来なら isDraft=True + published 指定"""
    cfg = PLATFORM_CONFIGS["blogger"]
    blog_id = cfg.get("blog_id") or ""
    if not blog_id:
        logger.error("❌ BLOGGER_BLOG_ID が未設定です")
        return ""
    try:
        svc = get_blogger_service()
        body = {
            "kind": "blogger#post",
            "title": article["title"],
            "content": enforce_anchor_attrs(article["content"]),
        }
        is_draft = bool(when and when > datetime.now())
        if is_draft:
            # Bloggerは published に未来時刻を入れつつ isDraft=True で予約（実際の動作はテンプレ/設定に依存）
            body["published"] = when.isoformat()
        post = svc.posts().insert(blogId=blog_id, body=body, isDraft=is_draft).execute()
        url = post.get("url", "")
        logger.info(f"✅ Blogger投稿成功: {url or '(draft)'}")
        return url or "blogger://draft"
    except Exception as e:
        logger.error(f"❌ Blogger投稿失敗: {e}\n{traceback.format_exc()}")
        return ""

# ----------------------------
# ユーティリティ
# ----------------------------
def get_value_safe(row: List[str], idx: int) -> str:
    return row[idx].strip() if len(row) > idx and row[idx] else ""

def to_int_safe(s: str, default: int = 0) -> int:
    try:
        return int(s)
    except Exception:
        return default

def get_max_posts_for_project(project_key: str, post_target: str = "") -> int:
    cfg = NON_WP_PROJECTS[project_key]
    max_posts = cfg["max_posts"]
    if isinstance(max_posts, dict):
        if post_target.lower() == "livedoor":
            return max_posts.get("livedoor", 15)
        if post_target.lower() == "blogger":
            return max_posts.get("blogger", 20)
        return max(max_posts.values()) if max_posts else 20
    return int(max_posts)

def parse_time_cell(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    # 1) YYYY/MM/DD HH:MM
    for fmt in ("%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    # 2) ISO 8601
    try:
        return datetime.fromisoformat(s)
    except Exception:
        pass
    # 3) HH:MM（当日/過ぎていれば翌日）
    m = re.match(r"^(\d{1,2}):(\d{2})$", s)
    if m:
        h, M = int(m.group(1)), int(m.group(2))
        now = datetime.now()
        d = now.replace(hour=h, minute=M, second=0, microsecond=0)
        return d if d >= now else d + timedelta(days=1)
    return None

# ----------------------------
# 投稿実行コア
# ----------------------------
def execute_scheduled_post(
    row: List[str],
    project_key: str,
    sheet,
    row_idx: int,
    col_idx: int,
    other_links: List[Dict],
    competitor_domains: List[str]
) -> bool:
    """
    単一の予約投稿を実行（完全ログ記録対応）
    列仕様（想定）:
      A:テーマ(0) / B:宣伝URL(1) / C:投稿先(2) / D:アンカー(3) / E:ステータス(4) /
      F:最終URL(6) / G:カウンタ(6) / H:(7)カテゴリ / I:(8)完了時刻 / K以降=予約
    """
    try:
        cfg = NON_WP_PROJECTS[project_key]

        # 現在カウンタ（G列=7）
        current_counter = to_int_safe(get_value_safe(row, 6), 0)

        # 投稿先（C列=3）未指定はデフォルト先頭
        post_target = (get_value_safe(row, 2) or (cfg["platforms"][0] if cfg["platforms"] else "")).lower()
        max_posts = get_max_posts_for_project(project_key, post_target)

        if current_counter >= max_posts:
            logger.info(f"⚠️ 既に {max_posts} 記事完了済み - スキップ")
            return False

        # 記事内容（最終記事は宣伝URL、それ以前はその他リンク）
        if current_counter == max_posts - 1:
            logger.info(f"📊 {max_posts}本目 → 宣伝URL使用")
            url = get_value_safe(row, 1)
            anchor = get_value_safe(row, 3) or project_key
            category = get_value_safe(row, 7) if len(row) >= 8 else None
        else:
            logger.info(f"📊 {current_counter + 1}本目 → その他リンク使用")
            chosen_link = choose_other_link(other_links, competitor_domains)
            if not chosen_link:
                logger.error("❌ その他リンクが空です")
                return False
            url = chosen_link["url"]
            anchor = chosen_link["anchor"]
            category = "お金のマメ知識"

        # 記事生成
        logger.info("🧠 記事生成中...")
        theme = get_value_safe(row, 0) or "金融・投資・資産運用"
        article = generate_article_with_link(theme, url, anchor)
        logger.info(f"📝 タイトル: {article['title']} / 🔗 {anchor}")

        posted_urls: List[str] = []

        # プラットフォーム別投稿
        if "seesaa" in cfg["platforms"] and (not post_target or post_target == "seesaa"):
            logger.info("📤 Seesaa 投稿...")
            u = post_to_seesaa(article, category)
            if u:
                posted_urls.append(u)
                logger.info(f"✅ Seesaa 成功: {u}")

        if "fc2" in cfg["platforms"] and (not post_target or post_target == "fc2"):
            logger.info("📤 FC2 投稿...")
            u = post_to_fc2(article, category)
            if u:
                posted_urls.append(u)
                logger.info(f"✅ FC2 成功: {u}")

        if "livedoor" in cfg["platforms"] and (not post_target or post_target == "livedoor"):
            logger.info("📤 livedoor 投稿...")
            u = post_to_livedoor(article, category)
            if u:
                posted_urls.append(u)
                logger.info(f"✅ livedoor 成功: {u}")

        if "blogger" in cfg["platforms"] and (not post_target or post_target == "blogger"):
            logger.info("📤 Blogger 投稿...")
            # K列時刻＝予約（未来）の場合は Blogger のみ予約対応
            u = post_to_blogger(article, when=None)
            if u:
                posted_urls.append(u)
                logger.info(f"✅ Blogger 成功: {u}")

        if not posted_urls:
            logger.error("❌ 全プラットフォーム投稿失敗")
            return False

        # 反映・記録
        new_counter = current_counter + 1
        timestamp = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        for u in posted_urls:
            logger.info(f"📋 記録: #{new_counter} {article['title']} → {u} @ {timestamp}")

        # G列=7: カウンタ更新
        sheet.update_cell(row_idx, 7, str(new_counter))
        # K列 該当セルを「完了」に
        sheet.update_cell(row_idx, col_idx, "完了")

        # 最終記事完了時の処理
        if new_counter >= max_posts:
            # E列=5: ステータス
            sheet.update_cell(row_idx, 5, "処理済み")
            # F列=6: 最終記事URL
            sheet.update_cell(row_idx, 6, ", ".join(posted_urls))
            # I列=9: 完了日時
            sheet.update_cell(row_idx, 9, datetime.now().strftime("%Y/%m/%d %H:%M"))
            logger.info(f"🎯 {project_key} 行{row_idx} 完了（{max_posts}本達成）")
        else:
            logger.info(f"📊 カウンタ更新: {new_counter}")

        return True

    except Exception as e:
        logger.error(f"❌ 投稿実行エラー: {e}\n{traceback.format_exc()}")
        return False

# ----------------------------
# K列チェック
# ----------------------------
def check_and_execute_k_column_schedules(window_minutes: int = 30, target_projects: dict = None) -> Dict[str, int]:
    """
    K列予約投稿チェック＆実行（プロジェクトフィルター対応）
    """
    if target_projects is None:
        target_projects = NON_WP_PROJECTS

    logger.info("⏰ K列予約投稿チェック開始")
    client = get_sheets_client()
    now = datetime.now()
    window_end = now + timedelta(minutes=window_minutes)

    competitor_domains = get_competitor_domains()
    other_links = get_other_links()

    executed_total = 0
    skipped_total = 0

    for project_key, cfg in target_projects.items():
        try:
            logger.info(f"📋 {project_key} ({cfg['worksheet']}) チェック中...")
            sheet = client.open_by_key(SPREADSHEET_ID).worksheet(cfg["worksheet"])
            rows = sheet.get_all_values()

            if len(rows) <= 1:
                logger.info(f"⚠️ {cfg['worksheet']} にデータなし")
                continue

            for row_idx, row in enumerate(rows[1:], start=2):
                # 既に処理済みはスキップ
                if get_value_safe(row, 4) == "処理済み":
                    continue

                # K列(11)以降の予約セルをチェック
                for col_idx0 in range(10, len(row)):
                    val = (row[col_idx0] or "").strip()
                    if not val or val == "完了":
                        continue

                    scheduled_time = parse_time_cell(val)
                    if not scheduled_time:
                        continue

                    # 実行対象（今〜ウィンドウ内）
                    if now <= scheduled_time <= window_end:
                        logger.info(f"🚀 実行対象: {project_key} 行{row_idx} 列{col_idx0+1} {scheduled_time:%Y/%m/%d %H:%M}")
                        ok = execute_scheduled_post(
                            row=row,
                            project_key=project_key,
                            sheet=sheet,
                            row_idx=row_idx,
                            col_idx=col_idx0 + 1,  # gspreadは1始まり
                            other_links=other_links,
                            competitor_domains=competitor_domains
                        )
                        if ok:
                            executed_total += 1
                            # 連投防止（3投稿ごとに休憩）
                            if executed_total % 3 == 0:
                                wait = random.randint(MIN_INTERVAL, MAX_INTERVAL)
                                logger.info(f"⏳ 連投防止: {wait}秒待機")
                                time.sleep(wait)
                        else:
                            skipped_total += 1

        except Exception as e:
            logger.error(f"❌ {project_key} 処理エラー: {e}\n{traceback.format_exc()}")

    logger.info(f"⏰ K列予約チェック完了: 実行 {executed_total} / スキップ {skipped_total}")
    return {"executed": executed_total, "skipped": skipped_total}

# ----------------------------
# メイン
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="K列予約投稿実行スクリプト（GitHub Actions用）")
    parser.add_argument("--window", type=int, default=30, help="実行ウィンドウ（分）")
    parser.add_argument("--test", action="store_true", help="テストモード（投稿しない）")
    parser.add_argument("--project", type=str, help="特定プロジェクトのみ（biggift / arigataya）")

    args = parser.parse_args()

    # プロジェクトフィルター
    target_projects = NON_WP_PROJECTS
    if args.project:
        if args.project in NON_WP_PROJECTS:
            target_projects = {args.project: NON_WP_PROJECTS[args.project]}
            logger.info(f"🎯 特定プロジェクト実行: {args.project}")
        else:
            logger.error(f"❌ 不明なプロジェクト: {args.project}")
            exit(1)

    if args.test:
        logger.info("🧪 テストモード開始（投稿は行いません）")
        try:
            client = get_sheets_client()
            logger.info("✅ Google Sheets 接続OK")
            competitor_domains = get_competitor_domains()
            other_links = get_other_links()
            logger.info(f"✅ データ取得: 競合 {len(competitor_domains)} / その他 {len(other_links)}")
            for k in target_projects:
                logger.info(f"📋 対象プロジェクト: {k}")
            logger.info("🧪 テストモード完了")
        except Exception as e:
            logger.error(f"❌ テスト失敗: {e}")
            exit(1)
        return

    logger.info(f"🚀 K列予約投稿チェック開始: ウィンドウ={args.window}分")
    logger.info("📋 実行対象: " + (args.project if args.project else f"全({', '.join(NON_WP_PROJECTS.keys())})"))

    try:
        result = check_and_execute_k_column_schedules(window_minutes=args.window, target_projects=target_projects)
        logger.info(f"✅ 処理完了: {result}")
        # GitHub Actions 出力（旧 set-output は非推奨だが既存互換のため残す）
        print(f"::set-output name=executed::{result['executed']}")
        print(f"::set-output name=skipped::{result['skipped']}")
    except Exception as e:
        logger.error(f"❌ メイン処理エラー: {e}\n{traceback.format_exc()}")
        exit(1)

if __name__ == "__main__":
    main()
