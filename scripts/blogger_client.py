# scripts/blogger_client.py
from datetime import datetime
from typing import Optional, Tuple, Dict, Any

try:
    import streamlit as st
    _HAS_ST = True
except Exception:
    _HAS_ST = False

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build


def _load_blogger_secrets() -> Dict[str, Any]:
    if _HAS_ST and "blogger" in st.secrets:
        s = st.secrets["blogger"]
        scopes = s.get("scopes") or ["https://www.googleapis.com/auth/blogger"]
        return {
            "client_id": s["client_id"],
            "client_secret": s["client_secret"],
            "refresh_token": s["refresh_token"],
            "token_uri": s.get("token_uri", "https://oauth2.googleapis.com/token"),
            "scopes": scopes,
            "blog_id": s.get("blog_id", ""),
        }
    raise RuntimeError("[blogger] secrets が見つかりません")


def get_blogger_service_and_blog_id(override_blog_id: Optional[str] = None):
    cfg = _load_blogger_secrets()
    if override_blog_id:
        cfg["blog_id"] = override_blog_id

    creds = Credentials(
        token=None,
        refresh_token=cfg["refresh_token"],
        token_uri=cfg["token_uri"],
        client_id=cfg["client_id"],
        client_secret=cfg["client_secret"],
        scopes=cfg["scopes"],
    )
    creds.refresh(Request())
    service = build("blogger", "v3", credentials=creds, cache_discovery=False)

    if not cfg.get("blog_id"):
        me = service.users().get(userId="self").execute()
        blogs = service.users().blogs().list(userId=me["id"]).execute()
        items = blogs.get("items", [])
        if not items:
            raise RuntimeError("Blogger: 利用可能なブログが見つかりません。blog_id を secrets に設定してください。")
        cfg["blog_id"] = items[0]["id"]

    return service, cfg["blog_id"]


def post_to_blogger(
    title: str,
    html_body: str,
    labels: Optional[list] = None,
    blog_id: Optional[str] = None,
    schedule_dt: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any]]:
    service, resolved_blog_id = get_blogger_service_and_blog_id(blog_id)
    body = {
        "kind": "blogger#post",
        "title": title,
        "content": html_body,
    }
    if labels:
        body["labels"] = labels

    is_draft = False
    if schedule_dt and schedule_dt > datetime.now():
        is_draft = True
        body["published"] = schedule_dt.isoformat()

    res = service.posts().insert(blogId=resolved_blog_id, body=body, isDraft=is_draft).execute()
    url = res.get("url") or res.get("selfLink", "")
    return url, res
