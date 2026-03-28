"""Shared Web UI authentication helpers."""

import hashlib
import hmac
import secrets
from typing import Optional

from fastapi import HTTPException, Request

from ..config.settings import get_settings


def build_webui_auth_token(password: str) -> str:
    secret = get_settings().webui_secret_key.get_secret_value().encode("utf-8")
    return hmac.new(secret, password.encode("utf-8"), hashlib.sha256).hexdigest()


def build_ui_identity_signature(identity: str) -> str:
    secret = get_settings().webui_secret_key.get_secret_value().encode("utf-8")
    return hmac.new(secret, identity.encode("utf-8"), hashlib.sha256).hexdigest()


def build_session_signature(session_id: str) -> str:
    secret = get_settings().webui_secret_key.get_secret_value().encode("utf-8")
    return hmac.new(secret, session_id.encode("utf-8"), hashlib.sha256).hexdigest()


def build_ui_identity_cookie_value(identity: str) -> str:
    return f"{identity}.{build_ui_identity_signature(identity)}"


def build_session_cookie_value(session_id: str) -> str:
    return f"{session_id}.{build_session_signature(session_id)}"


def parse_ui_identity_cookie(cookie_value: Optional[str]) -> Optional[str]:
    if not cookie_value or "." not in cookie_value:
        return None
    identity, signature = cookie_value.rsplit(".", 1)
    expected = build_ui_identity_signature(identity)
    if not identity or not secrets.compare_digest(signature, expected):
        return None
    return identity


def parse_session_cookie(cookie_value: Optional[str]) -> Optional[str]:
    if not cookie_value or "." not in cookie_value:
        return None
    session_id, signature = cookie_value.rsplit(".", 1)
    expected = build_session_signature(session_id)
    if not session_id or not secrets.compare_digest(signature, expected):
        return None
    return session_id


def generate_ui_identity() -> str:
    return secrets.token_urlsafe(32)


def generate_session_id() -> str:
    return secrets.token_urlsafe(32)


def get_current_ui_identity(request: Request) -> Optional[str]:
    return parse_ui_identity_cookie(request.cookies.get("ui_identity"))


def get_current_session_id(request: Request) -> Optional[str]:
    return parse_session_cookie(request.cookies.get("session_id"))


def ensure_ui_identity(request: Request) -> tuple[str, bool]:
    identity = get_current_ui_identity(request)
    if identity:
        return identity, False
    return generate_ui_identity(), True


def ensure_session_id(request: Request) -> tuple[str, bool]:
    session_id = get_current_session_id(request)
    if session_id:
        return session_id, False
    return generate_session_id(), True


def is_webui_authenticated(request: Request) -> bool:
    cookie = request.cookies.get("webui_auth")
    expected = build_webui_auth_token(get_settings().webui_access_password.get_secret_value())
    return bool(cookie) and secrets.compare_digest(cookie, expected)


def require_webui_auth(request: Request) -> None:
    if not is_webui_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
