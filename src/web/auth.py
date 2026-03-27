"""Shared Web UI authentication helpers."""

import hashlib
import hmac
import secrets

from fastapi import HTTPException, Request

from ..config.settings import get_settings


def build_webui_auth_token(password: str) -> str:
    secret = get_settings().webui_secret_key.get_secret_value().encode("utf-8")
    return hmac.new(secret, password.encode("utf-8"), hashlib.sha256).hexdigest()


def is_webui_authenticated(request: Request) -> bool:
    cookie = request.cookies.get("webui_auth")
    expected = build_webui_auth_token(get_settings().webui_access_password.get_secret_value())
    return bool(cookie) and secrets.compare_digest(cookie, expected)


def require_webui_auth(request: Request) -> None:
    if not is_webui_authenticated(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
