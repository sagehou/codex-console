import asyncio

import pytest
from fastapi import HTTPException

from src.web.routes import settings as settings_routes


class DummySettings:
    tempmail_base_url = "https://mail.example.com"
    tempmail_timeout = 30
    tempmail_max_retries = 3
    tempmail_domains = ["alpha.example", "beta.example"]
    tempmail_domain = "legacy.example"


def test_get_tempmail_settings_prefers_canonical_domains_over_legacy_domain(monkeypatch):
    monkeypatch.setattr(settings_routes, "get_settings", lambda: DummySettings())

    result = asyncio.run(settings_routes.get_tempmail_settings())

    assert result["domains"] == ["alpha.example", "beta.example"]
    assert result["domain"] == "alpha.example,beta.example"


def test_get_tempmail_settings_falls_back_to_legacy_domain_when_domains_missing(monkeypatch):
    class LegacyOnlySettings:
        tempmail_base_url = "https://mail.example.com"
        tempmail_timeout = 30
        tempmail_max_retries = 3
        tempmail_domain = "legacy.example"

    monkeypatch.setattr(settings_routes, "get_settings", lambda: LegacyOnlySettings())

    result = asyncio.run(settings_routes.get_tempmail_settings())

    assert result["domains"] == ["legacy.example"]
    assert result["domain"] == "legacy.example"


def test_update_tempmail_settings_accepts_domain_alias_but_persists_canonical_domains(monkeypatch):
    captured = {}

    def fake_update_settings(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(settings_routes, "update_settings", fake_update_settings)

    request = settings_routes.TempmailSettings(
        api_url="https://mail.example.com",
        domain=" @alpha.example, beta.example, @alpha.example ",
    )

    result = asyncio.run(settings_routes.update_tempmail_settings(request))

    assert result["success"] is True
    assert captured["tempmail_base_url"] == "https://mail.example.com"
    assert captured["tempmail_domains"] == ["alpha.example", "beta.example"]
    assert "tempmail_domain" not in captured


def test_update_tempmail_settings_rejects_invalid_domain_alias_entries(monkeypatch):
    monkeypatch.setattr(settings_routes, "update_settings", lambda **kwargs: None)

    request = settings_routes.TempmailSettings(domain="good.example,bad domain")

    with pytest.raises(HTTPException, match="bad domain"):
        asyncio.run(settings_routes.update_tempmail_settings(request))
