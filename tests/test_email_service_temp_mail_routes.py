import asyncio
from contextlib import contextmanager
from pathlib import Path

from src.database.models import Base, EmailService
from src.database.session import DatabaseSessionManager
from src.web.routes import email as email_routes


def make_test_db(monkeypatch, name: str):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / name
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    @contextmanager
    def fake_get_db():
        session = manager.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    monkeypatch.setattr(email_routes, "get_db", fake_get_db)
    return manager


def create_temp_mail_service(manager: DatabaseSessionManager) -> EmailService:
    with manager.session_scope() as session:
        service = EmailService(
            service_type="temp_mail",
            name="TempMail Primary",
            config={
                "base_url": "https://mail.example.com",
                "admin_password": "admin-secret",
                "domain": "old.com",
                "site_password": "site-secret",
                "enable_prefix": True,
            },
            enabled=True,
            priority=0,
        )
        session.add(service)
        session.flush()
        session.refresh(service)
        session.expunge(service)
        return service


def test_temp_mail_service_type_domain_placeholder_mentions_multiple_domains():
    result = asyncio.run(email_routes.get_service_types())
    temp_mail_type = next(item for item in result["types"] if item["value"] == "temp_mail")
    domain_field = next(field for field in temp_mail_type["config_fields"] if field["name"] == "domain")

    assert "example.com, example.org" in domain_field["placeholder"]


def test_normalize_temp_mail_config_trims_and_joins_domains():
    normalized = email_routes.normalize_temp_mail_config({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": " a.com, b.com ,, c.com ",
    })

    assert normalized["domain"] == "a.com,b.com,c.com"


def test_normalize_temp_mail_config_drops_blank_site_password_on_create():
    normalized = email_routes.normalize_temp_mail_config({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "a.com",
        "site_password": "",
    })

    assert "site_password" not in normalized


def test_normalize_temp_mail_config_preserves_existing_site_password_when_omitted():
    normalized = email_routes.normalize_temp_mail_config(
        {"domain": "a.com, b.com"},
        existing_config={"site_password": "site-secret", "domain": "old.com"},
        is_update=True,
    )

    assert normalized["site_password"] == "site-secret"


def test_normalize_temp_mail_config_clears_site_password_on_empty_string():
    normalized = email_routes.normalize_temp_mail_config(
        {"domain": "a.com", "site_password": ""},
        existing_config={"site_password": "site-secret", "domain": "old.com"},
        is_update=True,
    )

    assert normalized["site_password"] == ""


def test_create_email_service_persists_normalized_temp_mail_domain(monkeypatch):
    make_test_db(monkeypatch, "temp_mail_create_routes.db")
    request = email_routes.EmailServiceCreate(
        service_type="temp_mail",
        name="tm-1",
        config={
            "base_url": "https://mail.example.com",
            "admin_password": "admin-secret",
            "domain": " a.com, b.com ,, c.com ",
            "site_password": "site-secret",
        },
        enabled=True,
        priority=0,
    )

    service = asyncio.run(email_routes.create_email_service(request))

    assert service.config["domain"] == "a.com,b.com,c.com"
    assert service.config["has_site_password"] is True
    assert "site_password" not in service.config


def test_update_email_service_keeps_existing_site_password_when_omitted(monkeypatch):
    manager = make_test_db(monkeypatch, "temp_mail_update_keep_routes.db")
    temp_mail_service = create_temp_mail_service(manager)
    request = email_routes.EmailServiceUpdate(config={"domain": " a.com, b.com "})

    service = asyncio.run(email_routes.update_email_service(temp_mail_service.id, request))

    assert service.config["domain"] == "a.com,b.com"
    assert service.config["has_site_password"] is True
    assert "site_password" not in service.config


def test_update_email_service_clears_site_password_on_empty_string(monkeypatch):
    manager = make_test_db(monkeypatch, "temp_mail_update_clear_routes.db")
    temp_mail_service = create_temp_mail_service(manager)
    request = email_routes.EmailServiceUpdate(config={"domain": "a.com", "site_password": ""})

    service = asyncio.run(email_routes.update_email_service(temp_mail_service.id, request))

    assert service.config["domain"] == "a.com"
    assert service.config.get("has_site_password") is False
    assert "site_password" not in service.config
