import asyncio
from contextlib import contextmanager
from pathlib import Path

from src.config.constants import EmailServiceType
from src.database.models import Base, EmailService
from src.database.session import DatabaseSessionManager
from src.services.base import EmailServiceFactory
from src.web.routes import email as email_routes


def test_temp_mail_service_registered():
    service_type = EmailServiceType("temp_mail")
    service_class = EmailServiceFactory.get_service_class(service_type)
    assert service_class is not None
    assert service_class.__name__ == "TempMailService"


def test_email_service_types_include_temp_mail_site_password():
    result = asyncio.run(email_routes.get_service_types())
    temp_mail_type = next(item for item in result["types"] if item["value"] == "temp_mail")

    assert temp_mail_type["label"] == "Temp-Mail（自部署）"
    field_names = [field["name"] for field in temp_mail_type["config_fields"]]
    assert "base_url" in field_names
    assert "domain" in field_names
    assert "site_password" in field_names
    assert "admin_password" in field_names


def test_filter_sensitive_config_marks_temp_mail_site_password():
    filtered = email_routes.filter_sensitive_config({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "site_password": "site-secret",
        "domain": "example.com",
    })

    assert filtered["base_url"] == "https://mail.example.com"
    assert filtered["domain"] == "example.com"
    assert filtered["has_admin_password"] is True
    assert filtered["has_site_password"] is True
    assert "admin_password" not in filtered
    assert "site_password" not in filtered


def test_filter_sensitive_config_surfaces_tempmail_domains_and_display_alias():
    filtered = email_routes.filter_sensitive_config(
        {
            "base_url": "https://mail.example.com",
            "admin_password": "admin-secret",
            "domains": ["alpha.example", "beta.example"],
            "domain": "alpha.example,beta.example",
        }
    )

    assert filtered["domains"] == ["alpha.example", "beta.example"]
    assert filtered["domain"] == "alpha.example,beta.example"
    assert filtered["has_admin_password"] is True


def test_update_email_service_allows_clearing_optional_config_fields(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "temp_mail_update_routes.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        service = EmailService(
            service_type="cloudmail",
            name="CloudMail 主服务",
            config={
                "base_url": "https://cloudmail.example.com",
                "admin_token": "token",
                "domain": "example.com",
            },
            enabled=True,
            priority=0,
        )
        session.add(service)
        session.flush()
        service_id = service.id

    @contextmanager
    def fake_get_db():
        session = manager.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    monkeypatch.setattr(email_routes, "get_db", fake_get_db)

    result = asyncio.run(email_routes.update_email_service(
        service_id,
        email_routes.EmailServiceUpdate(config={"domain": ""}),
    ))

    assert result.config["base_url"] == "https://cloudmail.example.com"
    assert result.config["has_admin_token"] is True
    assert result.config.get("domain") == ""

    with manager.session_scope() as session:
        updated = session.query(EmailService).filter(EmailService.id == service_id).first()
        assert updated.config["domain"] == ""


def test_get_email_service_full_redacts_sensitive_fields_for_browser(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "temp_mail_full_routes.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        service = EmailService(
            service_type="temp_mail",
            name="TempMail 主服务",
            config={
                "base_url": "https://mail.example.com",
                "admin_password": "admin-secret",
                "site_password": "site-secret",
                "domain": "example.com",
            },
            enabled=True,
            priority=0,
        )
        session.add(service)
        session.flush()
        service_id = service.id

    @contextmanager
    def fake_get_db():
        session = manager.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    monkeypatch.setattr(email_routes, "get_db", fake_get_db)

    result = asyncio.run(email_routes.get_email_service_full(service_id))

    assert result["config"]["base_url"] == "https://mail.example.com"
    assert result["config"]["domain"] == "example.com"
    assert result["config"]["has_admin_password"] is True
    assert result["config"]["has_site_password"] is True
    assert "admin_password" not in result["config"]
    assert "site_password" not in result["config"]


def test_get_email_service_full_reads_legacy_tempmail_domain_as_domains_list(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "temp_mail_full_legacy_routes.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        service = EmailService(
            service_type="temp_mail",
            name="TempMail Legacy",
            config={
                "base_url": "https://mail.example.com",
                "admin_password": "admin-secret",
                "domain": "legacy.example",
            },
            enabled=True,
            priority=0,
        )
        session.add(service)
        session.flush()
        service_id = service.id

    @contextmanager
    def fake_get_db():
        session = manager.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    monkeypatch.setattr(email_routes, "get_db", fake_get_db)

    result = asyncio.run(email_routes.get_email_service_full(service_id))

    assert result["config"]["domains"] == ["legacy.example"]
    assert result["config"]["domain"] == "legacy.example"


def test_get_email_service_prefers_domains_over_legacy_domain_on_read(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "temp_mail_full_domains_routes.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        service = EmailService(
            service_type="temp_mail",
            name="TempMail Canonical",
            config={
                "base_url": "https://mail.example.com",
                "admin_password": "admin-secret",
                "domains": ["alpha.example", "beta.example"],
                "domain": "legacy.example",
            },
            enabled=True,
            priority=0,
        )
        session.add(service)
        session.flush()
        service_id = service.id

    @contextmanager
    def fake_get_db():
        session = manager.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    monkeypatch.setattr(email_routes, "get_db", fake_get_db)

    result = asyncio.run(email_routes.get_email_service(service_id))

    assert result.config["domains"] == ["alpha.example", "beta.example"]
    assert result.config["domain"] == "alpha.example,beta.example"


def test_email_services_js_preserves_tempmail_domain_alias_in_settings_flow():
    script = Path("/config/workspace/github.com/codex-console/.worktrees/account-workbench-maintain/static/js/email_services.js").read_text(encoding="utf-8")

    assert "tempmailDomain" in script
    assert "document.getElementById('tempmail-domain')" in script
    assert "elements.tempmailDomain.value = settings.tempmail.domain || ''" in script
    assert "domain: elements.tempmailDomain.value" in script
