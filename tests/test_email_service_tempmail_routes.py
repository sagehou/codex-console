import asyncio
from contextlib import contextmanager
from pathlib import Path

from src.database.models import Base, EmailService
from src.database.session import DatabaseSessionManager
from src.web.routes import email as email_routes
from src.web.routes import registration as registration_routes


class DummySettings:
    custom_domain_base_url = ""
    custom_domain_api_key = None


def test_email_service_types_include_temp_mail_domain_list_and_site_password():
    result = asyncio.run(email_routes.get_service_types())
    temp_mail_type = next(item for item in result["types"] if item["value"] == "temp_mail")

    field_names = [field["name"] for field in temp_mail_type["config_fields"]]
    assert "domains" in field_names
    assert "domain" not in field_names

    custom_auth_field = next(
        field for field in temp_mail_type["config_fields"] if field["name"] == "custom_auth"
    )
    assert "Site Password" in custom_auth_field["label"]


def test_normalize_service_config_keeps_temp_mail_domains():
    normalized = email_routes._normalize_service_config(
        "temp_mail",
        {
            "base_url": "https://mail.example.com",
            "domain": "alpha.example.com\nbeta.example.com",
        },
    )

    assert normalized["domain"] == "alpha.example.com"
    assert normalized["domains"] == ["alpha.example.com", "beta.example.com"]


def test_registration_available_services_include_temp_mail_domains(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "tempmail_routes.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        session.add(
            EmailService(
                service_type="temp_mail",
                name="TempMail Main Service",
                config={
                    "base_url": "https://mail.example.com",
                    "admin_password": "admin-secret",
                    "domains": ["alpha.example.com", "beta.example.com"],
                },
                enabled=True,
                priority=0,
            )
        )

    @contextmanager
    def fake_get_db():
        session = manager.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    monkeypatch.setattr(registration_routes, "get_db", fake_get_db)

    import src.config.settings as settings_module

    monkeypatch.setattr(settings_module, "get_settings", lambda: DummySettings())

    result = asyncio.run(registration_routes.get_available_email_services())

    assert result["temp_mail"]["available"] is True
    assert result["temp_mail"]["count"] == 1
    assert result["temp_mail"]["services"][0]["domain"] == "alpha.example.com"
    assert result["temp_mail"]["services"][0]["domains"] == [
        "alpha.example.com",
        "beta.example.com",
    ]