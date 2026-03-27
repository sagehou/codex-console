import asyncio
import importlib
from contextlib import contextmanager
from pathlib import Path

from src.config.constants import EmailServiceType
from src.database.models import Base, EmailService
from src.database.session import DatabaseSessionManager
import src.services as services_package
from src.services.cloudmail import CloudMailService
from src.services.base import EmailServiceFactory
from src.web.routes import email as email_routes


def test_cloudmail_service_registered_via_public_package_entrypoint(monkeypatch):
    monkeypatch.setattr(EmailServiceFactory, "_registry", {})

    importlib.reload(services_package)

    service_class = EmailServiceFactory.get_service_class(EmailServiceType.CLOUDMAIL)

    assert service_class is not None
    assert service_class.__name__ == "CloudMailService"


def test_email_service_types_include_cloudmail():
    result = asyncio.run(email_routes.get_service_types())
    cloudmail_type = next(item for item in result["types"] if item["value"] == "cloudmail")

    assert cloudmail_type["label"] == "CloudMail"
    field_names = [field["name"] for field in cloudmail_type["config_fields"]]
    assert "base_url" in field_names
    assert "admin_token" in field_names
    assert "domain" in field_names


def test_email_service_stats_include_cloudmail_count(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "cloudmail_routes.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        session.add(
            EmailService(
                service_type="cloudmail",
                name="CloudMail 主服务",
                config={
                    "base_url": "https://cloudmail.example.com",
                    "domain": "example.com",
                    "admin_token": "token",
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

    monkeypatch.setattr(email_routes, "get_db", fake_get_db)

    result = asyncio.run(email_routes.get_email_services_stats())

    assert result["cloudmail_count"] == 1
    assert result["enabled_count"] == 1


def test_cloudmail_service_uses_proxy_url_for_http_client():
    service = CloudMailService({
        "base_url": "https://cloudmail.example.com",
        "admin_token": "token",
        "proxy_url": "http://127.0.0.1:7890",
    })

    assert service.http_client.proxy_url == "http://127.0.0.1:7890"
