import uuid
from contextlib import contextmanager
from pathlib import Path

from src.database.crud import create_registration_task, get_registration_task_by_uuid
from src.database.models import Account, Base, EmailService
from src.database.session import DatabaseSessionManager
from src.core.register import RegistrationEngine, RegistrationResult
from src.web.routes import registration as registration_routes


class DummySettings:
    custom_domain_base_url = ""
    custom_domain_api_key = None
    tempmail_base_url = "https://api.tempmail.lol/v2"
    tempmail_timeout = 30
    tempmail_max_retries = 3


def test_registration_available_services_include_cloudmail(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "registration_cloudmail_routes.db"
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

    monkeypatch.setattr(registration_routes, "get_db", fake_get_db)
    monkeypatch.setattr(registration_routes, "get_settings", lambda: DummySettings())

    result = registration_routes.asyncio.run(registration_routes.get_available_email_services())

    assert result["cloudmail"]["available"] is True
    assert result["cloudmail"]["count"] == 1
    assert result["cloudmail"]["services"][0]["name"] == "CloudMail 主服务"
    assert result["cloudmail"]["services"][0]["type"] == "cloudmail"
    assert result["cloudmail"]["services"][0]["domain"] == "example.com"


def test_registration_task_uses_enabled_cloudmail_service_config(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "registration_cloudmail_task.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    task_uuid = str(uuid.uuid4())
    with manager.session_scope() as session:
        service = EmailService(
            service_type="cloudmail",
            name="CloudMail 主服务",
            config={
                "base_url": "https://cloudmail.example.com",
                "default_domain": "example.com",
                "admin_token": "token",
            },
            enabled=True,
            priority=0,
        )
        session.add(service)
        session.flush()
        service_id = service.id
        create_registration_task(session, task_uuid=task_uuid)

    @contextmanager
    def fake_get_db():
        session = manager.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    captured = {}

    class StopAfterCreate(Exception):
        pass

    def fake_create(service_type, config, name=None):
        captured["service_type"] = service_type
        captured["config"] = dict(config)
        raise StopAfterCreate()

    monkeypatch.setattr(registration_routes, "get_db", fake_get_db)
    monkeypatch.setattr(registration_routes, "get_settings", lambda: DummySettings())
    monkeypatch.setattr(registration_routes.task_manager, "is_cancelled", lambda _: False)
    monkeypatch.setattr(registration_routes.task_manager, "update_status", lambda *_: None)
    monkeypatch.setattr(registration_routes.EmailServiceFactory, "create", fake_create)

    try:
        registration_routes._run_sync_registration_task(
            task_uuid=task_uuid,
            email_service_type="cloudmail",
            proxy="http://proxy.local:8080",
            email_service_config=None,
            email_service_id=service_id,
        )
    except StopAfterCreate:
        pass

    assert captured["service_type"] == registration_routes.EmailServiceType.CLOUDMAIL
    assert captured["config"]["base_url"] == "https://cloudmail.example.com"
    assert captured["config"]["domain"] == "example.com"
    assert captured["config"]["admin_token"] == "token"
    assert captured["config"]["proxy_url"] == "http://proxy.local:8080"

    with manager.session_scope() as session:
        task = get_registration_task_by_uuid(session, task_uuid)
        assert task.email_service_id == service_id


def test_cloudmail_registration_persists_platform_source(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "registration_cloudmail_platform_source.db"
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

    class DummySettings:
        openai_client_id = "client-123"

    class DummyEmailService:
        service_type = registration_routes.EmailServiceType.CLOUDMAIL

    monkeypatch.setattr("src.core.register.get_db", fake_get_db)
    monkeypatch.setattr("src.core.register.get_settings", lambda: DummySettings())

    engine = RegistrationEngine.__new__(RegistrationEngine)
    engine.email_service = DummyEmailService()
    engine.email_info = {"service_id": "cloudmail-service-id"}
    engine.proxy_url = None
    engine._log = lambda *args, **kwargs: None

    result = RegistrationResult(
        success=True,
        email="cloudmail-user@example.com",
        password="secret",
        account_id="account-123",
        workspace_id="workspace-123",
        access_token="access-token",
        refresh_token="refresh-token",
        id_token="id-token",
        metadata={},
        source="register",
    )

    assert engine.save_to_database(result) is True

    with manager.session_scope() as session:
        saved_account = session.query(Account).filter_by(email=result.email).first()
        assert saved_account.platform_source == "cloudmail"
