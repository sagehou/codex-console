import asyncio
from contextlib import contextmanager
from pathlib import Path

from src.database.models import Base, EmailService
from src.database.session import DatabaseSessionManager
from src.web.routes import email as email_routes


def test_update_email_service_keeps_outlook_secrets_when_omitted(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "outlook_secret_preserve.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        service = EmailService(
            service_type="outlook",
            name="Outlook 主账户",
            config={
                "email": "user@example.com",
                "password": "stored-password",
                "client_id": "client-id",
                "refresh_token": "stored-refresh-token",
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
        email_routes.EmailServiceUpdate(config={
            "email": "updated@example.com",
            "client_id": "updated-client-id",
        }),
    ))

    assert result.config["email"] == "updated@example.com"
    assert result.config["client_id"] == "updated-client-id"
    assert result.config["has_password"] is True
    assert result.config["has_refresh_token"] is True

    with manager.session_scope() as session:
        updated = session.query(EmailService).filter(EmailService.id == service_id).first()
        assert updated.config["password"] == "stored-password"
        assert updated.config["refresh_token"] == "stored-refresh-token"


def test_update_email_service_allows_explicit_secret_clearing(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "cloudmail_secret_clear.db"
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
                "domain": "example.com",
                "admin_token": "stored-token",
            },
            enabled=False,
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
        email_routes.EmailServiceUpdate(config={"admin_token": None}),
    ))

    assert "has_admin_token" not in (result.config or {})

    with manager.session_scope() as session:
        updated = session.query(EmailService).filter(EmailService.id == service_id).first()
        assert "admin_token" not in updated.config


def test_update_email_service_rejects_clearing_required_cloudmail_secret_when_enabled(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "cloudmail_secret_reject.db"
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
                "admin_token": "stored-token",
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

    try:
        asyncio.run(email_routes.update_email_service(
            service_id,
            email_routes.EmailServiceUpdate(config={"admin_token": None}),
        ))
        assert False, "expected HTTPException"
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 400
        assert "admin_token" in getattr(exc, "detail", "")


def test_update_email_service_allows_outlook_auth_path_switch_when_other_remains(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "outlook_auth_switch.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        service = EmailService(
            service_type="outlook",
            name="Outlook 主账户",
            config={
                "email": "user@example.com",
                "password": "stored-password",
                "client_id": "client-id",
                "refresh_token": "stored-refresh-token",
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
        email_routes.EmailServiceUpdate(config={
            "password": None,
            "client_id": "client-id",
            "refresh_token": "stored-refresh-token",
        }),
    ))

    assert result.config["has_refresh_token"] is True
    assert "has_password" not in result.config


def test_update_email_service_rejects_outlook_when_all_auth_paths_cleared(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "outlook_auth_reject.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        service = EmailService(
            service_type="outlook",
            name="Outlook 主账户",
            config={
                "email": "user@example.com",
                "password": "stored-password",
                "client_id": "client-id",
                "refresh_token": "stored-refresh-token",
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

    try:
        asyncio.run(email_routes.update_email_service(
            service_id,
            email_routes.EmailServiceUpdate(config={
                "password": None,
                "refresh_token": None,
            }),
        ))
        assert False, "expected HTTPException"
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 400
        assert "Outlook" in getattr(exc, "detail", "")


def test_create_email_service_rejects_enabled_cloudmail_missing_required_config(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "cloudmail_create_reject.db"
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

    try:
        asyncio.run(email_routes.create_email_service(
            email_routes.EmailServiceCreate(
                service_type="cloudmail",
                name="CloudMail 主服务",
                config={"base_url": "https://cloudmail.example.com"},
                enabled=True,
                priority=0,
            )
        ))
        assert False, "expected HTTPException"
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 400
        assert "admin_token" in getattr(exc, "detail", "")


def test_enable_email_service_rejects_invalid_cloudmail_config(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "cloudmail_enable_reject.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        service = EmailService(
            service_type="cloudmail",
            name="CloudMail 主服务",
            config={"base_url": "https://cloudmail.example.com"},
            enabled=False,
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

    try:
        asyncio.run(email_routes.enable_email_service(service_id))
        assert False, "expected HTTPException"
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 400
        assert "admin_token" in getattr(exc, "detail", "")


def test_update_email_service_rejects_enabling_invalid_cloudmail_without_config_patch(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "cloudmail_patch_enable_reject.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        service = EmailService(
            service_type="cloudmail",
            name="CloudMail 主服务",
            config={"base_url": "https://cloudmail.example.com"},
            enabled=False,
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

    try:
        asyncio.run(email_routes.update_email_service(
            service_id,
            email_routes.EmailServiceUpdate(enabled=True),
        ))
        assert False, "expected HTTPException"
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 400
        assert "admin_token" in getattr(exc, "detail", "")


def test_email_services_js_avoids_inline_delete_handlers_with_service_names():
    js_path = Path("/config/workspace/github.com/codex-console/.worktrees/account-workbench-maintain/static/js/email_services.js")
    content = js_path.read_text(encoding="utf-8")

    assert "onclick=\"deleteService(" not in content
    assert "data-service-name=" not in content
    assert "serviceNameById" in content
    assert "addEventListener('click', handleDeleteServiceClick)" in content


def test_email_services_template_uses_password_inputs_for_secret_fields():
    template_path = Path("/config/workspace/github.com/codex-console/.worktrees/account-workbench-maintain/templates/email_services.html")
    content = template_path.read_text(encoding="utf-8")

    assert '<input type="password" id="edit-outlook-refresh-token"' in content
    assert '<input type="password" id="custom-api-key"' in content
    assert '<input type="password" id="edit-custom-api-key"' in content
    assert '<input type="password" id="custom-dm-api-key"' in content
    assert '<input type="password" id="edit-dm-api-key"' in content


def test_email_services_ui_supports_secret_clear_preserve_affordances():
    template_path = Path("/config/workspace/github.com/codex-console/.worktrees/account-workbench-maintain/templates/email_services.html")
    template = template_path.read_text(encoding="utf-8")
    js_path = Path("/config/workspace/github.com/codex-console/.worktrees/account-workbench-maintain/static/js/email_services.js")
    js = js_path.read_text(encoding="utf-8")

    assert 'id="edit-custom-api-key"' in template
    assert 'id="edit-custom-clear-api-key"' in template
    assert 'id="edit-outlook-refresh-token"' in template
    assert 'id="edit-outlook-clear-refresh-token"' in template

    assert "applySecretFieldState('edit-custom-api-key', 'edit-custom-clear-api-key'" in js
    assert "if (document.getElementById('edit-custom-clear-api-key').checked) config.api_key = null;" in js
    assert "applySecretFieldState('edit-outlook-refresh-token', 'edit-outlook-clear-refresh-token'" in js


def test_email_services_ui_preserves_outlook_client_id_and_limits_invalid_clears():
    js_path = Path("/config/workspace/github.com/codex-console/.worktrees/account-workbench-maintain/static/js/email_services.js")
    js = js_path.read_text(encoding="utf-8")

    assert "client_id: formData.get('client_id')?.trim() || ''" not in js
    assert "if (clientId) updateData.config.client_id = clientId;" in js
    assert "toggleSecretClearAvailability();" in js
    assert "edit-outlook-clear-password" in js
