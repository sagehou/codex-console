from datetime import datetime, timezone
import asyncio
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from src.core.upload import sub2api_upload
from src.database.models import Account, Base
from src.database.session import DatabaseSessionManager
from src.web.routes import accounts as account_routes
from src.web.routes import registration as registration_routes
from src.web.routes.upload import sub2api_services as sub2api_service_routes
from src.web.routes.upload.sub2api_services import Sub2ApiServiceCreate, Sub2ApiServiceUpdate


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("no json payload")
        return self._payload


def make_account():
    account = Account(
        id=1,
        email="tester@example.com",
        access_token="access-token",
        refresh_token="refresh-token",
        account_id="account-123",
        client_id="client-456",
        workspace_id="workspace-789",
        expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
    )
    return account


def create_persisted_account(manager, email="tester@example.com"):
    with manager.session_scope() as session:
        session.add(
            Account(
                id=1,
                email=email,
                access_token="access-token",
                refresh_token="refresh-token",
                account_id="account-123",
                client_id="client-456",
                workspace_id="workspace-789",
                email_service="cloudmail",
                expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
        )


@contextmanager
def database_manager_for_test(filename):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / filename
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)
    yield manager


def detach_account_by_email(manager, email="tester@example.com"):
    with manager.session_scope() as session:
        account = session.query(Account).filter_by(email=email).first()
        session.expunge(account)
    return account


def patch_upload_db(monkeypatch, manager):
    @contextmanager
    def fake_get_db():
        session = manager.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    monkeypatch.setattr(sub2api_upload, "get_db", fake_get_db)


async def read_streaming_response_content(response):
    chunks = []
    async for chunk in response.body_iterator:
        if isinstance(chunk, str):
            chunk = chunk.encode("utf-8")
        chunks.append(chunk)
    return b"".join(chunks).decode("utf-8")


def test_upload_to_sub2api_posts_newapi_payload_for_newapi_target(monkeypatch):
    calls = []
    with database_manager_for_test("newapi_upload_payload.db") as manager:
        create_persisted_account(manager)

        def fake_post(url, **kwargs):
            calls.append({"url": url, "kwargs": kwargs})
            return FakeResponse(status_code=201)

        monkeypatch.setattr(sub2api_upload.cffi_requests, "post", fake_post)
        patch_upload_db(monkeypatch, manager)

        account = detach_account_by_email(manager)

        success, message = sub2api_upload.upload_to_sub2api(
            [account],
            api_url="https://sub2api.example.com",
            api_key="key-123",
            target_type="newApi",
        )

        assert success is True
        assert message == "成功上传 1 个账号"
        assert calls[0]["url"] == "https://sub2api.example.com/api/v1/admin/accounts/data"
        payload = calls[0]["kwargs"]["json"]
        assert payload["data"]["type"] == "newApi-data"
        account_payload = payload["data"]["accounts"][0]
        assert account_payload["type"] == "newApi"
        assert account_payload["credentials"]["access_token"] == "access-token"
        assert account_payload["credentials"]["refresh_token"] == "refresh-token"


def test_upload_to_sub2api_persists_last_upload_target_for_successful_newapi_upload(monkeypatch):
    calls = []
    with database_manager_for_test("newapi_upload_last_target.db") as manager:
        create_persisted_account(manager)

        def fake_post(url, **kwargs):
            calls.append({"url": url, "kwargs": kwargs})
            return FakeResponse(status_code=201)

        monkeypatch.setattr(sub2api_upload.cffi_requests, "post", fake_post)
        patch_upload_db(monkeypatch, manager)

        account = detach_account_by_email(manager)

        success, message = sub2api_upload.upload_to_sub2api(
            [account],
            api_url="https://sub2api.example.com",
            api_key="key-123",
            target_type="newApi",
        )

        assert success is True
        assert message == "成功上传 1 个账号"

        assert calls[0]["url"] == "https://sub2api.example.com/api/v1/admin/accounts/data"

        with manager.session_scope() as session:
            reloaded_account = session.query(Account).filter_by(email="tester@example.com").first()
            assert reloaded_account.last_upload_target == "newApi"


def test_upload_to_sub2api_persists_last_upload_target_for_successful_default_target(monkeypatch):
    calls = []
    with database_manager_for_test("sub2api_upload_last_target.db") as manager:
        create_persisted_account(manager)

        def fake_post(url, **kwargs):
            calls.append({"url": url, "kwargs": kwargs})
            return FakeResponse(status_code=201)

        monkeypatch.setattr(sub2api_upload.cffi_requests, "post", fake_post)
        patch_upload_db(monkeypatch, manager)

        account = detach_account_by_email(manager)

        success, message = sub2api_upload.upload_to_sub2api(
            [account],
            api_url="https://sub2api.example.com",
            api_key="key-123",
        )

        assert success is True
        assert message == "成功上传 1 个账号"
        assert calls[0]["url"] == "https://sub2api.example.com/api/v1/admin/accounts/data"

        with manager.session_scope() as session:
            reloaded_account = session.query(Account).filter_by(email="tester@example.com").first()
            assert reloaded_account.last_upload_target == "sub2api"


def test_upload_to_sub2api_fails_when_detached_account_persistence_cannot_be_written(monkeypatch):
    calls = []
    account = make_account()

    def fake_post(url, **kwargs):
        calls.append({"url": url, "kwargs": kwargs})
        return FakeResponse(status_code=201)

    monkeypatch.setattr(sub2api_upload.cffi_requests, "post", fake_post)

    success, message = sub2api_upload.upload_to_sub2api(
        [account],
        api_url="https://sub2api.example.com",
        api_key="key-123",
        target_type="newApi",
    )

    assert calls[0]["url"] == "https://sub2api.example.com/api/v1/admin/accounts/data"
    assert success is False
    assert "last_upload_target" in message


def test_upload_to_sub2api_defaults_to_sub2api_payload(monkeypatch):
    calls = []
    with database_manager_for_test("sub2api_default_payload.db") as manager:
        create_persisted_account(manager)

        def fake_post(url, **kwargs):
            calls.append({"url": url, "kwargs": kwargs})
            return FakeResponse(status_code=201)

        monkeypatch.setattr(sub2api_upload.cffi_requests, "post", fake_post)
        patch_upload_db(monkeypatch, manager)

        account = detach_account_by_email(manager)

        success, message = sub2api_upload.upload_to_sub2api(
            [account],
            api_url="https://sub2api.example.com",
            api_key="key-123",
        )

        assert success is True
        assert message == "成功上传 1 个账号"
        payload = calls[0]["kwargs"]["json"]
        assert payload["data"]["type"] == "sub2api-data"
        assert payload["data"]["accounts"][0]["type"] == "oauth"


def test_upload_to_sub2api_keeps_tokenless_accounts_out_of_final_payload(monkeypatch):
    calls = []
    tokenless_account = Account(
        id=2,
        email="missing@example.com",
        access_token=None,
        refresh_token="refresh-token",
        account_id="account-456",
        client_id="client-789",
        workspace_id="workspace-999",
        expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
    )

    with database_manager_for_test("sub2api_tokenless_payload.db") as manager:
        create_persisted_account(manager)

        def fake_post(url, **kwargs):
            calls.append({"url": url, "kwargs": kwargs})
            return FakeResponse(status_code=201)

        monkeypatch.setattr(sub2api_upload.cffi_requests, "post", fake_post)
        patch_upload_db(monkeypatch, manager)

        persisted_account = detach_account_by_email(manager)

        success, message = sub2api_upload.upload_to_sub2api(
            [persisted_account, tokenless_account],
            api_url="https://sub2api.example.com",
            api_key="key-123",
        )

        assert success is True
        assert message == "成功上传 1 个账号"
        payload_accounts = calls[0]["kwargs"]["json"]["data"]["accounts"]
        assert len(payload_accounts) == 1
        assert payload_accounts[0]["name"] == "tester@example.com"


def test_upload_to_sub2api_rejects_invalid_target_type():
    with pytest.raises(ValueError, match="target_type"):
        sub2api_upload.upload_to_sub2api(
            [make_account()],
            api_url="https://sub2api.example.com",
            api_key="key-123",
            target_type="broken-target",
        )


def test_export_accounts_sub2api_uses_newapi_target_payload(monkeypatch):
    account = make_account()
    service = SimpleNamespace(api_url="https://sub2api.example.com", api_key="key-123", target_type="newApi")

    class FakeQuery:
        def filter(self, *args, **kwargs):
            return self

        def all(self):
            return [account]

    class FakeDb:
        def query(self, model):
            assert model is account_routes.Account
            return FakeQuery()

    @contextmanager
    def fake_get_db():
        yield FakeDb()

    request = account_routes.BatchExportRequest(ids=[account.id])
    setattr(request, "service_id", 9)

    monkeypatch.setattr(account_routes, "get_db", fake_get_db)
    monkeypatch.setattr(account_routes, "resolve_account_ids", lambda *args, **kwargs: [account.id])
    monkeypatch.setattr(account_routes.crud, "get_sub2api_service_by_id", lambda db, service_id: service)

    response = asyncio.run(account_routes.export_accounts_sub2api(request))
    content = asyncio.run(read_streaming_response_content(response))
    payload = __import__("json").loads(content)

    assert payload["data"]["type"] == "newApi-data"
    assert payload["data"]["accounts"][0]["type"] == "newApi"


def test_export_accounts_sub2api_rejects_invalid_persisted_target_type(monkeypatch):
    account = make_account()
    service = SimpleNamespace(api_url="https://sub2api.example.com", api_key="key-123", target_type="broken-target")

    class FakeQuery:
        def filter(self, *args, **kwargs):
            return self

        def all(self):
            return [account]

    class FakeDb:
        def query(self, model):
            return FakeQuery()

    @contextmanager
    def fake_get_db():
        yield FakeDb()

    request = account_routes.BatchExportRequest(ids=[1])
    setattr(request, "service_id", 9)

    monkeypatch.setattr(account_routes, "get_db", fake_get_db)
    monkeypatch.setattr(account_routes, "resolve_account_ids", lambda *args, **kwargs: [account.id])
    monkeypatch.setattr(account_routes.crud, "get_sub2api_service_by_id", lambda db, service_id: service)

    with pytest.raises(HTTPException, match="不支持的 Sub2API target_type") as exc_info:
        asyncio.run(account_routes.export_accounts_sub2api(request))

    assert exc_info.value.status_code == 400


def test_batch_upload_route_propagates_newapi_target_type(monkeypatch):
    service = SimpleNamespace(api_url="https://sub2api.example.com", api_key="key-123", target_type="newApi")
    captured = {}

    @contextmanager
    def fake_get_db():
        yield object()

    def fake_get_service(_db, service_id):
        assert service_id == 9
        return service

    def fake_resolve_ids(_db, ids, select_all, status_filter, email_service_filter, search_filter):
        return [101, 202]

    def fake_batch_upload(account_ids, api_url, api_key, concurrency, priority, target_type):
        captured.update({
            "account_ids": account_ids,
            "api_url": api_url,
            "api_key": api_key,
            "concurrency": concurrency,
            "priority": priority,
            "target_type": target_type,
        })
        return {"success_count": 2, "failed_count": 0, "skipped_count": 0, "details": []}

    monkeypatch.setattr(account_routes, "get_db", fake_get_db)
    monkeypatch.setattr(account_routes.crud, "get_sub2api_service_by_id", fake_get_service)
    monkeypatch.setattr(account_routes, "resolve_account_ids", fake_resolve_ids)
    monkeypatch.setattr(account_routes, "batch_upload_to_sub2api", fake_batch_upload)

    result = asyncio.run(account_routes.batch_upload_accounts_to_sub2api(
        account_routes.BatchSub2ApiUploadRequest(ids=[1], service_id=9, concurrency=5, priority=77)
    ))

    assert result["success_count"] == 2
    assert captured["account_ids"] == [101, 202]
    assert captured["target_type"] == "newApi"


def test_single_upload_route_defaults_to_sub2api_target_type(monkeypatch):
    service = SimpleNamespace(api_url="https://sub2api.example.com", api_key="key-123")
    captured = {}

    @contextmanager
    def fake_get_db():
        yield object()

    def fake_get_services(_db, enabled=True):
        assert enabled is True
        return [service]

    def fake_get_account(_db, account_id):
        assert account_id == 88
        return make_account()

    def fake_upload(accounts, api_url, api_key, concurrency, priority, target_type):
        captured.update({
            "accounts": accounts,
            "api_url": api_url,
            "api_key": api_key,
            "concurrency": concurrency,
            "priority": priority,
            "target_type": target_type,
        })
        return True, "ok"

    monkeypatch.setattr(account_routes, "get_db", fake_get_db)
    monkeypatch.setattr(account_routes.crud, "get_sub2api_services", fake_get_services)
    monkeypatch.setattr(account_routes.crud, "get_account_by_id", fake_get_account)
    monkeypatch.setattr(account_routes, "upload_to_sub2api", fake_upload)

    result = asyncio.run(account_routes.upload_account_to_sub2api(88))

    assert result == {"success": True, "message": "ok"}
    assert captured["target_type"] == "sub2api"
    assert captured["accounts"][0].email == "tester@example.com"


def test_registration_auto_upload_uses_service_newapi_target_type(monkeypatch):
    task_uuid = "task-123"
    saved_account = make_account()
    service = SimpleNamespace(id=5, name="newapi service", api_url="https://sub2api.example.com", api_key="key-123", target_type="newApi")
    upload_calls = []

    class DummySettings:
        tempmail_base_url = "https://temp.example.com"
        tempmail_timeout = 30
        tempmail_max_retries = 3
        custom_domain_base_url = ""
        custom_domain_api_key = None

    class DummyEmailService:
        pass

    class DummyResult:
        success = True
        email = saved_account.email

    class FakeQuery:
        def __init__(self, model):
            self.model = model

        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def filter_by(self, **kwargs):
            return self

        def first(self):
            model_name = getattr(self.model, "__name__", "")
            if model_name == "Account":
                return saved_account
            return None

        def all(self):
            return []

    class FakeDb:
        def query(self, model):
            return FakeQuery(model)

        def commit(self):
            return None

    @contextmanager
    def fake_get_db():
        yield FakeDb()

    class DummyEngine:
        def __init__(self, **kwargs):
            pass

        def run(self):
            return DummyResult()

        def save_to_database(self, result):
            return None

    monkeypatch.setattr(registration_routes, "get_db", fake_get_db)
    monkeypatch.setattr(registration_routes, "get_settings", lambda: DummySettings())
    monkeypatch.setattr(registration_routes, "get_proxy_for_registration", lambda db: ("http://proxy.local:8080", None))
    monkeypatch.setattr(registration_routes, "update_proxy_usage", lambda db, proxy_id: None)
    monkeypatch.setattr(registration_routes.task_manager, "is_cancelled", lambda _: False)
    monkeypatch.setattr(registration_routes.task_manager, "update_status", lambda *_: None)
    monkeypatch.setattr(registration_routes.task_manager, "create_log_callback", lambda *_, **__: (lambda message: None))
    monkeypatch.setattr(registration_routes.crud, "update_registration_task", lambda *args, **kwargs: object())
    monkeypatch.setattr(registration_routes.EmailServiceFactory, "create", lambda *args, **kwargs: DummyEmailService())
    monkeypatch.setattr(registration_routes, "RegistrationEngine", DummyEngine)
    monkeypatch.setattr(registration_routes.crud, "get_sub2api_service_by_id", lambda db, service_id: service)

    def fake_upload(accounts, api_url, api_key, concurrency=3, priority=50, target_type="sub2api"):
        upload_calls.append({
            "accounts": accounts,
            "api_url": api_url,
            "api_key": api_key,
            "target_type": target_type,
        })
        return True, "ok"

    monkeypatch.setattr("src.core.upload.sub2api_upload.upload_to_sub2api", fake_upload)

    registration_routes._run_sync_registration_task(
        task_uuid=task_uuid,
        email_service_type="tempmail",
        proxy=None,
        email_service_config=None,
        auto_upload_sub2api=True,
        sub2api_service_ids=[5],
    )

    assert upload_calls[0]["target_type"] == "newApi"
    assert upload_calls[0]["accounts"][0].email == saved_account.email


def test_sub2api_services_upload_returns_400_for_invalid_persisted_target_type(monkeypatch):
    service = SimpleNamespace(api_url="https://sub2api.example.com", api_key="key-123", target_type="broken-target")

    @contextmanager
    def fake_get_db():
        yield object()

    monkeypatch.setattr(sub2api_service_routes, "get_db", fake_get_db)
    monkeypatch.setattr(sub2api_service_routes.crud, "get_sub2api_service_by_id", lambda db, service_id: service)
    monkeypatch.setattr(sub2api_service_routes, "batch_upload_to_sub2api", lambda *args, **kwargs: {"success_count": 1})

    with pytest.raises(HTTPException, match="不支持的 Sub2API target_type") as exc_info:
        asyncio.run(sub2api_service_routes.upload_accounts_to_sub2api(
            sub2api_service_routes.Sub2ApiUploadRequest(account_ids=[1], service_id=9)
        ))

    assert exc_info.value.status_code == 400


def test_sub2api_service_models_accept_newapi_target():
    created = Sub2ApiServiceCreate(
        name="newapi",
        api_url="https://sub2api.example.com",
        api_key="key-123",
        target_type="newApi",
    )
    updated = Sub2ApiServiceUpdate(target_type="newApi")

    assert created.target_type == "newApi"
    assert updated.target_type == "newApi"


def test_sub2api_service_models_reject_unknown_target():
    with pytest.raises(ValidationError):
        Sub2ApiServiceCreate(
            name="bad",
            api_url="https://sub2api.example.com",
            api_key="key-123",
            target_type="unknown",
        )
