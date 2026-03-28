from pathlib import Path
import importlib
from concurrent.futures import ThreadPoolExecutor
import time
import sqlite3
import json

from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from src.database import crud
from src.database.models import BatchSubscriptionTask
from src.database.init_db import _backfill_batch_subscription_task_columns, initialize_database
from src.database.session import get_db
import src.database.session as session_module

web_app_module = importlib.import_module("src.web.app")
web_auth_module = importlib.import_module("src.web.auth")
payment_route_module = importlib.import_module("src.web.routes.payment")


def make_fernet_key() -> str:
    return Fernet.generate_key().decode("ascii")


def make_settings(project_root: Path):
    class DummySecret:
        def __init__(self, value):
            self._value = value

        def get_secret_value(self):
            return self._value

    class DummySettings:
        app_name = "test"
        app_version = "1.0"
        debug = False
        database_url = str(project_root / "test.db")
        webui_secret_key = DummySecret("secret")
        webui_access_password = DummySecret("password")
        proxy_url = "http://proxy.test"

    return DummySettings()


def build_client(monkeypatch, tmp_path: Path) -> TestClient:
    templates_dir = tmp_path / "templates"
    static_dir = tmp_path / "static"
    templates_dir.mkdir(exist_ok=True)
    static_dir.mkdir(exist_ok=True)

    for name in ["login.html", "index.html", "accounts.html", "email_services.html", "settings.html", "payment.html", "cliproxy.html"]:
        (templates_dir / name).write_text("<html><body>ok</body></html>", encoding="utf-8")

    db_path = tmp_path / "payment_batch_tasks.db"
    monkeypatch.setenv("APP_DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("CLIPROXY_ENCRYPTION_KEY", make_fernet_key())
    monkeypatch.setattr(session_module, "_db_manager", None)
    monkeypatch.setattr(web_app_module, "STATIC_DIR", static_dir)
    monkeypatch.setattr(web_app_module, "TEMPLATES_DIR", templates_dir)
    monkeypatch.setattr(web_app_module, "get_settings", lambda: make_settings(tmp_path))
    monkeypatch.setattr(web_auth_module, "get_settings", lambda: make_settings(tmp_path))
    monkeypatch.setattr(payment_route_module, "dispatch_batch_subscription_task", lambda *args, **kwargs: None)
    initialize_database(f"sqlite:///{db_path}")
    app = web_app_module.create_app()
    return TestClient(app)


def authenticate_client(client: TestClient) -> None:
    login_response = client.post(
        "/login",
        data={"password": "password", "next": "/payment"},
        follow_redirects=False,
    )
    auth_cookie = login_response.cookies.get("webui_auth")
    session_cookie = login_response.cookies.get("session_id")
    assert auth_cookie
    assert session_cookie
    client.cookies.set("webui_auth", auth_cookie)
    client.cookies.set("session_id", session_cookie)


def get_cookie_value(client: TestClient, name: str) -> str | None:
    value = None
    for cookie in client.cookies.jar:
        if cookie.name == name:
            value = cookie.value
    return value


def authenticated_client(monkeypatch, tmp_path: Path) -> TestClient:
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    return client


def wait_for_task_status(client: TestClient, task_id: str, expected_status: str, timeout: float = 3.0) -> dict:
    deadline = time.time() + timeout
    last_payload = None
    while time.time() < deadline:
        response = client.get(f"/api/payment/tasks/{task_id}")
        assert response.status_code == 200
        last_payload = response.json()
        if last_payload["status"] == expected_status:
            return last_payload
        time.sleep(0.05)
    raise AssertionError(f"task {task_id} did not reach {expected_status}: {last_payload}")


def seed_accounts(count: int = 4) -> list[int]:
    with get_db() as db:
        account_ids = []
        for index in range(count):
            account = crud.create_account(
                db,
                email=f"account-{index}@example.com",
                email_service="tempmail",
                account_id=f"acct-{index}",
                workspace_id=f"ws-{index}",
            )
            account_ids.append(account.id)
        return account_ids


def create_batch_task(
    *,
    owner_key: str,
    session_id: str,
    scope_key: str,
    status: str = "queued",
    task_number: int = 1,
    recent_logs: str = "",
):
    with get_db() as db:
        task = BatchSubscriptionTask(
            task_type=crud.BATCH_SUBSCRIPTION_TASK_TYPE,
            owner_key=owner_key,
            session_id=session_id,
            scope_key=scope_key,
            active_scope_key=scope_key if status in crud.BATCH_SUBSCRIPTION_ACTIVE_STATUSES else None,
            status=status,
            proxy="http://proxy.test",
            total_count=3,
            processed_count=0,
            success_count=0,
            failure_count=0,
            current_account=None,
            request_payload={"ids": [task_number]},
            recent_logs=recent_logs,
        )
        db.add(task)
        db.commit()
        db.refresh(task)
        return task.id


def mark_task_terminal(task_id: int, status: str) -> None:
    with get_db() as db:
        task = crud.mark_batch_subscription_task_terminal(db, task_id, status=status)
        assert task is not None


def test_batch_subscription_creates_task_and_returns_task_id(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(2)

    response = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["task_id"] == "1"
    assert payload["status"] == "queued"
    assert payload["reused"] is False
    assert len(payload["scope_key"]) == 64
    assert all(character in "0123456789abcdef" for character in payload["scope_key"])


def test_repeat_click_same_scope_reuses_active_task(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(2)

    first = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids},
    )
    second = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": list(reversed(account_ids))},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["task_id"] == first.json()["task_id"]
    assert second.json()["status"] == "queued"
    assert second.json()["reused"] is True
    assert second.json()["scope_key"] == first.json()["scope_key"]


def test_proxy_difference_creates_distinct_tasks(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(2)

    first = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids, "proxy": "http://proxy-a.test"},
    )
    second = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids, "proxy": "http://proxy-b.test"},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["task_id"] != second.json()["task_id"]
    assert first.json()["scope_key"] != second.json()["scope_key"]
    assert second.json()["reused"] is False


def test_different_scope_creates_new_task(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(3)

    first = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids[:2]},
    )
    second = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids[1:]},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["task_id"] != second.json()["task_id"]
    assert first.json()["scope_key"] != second.json()["scope_key"]
    assert second.json()["reused"] is False


def test_scope_key_normalizes_same_account_ids_in_different_order(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(3)

    first = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": [account_ids[2], account_ids[0], account_ids[1]]},
    )
    second = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": [account_ids[1], account_ids[2], account_ids[0]]},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["scope_key"] == second.json()["scope_key"]
    assert len(first.json()["scope_key"]) == 64


def test_scope_key_distinguishes_account_set_and_filter_snapshot_scopes(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(3)

    account_scope = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids[:2]},
    )
    filter_scope = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={
            "ids": [],
            "select_all": True,
            "status_filter": "active",
            "email_service_filter": "tempmail",
            "search_filter": "account-",
        },
    )

    assert account_scope.status_code == 200
    assert filter_scope.status_code == 200
    assert account_scope.json()["scope_key"] != filter_scope.json()["scope_key"]


def test_scope_key_is_fixed_length_for_large_selections(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(120)

    response = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids},
    )

    assert response.status_code == 200
    assert len(response.json()["scope_key"]) == 64
    with get_db() as db:
        task = crud.get_batch_subscription_task_by_id(db, int(response.json()["task_id"]))
    assert task is not None
    assert task.scope_key == response.json()["scope_key"]
    assert task.request_payload["ids"] == account_ids


def test_same_scope_is_not_reused_across_different_sessions(monkeypatch, tmp_path):
    first_client = authenticated_client(monkeypatch, tmp_path)
    second_client = authenticated_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(2)

    first_response = first_client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids},
    )
    second_response = second_client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids},
    )

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    assert get_cookie_value(first_client, "session_id") != get_cookie_value(second_client, "session_id")
    assert first_response.json()["scope_key"] == second_response.json()["scope_key"]
    assert first_response.json()["task_id"] != second_response.json()["task_id"]
    assert second_response.json()["reused"] is False


def test_concurrent_same_scope_creation_converges_to_one_active_task(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(3)
    session_id = get_cookie_value(client, "session_id")
    owner_key = crud.build_batch_subscription_owner_key(session_id)
    request_payload = {"ids": [account_ids[2], account_ids[0], account_ids[1]]}

    def create_task_in_isolated_session() -> tuple[int, bool, str]:
        with get_db() as db:
            task, reused = crud.create_or_reuse_batch_subscription_task(
                db,
                owner_key=owner_key,
                scope_key=crud.build_batch_subscription_request_key(
                    account_ids=[account_ids[2], account_ids[0], account_ids[1]],
                    select_all=False,
                    status_filter=None,
                    email_service_filter=None,
                    search_filter=None,
                    proxy="http://proxy.test",
                ),
                total_count=len(account_ids),
                proxy="http://proxy.test",
                session_id=session_id,
                request_payload=request_payload,
            )
            return task.id, reused, task.scope_key

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(create_task_in_isolated_session)
        second_future = executor.submit(create_task_in_isolated_session)

    first_task_id, first_reused, first_scope_key = first_future.result()
    second_task_id, second_reused, second_scope_key = second_future.result()

    with get_db() as db:
        tasks = crud.list_batch_subscription_tasks_for_owner(db, owner_key=owner_key)

    assert first_task_id == second_task_id
    assert first_scope_key == second_scope_key
    assert sorted([first_reused, second_reused]) == [False, True]
    assert len(tasks) == 1


def test_backfill_always_ensures_active_scope_unique_index_for_existing_database(tmp_path):
    db_path = tmp_path / "existing-upgrade.db"
    connection = sqlite3.connect(db_path)
    connection.execute(
        "CREATE TABLE batch_subscription_tasks ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "task_type VARCHAR(50) NOT NULL DEFAULT 'batch_subscription_check', "
        "owner_key VARCHAR(255) NOT NULL DEFAULT 'anonymous', "
        "session_id VARCHAR(255), "
        "scope_key VARCHAR(64) NOT NULL DEFAULT '', "
        "active_scope_key VARCHAR(64), "
        "status VARCHAR(20) NOT NULL DEFAULT 'queued', "
        "proxy VARCHAR(255), "
        "total_count INTEGER NOT NULL DEFAULT 0, "
        "processed_count INTEGER NOT NULL DEFAULT 0, "
        "success_count INTEGER NOT NULL DEFAULT 0, "
        "failure_count INTEGER NOT NULL DEFAULT 0, "
        "current_account VARCHAR(255), "
        "request_payload TEXT, "
        "recent_logs TEXT, "
        "started_at DATETIME, "
        "completed_at DATETIME, "
        "updated_at DATETIME)"
    )
    connection.execute(
        "INSERT INTO batch_subscription_tasks (task_type, owner_key, scope_key, active_scope_key, status) VALUES (?, ?, ?, ?, ?)",
        (crud.BATCH_SUBSCRIPTION_TASK_TYPE, "session:active", "active-scope-key", None, "queued"),
    )
    connection.execute(
        "INSERT INTO batch_subscription_tasks (task_type, owner_key, scope_key, active_scope_key, status) VALUES (?, ?, ?, ?, ?)",
        (crud.BATCH_SUBSCRIPTION_TASK_TYPE, "session:terminal", "terminal-scope-key", "stale-terminal-slot", "completed"),
    )
    connection.commit()
    connection.close()

    db_manager = session_module.DatabaseSessionManager(f"sqlite:///{db_path}")
    _backfill_batch_subscription_task_columns(db_manager)

    with sqlite3.connect(db_path) as verified_connection:
        indexes = verified_connection.execute("PRAGMA index_list('batch_subscription_tasks')").fetchall()
        rows = verified_connection.execute(
            "SELECT owner_key, scope_key, active_scope_key, status FROM batch_subscription_tasks ORDER BY owner_key"
        ).fetchall()

    assert any(index[1] == "uq_batch_subscription_tasks_owner_active_scope" for index in indexes)
    assert rows == [
        ("session:active", "active-scope-key", "active-scope-key", "queued"),
        ("session:terminal", "terminal-scope-key", None, "completed"),
    ]


def test_backfill_safely_resolves_legacy_duplicate_active_rows_before_unique_index(tmp_path):
    db_path = tmp_path / "legacy-duplicate-active.db"
    connection = sqlite3.connect(db_path)
    connection.execute(
        "CREATE TABLE batch_subscription_tasks ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "task_type VARCHAR(50) NOT NULL DEFAULT 'batch_subscription_check', "
        "owner_key VARCHAR(255) NOT NULL DEFAULT 'anonymous', "
        "session_id VARCHAR(255), "
        "scope_key VARCHAR(64), "
        "active_scope_key VARCHAR(64), "
        "status VARCHAR(20) NOT NULL DEFAULT 'queued', "
        "proxy VARCHAR(255), "
        "total_count INTEGER NOT NULL DEFAULT 0, "
        "processed_count INTEGER NOT NULL DEFAULT 0, "
        "success_count INTEGER NOT NULL DEFAULT 0, "
        "failure_count INTEGER NOT NULL DEFAULT 0, "
        "current_account VARCHAR(255), "
        "request_payload TEXT, "
        "recent_logs TEXT, "
        "started_at DATETIME, "
        "completed_at DATETIME, "
        "updated_at DATETIME)"
    )
    legacy_payload = json.dumps({"ids": [3, 1, 2], "proxy": "http://proxy.test"})
    connection.execute(
        "INSERT INTO batch_subscription_tasks (task_type, owner_key, scope_key, active_scope_key, status, request_payload) VALUES (?, ?, ?, ?, ?, ?)",
        (crud.BATCH_SUBSCRIPTION_TASK_TYPE, "session:dup", None, None, "queued", legacy_payload),
    )
    connection.execute(
        "INSERT INTO batch_subscription_tasks (task_type, owner_key, scope_key, active_scope_key, status, request_payload) VALUES (?, ?, ?, ?, ?, ?)",
        (crud.BATCH_SUBSCRIPTION_TASK_TYPE, "session:dup", "", None, "running", legacy_payload),
    )
    connection.commit()
    connection.close()

    db_manager = session_module.DatabaseSessionManager(f"sqlite:///{db_path}")
    _backfill_batch_subscription_task_columns(db_manager)

    with sqlite3.connect(db_path) as verified_connection:
        rows = verified_connection.execute(
            "SELECT id, scope_key, active_scope_key, status FROM batch_subscription_tasks ORDER BY id"
        ).fetchall()
        indexes = verified_connection.execute("PRAGMA index_list('batch_subscription_tasks')").fetchall()

    assert any(index[1] == "uq_batch_subscription_tasks_owner_active_scope" for index in indexes)
    assert rows[0][1]
    assert rows[1][1]
    assert rows[0][1] == rows[1][1]
    assert rows[0][2] != rows[1][2]


def test_batch_check_subscription_request_ids_default_is_not_shared():
    first = payment_route_module.BatchCheckSubscriptionRequest()
    second = payment_route_module.BatchCheckSubscriptionRequest()

    first.ids.append(123)

    assert first.ids == [123]
    assert second.ids == []


def test_completed_task_releases_active_scope_for_same_scope_rerun(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(2)

    first = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids},
    )
    mark_task_terminal(int(first.json()["task_id"]), "completed")

    second = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": list(reversed(account_ids))},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["task_id"] != first.json()["task_id"]
    assert second.json()["scope_key"] == first.json()["scope_key"]
    assert second.json()["reused"] is False


def test_terminal_tasks_do_not_hold_active_uniqueness_slot(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(2)
    session_id = get_cookie_value(client, "session_id")
    owner_key = crud.build_batch_subscription_owner_key(session_id)
    request_key = crud.build_batch_subscription_request_key(
        account_ids=account_ids,
        select_all=False,
        status_filter=None,
        email_service_filter=None,
        search_filter=None,
        proxy="http://proxy.test",
    )

    original_task_id = create_batch_task(
        owner_key=owner_key,
        session_id=session_id,
        scope_key=request_key,
        status="completed",
    )
    mark_task_terminal(original_task_id, "completed")

    with get_db() as db:
        replacement, reused = crud.create_or_reuse_batch_subscription_task(
            db,
            owner_key=owner_key,
            scope_key=request_key,
            total_count=len(account_ids),
            proxy="http://proxy.test",
            session_id=session_id,
            request_payload={"ids": account_ids, "request_key": request_key},
        )

    assert replacement.id != original_task_id
    assert replacement.scope_key == request_key
    assert reused is False

    with get_db() as db:
        original_task = crud.get_batch_subscription_task_by_id(db, original_task_id, owner_key=owner_key)

    assert original_task is not None
    assert original_task.active_scope_key is None


def test_batch_subscription_requires_authentication(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(1)

    response = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids},
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Unauthorized"}


def test_restart_coerces_running_batch_task_to_interrupted_or_failed_terminal_state(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    session_id = get_cookie_value(client, "session_id")
    owner_key = crud.build_batch_subscription_owner_key(session_id)
    task_id = create_batch_task(
        owner_key=owner_key,
        session_id=session_id,
        scope_key="account_ids:1,2",
        status="running",
    )

    restarted_client = build_client(monkeypatch, tmp_path)
    restarted_client.get("/login")
    web_app_module.reconcile_startup_batch_subscription_tasks()

    with get_db() as db:
        task = crud.get_batch_subscription_task_by_id(db, task_id, owner_key=owner_key)

    assert task is not None
    assert task.status == "interrupted"
    assert task.active_scope_key is None
    assert task.completed_at is not None


def test_restart_coerces_queued_batch_task_to_interrupted_terminal_state(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    session_id = get_cookie_value(client, "session_id")
    owner_key = crud.build_batch_subscription_owner_key(session_id)
    request_key = crud.build_batch_subscription_request_key(
        account_ids=[1, 2],
        select_all=False,
        status_filter=None,
        email_service_filter=None,
        search_filter=None,
        proxy="http://proxy.test",
    )
    task_id = create_batch_task(
        owner_key=owner_key,
        session_id=session_id,
        scope_key=request_key,
        status="queued",
    )

    with get_db() as db:
        task = crud.get_batch_subscription_task_by_id(db, task_id, owner_key=owner_key)
        task.active_scope_key = request_key
        task.current_account = "acct-1"
        db.commit()

    restarted_client = build_client(monkeypatch, tmp_path)
    restarted_client.get("/login")
    web_app_module.reconcile_startup_batch_subscription_tasks()

    with get_db() as db:
        task = crud.get_batch_subscription_task_by_id(db, task_id, owner_key=owner_key)

    assert task is not None
    assert task.status == "interrupted"
    assert task.active_scope_key is None
    assert task.current_account is None
    assert task.completed_at is not None


def test_same_scope_rerun_not_blocked_after_orphaned_queued_task_reconciliation(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(2)
    first = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids},
    )

    restarted_client = build_client(monkeypatch, tmp_path)
    restarted_client.cookies.update(client.cookies)
    web_app_module.reconcile_startup_batch_subscription_tasks()
    second = restarted_client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": list(reversed(account_ids))},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["task_id"] != first.json()["task_id"]
    assert second.json()["scope_key"] == first.json()["scope_key"]
    assert second.json()["reused"] is False


def test_latest_active_batch_task_excludes_interrupted_terminal_tasks(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    session_id = get_cookie_value(client, "session_id")
    owner_key = crud.build_batch_subscription_owner_key(session_id)
    create_batch_task(
        owner_key=owner_key,
        session_id=session_id,
        scope_key="account_ids:1,2",
        status="interrupted",
        task_number=1,
    )
    active_task_id = create_batch_task(
        owner_key=owner_key,
        session_id=session_id,
        scope_key="account_ids:3,4",
        status="running",
        task_number=2,
    )

    with get_db() as db:
        latest_active = crud.get_latest_active_batch_subscription_task(db, owner_key=owner_key)

    assert latest_active is not None
    assert latest_active.id == active_task_id
    assert latest_active.status == "running"


def test_batch_task_retention_caps_old_tasks_and_log_lines(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    session_id = get_cookie_value(client, "session_id")
    owner_key = crud.build_batch_subscription_owner_key(session_id)
    active_scope_keys = ["account_ids:active-running", "account_ids:active-queued"]

    with get_db() as db:
        create_batch_task(
            owner_key=owner_key,
            session_id=session_id,
            scope_key=active_scope_keys[0],
            status="running",
        )
        create_batch_task(
            owner_key=owner_key,
            session_id=session_id,
            scope_key=active_scope_keys[1],
            status="queued",
        )
        for task_number in range(55):
            task_id = create_batch_task(
                owner_key=owner_key,
                session_id=session_id,
                scope_key=f"account_ids:{task_number}",
                status="completed",
            )
            if task_number == 54:
                retained_task_id = task_id

        crud.create_or_reuse_batch_subscription_task(
            db,
            owner_key=owner_key,
            scope_key="account_ids:new-active-queued",
            total_count=1,
            proxy="http://proxy.test",
            session_id=session_id,
            request_payload={"ids": [999]},
        )

    log_lines = [f"line-{index}" for index in range(550)]
    with get_db() as db:
        retained_task = crud.get_batch_subscription_task_by_id(db, retained_task_id, owner_key=owner_key)
        crud.append_batch_subscription_task_logs(db, retained_task.id, log_lines)
        kept_tasks = crud.list_batch_subscription_tasks_for_owner(db, owner_key=owner_key)
        refreshed_task = crud.get_batch_subscription_task_by_id(db, retained_task.id, owner_key=owner_key)

    kept_scope_keys = [task.scope_key for task in kept_tasks]
    assert len(kept_tasks) == 53
    assert all(scope_key in kept_scope_keys for scope_key in active_scope_keys)
    assert "account_ids:new-active-queued" in kept_scope_keys
    assert all(f"account_ids:{index}" not in kept_scope_keys for index in range(5))
    assert all(f"account_ids:{index}" in kept_scope_keys for index in range(5, 55))
    assert refreshed_task is not None
    assert refreshed_task.recent_logs.splitlines() == [f"line-{index}" for index in range(50, 550)]


def test_completed_batch_task_remains_queryable_after_restart(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    session_id = get_cookie_value(client, "session_id")
    owner_key = crud.build_batch_subscription_owner_key(session_id)
    task_id = create_batch_task(
        owner_key=owner_key,
        session_id=session_id,
        scope_key="account_ids:9,10",
        status="completed",
    )

    restarted_client = build_client(monkeypatch, tmp_path)
    restarted_client.get("/login")
    web_app_module.reconcile_startup_batch_subscription_tasks()

    with get_db() as db:
        task = crud.get_batch_subscription_task_by_id(db, task_id, owner_key=owner_key)

    assert task is not None
    assert task.status == "completed"


def test_get_batch_task_detail_returns_progress_and_log_offsets(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    session_id = web_auth_module.parse_session_cookie(get_cookie_value(client, "session_id"))
    assert session_id is not None
    owner_key = crud.build_batch_subscription_owner_key(session_id)
    task_id = create_batch_task(
        owner_key=owner_key,
        session_id=session_id,
        scope_key="normalized-scope-key",
        status="running",
    )

    with get_db() as db:
        task = crud.get_batch_subscription_task_by_id(db, task_id, owner_key=owner_key)
        assert task is not None
        task.total_count = 8
        task.processed_count = 3
        task.success_count = 2
        task.failure_count = 1
        task.current_account = "acct-3@example.com"
        task.set_recent_log_lines(["queued", "running acct-1", "running acct-3", "failed acct-2"])
        db.commit()

    response = client.get(f"/api/payment/tasks/{task_id}?log_offset=2")

    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "task_id": str(task_id),
        "status": "running",
        "scope_key": "normalized-scope-key",
        "total_count": 8,
        "processed_count": 3,
        "success_count": 2,
        "failure_count": 1,
        "current_account": "acct-3@example.com",
        "progress_percent": 38,
        "logs": ["running acct-3", "failed acct-2"],
        "next_log_offset": 4,
    }


def test_latest_active_batch_task_is_session_scoped(monkeypatch, tmp_path):
    first_client = authenticated_client(monkeypatch, tmp_path)
    second_client = authenticated_client(monkeypatch, tmp_path)

    first_session_id = web_auth_module.parse_session_cookie(get_cookie_value(first_client, "session_id"))
    assert first_session_id is not None
    first_owner_key = crud.build_batch_subscription_owner_key(first_session_id)
    second_session_id = web_auth_module.parse_session_cookie(get_cookie_value(second_client, "session_id"))
    assert second_session_id is not None
    second_owner_key = crud.build_batch_subscription_owner_key(second_session_id)

    create_batch_task(
        owner_key=first_owner_key,
        session_id=first_session_id,
        scope_key="first-scope",
        status="running",
        task_number=1,
    )
    second_task_id = create_batch_task(
        owner_key=second_owner_key,
        session_id=second_session_id,
        scope_key="second-scope",
        status="queued",
        task_number=2,
    )

    scoped_response = first_client.get(
        "/api/payment/tasks/latest",
        params={
            "type": crud.BATCH_SUBSCRIPTION_TASK_TYPE,
            "status": "active",
            "scope": "first-scope",
        },
    )
    restore_response = second_client.get(
        "/api/payment/tasks/latest-active",
        params={"type": crud.BATCH_SUBSCRIPTION_TASK_TYPE},
    )

    assert scoped_response.status_code == 200
    assert scoped_response.json() == {
        "task_id": "1",
        "status": "running",
        "scope_key": "first-scope",
    }
    assert restore_response.status_code == 200
    assert restore_response.json() == {
        "task_id": str(second_task_id),
        "status": "queued",
        "scope_key": "second-scope",
    }


def test_get_batch_task_rejects_other_sessions_task(monkeypatch, tmp_path):
    owner_client = authenticated_client(monkeypatch, tmp_path)
    other_client = authenticated_client(monkeypatch, tmp_path)

    session_id = web_auth_module.parse_session_cookie(get_cookie_value(owner_client, "session_id"))
    assert session_id is not None
    owner_key = crud.build_batch_subscription_owner_key(session_id)
    task_id = create_batch_task(
        owner_key=owner_key,
        session_id=session_id,
        scope_key="owner-scope",
        status="running",
    )

    response = other_client.get(f"/api/payment/tasks/{task_id}")

    assert response.status_code == 404
    assert response.json() == {"detail": "任务不存在"}


def test_batch_subscription_task_updates_progress_and_logs_per_account(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(2)

    outcomes = {
        account_ids[0]: "plus",
        account_ids[1]: "team",
    }

    def fake_check_subscription_status(account, proxy=None):
        return outcomes[account.id]

    monkeypatch.setattr(payment_route_module, "check_subscription_status", fake_check_subscription_status)
    monkeypatch.setattr(
        payment_route_module,
        "dispatch_batch_subscription_task",
        lambda task_id, *, check_subscription_status_fn: payment_route_module.start_batch_subscription_check_task(
            task_id,
            check_subscription_status_fn=check_subscription_status_fn,
        ),
    )

    response = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "queued"

    completed = wait_for_task_status(client, payload["task_id"], "completed")

    assert completed["total_count"] == 2
    assert completed["processed_count"] == 2
    assert completed["success_count"] == 2
    assert completed["failure_count"] == 0
    assert completed["current_account"] is None
    assert completed["progress_percent"] == 100
    assert completed["logs"] == [
        f"checking account {account_ids[0]}",
        f"account {account_ids[0]} subscription updated to plus",
        f"checking account {account_ids[1]}",
        f"account {account_ids[1]} subscription updated to team",
        "batch subscription task completed",
    ]

    with get_db() as db:
        first_account = crud.get_account_by_id(db, account_ids[0])
        second_account = crud.get_account_by_id(db, account_ids[1])

    assert first_account is not None
    assert second_account is not None
    assert first_account.subscription_type == "plus"
    assert second_account.subscription_type == "team"


def test_single_account_failure_does_not_fail_whole_batch_task(monkeypatch, tmp_path):
    client = authenticated_client(monkeypatch, tmp_path)
    account_ids = seed_accounts(3)

    def fake_check_subscription_status(account, proxy=None):
        if account.id == account_ids[1]:
            raise RuntimeError("proxy timeout")
        return "plus" if account.id == account_ids[0] else "free"

    monkeypatch.setattr(payment_route_module, "check_subscription_status", fake_check_subscription_status)
    monkeypatch.setattr(
        payment_route_module,
        "dispatch_batch_subscription_task",
        lambda task_id, *, check_subscription_status_fn: payment_route_module.start_batch_subscription_check_task(
            task_id,
            check_subscription_status_fn=check_subscription_status_fn,
        ),
    )

    response = client.post(
        "/api/payment/accounts/batch-check-subscription",
        json={"ids": account_ids},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "queued"

    completed = wait_for_task_status(client, payload["task_id"], "completed")

    assert completed["total_count"] == 3
    assert completed["processed_count"] == 3
    assert completed["success_count"] == 2
    assert completed["failure_count"] == 1
    assert completed["progress_percent"] == 100
    assert completed["logs"] == [
        f"checking account {account_ids[0]}",
        f"account {account_ids[0]} subscription updated to plus",
        f"checking account {account_ids[1]}",
        f"account {account_ids[1]} subscription check failed: proxy timeout",
        f"checking account {account_ids[2]}",
        f"account {account_ids[2]} subscription updated to free",
        "batch subscription task completed",
    ]

    with get_db() as db:
        first_account = crud.get_account_by_id(db, account_ids[0])
        failed_account = crud.get_account_by_id(db, account_ids[1])
        third_account = crud.get_account_by_id(db, account_ids[2])

    assert first_account is not None
    assert failed_account is not None
    assert third_account is not None
    assert first_account.subscription_type == "plus"
    assert failed_account.subscription_type is None
    assert third_account.subscription_type is None
