from datetime import datetime, timedelta
from pathlib import Path
import importlib
import threading

from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

web_app_module = importlib.import_module("src.web.app")
web_auth_module = importlib.import_module("src.web.auth")
cliproxy_routes_module = importlib.import_module("src.web.routes.cliproxy")
cliproxy_maintenance_module = importlib.import_module("src.core.cliproxy.maintenance")
from src.database import crud
from src.database.init_db import initialize_database
from src.database.models import Base, CLIProxyAPIEnvironment, RemoteAuthInventory, MaintenanceActionLog, MaintenanceRun, AuditLog, Account
from src.database.session import DatabaseSessionManager
import src.database.session as session_module


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

    return DummySettings()


def build_client(monkeypatch, tmp_path: Path) -> TestClient:
    templates_dir = tmp_path / "templates"
    static_dir = tmp_path / "static"
    templates_dir.mkdir()
    static_dir.mkdir()

    for name in ["login.html", "index.html", "accounts.html", "email_services.html", "settings.html", "payment.html"]:
        (templates_dir / name).write_text("<html><body>ok</body></html>", encoding="utf-8")

    db_path = tmp_path / "cliproxy_routes.db"
    monkeypatch.setenv("APP_DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("CLIPROXY_ENCRYPTION_KEY", make_fernet_key())
    monkeypatch.setattr(session_module, "_db_manager", None)
    monkeypatch.setattr(web_app_module, "STATIC_DIR", static_dir)
    monkeypatch.setattr(web_app_module, "TEMPLATES_DIR", templates_dir)
    monkeypatch.setattr(web_app_module, "get_settings", lambda: make_settings(tmp_path))
    monkeypatch.setattr(web_auth_module, "get_settings", lambda: make_settings(tmp_path))
    initialize_database(f"sqlite:///{db_path}")
    app = web_app_module.create_app()
    return TestClient(app)


def authenticate_client(client: TestClient) -> None:
    login_response = client.post(
        "/login",
        data={"password": "password", "next": "/accounts"},
        follow_redirects=False,
    )
    cookie = login_response.cookies.get("webui_auth")
    assert cookie
    client.cookies.set("webui_auth", cookie)


def create_environment_via_api(client: TestClient, name: str = "primary") -> dict:
    response = client.post(
        "/api/cliproxy-environments",
        json={
            "name": name,
            "base_url": "https://cliproxy.example.com",
            "token": "cliproxy-secret-token",
            "target_type": "cpa",
            "provider": "cloudmail",
        },
    )
    assert response.status_code == 200
    return response.json()


def stub_cliproxy_background_dispatch(monkeypatch) -> None:
    async def _noop(run_id: int) -> None:
        return None

    monkeypatch.setattr(cliproxy_routes_module, "_dispatch_maintenance_job", _noop)


class FakeRouteCLIProxyClient:
    def __init__(self, base_url: str, token: str = "", timeout: int = 30):
        self.base_url = base_url
        self.token = token
        self.timeout = timeout

    def fetch_inventory(self):
        return [
            {
                "remote_file_id": "file-1",
                "remote_email": "one@example.com",
                "remote_account_id": "acct-1",
            }
        ]

    def probe_usage(self, remote_file_id: str):
        return {"status_code": 401}

    def disable_auth(self, remote_file_id: str):
        return {"status": "disabled"}

    def reenable_auth(self, remote_file_id: str):
        return {"status": "reenabled"}


def test_unauthorized_access_rejected_for_cliproxy_routes(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)

    response = client.get("/api/cliproxy-environments")

    assert response.status_code == 401


def test_create_environment_masks_token_in_api_response(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    response = client.post(
        "/api/cliproxy-environments",
        json={
            "name": "primary",
            "base_url": "https://cliproxy.example.com",
            "token": "cliproxy-secret-token",
            "target_type": "cpa",
            "provider": "cloudmail",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["has_token"] is True
    assert payload["token_masked"].startswith("clip")
    assert payload["token_masked"].endswith("oken")
    assert "token" not in payload
    assert "cliproxy-secret-token" not in str(payload)


def test_environment_list_masks_token_in_api_response(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    create_environment_via_api(client, name="listed")

    response = client.get("/api/cliproxy-environments")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["name"] == "listed"
    assert payload[0]["has_token"] is True
    assert payload[0]["token_masked"].startswith("clip")
    assert payload[0]["token_masked"].endswith("oken")
    assert "token" not in payload[0]
    assert "cliproxy-secret-token" not in str(payload[0])


def test_environment_detail_masks_token_in_api_response(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    created = create_environment_via_api(client, name="detailed")

    response = client.get(f"/api/cliproxy-environments/{created['id']}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["name"] == "detailed"
    assert payload["has_token"] is True
    assert payload["token_masked"].startswith("clip")
    assert payload["token_masked"].endswith("oken")
    assert "token" not in payload
    assert "cliproxy-secret-token" not in str(payload)


def test_replace_token_does_not_echo_secret(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    created = client.post(
        "/api/cliproxy-environments",
        json={
            "name": "primary",
            "base_url": "https://cliproxy.example.com",
            "token": "old-secret-token",
            "target_type": "cpa",
            "provider": "cloudmail",
        },
    )
    environment_id = created.json()["id"]

    response = client.patch(
        f"/api/cliproxy-environments/{environment_id}",
        json={"token": "new-secret-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["has_token"] is True
    assert payload["token_masked"] != "new-secret-token"
    assert "new-secret-token" not in str(payload)
    assert "token" not in payload


def test_replace_token_route_contract_masks_secret(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    created = client.post(
        "/api/cliproxy-environments",
        json={
            "name": "token-route",
            "base_url": "https://cliproxy.example.com",
            "token": "old-route-secret",
            "target_type": "cpa",
            "provider": "cloudmail",
        },
    )
    environment_id = created.json()["id"]

    response = client.post(
        f"/api/cliproxy-environments/{environment_id}/token",
        json={"token": "new-route-secret"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == environment_id
    assert payload["has_token"] is True
    assert payload["token_masked"]
    assert payload["token_masked"] != "new-route-secret"
    assert "token" not in payload
    assert "new-route-secret" not in str(payload)


def test_only_one_environment_can_be_default_at_a_time(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    first = client.post(
        "/api/cliproxy-environments",
        json={
            "name": "primary",
            "base_url": "https://cliproxy.example.com",
            "token": "secret-1",
            "target_type": "cpa",
            "provider": "cloudmail",
            "is_default": True,
        },
    ).json()
    second = client.post(
        "/api/cliproxy-environments",
        json={
            "name": "secondary",
            "base_url": "https://cliproxy-2.example.com",
            "token": "secret-2",
            "target_type": "tm",
            "provider": "tempmail",
            "is_default": True,
        },
    ).json()

    first_get = client.get(f"/api/cliproxy-environments/{first['id']}")
    second_get = client.get(f"/api/cliproxy-environments/{second['id']}")

    assert first_get.status_code == 200
    assert second_get.status_code == 200
    assert first_get.json()["is_default"] is False
    assert second_get.json()["is_default"] is True


def test_environment_scope_fields_round_trip_in_api_contract(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    create_response = client.post(
        "/api/cliproxy-environments",
        json={
            "name": "scoped",
            "base_url": "https://cliproxy.example.com",
            "token": "scope-secret-token",
            "target_type": "cpa",
            "provider": "cloudmail",
            "provider_scope": "provider-a",
            "target_scope": "target-b",
            "scope_rules_json": {"team": "alpha", "regions": ["us", "eu"]},
        },
    )

    assert create_response.status_code == 200
    payload = create_response.json()
    assert payload["provider_scope"] == "provider-a"
    assert payload["target_scope"] == "target-b"
    assert payload["scope_rules_json"] == {"team": "alpha", "regions": ["us", "eu"]}


def test_refill_boundary_is_reserved_in_v1(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    environment = create_environment_via_api(client, name="reserved-refill")

    detail_response = client.get(f"/api/cliproxy-environments/{environment['id']}")
    refill_response = client.post(
        f"/api/cliproxy-environments/{environment['id']}/refill",
        json={},
    )

    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["maintenance_contract"]["refill"] == {
        "state": "reserved",
        "enabled": False,
        "version": "v1",
    }

    assert refill_response.status_code == 501
    assert refill_response.json()["detail"] == "CLIProxy refill is reserved in v1 and not enabled"


def test_duplicate_environment_names_handled_deterministically(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    first = client.post(
        "/api/cliproxy-environments",
        json={
            "name": "primary",
            "base_url": "https://cliproxy.example.com",
            "token": "secret-1",
            "target_type": "cpa",
            "provider": "cloudmail",
        },
    )
    second = client.post(
        "/api/cliproxy-environments",
        json={
            "name": "primary",
            "base_url": "https://cliproxy-2.example.com",
            "token": "secret-2",
            "target_type": "tm",
            "provider": "tempmail",
        },
    )

    assert first.status_code == 200
    assert second.status_code == 409
    assert second.json()["detail"] == "CLIProxy environment name already exists"


def test_create_environment_returns_controlled_error_for_missing_cliproxy_key(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    monkeypatch.setenv("CLIPROXY_ENCRYPTION_KEY", "")

    response = client.post(
        "/api/cliproxy-environments",
        json={
            "name": "broken-key",
            "base_url": "https://cliproxy.example.com",
            "token": "secret-1",
            "target_type": "cpa",
            "provider": "cloudmail",
        },
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "CLIProxy encryption key is not configured correctly"


def test_update_environment_returns_controlled_error_for_invalid_cliproxy_key(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    created = client.post(
        "/api/cliproxy-environments",
        json={
            "name": "primary",
            "base_url": "https://cliproxy.example.com",
            "token": "secret-1",
            "target_type": "cpa",
            "provider": "cloudmail",
        },
    )
    environment_id = created.json()["id"]

    monkeypatch.setenv("CLIPROXY_ENCRYPTION_KEY", "not-a-fernet-key")
    response = client.patch(
        f"/api/cliproxy-environments/{environment_id}",
        json={"token": "secret-2"},
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "CLIProxy encryption key is not configured correctly"


def test_scan_upserts_remote_inventory_by_environment_and_remote_file_id(tmp_path):
    db_path = tmp_path / "inventory_upsert.db"
    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as db:
        environment = CLIProxyAPIEnvironment(
            name="primary",
            base_url="https://cliproxy.example.com",
            target_type="cpa",
            provider="cloudmail",
        )
        db.add(environment)
        db.flush()

        first = crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="file-1",
            email="one@example.com",
            payload_json={"status": "new"},
        )
        second = crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="file-1",
            email="two@example.com",
            payload_json={"status": "updated"},
        )

        rows = db.query(RemoteAuthInventory).all()

        assert first.id == second.id
        assert len(rows) == 1
        assert rows[0].email == "two@example.com"
        assert rows[0].payload_json == {"status": "updated"}


def test_maintenance_action_log_crud_helpers(tmp_path):
    db_path = tmp_path / "maintenance_action_log.db"
    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as db:
        environment = CLIProxyAPIEnvironment(
            name="primary",
            base_url="https://cliproxy.example.com",
            target_type="cpa",
            provider="cloudmail",
        )
        db.add(environment)
        db.flush()

        run = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment.id,
            status="running",
        )

        action_log = crud.create_maintenance_action_log(
            db,
            run_id=run.id,
            environment_id=environment.id,
            action_type="scan_file",
            status="pending",
            remote_file_id="file-1",
            message="queued",
            details_json={"attempt": 1},
        )

        fetched = crud.get_maintenance_action_log_by_id(db, action_log.id)
        updated = crud.update_maintenance_action_log(
            db,
            action_log.id,
            status="completed",
            message="done",
        )
        environment_logs = crud.get_maintenance_action_logs(db, environment_id=environment.id)
        run_logs = crud.get_maintenance_action_logs(db, run_id=run.id)
        rows = db.query(MaintenanceActionLog).all()

        assert fetched is not None
        assert fetched.id == action_log.id
        assert updated is not None
        assert updated.status == "completed"
        assert updated.message == "done"
        assert len(environment_logs) == 1
        assert len(run_logs) == 1
        assert rows[0].action_type == "scan_file"


def test_refill_run_type_is_rejected_by_backend_run_creation_in_v1(tmp_path):
    db_path = tmp_path / "refill_run_rejected.db"
    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as db:
        environment = CLIProxyAPIEnvironment(
            name="primary",
            base_url="https://cliproxy.example.com",
            target_type="cpa",
            provider="cloudmail",
        )
        db.add(environment)
        db.flush()

        try:
            crud.create_maintenance_run(
                db,
                run_type="refill",
                environment_id=environment.id,
                status="queued",
            )
        except ValueError as exc:
            assert str(exc) == "CLIProxy refill is reserved in v1 and not enabled"
        else:
            raise AssertionError("expected refill create_maintenance_run to be rejected")

        try:
            crud.create_maintenance_run_if_available(
                db,
                run_type="refill",
                environment_id=environment.id,
                request_data={"idempotency_key": "reserved-refill"},
            )
        except ValueError as exc:
            assert str(exc) == "CLIProxy refill is reserved in v1 and not enabled"
        else:
            raise AssertionError("expected refill create_maintenance_run_if_available to be rejected")


def test_second_scan_run_returns_409_when_environment_has_running_job(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    environment = create_environment_via_api(client)

    with session_module.get_db() as db:
        crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment["id"],
            status="running",
        )

    response = client.post(f"/api/cliproxy-environments/{environment['id']}/scan", json={})

    assert response.status_code == 409
    assert response.json()["detail"] == "CLIProxy environment already has an in-flight maintenance run"


def test_second_scan_run_returns_409_when_environment_has_queued_job(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    environment = create_environment_via_api(client)

    with session_module.get_db() as db:
        crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment["id"],
            status="queued",
        )

    response = client.post(f"/api/cliproxy-environments/{environment['id']}/scan", json={})

    assert response.status_code == 409
    assert response.json()["detail"] == "CLIProxy environment already has an in-flight maintenance run"


def test_scan_returns_existing_run_when_idempotency_key_matches_within_window(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    environment = create_environment_via_api(client)
    request_json = {"idempotency_key": "scan-123"}

    with session_module.get_db() as db:
        existing = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment["id"],
            status="completed",
            summary_json={"request": request_json},
        )
        crud.update_maintenance_run(
            db,
            existing.id,
            created_at=datetime.utcnow() - timedelta(minutes=5),
            current_stage="completed",
            progress_percent=100,
            cancellable=False,
        )

    response = client.post(f"/api/cliproxy-environments/{environment['id']}/scan", json=request_json)

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == existing.id
    assert payload["run_type"] == "scan"
    assert payload["status"] == "completed"
    assert payload["progress_percent"] == 100
    assert payload["cancellable"] is False


def test_same_idempotency_key_can_exist_for_scan_and_maintain_separately(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    environment = create_environment_via_api(client)
    request_json = {"idempotency_key": "shared-key", "dry_run": True}

    with session_module.get_db() as db:
        scan_run = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment["id"],
            status="completed",
            summary_json={"request": {"idempotency_key": "shared-key"}},
        )
        crud.update_maintenance_run(
            db,
            scan_run.id,
            created_at=datetime.utcnow() - timedelta(minutes=5),
            current_stage="completed",
            progress_percent=100,
            cancellable=False,
        )

    response = client.post(f"/api/cliproxy-environments/{environment['id']}/maintain", json=request_json)

    assert response.status_code == 202
    payload = response.json()
    assert payload["run_type"] == "maintain"
    assert payload["id"] != scan_run.id


def test_same_idempotency_key_with_different_request_params_does_not_replay(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    environment = create_environment_via_api(client)

    with session_module.get_db() as db:
        existing = crud.create_maintenance_run(
            db,
            run_type="maintain",
            environment_id=environment["id"],
            status="completed",
            summary_json={"request": {"idempotency_key": "same-key", "dry_run": False}},
        )
        crud.update_maintenance_run(
            db,
            existing.id,
            created_at=datetime.utcnow() - timedelta(minutes=5),
            current_stage="completed",
            progress_percent=100,
            cancellable=False,
        )

    response = client.post(
        f"/api/cliproxy-environments/{environment['id']}/maintain",
        json={"idempotency_key": "same-key", "dry_run": True},
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["id"] != existing.id
    assert payload["summary_json"]["request"] == {"idempotency_key": "same-key", "dry_run": True}


def test_same_idempotency_key_outside_window_does_not_replay(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    environment = create_environment_via_api(client)
    request_json = {"idempotency_key": "old-key"}

    with session_module.get_db() as db:
        existing = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment["id"],
            status="completed",
            summary_json={"request": request_json},
        )
        crud.update_maintenance_run(
            db,
            existing.id,
            created_at=datetime.utcnow() - timedelta(minutes=11),
            current_stage="completed",
            progress_percent=100,
            cancellable=False,
        )

    response = client.post(f"/api/cliproxy-environments/{environment['id']}/scan", json=request_json)

    assert response.status_code == 202
    payload = response.json()
    assert payload["id"] != existing.id


def test_same_idempotency_key_replays_existing_queued_run(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    environment = create_environment_via_api(client)
    request_json = {"idempotency_key": "queued-key", "dry_run": True}

    with session_module.get_db() as db:
        existing = crud.create_maintenance_run(
            db,
            run_type="maintain",
            environment_id=environment["id"],
            status="queued",
            summary_json={"request": request_json, "current_stage": "queued", "progress_percent": 0, "cancellable": True},
        )

    response = client.post(f"/api/cliproxy-environments/{environment['id']}/maintain", json=request_json)

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == existing.id
    assert payload["status"] == "queued"


def test_same_idempotency_key_replays_existing_running_run(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    environment = create_environment_via_api(client)
    request_json = {"idempotency_key": "running-key"}

    with session_module.get_db() as db:
        existing = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment["id"],
            status="running",
            summary_json={"request": request_json, "current_stage": "probing", "progress_percent": 20, "cancellable": True},
        )

    response = client.post(f"/api/cliproxy-environments/{environment['id']}/scan", json=request_json)

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == existing.id
    assert payload["status"] == "running"


def test_concurrent_creation_attempts_leave_at_most_one_active_run(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    environment = create_environment_via_api(client)
    barrier = threading.Barrier(2)
    results = []

    def post_scan():
        barrier.wait()
        response = client.post(
            f"/api/cliproxy-environments/{environment['id']}/scan",
            json={},
        )
        results.append(response.status_code)

    first = threading.Thread(target=post_scan)
    second = threading.Thread(target=post_scan)
    first.start()
    second.start()
    first.join()
    second.join()

    with session_module.get_db() as db:
        active_runs = db.query(MaintenanceRun).filter(
            MaintenanceRun.environment_id == environment["id"],
            MaintenanceRun.status.in_(["queued", "running", "cancelling"]),
        ).all()

    assert len(active_runs) <= 1
    assert sorted(results).count(202) <= 1


def test_cancelled_queued_run_before_worker_start_is_not_executed(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    environment = create_environment_via_api(client)
    with session_module.get_db() as db:
        run = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment["id"],
            status="queued",
            summary_json={
                "request": {"idempotency_key": "cancel-before-start"},
                "current_stage": "queued",
                "progress_percent": 0,
                "cancellable": True,
            },
        )

    cancel_response = client.post(f"/api/cliproxy-environments/runs/{run.id}/cancel")

    assert cancel_response.status_code == 200
    cliproxy_routes_module._run_maintenance_job(run.id)
    with session_module.get_db() as db:
        refreshed = db.get(MaintenanceRun, run.id)
        assert refreshed is not None
        assert refreshed.status == "cancelled"
        assert refreshed.summary_json["current_stage"] == "cancelled"
        assert refreshed.summary_json["progress_percent"] == 0


def test_cancel_marks_running_run_cancelling_when_supported(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    environment = create_environment_via_api(client)

    with session_module.get_db() as db:
        run = crud.create_maintenance_run(
            db,
            run_type="maintain",
            environment_id=environment["id"],
            status="running",
        )
        crud.update_maintenance_run(
            db,
            run.id,
            current_stage="probing",
            progress_percent=25,
            cancellable=True,
        )

    response = client.post(f"/api/cliproxy-environments/runs/{run.id}/cancel")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == run.id
    assert payload["status"] == "cancelling"
    assert payload["current_stage"] == "cancelling"
    assert payload["progress_percent"] == 25
    assert payload["cancellable"] is True

    with session_module.get_db() as db:
        refreshed = db.get(MaintenanceRun, run.id)
        assert refreshed is not None
        assert refreshed.status == "cancelling"


def test_cancel_marks_queued_run_cancelled_directly(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    environment = create_environment_via_api(client)

    with session_module.get_db() as db:
        run = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment["id"],
            status="queued",
            summary_json={"current_stage": "queued", "progress_percent": 0, "cancellable": True},
        )

    response = client.post(f"/api/cliproxy-environments/runs/{run.id}/cancel")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "cancelled"
    assert payload["current_stage"] == "cancelled"
    assert payload["progress_percent"] == 0
    assert payload["cancellable"] is False


def test_cancel_marks_running_run_cancelling(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    environment = create_environment_via_api(client)

    with session_module.get_db() as db:
        run = crud.create_maintenance_run(
            db,
            run_type="maintain",
            environment_id=environment["id"],
            status="running",
            summary_json={"current_stage": "probing", "progress_percent": 25, "cancellable": True},
        )

    response = client.post(f"/api/cliproxy-environments/runs/{run.id}/cancel")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "cancelling"
    assert payload["current_stage"] == "cancelling"
    assert payload["progress_percent"] == 25
    assert payload["cancellable"] is True


def test_post_environment_creates_environment_without_returning_token(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    response = client.post(
        "/api/cliproxy-environments",
        json={
            "name": "created-no-token-echo",
            "base_url": "https://cliproxy.example.com",
            "token": "very-secret-token",
            "target_type": "cpa",
            "provider": "cloudmail",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["name"] == "created-no-token-echo"
    assert payload["has_token"] is True
    assert "token" not in payload
    assert payload["token_masked"]
    assert "very-secret-token" not in str(payload)


def test_run_detail_and_inventory_shapes_include_required_fields(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    environment = create_environment_via_api(client)

    with session_module.get_db() as db:
        inventory = crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment["id"],
            remote_file_id="file-1",
            email="one@example.com",
            remote_account_id="acct-1",
            payload_json={"status": "new"},
            sync_state="linked",
            probe_status="unauthorized_401",
        )
        run = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment["id"],
            status="completed",
            summary_json={
                "request": {"idempotency_key": "shape-check"},
                "current_stage": "completed",
                "progress_percent": 100,
                "cancellable": False,
                "records": 1,
                "result_summary": {"records": 1, "matches": 0},
            },
        )
        run_id = run.id
        crud.create_maintenance_action_log(
            db,
            run_id=run.id,
            environment_id=environment["id"],
            action_type="scan_record",
            status="logged",
            remote_file_id=inventory.remote_file_id,
            message="recorded",
            details_json={"classification": "unauthorized_401"},
        )

    run_detail = client.get(f"/api/cliproxy-environments/runs/{run_id}")
    inventory_list = client.get(f"/api/cliproxy-environments/{environment['id']}/inventory")
    run_actions = client.get(f"/api/cliproxy-environments/runs/{run_id}/actions")

    assert run_detail.status_code == 200
    run_payload = run_detail.json()
    for field in [
        "id",
        "environment_id",
        "run_type",
        "status",
        "started_at",
        "completed_at",
        "current_stage",
        "progress_percent",
        "cancellable",
        "result_summary",
        "counters",
        "summary_json",
        "error_message",
    ]:
        assert field in run_payload
    assert run_payload["result_summary"] == {"records": 1, "matches": 0}
    assert run_payload["counters"]["action_count"] == 1

    assert inventory_list.status_code == 200
    inventory_payload = inventory_list.json()
    assert len(inventory_payload) == 1
    for field in [
        "id",
        "environment_id",
        "remote_file_id",
        "email",
        "remote_account_id",
        "local_account_id",
        "payload_json",
        "last_seen_at",
        "last_probed_at",
        "sync_state",
        "probe_status",
        "disable_source",
        "created_at",
        "updated_at",
    ]:
        assert field in inventory_payload[0]

    assert run_actions.status_code == 200
    action_payload = run_actions.json()
    assert len(action_payload) == 1
    for field in ["id", "run_id", "environment_id", "action_type", "status", "remote_file_id", "message", "details_json", "created_at"]:
        assert field in action_payload[0]


def test_run_list_route_returns_scoped_runs_with_required_fields(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    environment = create_environment_via_api(client, name="run-list-primary")
    other_environment = create_environment_via_api(client, name="run-list-secondary")

    with session_module.get_db() as db:
        run = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment["id"],
            status="completed",
            summary_json={
                "request": {"idempotency_key": "run-list"},
                "current_stage": "completed",
                "progress_percent": 100,
                "cancellable": False,
                "records": 1,
                "result_summary": {"records": 1, "matches": 0},
            },
        )
        crud.create_maintenance_action_log(
            db,
            run_id=run.id,
            environment_id=environment["id"],
            action_type="scan_record",
            status="logged",
            remote_file_id="file-1",
            message="recorded",
            details_json={"classification": "unauthorized_401"},
        )
        crud.create_maintenance_run(
            db,
            run_type="maintain",
            environment_id=other_environment["id"],
            status="completed",
            summary_json={
                "request": {"idempotency_key": "other-run-list"},
                "current_stage": "completed",
                "progress_percent": 100,
                "cancellable": False,
                "records": 2,
                "result_summary": {"records": 2, "matches": 1},
            },
        )

    response = client.get(f"/api/cliproxy-environments/{environment['id']}/runs")

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, list)
    assert len(payload) == 1
    assert payload[0]["environment_id"] == environment["id"]
    for field in [
        "id",
        "environment_id",
        "run_type",
        "status",
        "started_at",
        "completed_at",
        "current_stage",
        "progress_percent",
        "cancellable",
        "result_summary",
        "counters",
        "summary_json",
        "error_message",
    ]:
        assert field in payload[0]


def test_connection_test_returns_status_latency_and_error_shape(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    monkeypatch.setattr(cliproxy_routes_module, "CLIProxyAPIClient", FakeRouteCLIProxyClient)
    environment = create_environment_via_api(client)

    response = client.post(f"/api/cliproxy-environments/{environment['id']}/test-connection")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert isinstance(payload["latency_ms"], int)
    assert payload["latency_ms"] >= 0
    assert payload["error"] is None


def test_audit_endpoint_filters_by_resource_and_event_type(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    environment = create_environment_via_api(client)

    with session_module.get_db() as db:
        crud.write_audit_log(
            db,
            event_type="environment_update",
            environment_id=environment["id"],
            message="updated",
            details_json={"resource": "environment", "resource_id": environment["id"]},
        )
        crud.write_audit_log(
            db,
            event_type="run_create",
            environment_id=environment["id"],
            message="created run",
            details_json={"resource": "run", "resource_id": 99},
        )

    response = client.get("/api/cliproxy-audit", params={"resource": "environment", "event_type": "environment_update"})

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["event_type"] == "environment_update"
    assert payload[0]["details_json"]["resource"] == "environment"
    for field in ["id", "environment_id", "run_id", "event_type", "actor", "message", "details_json", "created_at"]:
        assert field in payload[0]


def test_audit_endpoint_filters_by_time_range(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    environment = create_environment_via_api(client)

    with session_module.get_db() as db:
        older = crud.write_audit_log(
            db,
            event_type="environment_update",
            environment_id=environment["id"],
            details_json={"resource": "environment"},
        )
        newer = crud.write_audit_log(
            db,
            event_type="connection_test",
            environment_id=environment["id"],
            details_json={"resource": "environment"},
        )
        db.query(AuditLog).filter(AuditLog.id == older.id).update({"created_at": datetime.utcnow() - timedelta(hours=2)})
        db.query(AuditLog).filter(AuditLog.id == newer.id).update({"created_at": datetime.utcnow() - timedelta(minutes=5)})
        db.commit()

    start_time = (datetime.utcnow() - timedelta(minutes=30)).isoformat()
    end_time = datetime.utcnow().isoformat()
    response = client.get(
        "/api/cliproxy-audit",
        params={"start_time": start_time, "end_time": end_time, "event_type": "connection_test"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["event_type"] == "connection_test"


def test_control_plane_actions_emit_required_audit_records(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    monkeypatch.setattr(cliproxy_routes_module, "CLIProxyAPIClient", FakeRouteCLIProxyClient)
    monkeypatch.setattr(cliproxy_maintenance_module, "CLIProxyAPIClient", FakeRouteCLIProxyClient)

    create_response = client.post(
        "/api/cliproxy-environments",
        json={
            "name": "audit-primary",
            "base_url": "https://cliproxy.example.com",
            "token": "audit-secret-token",
            "target_type": "cpa",
            "provider": "cloudmail",
        },
    )
    environment_id = create_response.json()["id"]

    update_response = client.patch(
        f"/api/cliproxy-environments/{environment_id}",
        json={"notes": "updated-from-test"},
    )
    token_response = client.post(
        f"/api/cliproxy-environments/{environment_id}/token",
        json={"token": "rotated-secret-token"},
    )
    test_response = client.post(f"/api/cliproxy-environments/{environment_id}/test-connection")
    run_response = client.post(
        f"/api/cliproxy-environments/{environment_id}/maintain",
        json={"dry_run": False, "idempotency_key": "audit-maintain"},
    )
    run_id = run_response.json()["id"]
    cancel_response = client.post(f"/api/cliproxy-environments/runs/{run_id}/cancel")

    with session_module.get_db() as db:
        account = crud.create_account(db, email="one@example.com", email_service="manual")
        inventory = crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment_id,
            remote_file_id="file-1",
            email=account.email,
            remote_account_id="acct-1",
            local_account_id=account.id,
            payload_json={"remote_file_id": "file-1", "remote_email": account.email, "remote_account_id": "acct-1"},
            sync_state="linked",
            probe_status="unauthorized_401",
        )
        assert inventory.id is not None
        engine = cliproxy_maintenance_module.CLIProxyMaintenanceEngine(db=db, client=FakeRouteCLIProxyClient("https://cliproxy.example.com", "token"))
        maintain_run = crud.create_maintenance_run(
            db,
            run_type="maintain",
            environment_id=environment_id,
            status="queued",
            summary_json={"current_stage": "queued", "progress_percent": 0, "cancellable": True},
        )
        engine.maintain(environment_id, dry_run=False, run_id=maintain_run.id)
        audit_logs = db.query(AuditLog).order_by(AuditLog.id.asc()).all()

    assert create_response.status_code == 200
    assert update_response.status_code == 200
    assert token_response.status_code == 200
    assert test_response.status_code == 200
    assert run_response.status_code == 202
    assert cancel_response.status_code == 200
    event_types = [row.event_type for row in audit_logs]
    for event_type in [
        "environment_create",
        "environment_update",
        "token_replace",
        "connection_test",
        "run_create",
        "run_cancel",
        "maintain_action",
    ]:
        assert event_type in event_types


def test_environment_summary_fields_update_after_test_scan_and_maintain(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    monkeypatch.setattr(cliproxy_routes_module, "CLIProxyAPIClient", FakeRouteCLIProxyClient)
    monkeypatch.setattr(cliproxy_maintenance_module, "CLIProxyAPIClient", FakeRouteCLIProxyClient)
    environment = create_environment_via_api(client)

    test_response = client.post(f"/api/cliproxy-environments/{environment['id']}/test-connection")
    scan_response = client.post(
        f"/api/cliproxy-environments/{environment['id']}/scan",
        json={"idempotency_key": "summary-scan"},
    )
    cliproxy_routes_module._run_maintenance_job(scan_response.json()["id"])
    maintain_response = client.post(
        f"/api/cliproxy-environments/{environment['id']}/maintain",
        json={"idempotency_key": "summary-maintain", "dry_run": True},
    )
    cliproxy_routes_module._run_maintenance_job(maintain_response.json()["id"])
    detail_response = client.get(f"/api/cliproxy-environments/{environment['id']}")

    assert test_response.status_code == 200
    assert scan_response.status_code == 202
    assert maintain_response.status_code == 202
    assert detail_response.status_code == 200
    payload = detail_response.json()
    assert payload["last_test_status"] == "ok"
    assert isinstance(payload["last_test_latency_ms"], int)
    assert payload["last_test_error"] is None
    assert payload["last_scanned_at"] is not None
    assert payload["last_maintained_at"] is not None


def test_environment_list_reflects_summary_fields_after_test_scan_and_maintain(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    stub_cliproxy_background_dispatch(monkeypatch)
    monkeypatch.setattr(cliproxy_routes_module, "CLIProxyAPIClient", FakeRouteCLIProxyClient)
    monkeypatch.setattr(cliproxy_maintenance_module, "CLIProxyAPIClient", FakeRouteCLIProxyClient)
    environment = create_environment_via_api(client, name="summary-list")

    test_response = client.post(f"/api/cliproxy-environments/{environment['id']}/test-connection")
    scan_response = client.post(
        f"/api/cliproxy-environments/{environment['id']}/scan",
        json={"idempotency_key": "summary-list-scan"},
    )
    cliproxy_routes_module._run_maintenance_job(scan_response.json()["id"])
    maintain_response = client.post(
        f"/api/cliproxy-environments/{environment['id']}/maintain",
        json={"idempotency_key": "summary-list-maintain", "dry_run": True},
    )
    cliproxy_routes_module._run_maintenance_job(maintain_response.json()["id"])

    response = client.get("/api/cliproxy-environments")

    assert test_response.status_code == 200
    assert scan_response.status_code == 202
    assert maintain_response.status_code == 202
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["id"] == environment["id"]
    assert payload[0]["last_test_status"] == "ok"
    assert isinstance(payload[0]["last_test_latency_ms"], int)
    assert payload[0]["last_test_error"] is None
    assert payload[0]["last_scanned_at"] is not None
    assert payload[0]["last_maintained_at"] is not None
