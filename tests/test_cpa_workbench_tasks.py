import importlib
from pathlib import Path

import src.database.session as session_module
from src.database import crud
from src.web.auth import build_session_cookie_value

from tests.test_cliproxy_routes import authenticate_client, build_client


cpa_scan_tasks_module = importlib.import_module("src.core.tasks.cpa_scan")
cpa_actions_tasks_module = importlib.import_module("src.core.tasks.cpa_actions")
cpa_workbench_routes_module = importlib.import_module("src.web.routes.cpa_workbench")


def stub_cpa_action_dispatch(monkeypatch) -> None:
    def _noop(task_id: int) -> None:
        return None

    monkeypatch.setattr(cpa_workbench_routes_module, "_dispatch_cpa_action_job", _noop)


def test_expired_defaults_to_delete(monkeypatch, tmp_path: Path):
    stub_cpa_action_dispatch(monkeypatch)
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="alpha-service",
            api_url="https://alpha.example.com",
            api_token="alpha-token",
            enabled=True,
            priority=1,
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service.id,
            credential_id="cred-expired",
            status="expired",
            quota_status="healthy",
        )
        service_id = service.id
        db.commit()

    response = client.post(
        "/api/cpa/actions",
        json={"service_ids": [service_id]},
    )

    assert response.status_code == 202
    assert response.json() == {
        "task_id": response.json()["task_id"],
        "type": "action",
        "status": "queued",
        "total": 1,
        "processed": 0,
        "current_item": None,
        "progress_percent": 0,
        "logs": [],
        "stats": {
            "service_ids": [service_id],
            "action_count": 1,
            "delete_count": 1,
            "disable_count": 0,
            "delete_concurrency": 2,
            "disable_concurrency": 2,
            "quota_action": "disable",
            "actions": [
                {
                    "service_id": service_id,
                    "credential_id": "cred-expired",
                    "status": "expired",
                    "quota_status": "healthy",
                    "action": "delete",
                }
            ],
        },
    }


def test_quota_limited_defaults_to_disable_but_can_switch_to_delete(monkeypatch, tmp_path: Path):
    stub_cpa_action_dispatch(monkeypatch)
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="alpha-service",
            api_url="https://alpha.example.com",
            api_token="alpha-token",
            enabled=True,
            priority=1,
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service.id,
            credential_id="cred-quota",
            status="quota_limited",
            quota_status="limited",
        )
        service_id = service.id
        db.commit()

    disable_response = client.post(
        "/api/cpa/actions",
        json={"service_ids": [service_id]},
    )

    assert disable_response.status_code == 202
    disable_payload = disable_response.json()
    assert disable_payload["stats"]["delete_count"] == 0
    assert disable_payload["stats"]["disable_count"] == 1
    assert disable_payload["stats"]["actions"] == [
        {
            "service_id": service_id,
            "credential_id": "cred-quota",
            "status": "quota_limited",
            "quota_status": "limited",
            "action": "disable",
        }
    ]

    delete_response = client.post(
        "/api/cpa/actions",
        json={"service_ids": [service_id], "quota_action": "delete"},
    )

    assert delete_response.status_code == 202
    delete_payload = delete_response.json()
    assert delete_payload["stats"]["delete_count"] == 1
    assert delete_payload["stats"]["disable_count"] == 0
    assert delete_payload["stats"]["actions"] == [
        {
            "service_id": service_id,
            "credential_id": "cred-quota",
            "status": "quota_limited",
            "quota_status": "limited",
            "action": "delete",
        }
    ]


def test_delete_and_disable_default_concurrency_is_two(monkeypatch, tmp_path: Path):
    stub_cpa_action_dispatch(monkeypatch)
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="alpha-service",
            api_url="https://alpha.example.com",
            api_token="alpha-token",
            enabled=True,
            priority=1,
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service.id,
            credential_id="cred-expired",
            status="expired",
            quota_status="healthy",
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service.id,
            credential_id="cred-quota",
            status="quota_limited",
            quota_status="limited",
        )
        service_id = service.id
        db.commit()

    response = client.post(
        "/api/cpa/actions",
        json={"service_ids": [service_id]},
    )

    assert response.status_code == 202
    assert response.json()["stats"]["delete_concurrency"] == 2
    assert response.json()["stats"]["disable_concurrency"] == 2


def test_cpa_action_task_executes_from_frozen_snapshot_inputs(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    class FakeCLIProxyClient:
        delete_calls = []
        disable_calls = []

        def __init__(self, base_url: str, token: str = "", timeout: int = 30):
            self.base_url = base_url
            self.token = token
            self.timeout = timeout

        def _request(self, method: str, path: str, json=None):
            if method == "DELETE":
                self.delete_calls.append(path)
                return {"status": "deleted"}
            raise AssertionError(f"unexpected request: {method} {path}")

        def disable_auth(self, remote_file_id: str):
            self.disable_calls.append(remote_file_id)
            return {"status": "disabled", "remote_file_id": remote_file_id}

    monkeypatch.setattr(cpa_actions_tasks_module, "CLIProxyAPIClient", FakeCLIProxyClient)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="alpha-service",
            api_url="https://alpha.example.com",
            api_token="alpha-token",
            enabled=True,
            priority=1,
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service.id,
            credential_id="cred-expired",
            status="expired",
            quota_status="healthy",
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service.id,
            credential_id="cred-quota",
            status="quota_limited",
            quota_status="limited",
        )
        service_id = service.id
        task = crud.create_cpa_action_task(
            db,
            owner_session_id="session-1",
            service_ids=[service_id],
        )
        task_id = task.id

        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-new",
            status="expired",
            quota_status="healthy",
        )
        db.commit()

    cpa_actions_tasks_module.run_cpa_action_task(task_id)

    with session_module.get_db() as db:
        task = crud.get_cpa_workbench_task_by_id(db, task_id)
        expired_snapshot = crud.get_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-expired",
        )
        quota_snapshot = crud.get_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-quota",
        )
        new_snapshot = crud.get_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-new",
        )

    assert task is not None
    assert task.status == "completed"
    assert task.total_count == 2
    assert task.processed_count == 2
    assert FakeCLIProxyClient.delete_calls == ["/inventory/cred-expired"]
    assert FakeCLIProxyClient.disable_calls == ["cred-quota"]
    assert expired_snapshot is None
    assert quota_snapshot is not None
    assert quota_snapshot.status == "disabled"
    assert new_snapshot is not None
    assert new_snapshot.status == "expired"


def stub_cpa_scan_dispatch(monkeypatch) -> None:
    def _noop(task_id: int) -> None:
        return None

    monkeypatch.setattr(cpa_workbench_routes_module, "_dispatch_cpa_scan_job", _noop)


def test_cpa_scan_task_returns_task_id_and_progress_shape(monkeypatch, tmp_path: Path):
    stub_cpa_scan_dispatch(monkeypatch)
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="alpha-service",
            api_url="https://alpha.example.com",
            api_token="alpha-token",
            enabled=True,
            priority=1,
        )

    response = client.post("/api/cpa/scan", json={"service_ids": [service.id]})

    assert response.status_code == 202
    payload = response.json()
    assert payload == {
        "task_id": payload["task_id"],
        "type": "scan",
        "status": "queued",
        "total": 1,
        "processed": 0,
        "current_item": None,
        "progress_percent": 0,
        "logs": [],
        "stats": {
            "service_ids": [service.id],
            "service_count": 1,
            "scan_concurrency": 2,
        },
    }
    assert payload["task_id"]

    detail_response = client.get(f"/api/cpa/tasks/{payload['task_id']}")

    assert detail_response.status_code == 200
    assert detail_response.json() == payload


def test_cpa_scan_task_reports_running_progress_and_logs(monkeypatch, tmp_path: Path):
    stub_cpa_scan_dispatch(monkeypatch)
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        alpha = crud.create_cpa_service(
            db,
            name="alpha-service",
            api_url="https://alpha.example.com",
            api_token="alpha-token",
            enabled=True,
            priority=1,
        )
        beta = crud.create_cpa_service(
            db,
            name="beta-service",
            api_url="https://beta.example.com",
            api_token="beta-token",
            enabled=True,
            priority=2,
        )
        alpha_id = alpha.id
        beta_id = beta.id

    start_response = client.post("/api/cpa/scan", json={"service_ids": [alpha_id, beta_id]})

    assert start_response.status_code == 202
    assert client.cookies.get("session_id")
    started_task = start_response.json()

    with session_module.get_db() as db:
        task = crud.start_cpa_workbench_task(db, int(started_task["task_id"]))
        assert task is not None

        task = crud.update_cpa_workbench_task_progress(
            db,
            int(started_task["task_id"]),
            processed_count=1,
            current_item=f"service:{alpha_id}",
        )
        assert task is not None

        task = crud.append_cpa_workbench_task_logs(
            db,
            int(started_task["task_id"]),
            [
                "scan started",
                f"processing service:{alpha_id}",
            ],
        )
        assert task is not None

    detail_response = client.get(f"/api/cpa/tasks/{started_task['task_id']}")

    assert detail_response.status_code == 200
    assert detail_response.json() == {
        "task_id": started_task["task_id"],
        "type": "scan",
        "status": "running",
        "total": 2,
        "processed": 1,
        "current_item": f"service:{alpha_id}",
        "progress_percent": 50,
        "logs": [
            "scan started",
            f"processing service:{alpha_id}",
        ],
        "stats": {
            "service_ids": [alpha_id, beta_id],
            "service_count": 2,
            "scan_concurrency": 2,
        },
    }


def test_cpa_scan_task_supports_refresh_recovery(monkeypatch, tmp_path: Path):
    stub_cpa_scan_dispatch(monkeypatch)
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="alpha-service",
            api_url="https://alpha.example.com",
            api_token="alpha-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id

    start_response = client.post("/api/cpa/scan", json={"service_ids": [service_id]})

    assert start_response.status_code == 202
    started_task = start_response.json()

    with session_module.get_db() as db:
        task = crud.start_cpa_workbench_task(db, int(started_task["task_id"]))
        assert task is not None
        task = crud.update_cpa_workbench_task_progress(
            db,
            int(started_task["task_id"]),
            processed_count=0,
            current_item=f"service:{service_id}",
        )
        assert task is not None
        task = crud.append_cpa_workbench_task_logs(
            db,
            int(started_task["task_id"]),
            ["recoverable running task"],
        )
        assert task is not None

    recovery_response = client.get("/api/cpa/tasks/latest-active", params={"type": "scan"})

    assert recovery_response.status_code == 200
    assert recovery_response.json() == {
        "task_id": started_task["task_id"],
        "type": "scan",
        "status": "running",
        "total": 1,
        "processed": 0,
        "current_item": f"service:{service_id}",
        "progress_percent": 0,
        "logs": ["recoverable running task"],
        "stats": {
            "service_ids": [service_id],
            "service_count": 1,
            "scan_concurrency": 2,
        },
    }

    client.cookies.set("session_id", build_session_cookie_value("other-session"))

    other_detail_response = client.get(f"/api/cpa/tasks/{started_task['task_id']}")
    other_latest_response = client.get("/api/cpa/tasks/latest-active", params={"type": "scan"})

    assert other_detail_response.status_code == 404
    assert other_latest_response.status_code == 404


def test_cpa_scan_task_terminal_status_is_visible_but_not_latest_active(monkeypatch, tmp_path: Path):
    stub_cpa_scan_dispatch(monkeypatch)
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="alpha-service",
            api_url="https://alpha.example.com",
            api_token="alpha-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id

    start_response = client.post("/api/cpa/scan", json={"service_ids": [service_id]})

    assert start_response.status_code == 202
    started_task = start_response.json()

    with session_module.get_db() as db:
        task = crud.start_cpa_workbench_task(db, int(started_task["task_id"]))
        assert task is not None
        task = crud.finalize_cpa_workbench_task(
            db,
            int(started_task["task_id"]),
            status="completed",
            current_item=None,
        )
        assert task is not None

    detail_response = client.get(f"/api/cpa/tasks/{started_task['task_id']}")
    latest_active_response = client.get("/api/cpa/tasks/latest-active", params={"type": "scan"})

    assert detail_response.status_code == 200
    assert detail_response.json() == {
        "task_id": started_task["task_id"],
        "type": "scan",
        "status": "completed",
        "total": 1,
        "processed": 0,
        "current_item": None,
        "progress_percent": 0,
        "logs": [],
        "stats": {
            "service_ids": [service_id],
            "service_count": 1,
            "scan_concurrency": 2,
        },
    }
    assert latest_active_response.status_code == 404


def test_cpa_scan_uses_default_concurrency_two(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    class FakeExecutor:
        created_max_workers = []

        def __init__(self, max_workers: int):
            self.created_max_workers.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def map(self, fn, items):
            return [fn(item) for item in items]

    class FakeCLIProxyClient:
        def __init__(self, base_url: str, token: str = "", timeout: int = 30):
            self.base_url = base_url
            self.token = token
            self.timeout = timeout

        def fetch_inventory(self):
            return [
                {"remote_file_id": "cred-1"},
                {"remote_file_id": "cred-2"},
            ]

        def probe_usage(self, remote_file_id: str):
            return {"status": "ok"}

    monkeypatch.setattr(cpa_scan_tasks_module, "ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr(cpa_scan_tasks_module, "CLIProxyAPIClient", FakeCLIProxyClient)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="alpha-service",
            api_url="https://alpha.example.com",
            api_token="alpha-token",
            enabled=True,
            priority=1,
        )
        task = crud.create_cpa_scan_task(
            db,
            owner_session_id="session-1",
            service_ids=[service.id],
        )
        service_id = service.id
        task_id = task.id

    cpa_scan_tasks_module.run_cpa_scan_task(task_id)

    with session_module.get_db() as db:
        task = crud.get_cpa_workbench_task_by_id(db, task_id)

    assert task is not None
    assert FakeExecutor.created_max_workers == [2]
    assert task.status == "completed"
    assert task.total_count == 2
    assert task.processed_count == 2
    assert task.stats_json["scan_concurrency"] == 2


def test_cpa_scan_continues_when_one_credential_fails(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    class FakeCLIProxyClient:
        def __init__(self, base_url: str, token: str = "", timeout: int = 30):
            self.base_url = base_url
            self.token = token
            self.timeout = timeout

        def fetch_inventory(self):
            return [
                {"remote_file_id": "cred-ok"},
                {},
                {"remote_file_id": "cred-ok-2"},
            ]

        def probe_usage(self, remote_file_id: str):
            return {"status": "ok"}

    monkeypatch.setattr(cpa_scan_tasks_module, "CLIProxyAPIClient", FakeCLIProxyClient)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="alpha-service",
            api_url="https://alpha.example.com",
            api_token="alpha-token",
            enabled=True,
            priority=1,
        )
        task = crud.create_cpa_scan_task(
            db,
            owner_session_id="session-1",
            service_ids=[service.id],
        )
        service_id = service.id
        task_id = task.id

    cpa_scan_tasks_module.run_cpa_scan_task(task_id)

    with session_module.get_db() as db:
        task = crud.get_cpa_workbench_task_by_id(db, task_id)
        first_snapshot = crud.get_cpa_remote_credential_snapshot(db, service_id=service_id, credential_id="cred-ok")
        second_snapshot = crud.get_cpa_remote_credential_snapshot(db, service_id=service_id, credential_id="cred-ok-2")

    assert task is not None
    assert task.status == "completed"
    assert task.total_count == 3
    assert task.processed_count == 3
    assert task.stats_json["valid_count"] == 2
    assert task.stats_json["error_count"] == 1
    assert first_snapshot is not None
    assert second_snapshot is not None


def test_cpa_scan_logs_include_service_credential_and_failure_reason(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    class FakeCLIProxyClient:
        def __init__(self, base_url: str, token: str = "", timeout: int = 30):
            self.base_url = base_url
            self.token = token
            self.timeout = timeout

        def fetch_inventory(self):
            return [{"remote_file_id": "cred-fail"}]

        def probe_usage(self, remote_file_id: str):
            raise RuntimeError("upstream timeout")

    monkeypatch.setattr(cpa_scan_tasks_module, "CLIProxyAPIClient", FakeCLIProxyClient)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="alpha-service",
            api_url="https://alpha.example.com",
            api_token="alpha-token",
            enabled=True,
            priority=1,
        )
        task = crud.create_cpa_scan_task(
            db,
            owner_session_id="session-1",
            service_ids=[service.id],
        )
        service_id = service.id
        task_id = task.id

    cpa_scan_tasks_module.run_cpa_scan_task(task_id)

    with session_module.get_db() as db:
        task = crud.get_cpa_workbench_task_by_id(db, task_id)

    assert task is not None
    task_logs = task.get_log_lines()
    assert any(f"service_id={service_id}" in line for line in task_logs)
    assert any("service_name=alpha-service" in line for line in task_logs)
    assert any("credential_id=cred-fail" in line for line in task_logs)
    assert any("failure_reason=upstream timeout" in line for line in task_logs)
