from datetime import datetime
from pathlib import Path
import importlib

from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from src.database import crud
from src.database.init_db import initialize_database
from src.database.models import CLIProxyAPIEnvironment
from src.database.session import get_db
import src.database.session as session_module

web_app_module = importlib.import_module("src.web.app")
web_auth_module = importlib.import_module("src.web.auth")


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

    db_path = tmp_path / "accounts_workbench_routes.db"
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


def seed_account_workbench_data() -> int:
    with get_db() as db:
        account = crud.create_account(
            db,
            email="workbench@example.com",
            email_service="tempmail",
            account_id="acct-workbench",
            workspace_id="ws-123",
            platform_source="cloudmail",
            last_upload_target="newApi",
            extra_data={"plan": "pro"},
        )
        account.subscription_type = "plus"
        account.subscription_at = datetime(2026, 3, 20, 10, 0, 0)
        account.cpa_uploaded = True
        account.cpa_uploaded_at = datetime(2026, 3, 21, 8, 30, 0)

        environment = CLIProxyAPIEnvironment(
            name="primary-env",
            base_url="https://cliproxy.example.com",
            target_type="newApi",
            provider="cloudmail",
            last_maintained_at=datetime(2026, 3, 22, 9, 0, 0),
        )
        db.add(environment)
        db.flush()

        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="file-123",
            email=account.email,
            remote_account_id=account.account_id,
            local_account_id=account.id,
            payload_json={"slots_used": 2, "slots_total": 5},
            last_seen_at=datetime(2026, 3, 22, 8, 0, 0),
            last_probed_at=datetime(2026, 3, 22, 8, 15, 0),
            sync_state="linked",
            probe_status="quota_limited",
            disable_source="system",
        )

        crud.create_maintenance_run(
            db,
            run_type="maintain",
            environment_id=environment.id,
            status="completed",
            summary_json={
                "result_summary": {"records": 1, "matches": 1, "disabled": 0},
                "current_stage": "completed",
                "progress_percent": 100,
            },
        )

        db.commit()
        return account.id


def test_accounts_list_includes_remote_maintenance_summary_fields(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    account_id = seed_account_workbench_data()

    response = client.get("/api/accounts")

    assert response.status_code == 200
    payload = response.json()
    account = next(item for item in payload["accounts"] if item["id"] == account_id)
    assert account["platform_source"] == "cloudmail"
    assert account["remote_sync_state"] == "linked"
    assert account["remote_environment_name"] == "primary-env"
    assert account["last_maintenance_status"] == "completed"
    assert account["last_maintenance_at"] is not None
    assert account["last_upload_target"] == "newApi"
    assert "export_status_summary" not in account
    assert "billing_status_summary" not in account
    assert "remote_inventory_summary" not in account
    assert "recent_task_summary" not in account


def test_account_detail_includes_upload_target_and_remote_inventory_summary(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    account_id = seed_account_workbench_data()

    response = client.get(f"/api/accounts/{account_id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["last_upload_target"] == "newApi"
    assert payload["remote_inventory_summary"] == {
        "environment_id": 1,
        "environment_name": "primary-env",
        "remote_file_id": "file-123",
        "remote_account_id": "acct-workbench",
        "sync_state": "linked",
        "probe_status": "quota_limited",
        "disable_source": "system",
        "last_seen_at": "2026-03-22T08:00:00",
        "last_probed_at": "2026-03-22T08:15:00",
    }


def test_account_list_and_detail_include_full_workbench_summary_contract(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    account_id = seed_account_workbench_data()

    list_response = client.get("/api/accounts")
    detail_response = client.get(f"/api/accounts/{account_id}")

    assert list_response.status_code == 200
    assert detail_response.status_code == 200

    list_account = next(item for item in list_response.json()["accounts"] if item["id"] == account_id)
    detail_account = detail_response.json()

    assert set(
        [
            "platform_source",
            "subscription_summary",
            "quota_summary",
            "remote_sync_state",
            "remote_environment_name",
            "last_maintenance_status",
            "last_maintenance_at",
            "last_upload_target",
        ]
    ).issubset(list_account.keys())
    assert list_account["subscription_summary"] == {
        "subscription_type": "plus",
        "subscription_at": "2026-03-20T10:00:00",
        "has_subscription": True,
    }
    assert list_account["quota_summary"] == {
        "probe_status": "quota_limited",
        "slots_used": 2,
        "slots_total": 5,
    }
    assert "export_status_summary" not in list_account
    assert "billing_status_summary" not in list_account
    assert "remote_inventory_summary" not in list_account
    assert "recent_task_summary" not in list_account

    assert set(
        [
            "platform_source",
            "subscription_summary",
            "quota_summary",
            "remote_sync_state",
            "remote_environment_name",
            "last_maintenance_status",
            "last_maintenance_at",
            "last_upload_target",
            "export_status_summary",
            "billing_status_summary",
            "remote_inventory_summary",
            "recent_task_summary",
        ]
    ).issubset(detail_account.keys())
    assert detail_account["export_status_summary"] == {
        "cpa_uploaded": True,
        "cpa_uploaded_at": "2026-03-21T08:30:00",
        "last_upload_target": "newApi",
    }
    assert detail_account["billing_status_summary"] == {
        "subscription_type": "plus",
        "subscription_at": "2026-03-20T10:00:00",
        "status": "active",
    }
    assert detail_account["recent_task_summary"] == {
        "task_id": None,
        "status": None,
        "created_at": None,
        "completed_at": None,
    }


def test_account_detail_exposes_workbench_panels_required_for_summary_rendering(monkeypatch, tmp_path):
    client = build_client(monkeypatch, tmp_path)
    account_id = seed_account_workbench_data()

    response = client.get(f"/api/accounts/{account_id}")

    assert response.status_code == 200
    payload = response.json()

    assert payload["subscription_summary"] == {
        "subscription_type": "plus",
        "subscription_at": "2026-03-20T10:00:00",
        "has_subscription": True,
    }
    assert payload["quota_summary"] == {
        "probe_status": "quota_limited",
        "slots_used": 2,
        "slots_total": 5,
    }
    assert payload["remote_inventory_summary"] == {
        "environment_id": 1,
        "environment_name": "primary-env",
        "remote_file_id": "file-123",
        "remote_account_id": "acct-workbench",
        "sync_state": "linked",
        "probe_status": "quota_limited",
        "disable_source": "system",
        "last_seen_at": "2026-03-22T08:00:00",
        "last_probed_at": "2026-03-22T08:15:00",
    }
    assert payload["recent_task_summary"] == {
        "task_id": None,
        "status": None,
        "created_at": None,
        "completed_at": None,
    }
    assert payload["billing_status_summary"] == {
        "subscription_type": "plus",
        "subscription_at": "2026-03-20T10:00:00",
        "status": "active",
    }
    assert payload["export_status_summary"] == {
        "cpa_uploaded": True,
        "cpa_uploaded_at": "2026-03-21T08:30:00",
        "last_upload_target": "newApi",
    }
    assert payload["cliproxy_jump_entry"] == {
        "label": "Open CLIProxy maintenance context",
        "href": "/api/cliproxy-environments/runs/1",
        "environment_id": 1,
        "run_id": 1,
    }
    assert payload["automation_trace_summary"] == {
        "source": "cloudmail",
        "batch_target": "newApi",
        "proxy": None,
        "recent_task_status": None,
        "recent_task_label": "No recent account task",
        "log_excerpt": "Maintain run completed at 2026-03-22T09:00:00 with records=1, matches=1, disabled=0.",
    }
