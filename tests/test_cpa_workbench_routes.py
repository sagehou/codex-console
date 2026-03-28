import importlib
from datetime import datetime
from pathlib import Path

import src.database.session as session_module
from src.database import crud
from src.database.models import CpaRemoteCredentialSnapshot

from tests.test_cliproxy_routes import authenticate_client, build_client


cpa_workbench_routes_module = importlib.import_module("src.web.routes.cpa_workbench")
cpa_scan_tasks_module = importlib.import_module("src.core.tasks.cpa_scan")


def test_cpa_workbench_lists_services_with_state_classification(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        usable = crud.create_cpa_service(
            db,
            name="usable-service",
            api_url="https://usable.example.com",
            api_token="usable-token",
            enabled=True,
            priority=2,
        )
        incomplete = crud.create_cpa_service(
            db,
            name="incomplete-service",
            api_url="",
            api_token="",
            enabled=True,
            priority=1,
        )
        disabled = crud.create_cpa_service(
            db,
            name="disabled-service",
            api_url="https://disabled.example.com",
            api_token="disabled-token",
            enabled=False,
            priority=3,
        )
        usable_id = usable.id
        incomplete_id = incomplete.id
        disabled_id = disabled.id

    response = client.get("/api/cpa/services")

    assert response.status_code == 200
    payload = response.json()
    assert payload["services"] == [
        {
            "service_id": incomplete_id,
            "service_name": "incomplete-service",
            "state": "enabled_incomplete",
            "selectable": False,
            "status_message": "Configuration incomplete",
        },
        {
            "service_id": usable_id,
            "service_name": "usable-service",
            "state": "enabled_usable",
            "selectable": True,
            "status_message": "Ready",
        },
        {
            "service_id": disabled_id,
            "service_name": "disabled-service",
            "state": "disabled",
            "selectable": False,
            "status_message": "Disabled in settings",
        },
    ]


def test_disabled_and_incomplete_services_are_visible_but_not_selectable(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        usable = crud.create_cpa_service(
            db,
            name="usable-service",
            api_url="https://usable.example.com",
            api_token="usable-token",
            enabled=True,
            priority=2,
        )
        crud.create_cpa_service(
            db,
            name="incomplete-service",
            api_url="",
            api_token="",
            enabled=True,
            priority=1,
        )
        crud.create_cpa_service(
            db,
            name="disabled-service",
            api_url="https://disabled.example.com",
            api_token="disabled-token",
            enabled=False,
            priority=3,
        )
        usable_id = usable.id

    response = client.get("/api/cpa/services")

    assert response.status_code == 200
    payload = response.json()
    assert [item["service_name"] for item in payload["services"]] == [
        "incomplete-service",
        "usable-service",
        "disabled-service",
    ]
    assert [item["selectable"] for item in payload["services"]] == [False, True, False]
    assert payload["selected_service_ids"] == [usable_id]


def test_single_usable_service_is_selected_by_default(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        usable = crud.create_cpa_service(
            db,
            name="only-usable-service",
            api_url="https://usable.example.com",
            api_token="usable-token",
            enabled=True,
            priority=1,
        )
        crud.create_cpa_service(
            db,
            name="disabled-service",
            api_url="https://disabled.example.com",
            api_token="disabled-token",
            enabled=False,
            priority=2,
        )
        usable_id = usable.id

    response = client.get("/api/cpa/services")

    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_service_ids"] == [usable_id]


def test_multiple_usable_services_are_all_selected_by_default(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        first = crud.create_cpa_service(
            db,
            name="alpha-service",
            api_url="https://alpha.example.com",
            api_token="alpha-token",
            enabled=True,
            priority=2,
        )
        second = crud.create_cpa_service(
            db,
            name="beta-service",
            api_url="https://beta.example.com",
            api_token="beta-token",
            enabled=True,
            priority=1,
        )
        crud.create_cpa_service(
            db,
            name="disabled-service",
            api_url="https://disabled.example.com",
            api_token="disabled-token",
            enabled=False,
            priority=3,
        )
        first_id = first.id
        second_id = second.id

    response = client.get("/api/cpa/services")

    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_service_ids"] == [second_id, first_id]


def test_unconfigured_services_only_appear_as_empty_state_not_selector_rows(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    response = client.get("/api/cpa/services")

    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "services": [],
        "selected_service_ids": [],
        "latest_active_task": None,
        "empty_state": {
            "code": "no_configured_services",
            "message": "Configure at least one CPA service to start managing remote credentials.",
        },
    }


def test_cpa_workbench_restores_selected_credential_by_service_and_credential_id(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        usable = crud.create_cpa_service(
            db,
            name="usable-service",
            api_url="https://usable.example.com",
            api_token="usable-token",
            enabled=True,
            priority=1,
        )
        usable_id = usable.id

    response = client.get(
        "/api/cpa/services",
        params={"selected_service_id": usable_id, "selected_credential_id": "cred-123"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["selection_recovery"] == {
        "service_id": usable_id,
        "credential_id": "cred-123",
    }
    assert payload["selection_notice"] is None
    assert payload["latest_active_task"] is None


def test_cpa_workbench_shows_notice_when_previously_selected_credential_disappears(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        usable = crud.create_cpa_service(
            db,
            name="usable-service",
            api_url="https://usable.example.com",
            api_token="usable-token",
            enabled=True,
            priority=1,
        )
        usable_id = usable.id

    response = client.get(
        "/api/cpa/services",
        params={
            "selected_service_id": usable_id,
            "selected_credential_id": "cred-123",
            "selected_credential_missing": "1",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["selection_recovery"] is None
    assert payload["selection_notice"] == {
        "code": "selected_credential_missing",
        "message": "The previously selected credential is no longer available in the current view.",
        "service_id": usable_id,
        "credential_id": "cred-123",
    }


def test_cpa_workbench_service_bootstrap_does_not_include_cliproxy_task_data(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        usable = crud.create_cpa_service(
            db,
            name="usable-service",
            api_url="https://usable.example.com",
            api_token="usable-token",
            enabled=True,
            priority=1,
        )
        usable_id = usable.id
        task = crud.create_cpa_scan_task(
            db,
            owner_session_id=client.cookies.get("session_id").split(".", 1)[0],
            service_ids=[usable_id],
        )
        task_id = task.id
        crud.start_cpa_workbench_task(db, task.id)

    response = client.get("/api/cpa/services")

    assert response.status_code == 200
    payload = response.json()
    assert payload["latest_active_task"]["task_id"] == str(task_id)
    assert payload["latest_active_task"]["type"] == "scan"
    assert "aggregate_key" not in payload["latest_active_task"]
    assert payload["latest_active_task"]["stats"]["service_ids"] == [usable_id]


def test_cpa_workbench_service_bootstrap_omits_mismatched_default_scope_task(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    session_id = client.cookies.get("session_id").split(".", 1)[0]

    with session_module.get_db() as db:
        first = crud.create_cpa_service(
            db,
            name="alpha-service",
            api_url="https://alpha.example.com",
            api_token="alpha-token",
            enabled=True,
            priority=1,
        )
        second = crud.create_cpa_service(
            db,
            name="beta-service",
            api_url="https://beta.example.com",
            api_token="beta-token",
            enabled=True,
            priority=2,
        )
        first_id = first.id
        second_id = second.id
        task = crud.create_cpa_scan_task(
            db,
            owner_session_id=session_id,
            service_ids=[first_id],
        )
        crud.start_cpa_workbench_task(db, task.id)

    response = client.get("/api/cpa/services")

    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_service_ids"] == [first_id, second_id]
    assert payload["latest_active_task"] is None


def test_cpa_workbench_exposes_no_usable_services_empty_state_when_services_exist_but_none_selectable(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        crud.create_cpa_service(
            db,
            name="incomplete-service",
            api_url="",
            api_token="",
            enabled=True,
            priority=1,
        )
        crud.create_cpa_service(
            db,
            name="disabled-service",
            api_url="https://disabled.example.com",
            api_token="disabled-token",
            enabled=False,
            priority=2,
        )

    response = client.get("/api/cpa/services")

    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_service_ids"] == []
    assert payload["empty_state"] == {
        "code": "no_usable_services",
        "message": "Enable and fully configure at least one CPA service to manage remote credentials.",
    }


def test_selector_default_selection_matches_service_state_rules(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        first = crud.create_cpa_service(
            db,
            name="alpha-service",
            api_url="https://alpha.example.com",
            api_token="alpha-token",
            enabled=True,
            priority=2,
        )
        second = crud.create_cpa_service(
            db,
            name="beta-service",
            api_url="https://beta.example.com",
            api_token="beta-token",
            enabled=True,
            priority=1,
        )
        crud.create_cpa_service(
            db,
            name="incomplete-service",
            api_url="",
            api_token="",
            enabled=True,
            priority=3,
        )
        crud.create_cpa_service(
            db,
            name="disabled-service",
            api_url="https://disabled.example.com",
            api_token="disabled-token",
            enabled=False,
            priority=4,
        )
        first_id = first.id
        second_id = second.id

    response = client.get("/api/cpa/services")

    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_service_ids"] == [second_id, first_id]
    assert [service["selectable"] for service in payload["services"]] == [True, True, False, False]


def test_selector_changes_reload_inventory_scope(monkeypatch, tmp_path: Path):
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
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=alpha_id,
            credential_id="alpha-cred",
            status="valid",
            quota_status="ok",
            last_scanned_at=datetime(2026, 3, 28, 8, 0, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=beta_id,
            credential_id="beta-cred",
            status="quota_limited",
            quota_status="quota_limited",
            last_scanned_at=datetime(2026, 3, 28, 9, 0, 0),
        )
        db.commit()

    alpha_inventory = client.get("/api/cpa/credentials", params={"service_ids": str(alpha_id)})

    assert alpha_inventory.status_code == 200
    alpha_payload = alpha_inventory.json()
    assert [row["service_id"] for row in alpha_payload["rows"]] == [alpha_id]
    assert alpha_payload["counts"]["total"] == 1

    beta_inventory = client.get("/api/cpa/credentials", params={"service_ids": str(beta_id)})

    assert beta_inventory.status_code == 200
    beta_payload = beta_inventory.json()
    assert [row["service_id"] for row in beta_payload["rows"]] == [beta_id]
    assert beta_payload["counts"]["total"] == 1

    combined_inventory = client.get(
        "/api/cpa/credentials",
        params={"service_ids": f"{alpha_id},{beta_id}"},
    )

    assert combined_inventory.status_code == 200
    combined_payload = combined_inventory.json()
    assert [row["service_id"] for row in combined_payload["rows"]] == [alpha_id, beta_id]
    assert combined_payload["counts"]["total"] == 2


def test_selector_changes_reload_summary_scope_through_summary_endpoint(monkeypatch, tmp_path: Path):
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
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=alpha_id,
            credential_id="alpha-valid",
            status="valid",
            quota_status="ok",
            last_scanned_at=datetime(2026, 3, 28, 8, 0, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=alpha_id,
            credential_id="alpha-error",
            status="error",
            quota_status="error",
            last_scanned_at=datetime(2026, 3, 28, 8, 30, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=beta_id,
            credential_id="beta-quota",
            status="quota_limited",
            quota_status="quota_limited",
            last_scanned_at=datetime(2026, 3, 28, 9, 0, 0),
        )
        db.commit()

    alpha_summary = client.get("/api/cpa/summary", params={"service_ids": str(alpha_id)})

    assert alpha_summary.status_code == 200
    assert alpha_summary.json() == {
        "total": 2,
        "valid_count": 1,
        "expired_count": 0,
        "quota_count": 0,
        "error_count": 1,
        "unknown_count": 0,
    }

    beta_summary = client.get("/api/cpa/summary", params={"service_ids": str(beta_id)})

    assert beta_summary.status_code == 200
    assert beta_summary.json() == {
        "total": 1,
        "valid_count": 0,
        "expired_count": 0,
        "quota_count": 1,
        "error_count": 0,
        "unknown_count": 0,
    }

    combined_summary = client.get(
        "/api/cpa/summary",
        params={"service_ids": f"{alpha_id},{beta_id}"},
    )

    assert combined_summary.status_code == 200
    assert combined_summary.json() == {
        "total": 3,
        "valid_count": 1,
        "expired_count": 0,
        "quota_count": 1,
        "error_count": 1,
        "unknown_count": 0,
    }


def test_inventory_endpoint_does_not_carry_task9_selector_summary_dependency(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="usable-service",
            api_url="https://usable.example.com",
            api_token="usable-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-valid",
            status="valid",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 13, 0, 0),
        )
        db.commit()

    response = client.get("/api/cpa/credentials", params={"service_ids": str(service_id)})

    assert response.status_code == 200
    payload = response.json()
    assert payload["rows"][0]["credential_id"] == "cred-valid"
    assert payload["counts"] == {"returned": 1, "total": 1}
    assert "summary" not in payload


def test_cpa_inventory_rows_are_keyed_by_service_and_credential_id(monkeypatch, tmp_path: Path):
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

        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=alpha.id,
            credential_id="cred-shared",
            status="valid",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 12, 0, 0),
            summary_json={"label": "alpha summary"},
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=beta.id,
            credential_id="cred-shared",
            status="expired",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 12, 5, 0),
            summary_json={"label": "beta summary"},
        )
        db.commit()

    response = client.get("/api/cpa/credentials", params={"service_ids": "1,2"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["rows"] == [
        {
            "service_id": 1,
            "credential_id": "cred-shared",
            "service_name": "alpha-service",
            "status": "valid",
            "quota_status": "healthy",
            "last_scanned_at": "2026-03-28T12:00:00",
        },
        {
            "service_id": 2,
            "credential_id": "cred-shared",
            "service_name": "beta-service",
            "status": "expired",
            "quota_status": "healthy",
            "last_scanned_at": "2026-03-28T12:05:00",
        },
    ]
    assert payload["counts"] == {"returned": 2, "total": 2}


def test_cpa_remote_credential_snapshot_upsert_updates_existing_pair(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="upsert-service",
            api_url="https://upsert.example.com",
            api_token="upsert-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id

        first = crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-1",
            status="unknown",
            quota_status="unknown",
            last_scanned_at=datetime(2026, 3, 28, 10, 0, 0),
            summary_json={"version": 1},
        )
        db.commit()
        first_id = first.id

        updated = crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-1",
            status="valid",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 11, 0, 0),
            summary_json={"version": 2},
        )
        db.commit()

        rows = (
            db.query(CpaRemoteCredentialSnapshot)
            .filter(CpaRemoteCredentialSnapshot.service_id == service_id)
            .filter(CpaRemoteCredentialSnapshot.credential_id == "cred-1")
            .all()
        )

    assert updated.id == first_id
    assert len(rows) == 1
    assert rows[0].id == first_id
    assert rows[0].status == "valid"
    assert rows[0].quota_status == "healthy"
    assert rows[0].last_scanned_at == datetime(2026, 3, 28, 11, 0, 0)
    assert rows[0].summary_json == {"version": 2}


def test_inventory_response_includes_counts_and_row_fields(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="usable-service",
            api_url="https://usable.example.com",
            api_token="usable-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id

        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-valid",
            status="valid",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 13, 0, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-expired",
            status="expired",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 13, 5, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-quota",
            status="quota_limited",
            quota_status="limited",
            last_scanned_at=datetime(2026, 3, 28, 13, 10, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-error",
            status="error",
            quota_status="unknown",
            last_scanned_at=datetime(2026, 3, 28, 13, 15, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-unknown",
            status="unknown",
            quota_status="unknown",
            last_scanned_at=datetime(2026, 3, 28, 13, 20, 0),
        )
        db.commit()

    response = client.get("/api/cpa/credentials", params={"service_ids": str(service_id)})

    assert response.status_code == 200
    payload = response.json()
    assert payload["counts"] == {"returned": 5, "total": 5}
    assert "summary" not in payload
    assert payload["rows"][0].keys() == {
        "service_id",
        "credential_id",
        "service_name",
        "status",
        "quota_status",
        "last_scanned_at",
    }


def test_cpa_workbench_summary_endpoint_returns_aggregate_counts_for_stats_panel(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="stats-service",
            api_url="https://stats.example.com",
            api_token="stats-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-valid",
            status="valid",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 14, 0, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-expired",
            status="expired",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 14, 5, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-quota",
            status="quota_limited",
            quota_status="limited",
            last_scanned_at=datetime(2026, 3, 28, 14, 10, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-error",
            status="error",
            quota_status="unknown",
            last_scanned_at=datetime(2026, 3, 28, 14, 15, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-unknown",
            status="unknown",
            quota_status="unknown",
            last_scanned_at=datetime(2026, 3, 28, 14, 20, 0),
        )
        db.commit()

    response = client.get("/api/cpa/summary", params={"service_ids": str(service_id)})

    assert response.status_code == 200
    assert response.json() == {
        "total": 5,
        "valid_count": 1,
        "expired_count": 1,
        "quota_count": 1,
        "error_count": 1,
        "unknown_count": 1,
    }


def test_cpa_workbench_latest_active_task_shows_type_totals_current_item_and_logs(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    session_cookie = client.cookies.get("session_id")
    session_id = session_cookie.split(".", 1)[0] if session_cookie else None
    assert session_id

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="task-service",
            api_url="https://task.example.com",
            api_token="task-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id
        task = crud.create_cpa_scan_task(
            db,
            owner_session_id=session_id,
            service_ids=[service_id],
        )
        crud.start_cpa_workbench_task(db, task.id)
        crud.update_cpa_workbench_task_progress(
            db,
            task.id,
            processed_count=2,
            total_count=4,
            current_item=f"service:{service_id}/credential:cred-2",
        )
        crud.append_cpa_workbench_task_logs(db, task.id, ["Loaded service inventory", "Scanning credential cred-2"])
        task_id = task.id

    latest_response = client.get("/api/cpa/tasks/latest-active", params={"type": "scan"})

    assert latest_response.status_code == 200
    assert latest_response.json() == {
        "task_id": str(task_id),
        "type": "scan",
        "status": "running",
        "total": 4,
        "processed": 2,
        "current_item": f"service:{service_id}/credential:cred-2",
        "progress_percent": 50,
        "logs": [
            "Loaded service inventory",
            "Scanning credential cred-2",
        ],
        "stats": {
            "service_ids": [service_id],
            "service_count": 1,
            "scan_concurrency": 2,
        },
    }

    detail_response = client.get(f"/api/cpa/tasks/{task_id}")

    assert detail_response.status_code == 200
    assert detail_response.json() == {
        "task_id": str(task_id),
        "type": "scan",
        "status": "running",
        "total": 4,
        "processed": 2,
        "current_item": f"service:{service_id}/credential:cred-2",
        "progress_percent": 50,
        "logs": [
            "Loaded service inventory",
            "Scanning credential cred-2",
        ],
        "stats": {
            "service_ids": [service_id],
            "service_count": 1,
            "scan_concurrency": 2,
        },
    }


def test_cpa_workbench_scan_action_defaults_use_concurrency_two(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="default-concurrency-service",
            api_url="https://default.example.com",
            api_token="default-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-expired",
            status="expired",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 15, 0, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-quota",
            status="quota_limited",
            quota_status="limited",
            last_scanned_at=datetime(2026, 3, 28, 15, 5, 0),
        )
        db.commit()

    def run_scan_inline(task_id: int) -> None:
        with session_module.get_db() as db:
            crud.finalize_cpa_workbench_task(db, task_id, status="completed")

    def run_action_inline(task_id: int) -> None:
        with session_module.get_db() as db:
            crud.finalize_cpa_workbench_task(db, task_id, status="completed")

    monkeypatch.setattr(cpa_workbench_routes_module, "_dispatch_cpa_scan_job", run_scan_inline)
    monkeypatch.setattr(cpa_workbench_routes_module, "_dispatch_cpa_action_job", run_action_inline)

    scan_response = client.post("/api/cpa/scan", json={"service_ids": [service_id]})

    assert scan_response.status_code == 202
    assert scan_response.json()["stats"]["scan_concurrency"] == 2

    delete_response = client.post(
        "/api/cpa/actions",
        json={
            "service_ids": [service_id],
            "quota_action": "delete",
        },
    )

    assert delete_response.status_code == 202
    assert delete_response.json()["stats"]["delete_concurrency"] == 2

    disable_response = client.post(
        "/api/cpa/actions",
        json={
            "service_ids": [service_id],
            "quota_action": "disable",
        },
    )

    assert disable_response.status_code == 202
    assert disable_response.json()["stats"]["disable_concurrency"] == 2


def test_cpa_workbench_action_tasks_expose_distinct_quota_action_metadata(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="action-type-service",
            api_url="https://action-type.example.com",
            api_token="action-type-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-expired",
            status="expired",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 16, 0, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-quota",
            status="quota_limited",
            quota_status="limited",
            last_scanned_at=datetime(2026, 3, 28, 16, 5, 0),
        )
        db.commit()

    def run_action_inline(task_id: int) -> None:
        with session_module.get_db() as db:
            crud.finalize_cpa_workbench_task(db, task_id, status="completed")

    monkeypatch.setattr(cpa_workbench_routes_module, "_dispatch_cpa_action_job", run_action_inline)

    delete_response = client.post(
        "/api/cpa/actions",
        json={
            "service_ids": [service_id],
            "quota_action": "delete",
        },
    )

    assert delete_response.status_code == 202
    assert delete_response.json()["type"] == "action"
    assert delete_response.json()["stats"]["quota_action"] == "delete"

    disable_response = client.post(
        "/api/cpa/actions",
        json={
            "service_ids": [service_id],
            "quota_action": "disable",
        },
    )

    assert disable_response.status_code == 202
    assert disable_response.json()["type"] == "action"
    assert disable_response.json()["stats"]["quota_action"] == "disable"


def test_cpa_workbench_latest_active_retrieval_honors_selected_service_scope(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    session_cookie = client.cookies.get("session_id")
    session_id = session_cookie.split(".", 1)[0] if session_cookie else None
    assert session_id

    with session_module.get_db() as db:
        alpha = crud.create_cpa_service(
            db,
            name="alpha-scope-service",
            api_url="https://alpha-scope.example.com",
            api_token="alpha-scope-token",
            enabled=True,
            priority=1,
        )
        beta = crud.create_cpa_service(
            db,
            name="beta-scope-service",
            api_url="https://beta-scope.example.com",
            api_token="beta-scope-token",
            enabled=True,
            priority=2,
        )
        alpha_id = alpha.id
        beta_id = beta.id
        alpha_task = crud.create_cpa_scan_task(db, owner_session_id=session_id, service_ids=[alpha_id])
        beta_task = crud.create_cpa_scan_task(db, owner_session_id=session_id, service_ids=[beta_id])
        alpha_task_id = alpha_task.id
        beta_task_id = beta_task.id
        crud.start_cpa_workbench_task(db, alpha_task.id)
        crud.start_cpa_workbench_task(db, beta_task.id)

    alpha_response = client.get(
        "/api/cpa/tasks/latest-active",
        params={"type": "scan", "service_ids": str(alpha_id)},
    )

    assert alpha_response.status_code == 200
    assert alpha_response.json()["task_id"] == str(alpha_task_id)
    assert alpha_response.json()["stats"]["service_ids"] == [alpha_id]

    beta_response = client.get(
        "/api/cpa/tasks/latest-active",
        params={"type": "scan", "service_ids": str(beta_id)},
    )

    assert beta_response.status_code == 200
    assert beta_response.json()["task_id"] == str(beta_task_id)
    assert beta_response.json()["stats"]["service_ids"] == [beta_id]


def test_cpa_workbench_latest_active_with_explicit_empty_scope_returns_no_task(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)
    session_cookie = client.cookies.get("session_id")
    session_id = session_cookie.split(".", 1)[0] if session_cookie else None
    assert session_id

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="empty-scope-service",
            api_url="https://empty-scope.example.com",
            api_token="empty-scope-token",
            enabled=True,
            priority=1,
        )
        task = crud.create_cpa_scan_task(db, owner_session_id=session_id, service_ids=[service.id])
        crud.start_cpa_workbench_task(db, task.id)

    response = client.get(
        "/api/cpa/tasks/latest-active",
        params={"type": "scan", "service_scope": "empty"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "CPA workbench task not found"


def test_cpa_inventory_status_filter_applies_to_rows_and_counts(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="filter-service",
            api_url="https://filter.example.com",
            api_token="filter-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id

        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-valid-1",
            status="valid",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 14, 0, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-valid-2",
            status="valid",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 14, 5, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-expired",
            status="expired",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 14, 10, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-error",
            status="error",
            quota_status="unknown",
            last_scanned_at=datetime(2026, 3, 28, 14, 15, 0),
        )
        db.commit()

    response = client.get(
        "/api/cpa/credentials",
        params={"service_ids": str(service_id), "status": "valid"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["rows"] == [
        {
            "service_id": service_id,
            "credential_id": "cred-valid-1",
            "service_name": "filter-service",
            "status": "valid",
            "quota_status": "healthy",
            "last_scanned_at": "2026-03-28T14:00:00",
        },
        {
            "service_id": service_id,
            "credential_id": "cred-valid-2",
            "service_name": "filter-service",
            "status": "valid",
            "quota_status": "healthy",
            "last_scanned_at": "2026-03-28T14:05:00",
        },
    ]
    assert payload["counts"] == {"returned": 2, "total": 2}
    assert "summary" not in payload


def test_summary_endpoint_returns_aggregate_counts(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="usable-service",
            api_url="https://usable.example.com",
            api_token="usable-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id

        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-valid",
            status="valid",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 13, 0, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-expired",
            status="expired",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 13, 5, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-quota",
            status="quota_limited",
            quota_status="limited",
            last_scanned_at=datetime(2026, 3, 28, 13, 10, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-error",
            status="error",
            quota_status="unknown",
            last_scanned_at=datetime(2026, 3, 28, 13, 15, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-unknown",
            status="unknown",
            quota_status="unknown",
            last_scanned_at=datetime(2026, 3, 28, 13, 20, 0),
        )
        db.commit()

    response = client.get("/api/cpa/summary", params={"service_ids": str(service_id)})

    assert response.status_code == 200
    assert response.json() == {
        "total": 5,
        "valid_count": 1,
        "expired_count": 1,
        "quota_count": 1,
        "error_count": 1,
        "unknown_count": 1,
    }


def test_summary_endpoint_status_filter_applies_to_counts(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="filter-service",
            api_url="https://filter.example.com",
            api_token="filter-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id

        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-valid-1",
            status="valid",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 14, 0, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-valid-2",
            status="valid",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 14, 5, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-expired",
            status="expired",
            quota_status="healthy",
            last_scanned_at=datetime(2026, 3, 28, 14, 10, 0),
        )
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-error",
            status="error",
            quota_status="unknown",
            last_scanned_at=datetime(2026, 3, 28, 14, 15, 0),
        )
        db.commit()

    response = client.get(
        "/api/cpa/summary",
        params={"service_ids": str(service_id), "status": "valid"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "total": 2,
        "valid_count": 2,
        "expired_count": 0,
        "quota_count": 0,
        "error_count": 0,
        "unknown_count": 0,
    }


def test_cpa_scan_classifies_valid_expired_quota_and_error(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    class FakeCLIProxyClient:
        def __init__(self, base_url: str, token: str = "", timeout: int = 30):
            self.base_url = base_url
            self.token = token
            self.timeout = timeout

        def fetch_inventory(self):
            return [
                {"remote_file_id": "cred-valid"},
                {"remote_file_id": "cred-expired"},
                {"remote_file_id": "cred-quota"},
                {"remote_file_id": "cred-error"},
            ]

        def probe_usage(self, remote_file_id: str):
            if remote_file_id == "cred-valid":
                return {"status": "ok"}
            if remote_file_id == "cred-expired":
                return {"status_code": 401}
            if remote_file_id == "cred-quota":
                return {"quota_limited": True}
            raise RuntimeError("probe exploded")

    def run_inline(task_id: int) -> None:
        cpa_scan_tasks_module.run_cpa_scan_task(task_id)

    monkeypatch.setattr(cpa_scan_tasks_module, "CLIProxyAPIClient", FakeCLIProxyClient)
    monkeypatch.setattr(cpa_workbench_routes_module, "_dispatch_cpa_scan_job", run_inline)

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

    response = client.post("/api/cpa/scan", json={"service_ids": [service_id], "concurrency": 4})

    assert response.status_code == 202
    task_payload = response.json()
    assert task_payload["stats"]["scan_concurrency"] == 4

    detail_response = client.get(f"/api/cpa/tasks/{task_payload['task_id']}")

    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["stats"]["scan_concurrency"] == 4
    assert detail_payload["stats"]["valid_count"] == 1
    assert detail_payload["stats"]["expired_count"] == 1
    assert detail_payload["stats"]["quota_count"] == 1
    assert detail_payload["stats"]["error_count"] == 1
    assert detail_payload["stats"]["unknown_count"] == 0
    assert detail_payload["processed"] == 4
    assert detail_payload["total"] == 4
    assert detail_payload["progress_percent"] == 100

    inventory_response = client.get("/api/cpa/credentials", params={"service_ids": str(service_id)})

    assert inventory_response.status_code == 200
    payload = inventory_response.json()
    assert "summary" not in payload
    assert payload["rows"] == [
        {
            "service_id": service_id,
            "credential_id": "cred-error",
            "service_name": "alpha-service",
            "status": "error",
            "quota_status": "unknown",
            "last_scanned_at": payload["rows"][0]["last_scanned_at"],
        },
        {
            "service_id": service_id,
            "credential_id": "cred-expired",
            "service_name": "alpha-service",
            "status": "expired",
            "quota_status": "healthy",
            "last_scanned_at": payload["rows"][1]["last_scanned_at"],
        },
        {
            "service_id": service_id,
            "credential_id": "cred-quota",
            "service_name": "alpha-service",
            "status": "quota_limited",
            "quota_status": "limited",
            "last_scanned_at": payload["rows"][2]["last_scanned_at"],
        },
        {
            "service_id": service_id,
            "credential_id": "cred-valid",
            "service_name": "alpha-service",
            "status": "valid",
            "quota_status": "healthy",
            "last_scanned_at": payload["rows"][3]["last_scanned_at"],
        },
    ]

    summary_response = client.get("/api/cpa/summary", params={"service_ids": str(service_id)})

    assert summary_response.status_code == 200
    assert summary_response.json() == {
        "total": 4,
        "valid_count": 1,
        "expired_count": 1,
        "quota_count": 1,
        "error_count": 1,
        "unknown_count": 0,
    }


def test_cpa_scan_preserves_explicit_overlay_statuses_from_probe(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    class FakeCLIProxyClient:
        def __init__(self, base_url: str, token: str = "", timeout: int = 30):
            self.base_url = base_url
            self.token = token
            self.timeout = timeout

        def fetch_inventory(self):
            return [
                {"remote_file_id": "cred-valid"},
                {"remote_file_id": "cred-expired"},
                {"remote_file_id": "cred-quota"},
                {"remote_file_id": "cred-error"},
                {"remote_file_id": "cred-unknown"},
            ]

        def probe_usage(self, remote_file_id: str):
            if remote_file_id == "cred-valid":
                return {"status": "valid"}
            if remote_file_id == "cred-expired":
                return {"status": "expired"}
            if remote_file_id == "cred-quota":
                return {"status": "quota_limited"}
            if remote_file_id == "cred-error":
                return {"status": "error"}
            return {"status": "unknown"}

    def run_inline(task_id: int) -> None:
        cpa_scan_tasks_module.run_cpa_scan_task(task_id)

    monkeypatch.setattr(cpa_scan_tasks_module, "CLIProxyAPIClient", FakeCLIProxyClient)
    monkeypatch.setattr(cpa_workbench_routes_module, "_dispatch_cpa_scan_job", run_inline)

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

    response = client.post("/api/cpa/scan", json={"service_ids": [service_id]})

    assert response.status_code == 202
    task_payload = response.json()

    detail_response = client.get(f"/api/cpa/tasks/{task_payload['task_id']}")

    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["stats"]["valid_count"] == 1
    assert detail_payload["stats"]["expired_count"] == 1
    assert detail_payload["stats"]["quota_count"] == 1
    assert detail_payload["stats"]["error_count"] == 1
    assert detail_payload["stats"]["unknown_count"] == 1

    inventory_response = client.get("/api/cpa/credentials", params={"service_ids": str(service_id)})

    assert inventory_response.status_code == 200
    payload = inventory_response.json()
    assert [row["status"] for row in payload["rows"]] == [
        "error",
        "expired",
        "quota_limited",
        "unknown",
        "valid",
    ]


def test_cpa_detail_panel_shows_recent_log_excerpt_and_view_logs_action(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="detail-service",
            api_url="https://detail.example.com",
            api_token="detail-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id
        account = crud.create_account(
            db,
            email="linked@example.com",
            email_service="manual",
            password="pw",
            status="active",
        )
        account_id = account.id
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-detail",
            status="quota_limited",
            quota_status="limited",
            summary_json={
                "recent_log_excerpt": [
                    "2026-03-28 18:00 quota threshold reached",
                    "2026-03-28 18:02 auto-disable recommended",
                ]
            },
            last_scanned_at=datetime(2026, 3, 28, 18, 5, 0),
            local_account_id=account.id,
        )
        db.commit()

    response = client.get(f"/api/cpa/credentials/{service_id}/cred-detail")

    assert response.status_code == 200
    assert response.json() == {
        "service_id": service_id,
        "credential_id": "cred-detail",
        "service_name": "detail-service",
        "status": "quota_limited",
        "quota_status": "limited",
        "last_scanned_at": "2026-03-28T18:05:00",
        "local_account_summary": {
            "account_id": account_id,
            "email": "linked@example.com",
            "status": "active",
            "jump_href": f"/accounts?account_id={account_id}",
        },
        "status_summary": "quota_limited / limited",
        "default_action": "disable",
        "recent_log_excerpt": [
            "2026-03-28 18:00 quota threshold reached",
            "2026-03-28 18:02 auto-disable recommended",
        ],
        "view_logs_target": f"/api/cpa/credentials/{service_id}/cred-detail/logs",
    }


def test_cpa_detail_shows_local_account_summary_when_available(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="linked-service",
            api_url="https://linked.example.com",
            api_token="linked-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id
        account = crud.create_account(
            db,
            email="aux@example.com",
            email_service="manual",
            status="active",
        )
        account_id = account.id
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-aux",
            status="valid",
            quota_status="healthy",
            local_account_id=account_id,
        )
        db.commit()

    response = client.get(f"/api/cpa/credentials/{service_id}/cred-aux")

    assert response.status_code == 200
    assert response.json()["local_account_summary"] == {
        "account_id": account_id,
        "email": "aux@example.com",
        "status": "active",
        "jump_href": f"/accounts?account_id={account_id}",
    }


def test_cpa_detail_panel_supports_single_item_scan_delete_and_disable_actions(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="single-item-service",
            api_url="https://single-item.example.com",
            api_token="single-item-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-single",
            status="expired",
            quota_status="healthy",
            summary_json={"recent_log_excerpt": ["401 detected"]},
            last_scanned_at=datetime(2026, 3, 28, 19, 0, 0),
        )
        db.commit()

    original_create_scan_task = crud.create_cpa_scan_task
    original_create_action_task = crud.create_cpa_action_task
    captured_calls = []

    def record_scan(db, *, owner_session_id, service_ids, credential_ids=None, concurrency=2):
        task = original_create_scan_task(
            db,
            owner_session_id=owner_session_id,
            service_ids=service_ids,
            credential_ids=credential_ids,
            concurrency=concurrency,
        )
        captured_calls.append(("scan", task.stats_json))
        return task

    def record_action(db, *, owner_session_id, service_ids, credential_ids=None, quota_action="disable", delete_concurrency=2, disable_concurrency=2):
        task = original_create_action_task(
            db,
            owner_session_id=owner_session_id,
            service_ids=service_ids,
            credential_ids=credential_ids,
            quota_action=quota_action,
            delete_concurrency=delete_concurrency,
            disable_concurrency=disable_concurrency,
        )
        captured_calls.append((quota_action, task.stats_json))
        return task

    monkeypatch.setattr(crud, "create_cpa_scan_task", record_scan)
    monkeypatch.setattr(crud, "create_cpa_action_task", record_action)
    monkeypatch.setattr(cpa_workbench_routes_module, "_dispatch_cpa_scan_job", lambda task_id: None)
    monkeypatch.setattr(cpa_workbench_routes_module, "_dispatch_cpa_action_job", lambda task_id: None)

    scan_response = client.post(
        "/api/cpa/scan",
        json={"service_ids": [service_id], "credential_ids": ["cred-single"]},
    )
    delete_response = client.post(
        "/api/cpa/actions",
        json={"service_ids": [service_id], "credential_ids": ["cred-single"], "quota_action": "delete"},
    )
    disable_response = client.post(
        "/api/cpa/actions",
        json={"service_ids": [service_id], "credential_ids": ["cred-single"], "quota_action": "disable"},
    )

    assert scan_response.status_code == 202
    assert delete_response.status_code == 202
    assert disable_response.status_code == 202
    assert captured_calls == [
        (
            "scan",
            {"service_ids": [service_id], "credential_ids": ["cred-single"], "service_count": 1, "scan_concurrency": 2},
        ),
        (
            "delete",
            {
                "service_ids": [service_id],
                "credential_ids": ["cred-single"],
                "action_count": 1,
                "delete_count": 1,
                "disable_count": 0,
                "delete_concurrency": 2,
                "disable_concurrency": 2,
                "quota_action": "delete",
                "actions": [
                    {
                        "service_id": service_id,
                        "credential_id": "cred-single",
                        "status": "expired",
                        "quota_status": "healthy",
                        "action": "delete",
                    }
                ],
            },
        ),
        (
            "disable",
            {
                "service_ids": [service_id],
                "credential_ids": ["cred-single"],
                "action_count": 1,
                "delete_count": 0,
                "disable_count": 1,
                "delete_concurrency": 2,
                "disable_concurrency": 2,
                "quota_action": "disable",
                "actions": [
                    {
                        "service_id": service_id,
                        "credential_id": "cred-single",
                        "status": "expired",
                        "quota_status": "healthy",
                        "action": "disable",
                    }
                ],
            },
        ),
    ]


def test_cpa_detail_payload_exposes_stable_credential_scoped_log_target(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="history-service",
            api_url="https://history.example.com",
            api_token="history-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-history",
            status="expired",
            quota_status="healthy",
            summary_json={"recent_log_excerpt": ["401 detected", "manual review queued"]},
            last_scanned_at=datetime(2026, 3, 28, 20, 0, 0),
        )
        db.commit()

    response = client.get(f"/api/cpa/credentials/{service_id}/cred-history")

    assert response.status_code == 200
    payload = response.json()
    assert payload["view_logs_target"] == f"/api/cpa/credentials/{service_id}/cred-history/logs"
    assert "latest-active" not in payload["view_logs_target"]
    assert str(service_id) in payload["view_logs_target"]
    assert "cred-history" in payload["view_logs_target"]


def test_cpa_detail_payload_log_target_resolves_for_selected_credential(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="resolvable-history-service",
            api_url="https://resolvable-history.example.com",
            api_token="resolvable-history-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id
        crud.upsert_cpa_remote_credential_snapshot(
            db,
            service_id=service_id,
            credential_id="cred-loggable",
            status="quota_limited",
            quota_status="limited",
            summary_json={
                "recent_log_excerpt": [
                    "quota limited during probe",
                    "explicit disable recommended",
                ]
            },
            last_scanned_at=datetime(2026, 3, 28, 20, 15, 0),
        )
        db.commit()

    detail_response = client.get(f"/api/cpa/credentials/{service_id}/cred-loggable")

    assert detail_response.status_code == 200
    log_target = detail_response.json()["view_logs_target"]

    logs_response = client.get(log_target)

    assert logs_response.status_code == 200
    assert logs_response.json() == {
        "service_id": service_id,
        "credential_id": "cred-loggable",
        "service_name": "resolvable-history-service",
        "status": "quota_limited",
        "quota_status": "limited",
        "last_scanned_at": "2026-03-28T20:15:00",
        "logs": [
            "quota limited during probe",
            "explicit disable recommended",
        ],
        "history": [
            {
                "kind": "recent_log_excerpt",
                "lines": [
                    "quota limited during probe",
                    "explicit disable recommended",
                ],
            }
        ],
    }
