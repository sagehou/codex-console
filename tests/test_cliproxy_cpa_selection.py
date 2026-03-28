from pathlib import Path

from fastapi.testclient import TestClient

import src.database.session as session_module
from src.database import crud

from tests.test_cliproxy_routes import authenticate_client, build_client


def test_cliproxy_lists_enabled_cpa_services_as_selectable_targets(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        complete_service = crud.create_cpa_service(
            db,
            name="complete-service",
            api_url="https://cpa-one.example.com",
            api_token="token-one",
            enabled=True,
            priority=2,
        )
        incomplete_service = crud.create_cpa_service(
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
            api_url="https://cpa-disabled.example.com",
            api_token="disabled-token",
            enabled=False,
            priority=0,
        )
        complete_service_id = complete_service.id
        incomplete_service_id = incomplete_service.id

    response = client.get("/api/cliproxy/cpa-services")

    assert response.status_code == 200
    payload = response.json()
    assert [item["name"] for item in payload] == ["incomplete-service", "complete-service"]
    assert [item["id"] for item in payload] == [incomplete_service_id, complete_service_id]
    assert all(item["enabled"] is True for item in payload)

    incomplete = payload[0]
    assert incomplete["config_status"] == "config incomplete"
    assert incomplete["missing_required_fields"] == ["api_url", "api_token"]
    assert incomplete["action_state"] == {
        "test_connection": {"enabled": False, "reason": "config incomplete"},
        "scan": {"enabled": False, "reason": "config incomplete"},
        "maintain": {"enabled": False, "reason": "config incomplete"},
    }

    complete = payload[1]
    assert complete["config_status"] == "ready"
    assert complete["missing_required_fields"] == []
    assert complete["action_state"] == {
        "test_connection": {"enabled": True, "reason": None},
        "scan": {"enabled": True, "reason": None},
        "maintain": {"enabled": True, "reason": None},
    }


def test_cliproxy_page_can_recover_latest_active_task_without_preselected_service_set(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        first = crud.create_cpa_service(
            db,
            name="alpha",
            api_url="https://alpha.example.com",
            api_token="token-alpha",
            enabled=True,
            priority=1,
        )
        second = crud.create_cpa_service(
            db,
            name="beta",
            api_url="https://beta.example.com",
            api_token="token-beta",
            enabled=True,
            priority=2,
        )
        environment = crud.ensure_cliproxy_environment_for_cpa_service(db, first)
        run = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment.id,
            status="running",
            summary_json={
                "owner_session_id": client.cookies.get("session_id"),
                "aggregate_key": f"scan:{first.id},{second.id}",
                "service_ids": [first.id, second.id],
                "service_total": 2,
                "service_completed": 0,
                "known_record_total": None,
                "processed_record_total": 0,
                "progress_percent": 10,
                "services": [
                    {
                        "service_id": first.id,
                        "service_name": first.name,
                        "status": "running",
                        "known_record_total": None,
                        "processed_count": 0,
                        "success_count": 0,
                        "failure_count": 0,
                        "current_stage": "fetching_inventory",
                        "last_error": None,
                    },
                    {
                        "service_id": second.id,
                        "service_name": second.name,
                        "status": "queued",
                        "known_record_total": None,
                        "processed_count": 0,
                        "success_count": 0,
                        "failure_count": 0,
                        "current_stage": "queued",
                        "last_error": None,
                    },
                ],
            },
        )

    response = client.get("/api/cliproxy/tasks/latest-active")

    assert response.status_code == 200
    payload = response.json()
    assert payload["task_id"] == str(run.id)
    assert payload["status"] == "running"
    assert payload["service_total"] == 2
    assert payload["services"][0]["service_name"] == "alpha"
    assert payload["services"][1]["service_name"] == "beta"


def test_cliproxy_test_connection_limits_selection_count(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service_ids = [
            crud.create_cpa_service(
                db,
                name=f"service-{index}",
                api_url=f"https://service-{index}.example.com",
                api_token=f"token-{index}",
                enabled=True,
                priority=index,
            ).id
            for index in range(11)
        ]

    response = client.post("/api/cliproxy/test-connection", json={"service_ids": service_ids})

    assert response.status_code == 422
    assert response.json()["detail"] == "Select at most 10 CPA services"


def test_cliproxy_test_connection_limits_selection_count(monkeypatch, tmp_path: Path):
    client = build_client(monkeypatch, tmp_path)
    authenticate_client(client)

    with session_module.get_db() as db:
        service_ids = []
        for index in range(11):
            service = crud.create_cpa_service(
                db,
                name=f"service-{index}",
                api_url=f"https://service-{index}.example.com",
                api_token=f"token-{index}",
                enabled=True,
                priority=index,
            )
            service_ids.append(service.id)

    response = client.post(
        "/api/cliproxy/test-connection",
        json={"service_ids": service_ids},
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "Select at most 10 CPA services"
