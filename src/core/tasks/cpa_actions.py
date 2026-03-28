from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

from ...database import crud
from ...database.session import get_db
from ..cliproxy.client import CLIProxyAPIClient


def _delete_remote_credential(service, credential_id: str) -> Dict[str, Any]:
    client = CLIProxyAPIClient(base_url=service.api_url, token=service.api_token)
    try:
        result = client._request("DELETE", f"/inventory/{credential_id}")
        return {
            "credential_id": credential_id,
            "status": "completed",
            "log_line": f"[{service.name}] deleted {credential_id}",
            "result": dict(result or {}),
        }
    except Exception as exc:
        return {
            "credential_id": credential_id,
            "status": "failed",
            "log_line": f"[{service.name}] delete failed for {credential_id}: {str(exc) or exc.__class__.__name__}",
            "error": str(exc) or exc.__class__.__name__,
        }


def _disable_remote_credential(service, credential_id: str) -> Dict[str, Any]:
    client = CLIProxyAPIClient(base_url=service.api_url, token=service.api_token)
    try:
        result = client.disable_auth(credential_id)
        return {
            "credential_id": credential_id,
            "status": "completed",
            "log_line": f"[{service.name}] disabled {credential_id}",
            "result": dict(result or {}),
        }
    except Exception as exc:
        return {
            "credential_id": credential_id,
            "status": "failed",
            "log_line": f"[{service.name}] disable failed for {credential_id}: {str(exc) or exc.__class__.__name__}",
            "error": str(exc) or exc.__class__.__name__,
        }


def run_cpa_action_task(task_id: int) -> None:
    with get_db() as db:
        task = crud.start_cpa_workbench_task(db, task_id)
        if task is None or task.status != "running":
            return

        stats = dict(task.stats_json or {})
        actions = list(stats.get("actions") or [])
        services_by_id = {
            service.id: service
            for service in [crud.get_cpa_service_by_id(db, int(item.get("service_id") or 0)) for item in actions]
            if service is not None
        }
        delete_concurrency = max(int(stats.get("delete_concurrency") or crud.CPA_DELETE_DEFAULT_CONCURRENCY), 1)
        disable_concurrency = max(int(stats.get("disable_concurrency") or crud.CPA_DISABLE_DEFAULT_CONCURRENCY), 1)

    def _run_group(group_actions: List[Dict[str, Any]], action_name: str, max_workers: int) -> List[Dict[str, Any]]:
        if not group_actions:
            return []

        def _execute(item: Dict[str, Any]) -> Dict[str, Any]:
            service = services_by_id.get(int(item["service_id"]))
            credential_id = str(item["credential_id"])
            if service is None:
                return {
                    "service_id": int(item["service_id"]),
                    "credential_id": credential_id,
                    "action": action_name,
                    "status": "failed",
                    "log_line": f"service {item['service_id']} not found for {credential_id}",
                }
            if action_name == "delete":
                result = _delete_remote_credential(service, credential_id)
            else:
                result = _disable_remote_credential(service, credential_id)
            result["service_id"] = int(item["service_id"])
            result["action"] = action_name
            return result

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(_execute, group_actions))

    delete_actions = [item for item in actions if item.get("action") == "delete"]
    disable_actions = [item for item in actions if item.get("action") == "disable"]
    results = _run_group(delete_actions, "delete", delete_concurrency) + _run_group(
        disable_actions,
        "disable",
        disable_concurrency,
    )

    processed_count = 0
    failure = False
    for result in results:
        processed_count += 1
        current_item = f"service:{result['service_id']}/credential:{result['credential_id']}"
        with get_db() as db:
            crud.append_cpa_workbench_task_logs(db, task_id, [result["log_line"]])
            if result["status"] == "completed":
                if result["action"] == "delete":
                    crud.delete_cpa_remote_credential_snapshot(
                        db,
                        service_id=int(result["service_id"]),
                        credential_id=str(result["credential_id"]),
                    )
                else:
                    snapshot = crud.get_cpa_remote_credential_snapshot(
                        db,
                        service_id=int(result["service_id"]),
                        credential_id=str(result["credential_id"]),
                    )
                    if snapshot is not None:
                        crud.upsert_cpa_remote_credential_snapshot(
                            db,
                            service_id=snapshot.service_id,
                            credential_id=snapshot.credential_id,
                            status="disabled",
                            quota_status=snapshot.quota_status,
                            summary_json=dict(snapshot.summary_json or {}),
                            last_scanned_at=snapshot.last_scanned_at,
                            local_account_id=snapshot.local_account_id,
                        )
                        db.commit()
            else:
                failure = True
            crud.update_cpa_workbench_task_progress(
                db,
                task_id,
                processed_count=processed_count,
                current_item=current_item,
            )

    with get_db() as db:
        crud.finalize_cpa_workbench_task(
            db,
            task_id,
            status="failed" if failure else "completed",
            current_item=None,
        )
