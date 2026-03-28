from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List

from ...database import crud
from ...database.session import get_db
from ..cliproxy.client import CLIProxyAPIClient


def _classify_scan_result(probe_result: Dict[str, Any]) -> tuple[str, str]:
    status_code = probe_result.get("status_code")
    status = probe_result.get("status")
    if status in {"valid", "expired", "quota_limited", "error", "unknown"}:
        if status == "valid":
            return status, "healthy"
        if status == "quota_limited":
            return status, "limited"
        if status == "expired":
            return status, "healthy"
        return status, "unknown"
    if status_code == 401:
        return "expired", "healthy"
    if probe_result.get("quota_limited") is True:
        return "quota_limited", "limited"
    if status in {"ok", "healthy", "success"}:
        return "valid", "healthy"
    return "unknown", "unknown"


def _empty_scan_counts() -> Dict[str, int]:
    return {
        "valid_count": 0,
        "expired_count": 0,
        "quota_count": 0,
        "error_count": 0,
        "unknown_count": 0,
    }


def _increment_scan_count(counts: Dict[str, int], status: str) -> None:
    if status == "valid":
        counts["valid_count"] += 1
    elif status == "expired":
        counts["expired_count"] += 1
    elif status == "quota_limited":
        counts["quota_count"] += 1
    elif status == "error":
        counts["error_count"] += 1
    else:
        counts["unknown_count"] += 1


def _scan_credential(service, remote_record: Dict[str, Any]) -> Dict[str, Any]:
    try:
        credential_id = str(
            remote_record.get("credential_id")
            or remote_record.get("remote_file_id")
            or remote_record.get("id")
            or ""
        )
        if not credential_id:
            raise ValueError("Remote credential is missing an identifier")

        client = CLIProxyAPIClient(base_url=service.api_url, token=service.api_token)
        probe_result = client.probe_usage(credential_id)
        status, quota_status = _classify_scan_result(dict(probe_result or {}))
        return {
            "credential_id": credential_id,
            "status": status,
            "quota_status": quota_status,
            "summary_json": {"probe_result": dict(probe_result or {})},
            "log_line": (
                f"service_id={service.id} service_name={service.name} "
                f"credential_id={credential_id} status={status}"
            ),
        }
    except Exception as exc:
        credential_id = str(
            remote_record.get("credential_id")
            or remote_record.get("remote_file_id")
            or remote_record.get("id")
            or "<missing>"
        )
        failure_reason = str(exc) or exc.__class__.__name__
        return {
            "credential_id": credential_id,
            "status": "error",
            "quota_status": "unknown",
            "summary_json": {"error": failure_reason},
            "log_line": (
                f"service_id={service.id} service_name={service.name} "
                f"credential_id={credential_id} status=error failure_reason={failure_reason}"
            ),
        }


def run_cpa_scan_task(task_id: int) -> None:
    with get_db() as db:
        task = crud.start_cpa_workbench_task(db, task_id)
        if task is None or task.status != "running":
            return

        stats = dict(task.stats_json or {})
        service_ids = list(stats.get("service_ids") or [])
        concurrency = max(int(stats.get("scan_concurrency") or crud.CPA_SCAN_DEFAULT_CONCURRENCY), 1)
        services = [crud.get_cpa_service_by_id(db, int(service_id)) for service_id in service_ids]
        services = [service for service in services if service is not None]

    processed_count = 0
    total_count = 0
    counts = _empty_scan_counts()
    failure = False

    for service in services:
        client = CLIProxyAPIClient(base_url=service.api_url, token=service.api_token)
        try:
            inventory = list(client.fetch_inventory())
        except Exception as exc:
            failure = True
            with get_db() as db:
                processed_count += 1
                total_count += 1
                counts["error_count"] += 1
                crud.append_cpa_workbench_task_logs(
                    db,
                    task_id,
                    [
                        f"service_id={service.id} service_name={service.name} "
                        f"credential_id=<inventory> status=error "
                        f"failure_reason={str(exc) or exc.__class__.__name__}"
                    ],
                )
                crud.update_cpa_workbench_task_progress(
                    db,
                    task_id,
                    processed_count=processed_count,
                    total_count=total_count,
                    current_item=f"service:{service.id}",
                    stats_json=counts,
                )
            continue

        total_count += len(inventory)
        with get_db() as db:
            crud.update_cpa_workbench_task_progress(
                db,
                task_id,
                total_count=total_count,
                current_item=f"service:{service.id}",
            )

        def _scan(remote_record: Dict[str, Any]) -> Dict[str, Any]:
            return _scan_credential(service, remote_record)

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            results: List[Dict[str, Any]] = list(executor.map(_scan, inventory))

        for result in results:
            processed_count += 1
            _increment_scan_count(counts, result["status"])
            now = datetime.utcnow()
            with get_db() as db:
                crud.upsert_cpa_remote_credential_snapshot(
                    db,
                    service_id=service.id,
                    credential_id=result["credential_id"],
                    status=result["status"],
                    quota_status=result["quota_status"],
                    last_scanned_at=now,
                    summary_json=result["summary_json"],
                )
                crud.append_cpa_workbench_task_logs(db, task_id, [result["log_line"]])
                crud.update_cpa_workbench_task_progress(
                    db,
                    task_id,
                    processed_count=processed_count,
                    total_count=total_count,
                    current_item=f"service:{service.id}/credential:{result['credential_id']}",
                    stats_json=counts,
                )
                db.commit()

    with get_db() as db:
        crud.finalize_cpa_workbench_task(
            db,
            task_id,
            status="failed" if failure else "completed",
            current_item=None,
        )
