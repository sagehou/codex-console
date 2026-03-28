from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from ...database import crud
from ...database.session import get_db
from ..cliproxy.client import CLIProxyAPIClient
from ..cliproxy.matching import match_remote_record


def _classify_probe(probe_result: Dict[str, Any]) -> str:
    status_code = probe_result.get("status_code")
    status = probe_result.get("status")
    if status_code == 401:
        return "unauthorized_401"
    if probe_result.get("quota_limited") is True:
        return "quota_limited"
    if status in {"ok", "healthy", "success"}:
        return "recovered"
    return "probe_failed"


def _build_service_log_prefix(service_name: str) -> str:
    return f"[{service_name}]"


def _scan_decision(classification: str) -> bool:
    return classification in {"unauthorized_401", "recovered", "quota_limited", "probe_failed"}


def _run_service_task(task_id: int, run_type: str, service_id: int, *, dry_run: bool = False) -> None:
    with get_db() as db:
        service = crud.get_cpa_service_by_id(db, service_id)
        if service is None:
            return
        environment = crud.ensure_cliproxy_environment_for_cpa_service(db, service)
        service_name = service.name
        prefix = _build_service_log_prefix(service_name)
        crud.update_cliproxy_aggregate_service(
            db,
            task_id,
            service_id=service_id,
            status="running",
            current_stage="fetching_inventory",
            log_lines=[f"{prefix} fetching inventory"],
        )
        client = CLIProxyAPIClient(base_url=service.api_url, token=service.api_token)

        try:
            inventory = list(client.fetch_inventory())
            total = len(inventory)
            crud.update_cliproxy_aggregate_service(
                db,
                task_id,
                service_id=service_id,
                known_record_total=total,
                current_stage="probing" if run_type == "scan" else "executing",
                log_lines=[f"{prefix} inventory ready: {total} records"],
            )

            success_count = 0
            failure_count = 0
            processed_count = 0
            for remote_record in inventory:
                match = match_remote_record(db, environment.id, remote_record)
                classification = _classify_probe(client.probe_usage(remote_record["remote_file_id"]))
                processed_count += 1
                if _scan_decision(classification):
                    success_count += 1
                else:
                    failure_count += 1
                if run_type == "maintain" and not dry_run:
                    if classification == "unauthorized_401":
                        client.disable_auth(remote_record["remote_file_id"])
                    elif classification == "recovered":
                        client.reenable_auth(remote_record["remote_file_id"])
                crud.upsert_remote_auth_inventory(
                    db,
                    environment_id=environment.id,
                    remote_file_id=remote_record["remote_file_id"],
                    email=remote_record.get("remote_email") or remote_record.get("email"),
                    remote_account_id=remote_record.get("remote_account_id"),
                    local_account_id=match.get("account_id"),
                    payload_json=remote_record,
                    sync_state=match.get("outcome") or "missing_local",
                    probe_status=classification,
                )
                crud.update_cliproxy_aggregate_service(
                    db,
                    task_id,
                    service_id=service_id,
                    processed_count=processed_count,
                    success_count=success_count,
                    failure_count=failure_count,
                    current_stage="probing" if run_type == "scan" else "executing",
                    log_lines=[f"{prefix} processed {remote_record['remote_file_id']}"],
                )

            crud.update_cliproxy_aggregate_service(
                db,
                task_id,
                service_id=service_id,
                status="completed",
                processed_count=processed_count,
                success_count=success_count,
                failure_count=failure_count,
                current_stage="completed",
                last_error=None,
                log_lines=[f"{prefix} completed"],
                result_summary={
                    "records": processed_count,
                    "success_count": success_count,
                    "failure_count": failure_count,
                    "status": "completed",
                    "last_error": None,
                },
            )
        except Exception as exc:
            crud.update_cliproxy_aggregate_service(
                db,
                task_id,
                service_id=service_id,
                status="failed",
                current_stage="failed",
                last_error=str(exc) or exc.__class__.__name__,
                log_lines=[f"{prefix} failed: {str(exc) or exc.__class__.__name__}"],
            )


def run_cliproxy_aggregate_task(task_id: int) -> None:
    with get_db() as db:
        run = crud.get_maintenance_run_by_id(db, task_id)
        if run is None or not crud.is_cliproxy_aggregate_task(run):
            return
        if run.status != "queued":
            return
        summary = dict(run.summary_json or {})
        services = list(summary.get("services") or [])
        request = dict(summary.get("request") or {})
        run_type = run.run_type
        dry_run = bool(request.get("dry_run", False))
        run.status = "running"
        summary["status"] = "running"
        summary["current_stage"] = "running"
        run.summary_json = summary
        db.commit()

    final_status = "completed"
    for service in services:
        _run_service_task(task_id, run_type, int(service["service_id"]), dry_run=dry_run)

    with get_db() as db:
        refreshed = crud.get_maintenance_run_by_id(db, task_id)
        if refreshed is None:
            return
        refreshed_summary = dict(refreshed.summary_json or {})
        service_items = list(refreshed_summary.get("services") or [])
        if any(item.get("status") == "failed" for item in service_items):
            final_status = "failed"
        crud.finalize_cliproxy_aggregate_task(
            db,
            task_id,
            status=final_status,
            current_stage="failed" if final_status == "failed" else "completed",
        )
