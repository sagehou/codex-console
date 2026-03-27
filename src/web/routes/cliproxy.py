"""CLIProxy environment and maintenance run routes."""

import asyncio
import time
from typing import List
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError

from ...database import crud
from ...database.models import MaintenanceActionLog, MaintenanceRun, RemoteAuthInventory
from ...database.session import get_db
from ...core.cliproxy.client import CLIProxyAPIClient
from ...core.cliproxy.maintenance import CLIProxyMaintenanceEngine
from ..auth import require_webui_auth
from ..task_manager import task_manager

router = APIRouter()


class CLIProxyEnvironmentCreate(BaseModel):
    name: str
    base_url: str
    token: Optional[str] = None
    target_type: str
    provider: str
    provider_scope: Optional[str] = None
    target_scope: Optional[str] = None
    scope_rules_json: Optional[Dict[str, Any]] = None
    enabled: bool = True
    is_default: bool = False
    notes: Optional[str] = None


class CLIProxyEnvironmentUpdate(BaseModel):
    name: Optional[str] = None
    base_url: Optional[str] = None
    token: Optional[str] = None
    target_type: Optional[str] = None
    provider: Optional[str] = None
    provider_scope: Optional[str] = None
    target_scope: Optional[str] = None
    scope_rules_json: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = None
    is_default: Optional[bool] = None
    notes: Optional[str] = None


class CLIProxyScanRequest(BaseModel):
    idempotency_key: Optional[str] = None


class CLIProxyMaintainRequest(BaseModel):
    idempotency_key: Optional[str] = None
    dry_run: bool = False


class CLIProxyTokenReplaceRequest(BaseModel):
    token: str


def _run_to_dict(run) -> Dict[str, Any]:
    summary = run.summary_json or {}
    action_logs = []
    with get_db() as db:
        action_logs = crud.get_maintenance_action_logs(db, run_id=run.id)
    return {
        "id": run.id,
        "environment_id": run.environment_id,
        "run_type": run.run_type,
        "status": run.status,
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "updated_at": run.updated_at.isoformat() if run.updated_at else None,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "current_stage": summary.get("current_stage"),
        "progress_percent": summary.get("progress_percent"),
        "cancellable": bool(summary.get("cancellable", False)),
        "result_summary": summary.get("result_summary") or {"records": summary.get("records", 0)},
        "counters": {
            "record_count": int(summary.get("records", 0) or 0),
            "action_count": len(action_logs),
        },
        "summary_json": summary,
        "error_message": run.error_message,
    }


def _inventory_to_dict(item: RemoteAuthInventory) -> Dict[str, Any]:
    return {
        "id": item.id,
        "environment_id": item.environment_id,
        "remote_file_id": item.remote_file_id,
        "email": item.email,
        "remote_account_id": item.remote_account_id,
        "local_account_id": item.local_account_id,
        "payload_json": item.payload_json or {},
        "last_seen_at": item.last_seen_at.isoformat() if item.last_seen_at else None,
        "last_probed_at": item.last_probed_at.isoformat() if item.last_probed_at else None,
        "sync_state": item.sync_state,
        "probe_status": item.probe_status,
        "disable_source": item.disable_source,
        "created_at": item.created_at.isoformat() if item.created_at else None,
        "updated_at": item.updated_at.isoformat() if item.updated_at else None,
    }


def _action_log_to_dict(item: MaintenanceActionLog) -> Dict[str, Any]:
    return {
        "id": item.id,
        "run_id": item.run_id,
        "environment_id": item.environment_id,
        "action_type": item.action_type,
        "status": item.status,
        "remote_file_id": item.remote_file_id,
        "message": item.message,
        "details_json": item.details_json or {},
        "created_at": item.created_at.isoformat() if item.created_at else None,
    }


def _environment_resource_details(environment_id: int) -> Dict[str, Any]:
    return {"resource": "environment", "resource_id": environment_id}


def _run_resource_details(run_id: int, environment_id: int, run_type: Optional[str] = None) -> Dict[str, Any]:
    details = {"resource": "run", "resource_id": run_id, "environment_id": environment_id}
    if run_type is not None:
        details["run_type"] = run_type
    return details


def _write_webui_audit(
    *,
    event_type: str,
    environment_id: Optional[int] = None,
    run_id: Optional[int] = None,
    message: Optional[str] = None,
    details_json: Optional[Dict[str, Any]] = None,
) -> None:
    with get_db() as db:
        crud.write_audit_log(
            db,
            event_type=event_type,
            actor="webui",
            environment_id=environment_id,
            run_id=run_id,
            message=message,
            details_json=details_json,
        )


def _request_payload_for_idempotency(run_type: str, request: BaseModel) -> Dict[str, Any]:
    payload = request.model_dump(exclude_none=True)
    if run_type == "scan":
        return payload
    return payload


def _run_maintenance_job(run_id: int) -> None:
    with get_db() as db:
        run = crud.get_maintenance_run_by_id(db, run_id)
        if run is None:
            return
        if run.status in {"completed", "failed", "cancelled"}:
            return
        engine = CLIProxyMaintenanceEngine(db=db)
        if run.run_type == "scan":
            engine.scan(run.environment_id, run_id=run.id)
        elif run.run_type == "maintain":
            dry_run = bool((run.summary_json or {}).get("request", {}).get("dry_run", False))
            engine.maintain(run.environment_id, dry_run=dry_run, run_id=run.id)


async def _dispatch_maintenance_job(run_id: int) -> None:
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(task_manager.executor, _run_maintenance_job, run_id)


def _create_or_replay_run(environment_id: int, run_type: str, request_data: Dict[str, Any]):
    with get_db() as db:
        run, created = crud.create_maintenance_run_if_available(
            db,
            run_type=run_type,
            environment_id=environment_id,
            request_data=request_data,
        )
        if created:
            return run, True
        summary = run.summary_json or {}
        if summary.get("request") == request_data:
            return run, False
        if run.status in {"queued", "running", "cancelling"}:
            raise HTTPException(status_code=409, detail="CLIProxy environment already has an in-flight maintenance run")
        return run, False


@router.get("")
async def list_cliproxy_environments(request: Request, enabled: Optional[bool] = None):
    require_webui_auth(request)
    with get_db() as db:
        return [environment.to_detail_dict() for environment in crud.get_cliproxy_environments(db, enabled=enabled)]


@router.post("")
async def create_cliproxy_environment(http_request: Request, request: CLIProxyEnvironmentCreate):
    require_webui_auth(http_request)
    with get_db() as db:
        try:
            environment = crud.create_cliproxy_environment(
                db,
                name=request.name,
                base_url=request.base_url,
                target_type=request.target_type,
                provider=request.provider,
                token=request.token,
                provider_scope=request.provider_scope,
                target_scope=request.target_scope,
                scope_rules_json=request.scope_rules_json,
                enabled=request.enabled,
                is_default=request.is_default,
                notes=request.notes,
            )
        except ValueError as exc:
            raise HTTPException(status_code=500, detail="CLIProxy encryption key is not configured correctly") from exc
        except IntegrityError as exc:
            raise HTTPException(status_code=409, detail="CLIProxy environment name already exists") from exc
        crud.write_audit_log(
            db,
            event_type="environment_create",
            actor="webui",
            environment_id=environment.id,
            message=f"created environment {environment.name}",
            details_json=_environment_resource_details(environment.id),
        )
        return environment.to_detail_dict()


@router.get("/{environment_id}")
async def get_cliproxy_environment(request: Request, environment_id: int):
    require_webui_auth(request)
    with get_db() as db:
        environment = crud.get_cliproxy_environment_by_id(db, environment_id)
        if not environment:
            raise HTTPException(status_code=404, detail="CLIProxy environment not found")
        return environment.to_detail_dict()


@router.patch("/{environment_id}")
async def update_cliproxy_environment(http_request: Request, environment_id: int, request: CLIProxyEnvironmentUpdate):
    require_webui_auth(http_request)
    update_data = request.model_dump(exclude_unset=True)
    with get_db() as db:
        try:
            environment = crud.update_cliproxy_environment(db, environment_id, **update_data)
        except ValueError as exc:
            raise HTTPException(status_code=500, detail="CLIProxy encryption key is not configured correctly") from exc
        except IntegrityError as exc:
            raise HTTPException(status_code=409, detail="CLIProxy environment name already exists") from exc
        if not environment:
            raise HTTPException(status_code=404, detail="CLIProxy environment not found")
        crud.write_audit_log(
            db,
            event_type="environment_update",
            actor="webui",
            environment_id=environment.id,
            message=f"updated environment {environment.name}",
            details_json={**_environment_resource_details(environment.id), "changes": update_data},
        )
        return environment.to_detail_dict()


@router.post("/{environment_id}/token")
async def replace_cliproxy_token(http_request: Request, environment_id: int, request: CLIProxyTokenReplaceRequest):
    require_webui_auth(http_request)
    with get_db() as db:
        try:
            environment = crud.update_cliproxy_environment(db, environment_id, token=request.token)
        except ValueError as exc:
            raise HTTPException(status_code=500, detail="CLIProxy encryption key is not configured correctly") from exc
        if not environment:
            raise HTTPException(status_code=404, detail="CLIProxy environment not found")
        crud.write_audit_log(
            db,
            event_type="token_replace",
            actor="webui",
            environment_id=environment.id,
            message=f"replaced token for environment {environment.name}",
            details_json=_environment_resource_details(environment.id),
        )
        return environment.to_detail_dict()


@router.post("/{environment_id}/test-connection")
async def test_cliproxy_connection(http_request: Request, environment_id: int):
    require_webui_auth(http_request)
    with get_db() as db:
        environment = crud.get_cliproxy_environment_by_id(db, environment_id)
        if not environment:
            raise HTTPException(status_code=404, detail="CLIProxy environment not found")

        status = "ok"
        error = None
        started = time.perf_counter()
        try:
            client = CLIProxyAPIClient(base_url=environment.base_url, token=environment.get_token())
            client.fetch_inventory()
        except Exception as exc:
            status = "error"
            error = str(exc) or exc.__class__.__name__
        latency_ms = max(0, int((time.perf_counter() - started) * 1000))
        crud.update_cliproxy_environment(
            db,
            environment.id,
            last_test_status=status,
            last_test_latency_ms=latency_ms,
            last_test_error=error,
        )
        crud.write_audit_log(
            db,
            event_type="connection_test",
            actor="webui",
            environment_id=environment.id,
            message=f"tested connection for environment {environment.name}",
            details_json={**_environment_resource_details(environment.id), "status": status, "latency_ms": latency_ms, "error": error},
        )
        return {"status": status, "latency_ms": latency_ms, "error": error}


@router.post("/{environment_id}/scan")
async def start_cliproxy_scan(
    http_request: Request,
    environment_id: int,
    request: CLIProxyScanRequest,
    background_tasks: BackgroundTasks,
    response: Response,
):
    require_webui_auth(http_request)
    request_data = _request_payload_for_idempotency("scan", request)
    run, created = _create_or_replay_run(environment_id, "scan", request_data)
    if created:
        _write_webui_audit(
            event_type="run_create",
            environment_id=environment_id,
            run_id=run.id,
            message="created scan run",
            details_json=_run_resource_details(run.id, environment_id, "scan"),
        )
        background_tasks.add_task(_dispatch_maintenance_job, run.id)
        response.status_code = 202
    else:
        response.status_code = 200
    return _run_to_dict(run)


@router.post("/{environment_id}/maintain")
async def start_cliproxy_maintain(
    http_request: Request,
    environment_id: int,
    request: CLIProxyMaintainRequest,
    background_tasks: BackgroundTasks,
    response: Response,
):
    require_webui_auth(http_request)
    request_data = _request_payload_for_idempotency("maintain", request)
    run, created = _create_or_replay_run(environment_id, "maintain", request_data)
    if created:
        _write_webui_audit(
            event_type="run_create",
            environment_id=environment_id,
            run_id=run.id,
            message="created maintain run",
            details_json={**_run_resource_details(run.id, environment_id, "maintain"), "dry_run": request.dry_run},
        )
        background_tasks.add_task(_dispatch_maintenance_job, run.id)
        response.status_code = 202
    else:
        response.status_code = 200
    return _run_to_dict(run)


@router.post("/{environment_id}/refill")
async def start_cliproxy_refill(http_request: Request, environment_id: int):
    require_webui_auth(http_request)
    with get_db() as db:
        environment = crud.get_cliproxy_environment_by_id(db, environment_id)
        if not environment:
            raise HTTPException(status_code=404, detail="CLIProxy environment not found")
    raise HTTPException(status_code=501, detail="CLIProxy refill is reserved in v1 and not enabled")


@router.post("/runs/{run_id}/cancel")
async def cancel_cliproxy_run(http_request: Request, run_id: int):
    require_webui_auth(http_request)
    with get_db() as db:
        engine = CLIProxyMaintenanceEngine(db=db)
        try:
            engine.cancel(run_id)
        except ValueError as exc:
            detail = str(exc)
            if detail.endswith("not found"):
                raise HTTPException(status_code=404, detail=detail) from exc
            raise HTTPException(status_code=409, detail=detail) from exc
        run = crud.get_maintenance_run_by_id(db, run_id)
        assert run is not None
        crud.write_audit_log(
            db,
            event_type="run_cancel",
            actor="webui",
            environment_id=run.environment_id,
            run_id=run.id,
            message=f"cancelled run {run.id}",
            details_json=_run_resource_details(run.id, run.environment_id, run.run_type),
        )
        return _run_to_dict(run)


@router.get("/{environment_id}/inventory")
async def list_cliproxy_inventory(request: Request, environment_id: int):
    require_webui_auth(request)
    with get_db() as db:
        environment = crud.get_cliproxy_environment_by_id(db, environment_id)
        if not environment:
            raise HTTPException(status_code=404, detail="CLIProxy environment not found")
        return [_inventory_to_dict(item) for item in crud.get_remote_auth_inventory(db, environment_id=environment_id)]


@router.get("/{environment_id}/runs")
async def list_cliproxy_runs(request: Request, environment_id: int):
    require_webui_auth(request)
    with get_db() as db:
        environment = crud.get_cliproxy_environment_by_id(db, environment_id)
        if not environment:
            raise HTTPException(status_code=404, detail="CLIProxy environment not found")
        return [_run_to_dict(run) for run in crud.get_maintenance_runs(db, environment_id=environment_id)]


@router.get("/runs/{run_id}")
async def get_cliproxy_run(request: Request, run_id: int):
    require_webui_auth(request)
    with get_db() as db:
        run = crud.get_maintenance_run_by_id(db, run_id)
        if not run:
            raise HTTPException(status_code=404, detail="CLIProxy maintenance run not found")
        return _run_to_dict(run)


@router.get("/runs/{run_id}/actions")
async def list_cliproxy_run_actions(request: Request, run_id: int):
    require_webui_auth(request)
    with get_db() as db:
        run = crud.get_maintenance_run_by_id(db, run_id)
        if not run:
            raise HTTPException(status_code=404, detail="CLIProxy maintenance run not found")
        return [_action_log_to_dict(item) for item in crud.get_maintenance_action_logs(db, run_id=run_id)]
