"""CPA workbench API routes."""

import threading
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, Response
from typing import Optional

from pydantic import BaseModel

from ...core.tasks.cpa_actions import run_cpa_action_task
from ...core.tasks.cpa_scan import run_cpa_scan_task
from ...database import crud
from ...database.session import get_db
from ..auth import build_session_cookie_value, ensure_session_id, get_current_session_id, require_webui_auth

router = APIRouter()


class CpaScanTaskRequest(BaseModel):
    service_ids: list[int]
    credential_ids: Optional[list[str]] = None
    concurrency: Optional[int] = None


class CpaActionTaskRequest(BaseModel):
    service_ids: list[int]
    credential_ids: Optional[list[str]] = None
    quota_action: Optional[str] = None
    delete_concurrency: Optional[int] = None
    disable_concurrency: Optional[int] = None


def _dispatch_cpa_scan_job(task_id: int) -> None:
    thread = threading.Thread(target=run_cpa_scan_task, args=(task_id,), daemon=True)
    thread.start()


def _dispatch_cpa_action_job(task_id: int) -> None:
    thread = threading.Thread(target=run_cpa_action_task, args=(task_id,), daemon=True)
    thread.start()


def _parse_service_ids(service_ids: Optional[str]) -> list[int]:
    if not service_ids:
        return []
    parsed_ids = []
    for item in service_ids.split(","):
        value = item.strip()
        if not value:
            continue
        parsed_ids.append(int(value))
    return parsed_ids


def _ensure_cpa_task_session(request: Request, response: Optional[Response] = None) -> str:
    require_webui_auth(request)
    session_id, reissued = ensure_session_id(request)
    if reissued and response is not None:
        response.set_cookie(
            "session_id",
            build_session_cookie_value(session_id),
            httponly=True,
            samesite="lax",
        )
    return session_id


def _get_current_session_id_or_raise(request: Request) -> str:
    session_id = get_current_session_id(request)
    if not session_id:
        raise HTTPException(status_code=401, detail="Session not established")
    return session_id


def _get_scoped_latest_active_task_payload(db, *, session_id: Optional[str], selected_service_ids: list[int]):
    if not session_id or not selected_service_ids:
        return None

    scoped_tasks = []
    for task_type in (crud.CPA_WORKBENCH_SCAN_TASK_TYPE, crud.CPA_WORKBENCH_ACTION_TASK_TYPE):
        task = crud.get_latest_active_cpa_workbench_task(
            db,
            owner_session_id=session_id,
            task_type=task_type,
            service_ids=selected_service_ids,
        )
        if task is not None:
            scoped_tasks.append(task)

    if not scoped_tasks:
        return None

    latest_task = max(scoped_tasks, key=lambda task: int(task.id))
    return crud.serialize_cpa_workbench_task(latest_task)


@router.get("/services")
async def list_cpa_workbench_services(
    request: Request,
    selected_service_id: Optional[int] = None,
    selected_credential_id: Optional[str] = None,
    selected_credential_missing: bool = False,
):
    require_webui_auth(request)
    with get_db() as db:
        payload = crud.get_cpa_workbench_service_selector_data(db)

        session_id = get_current_session_id(request)
        payload["latest_active_task"] = _get_scoped_latest_active_task_payload(
            db,
            session_id=session_id,
            selected_service_ids=payload.get("selected_service_ids") or [],
        )

        if selected_service_id is not None and selected_credential_id:
            if selected_credential_missing:
                payload["selection_recovery"] = None
                payload["selection_notice"] = {
                    "code": "selected_credential_missing",
                    "message": "The previously selected credential is no longer available in the current view.",
                    "service_id": selected_service_id,
                    "credential_id": selected_credential_id,
                }
            else:
                payload["selection_recovery"] = {
                    "service_id": selected_service_id,
                    "credential_id": selected_credential_id,
                }
                payload["selection_notice"] = None

        if payload["services"] and not payload["selected_service_ids"]:
            payload["empty_state"] = {
                "code": "no_usable_services",
                "message": "Enable and fully configure at least one CPA service to manage remote credentials.",
            }

        return payload


@router.get("/credentials")
async def list_cpa_workbench_credentials(
    request: Request,
    service_ids: Optional[str] = None,
    status: Optional[str] = None,
):
    require_webui_auth(request)
    parsed_service_ids = _parse_service_ids(service_ids)
    with get_db() as db:
        return crud.list_cpa_remote_credential_snapshots(
            db,
            service_ids=parsed_service_ids,
            status=status,
        )


@router.get("/credentials/{service_id}/{credential_id}")
async def get_cpa_workbench_credential_detail(request: Request, service_id: int, credential_id: str):
    require_webui_auth(request)
    with get_db() as db:
        detail = crud.serialize_cpa_remote_credential_detail(
            db,
            service_id=service_id,
            credential_id=credential_id,
        )
        if detail is None:
            raise HTTPException(status_code=404, detail="CPA credential not found")
        local_account_summary = detail.get("local_account_summary")
        if local_account_summary and local_account_summary.get("account_id") is not None:
            account_id = int(local_account_summary["account_id"])
            detail["local_account_summary"] = {
                **local_account_summary,
                "jump_href": f"/accounts?account_id={account_id}",
            }
        return detail


@router.get("/credentials/{service_id}/{credential_id}/logs")
async def get_cpa_workbench_credential_logs(request: Request, service_id: int, credential_id: str):
    require_webui_auth(request)
    with get_db() as db:
        payload = crud.serialize_cpa_remote_credential_logs(
            db,
            service_id=service_id,
            credential_id=credential_id,
        )
        if payload is None:
            raise HTTPException(status_code=404, detail="CPA credential not found")
        return payload


@router.get("/summary")
async def get_cpa_workbench_summary(
    request: Request,
    service_ids: Optional[str] = None,
    status: Optional[str] = None,
):
    require_webui_auth(request)
    parsed_service_ids = _parse_service_ids(service_ids)
    with get_db() as db:
        return crud.get_cpa_remote_credential_summary(
            db,
            service_ids=parsed_service_ids,
            status=status,
        )


@router.post("/scan")
async def start_cpa_scan_task(
    request: Request,
    payload: CpaScanTaskRequest,
    background_tasks: BackgroundTasks,
    response: Response,
):
    session_id = _ensure_cpa_task_session(request, response)
    with get_db() as db:
        task = crud.create_cpa_scan_task(
            db,
            owner_session_id=session_id,
            service_ids=payload.service_ids,
            credential_ids=payload.credential_ids,
            concurrency=payload.concurrency or crud.CPA_SCAN_DEFAULT_CONCURRENCY,
        )
        background_tasks.add_task(_dispatch_cpa_scan_job, task.id)
        response.status_code = 202
        return crud.serialize_cpa_workbench_task(task)


@router.post("/actions")
async def start_cpa_action_task(
    request: Request,
    payload: CpaActionTaskRequest,
    background_tasks: BackgroundTasks,
    response: Response,
):
    session_id = _ensure_cpa_task_session(request, response)
    with get_db() as db:
        task = crud.create_cpa_action_task(
            db,
            owner_session_id=session_id,
            service_ids=payload.service_ids,
            credential_ids=payload.credential_ids,
            quota_action=payload.quota_action or "disable",
            delete_concurrency=payload.delete_concurrency or crud.CPA_DELETE_DEFAULT_CONCURRENCY,
            disable_concurrency=payload.disable_concurrency or crud.CPA_DISABLE_DEFAULT_CONCURRENCY,
        )
        background_tasks.add_task(_dispatch_cpa_action_job, task.id)
        response.status_code = 202
        return crud.serialize_cpa_workbench_task(task)


@router.get("/tasks/latest-active")
async def get_latest_active_cpa_task(
    request: Request,
    type: str,
    service_ids: Optional[str] = None,
    service_scope: Optional[str] = None,
):
    session_id = _get_current_session_id_or_raise(request)
    if service_scope == "empty":
        raise HTTPException(status_code=404, detail="CPA workbench task not found")

    parsed_service_ids = _parse_service_ids(service_ids)
    with get_db() as db:
        task = crud.get_latest_active_cpa_workbench_task(
            db,
            owner_session_id=session_id,
            task_type=type,
            service_ids=parsed_service_ids if parsed_service_ids else None,
        )
        if task is None:
            raise HTTPException(status_code=404, detail="CPA workbench task not found")
        return crud.serialize_cpa_workbench_task(task)


@router.get("/tasks/{task_id}")
async def get_cpa_task_detail(request: Request, task_id: int):
    session_id = _get_current_session_id_or_raise(request)
    with get_db() as db:
        task = crud.get_cpa_workbench_task_by_id(db, task_id, owner_session_id=session_id)
        if task is None:
            raise HTTPException(status_code=404, detail="CPA workbench task not found")
        return crud.serialize_cpa_workbench_task(task)
