"""CLIProxy audit routes."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request

from ...database import crud
from ...database.session import get_db
from ..auth import require_webui_auth

router = APIRouter()


def _audit_to_dict(audit_log) -> Dict[str, Any]:
    return {
        "id": audit_log.id,
        "environment_id": audit_log.environment_id,
        "run_id": audit_log.run_id,
        "event_type": audit_log.event_type,
        "actor": audit_log.actor,
        "message": audit_log.message,
        "details_json": audit_log.details_json or {},
        "created_at": audit_log.created_at.isoformat() if audit_log.created_at else None,
    }


def _is_cliproxy_audit_row(audit_log) -> bool:
    details = audit_log.details_json or {}
    return (
        details.get("resource") == "cliproxy"
        and details.get("service_id") is not None
        and bool(details.get("service_name"))
        and bool(details.get("status"))
    )


@router.get("")
async def list_audit_logs(
    request: Request,
    environment_id: Optional[int] = None,
    run_id: Optional[int] = None,
    event_type: Optional[str] = None,
    resource: Optional[str] = None,
    resource_type: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
):
    require_webui_auth(request)
    normalized_resource = resource
    with get_db() as db:
        rows = crud.get_audit_logs(
            db,
            environment_id=environment_id,
            run_id=run_id,
            event_type=event_type,
            resource=normalized_resource,
            start_time=start_time,
            end_time=end_time,
        )
        if resource_type != "cliproxy":
            return [_audit_to_dict(row) for row in rows]

        payload = []
        for row in rows:
            if not _is_cliproxy_audit_row(row):
                continue
            details = row.details_json or {}
            payload.append(
                {
                    "timestamp": row.created_at.isoformat() if row.created_at else None,
                    "event_type": row.event_type,
                    "service_id": details.get("service_id"),
                    "service_name": details.get("service_name"),
                    "status": details.get("status"),
                    "message": row.message,
                }
            )
        return payload
