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


@router.get("")
async def list_audit_logs(
    request: Request,
    environment_id: Optional[int] = None,
    run_id: Optional[int] = None,
    event_type: Optional[str] = None,
    resource: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
):
    require_webui_auth(request)
    with get_db() as db:
        rows = crud.get_audit_logs(
            db,
            environment_id=environment_id,
            run_id=run_id,
            event_type=event_type,
            resource=resource,
            start_time=start_time,
            end_time=end_time,
        )
        return [_audit_to_dict(row) for row in rows]
