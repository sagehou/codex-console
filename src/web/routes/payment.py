"""
支付相关 API 路由
"""

import logging
from typing import Optional, List
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from ...database.session import get_db
from ...database.models import Account
from ...database import crud
from ...config.settings import get_settings
from .accounts import resolve_account_ids
from ..auth import build_session_cookie_value, ensure_session_id, require_webui_auth
from ...core.openai.payment import (
    generate_plus_link,
    generate_team_link,
    open_url_incognito,
    check_subscription_status,
)
from ...core.tasks.batch_subscription_check import start_batch_subscription_check_task

logger = logging.getLogger(__name__)
router = APIRouter()
dispatch_batch_subscription_task = start_batch_subscription_check_task


def _get_batch_task_owner_key(http_request: Request) -> tuple[str, Optional[str]]:
    require_webui_auth(http_request)
    session_id, _ = ensure_session_id(http_request)
    return crud.build_batch_subscription_owner_key(session_id), session_id


def _serialize_batch_subscription_task(task, *, log_offset: int = 0) -> dict:
    logs, next_log_offset = crud.get_batch_subscription_task_log_slice(task, offset=log_offset)
    return {
        "task_id": str(task.id),
        "status": task.status,
        "scope_key": task.scope_key,
        "total_count": task.total_count,
        "processed_count": task.processed_count,
        "success_count": task.success_count,
        "failure_count": task.failure_count,
        "current_account": task.current_account,
        "progress_percent": crud.calculate_batch_subscription_progress_percent(task),
        "logs": logs,
        "next_log_offset": next_log_offset,
    }


# ============== Pydantic Models ==============

class GenerateLinkRequest(BaseModel):
    account_id: int
    plan_type: str  # 'plus' or 'team'
    workspace_name: str = "MyTeam"
    price_interval: str = "month"
    seat_quantity: int = 5
    proxy: Optional[str] = None
    auto_open: bool = False  # 生成后是否自动无痕打开
    country: str = "SG"  # 计费国家，决定货币  # 生成后是否自动无痕打开


class OpenIncognitoRequest(BaseModel):
    url: str
    account_id: Optional[int] = None  # 可选，用于注入账号 cookie


class MarkSubscriptionRequest(BaseModel):
    subscription_type: str  # 'free' / 'plus' / 'team'


class BatchCheckSubscriptionRequest(BaseModel):
    ids: List[int] = Field(default_factory=list)
    proxy: Optional[str] = None
    select_all: bool = False
    status_filter: Optional[str] = None
    email_service_filter: Optional[str] = None
    search_filter: Optional[str] = None


# ============== 支付链接生成 ==============

@router.post("/generate-link")
def generate_payment_link(request: GenerateLinkRequest):
    """生成 Plus 或 Team 支付链接，可选自动无痕打开"""
    with get_db() as db:
        account = db.query(Account).filter(Account.id == request.account_id).first()
        if not account:
            raise HTTPException(status_code=404, detail="账号不存在")

        proxy = request.proxy or get_settings().proxy_url

        try:
            if request.plan_type == "plus":
                link = generate_plus_link(account, proxy, country=request.country)
            elif request.plan_type == "team":
                link = generate_team_link(
                    account,
                    workspace_name=request.workspace_name,
                    price_interval=request.price_interval,
                    seat_quantity=request.seat_quantity,
                    proxy=proxy,
                    country=request.country,
                )
            else:
                raise HTTPException(status_code=400, detail="plan_type 必须为 plus 或 team")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"生成支付链接失败: {e}")
            raise HTTPException(status_code=500, detail=f"生成链接失败: {str(e)}")

    opened = False
    if request.auto_open and link:
        cookies_str = account.cookies if account else None
        opened = open_url_incognito(link, cookies_str)

    return {
        "success": True,
        "link": link,
        "plan_type": request.plan_type,
        "auto_opened": opened,
    }


@router.post("/open-incognito")
def open_browser_incognito(request: OpenIncognitoRequest):
    """后端以无痕模式打开指定 URL，可注入账号 cookie"""
    if not request.url:
        raise HTTPException(status_code=400, detail="URL 不能为空")

    cookies_str = None
    if request.account_id:
        with get_db() as db:
            account = db.query(Account).filter(Account.id == request.account_id).first()
            if account:
                cookies_str = account.cookies

    success = open_url_incognito(request.url, cookies_str)
    if success:
        return {"success": True, "message": "已在无痕模式打开浏览器"}
    return {"success": False, "message": "未找到可用的浏览器，请手动复制链接"}


# ============== 订阅状态 ==============

@router.post("/accounts/batch-check-subscription")
def batch_check_subscription(http_request: Request, response: Response, request: BatchCheckSubscriptionRequest):
    """批量检测账号订阅状态"""
    require_webui_auth(http_request)
    session_id, reissued_session_id = ensure_session_id(http_request)
    proxy = request.proxy or get_settings().proxy_url

    with get_db() as db:
        ids = resolve_account_ids(
            db, request.ids, request.select_all,
            request.status_filter, request.email_service_filter, request.search_filter
        )
        scope_key = crud.build_batch_subscription_request_key(
            account_ids=ids,
            select_all=request.select_all,
            status_filter=request.status_filter,
            email_service_filter=request.email_service_filter,
            search_filter=request.search_filter,
            proxy=proxy,
        )
        task, reused = crud.create_or_reuse_batch_subscription_task(
            db,
            owner_key=crud.build_batch_subscription_owner_key(session_id),
            scope_key=scope_key,
            total_count=len(ids),
            proxy=proxy,
            session_id=session_id,
            request_payload={
                "ids": ids,
                "select_all": request.select_all,
                "status_filter": request.status_filter,
                "email_service_filter": request.email_service_filter,
                "search_filter": request.search_filter,
                "proxy": proxy,
                "request_key": scope_key,
            },
        )
        created_task_id = task.id if not reused else None

    if reissued_session_id:
        response.set_cookie(
            "session_id",
            build_session_cookie_value(session_id),
            httponly=True,
            samesite="lax",
        )

    if created_task_id is not None:
        dispatch_batch_subscription_task(
            created_task_id,
            check_subscription_status_fn=check_subscription_status,
        )

    return {
        "task_id": str(task.id),
        "status": task.status,
        "reused": reused,
        "scope_key": scope_key,
    }


@router.get("/tasks/latest")
def get_latest_batch_task(http_request: Request, type: str, status: str, scope: Optional[str] = None):
    owner_key, _ = _get_batch_task_owner_key(http_request)
    if type != crud.BATCH_SUBSCRIPTION_TASK_TYPE:
        raise HTTPException(status_code=400, detail="不支持的任务类型")
    if status != "active":
        raise HTTPException(status_code=400, detail="不支持的任务状态")

    with get_db() as db:
        task = crud.get_latest_active_batch_subscription_task(db, owner_key=owner_key, scope_key=scope)

    if task is None:
        return None
    return {
        "task_id": str(task.id),
        "status": task.status,
        "scope_key": task.scope_key,
    }


@router.get("/tasks/latest-active")
def get_latest_active_batch_task(http_request: Request, type: str):
    owner_key, _ = _get_batch_task_owner_key(http_request)
    if type != crud.BATCH_SUBSCRIPTION_TASK_TYPE:
        raise HTTPException(status_code=400, detail="不支持的任务类型")

    with get_db() as db:
        task = crud.get_latest_active_batch_subscription_task(db, owner_key=owner_key)

    if task is None:
        return None
    return {
        "task_id": str(task.id),
        "status": task.status,
        "scope_key": task.scope_key,
    }


@router.get("/tasks/{task_id}")
def get_batch_task_detail(http_request: Request, task_id: int, log_offset: int = 0):
    owner_key, _ = _get_batch_task_owner_key(http_request)

    with get_db() as db:
        task = crud.get_batch_subscription_task_by_id(db, task_id, owner_key=owner_key)

    if task is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return _serialize_batch_subscription_task(task, log_offset=log_offset)


@router.post("/accounts/{account_id}/mark-subscription")
def mark_subscription(account_id: int, request: MarkSubscriptionRequest):
    """手动标记账号订阅类型"""
    allowed = ("free", "plus", "team")
    if request.subscription_type not in allowed:
        raise HTTPException(status_code=400, detail=f"subscription_type 必须为 {allowed}")

    with get_db() as db:
        account = db.query(Account).filter(Account.id == account_id).first()
        if not account:
            raise HTTPException(status_code=404, detail="账号不存在")

        account.subscription_type = None if request.subscription_type == "free" else request.subscription_type
        account.subscription_at = datetime.utcnow() if request.subscription_type != "free" else None
        db.commit()

    return {"success": True, "subscription_type": request.subscription_type}
