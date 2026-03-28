"""
数据库 CRUD 操作
"""

import hashlib
import json

from typing import List, Optional, Dict, Any, Union, Iterable
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc, asc, func, text
from sqlalchemy.exc import IntegrityError

from .models import (
    Account,
    BatchSubscriptionTask,
    EmailService,
    RegistrationTask,
    Setting,
    Proxy,
    CpaService,
    Sub2ApiService,
    CLIProxyAPIEnvironment,
    RemoteAuthInventory,
    MaintenanceRun,
    MaintenanceActionLog,
    AuditLog,
)


BATCH_SUBSCRIPTION_TASK_TYPE = "batch_subscription_check"
BATCH_SUBSCRIPTION_ACTIVE_STATUSES = {"queued", "running"}
BATCH_SUBSCRIPTION_TERMINAL_STATUSES = {"completed", "failed", "cancelled", "interrupted"}
BATCH_SUBSCRIPTION_TASK_RETENTION_LIMIT = 50
BATCH_SUBSCRIPTION_LOG_RETENTION_LIMIT = 500


def build_batch_subscription_owner_key(session_id: str) -> str:
    digest = hashlib.sha256(session_id.encode("utf-8")).hexdigest()
    return f"session:{digest}"


def build_batch_subscription_request_key(
    *,
    account_ids: List[int],
    select_all: bool,
    status_filter: Optional[str],
    email_service_filter: Optional[str],
    search_filter: Optional[str],
    proxy: Optional[str],
) -> str:
    if select_all:
        scope_payload = {
            "kind": "filter_snapshot",
            "email_service_filter": email_service_filter or "",
            "search_filter": search_filter or "",
            "status_filter": status_filter or "",
        }
    else:
        scope_payload = {
            "kind": "account_ids",
            "account_ids": sorted({int(account_id) for account_id in account_ids}),
        }

    request_identity = {
        "task_type": BATCH_SUBSCRIPTION_TASK_TYPE,
        "proxy": proxy or "",
        "scope": scope_payload,
    }
    serialized = json.dumps(request_identity, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _utcnow() -> datetime:
    return datetime.utcnow()


def _apply_batch_subscription_task_retention(db: Session, owner_key: str) -> None:
    retained_terminal_ids = [
        task_id
        for (task_id,) in (
            db.query(BatchSubscriptionTask.id)
            .filter(BatchSubscriptionTask.task_type == BATCH_SUBSCRIPTION_TASK_TYPE)
            .filter(BatchSubscriptionTask.owner_key == owner_key)
            .filter(BatchSubscriptionTask.status.in_(BATCH_SUBSCRIPTION_TERMINAL_STATUSES))
            .order_by(desc(BatchSubscriptionTask.id))
            .limit(BATCH_SUBSCRIPTION_TASK_RETENTION_LIMIT)
            .all()
        )
    ]
    if not retained_terminal_ids:
        return

    (
        db.query(BatchSubscriptionTask)
        .filter(BatchSubscriptionTask.task_type == BATCH_SUBSCRIPTION_TASK_TYPE)
        .filter(BatchSubscriptionTask.owner_key == owner_key)
        .filter(BatchSubscriptionTask.status.in_(BATCH_SUBSCRIPTION_TERMINAL_STATUSES))
        .filter(~BatchSubscriptionTask.id.in_(retained_terminal_ids))
        .delete(synchronize_session=False)
    )


def _trim_batch_subscription_task_logs(task: BatchSubscriptionTask) -> None:
    lines = task.get_recent_log_lines()
    if len(lines) <= BATCH_SUBSCRIPTION_LOG_RETENTION_LIMIT:
        return
    task.set_recent_log_lines(lines[-BATCH_SUBSCRIPTION_LOG_RETENTION_LIMIT:])


def mark_batch_subscription_task_terminal(
    db: Session,
    task_id: int,
    *,
    status: str,
) -> Optional[BatchSubscriptionTask]:
    if status not in BATCH_SUBSCRIPTION_TERMINAL_STATUSES:
        raise ValueError(f"Unsupported terminal status: {status}")

    task = db.query(BatchSubscriptionTask).filter(BatchSubscriptionTask.id == task_id).first()
    if task is None:
        return None

    task.status = status
    task.active_scope_key = None
    if task.completed_at is None:
        task.completed_at = _utcnow()
    task.updated_at = _utcnow()
    db.commit()
    db.refresh(task)
    return task


def create_or_reuse_batch_subscription_task(
    db: Session,
    *,
    owner_key: str,
    scope_key: str,
    total_count: int,
    proxy: Optional[str],
    session_id: Optional[str],
    request_payload: Dict[str, Any],
) -> tuple[BatchSubscriptionTask, bool]:
    request_payload = dict(request_payload)
    request_key = request_payload.get("request_key") or scope_key
    existing = (
        db.query(BatchSubscriptionTask)
        .filter(BatchSubscriptionTask.task_type == BATCH_SUBSCRIPTION_TASK_TYPE)
        .filter(BatchSubscriptionTask.owner_key == owner_key)
        .filter(BatchSubscriptionTask.active_scope_key == request_key)
        .filter(BatchSubscriptionTask.status.in_(BATCH_SUBSCRIPTION_ACTIVE_STATUSES))
        .order_by(desc(BatchSubscriptionTask.id))
        .first()
    )
    if existing:
        return existing, True

    task = BatchSubscriptionTask(
        task_type=BATCH_SUBSCRIPTION_TASK_TYPE,
        owner_key=owner_key,
        session_id=session_id,
        scope_key=request_key,
        active_scope_key=request_key,
        status="queued",
        proxy=proxy,
        total_count=total_count,
        processed_count=0,
        success_count=0,
        failure_count=0,
        current_account=None,
        request_payload=request_payload,
        recent_logs="",
    )
    db.add(task)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = (
            db.query(BatchSubscriptionTask)
            .filter(BatchSubscriptionTask.task_type == BATCH_SUBSCRIPTION_TASK_TYPE)
            .filter(BatchSubscriptionTask.owner_key == owner_key)
            .filter(BatchSubscriptionTask.active_scope_key == request_key)
            .filter(BatchSubscriptionTask.status.in_(BATCH_SUBSCRIPTION_ACTIVE_STATUSES))
            .order_by(desc(BatchSubscriptionTask.id))
            .first()
        )
        if existing:
            return existing, True
        raise
    _apply_batch_subscription_task_retention(db, owner_key)
    db.commit()
    db.refresh(task)
    return task, False


def get_batch_subscription_task_by_id(
    db: Session,
    task_id: int,
    *,
    owner_key: Optional[str] = None,
) -> Optional[BatchSubscriptionTask]:
    query = db.query(BatchSubscriptionTask).filter(BatchSubscriptionTask.id == task_id)
    if owner_key is not None:
        query = query.filter(BatchSubscriptionTask.owner_key == owner_key)
    return query.first()


def get_latest_active_batch_subscription_task(
    db: Session,
    *,
    owner_key: str,
    scope_key: Optional[str] = None,
) -> Optional[BatchSubscriptionTask]:
    query = (
        db.query(BatchSubscriptionTask)
        .filter(BatchSubscriptionTask.task_type == BATCH_SUBSCRIPTION_TASK_TYPE)
        .filter(BatchSubscriptionTask.owner_key == owner_key)
        .filter(BatchSubscriptionTask.status.in_(BATCH_SUBSCRIPTION_ACTIVE_STATUSES))
    )
    if scope_key is not None:
        query = query.filter(BatchSubscriptionTask.scope_key == scope_key)
    return query.order_by(desc(BatchSubscriptionTask.id)).first()


def get_batch_subscription_task_log_slice(
    task: BatchSubscriptionTask,
    *,
    offset: int = 0,
) -> tuple[List[str], int]:
    normalized_offset = max(int(offset or 0), 0)
    lines = task.get_recent_log_lines()
    return lines[normalized_offset:], len(lines)


def calculate_batch_subscription_progress_percent(task: BatchSubscriptionTask) -> int:
    total_count = max(int(task.total_count or 0), 0)
    processed_count = max(int(task.processed_count or 0), 0)
    if total_count <= 0:
        return 0
    return min(100, round((processed_count * 100) / total_count))


def get_latest_batch_subscription_task(
    db: Session,
    *,
    owner_key: str,
) -> Optional[BatchSubscriptionTask]:
    return (
        db.query(BatchSubscriptionTask)
        .filter(BatchSubscriptionTask.task_type == BATCH_SUBSCRIPTION_TASK_TYPE)
        .filter(BatchSubscriptionTask.owner_key == owner_key)
        .order_by(desc(BatchSubscriptionTask.id))
        .first()
    )


def list_batch_subscription_tasks_for_owner(
    db: Session,
    *,
    owner_key: str,
) -> List[BatchSubscriptionTask]:
    return (
        db.query(BatchSubscriptionTask)
        .filter(BatchSubscriptionTask.task_type == BATCH_SUBSCRIPTION_TASK_TYPE)
        .filter(BatchSubscriptionTask.owner_key == owner_key)
        .order_by(desc(BatchSubscriptionTask.id))
        .all()
    )


def append_batch_subscription_task_logs(
    db: Session,
    task_id: int,
    lines: Iterable[str],
) -> Optional[BatchSubscriptionTask]:
    task = db.query(BatchSubscriptionTask).filter(BatchSubscriptionTask.id == task_id).first()
    if task is None:
        return None

    existing_lines = task.get_recent_log_lines()
    existing_lines.extend(str(line) for line in lines)
    task.set_recent_log_lines(existing_lines[-BATCH_SUBSCRIPTION_LOG_RETENTION_LIMIT:])
    task.updated_at = _utcnow()
    db.commit()
    db.refresh(task)
    return task


def start_batch_subscription_task(db: Session, task_id: int) -> Optional[BatchSubscriptionTask]:
    task = db.query(BatchSubscriptionTask).filter(BatchSubscriptionTask.id == task_id).first()
    if task is None:
        return None
    if task.status != "queued":
        return task

    now = _utcnow()
    task.status = "running"
    task.started_at = task.started_at or now
    task.updated_at = now
    db.commit()
    db.refresh(task)
    return task


def update_batch_subscription_task_progress(
    db: Session,
    task_id: int,
    *,
    processed_count: int,
    success_count: int,
    failure_count: int,
    current_account: Optional[str],
    log_lines: Iterable[str] = (),
) -> Optional[BatchSubscriptionTask]:
    task = db.query(BatchSubscriptionTask).filter(BatchSubscriptionTask.id == task_id).first()
    if task is None:
        return None

    task.processed_count = processed_count
    task.success_count = success_count
    task.failure_count = failure_count
    task.current_account = current_account
    if log_lines:
        existing_lines = task.get_recent_log_lines()
        existing_lines.extend(str(line) for line in log_lines)
        task.set_recent_log_lines(existing_lines[-BATCH_SUBSCRIPTION_LOG_RETENTION_LIMIT:])
    task.updated_at = _utcnow()
    db.commit()
    db.refresh(task)
    return task


def finalize_batch_subscription_task(
    db: Session,
    task_id: int,
    *,
    status: str,
    current_account: Optional[str] = None,
    log_lines: Iterable[str] = (),
) -> Optional[BatchSubscriptionTask]:
    if status not in BATCH_SUBSCRIPTION_TERMINAL_STATUSES:
        raise ValueError(f"Unsupported terminal status: {status}")

    task = db.query(BatchSubscriptionTask).filter(BatchSubscriptionTask.id == task_id).first()
    if task is None:
        return None

    task.status = status
    task.active_scope_key = None
    task.current_account = current_account
    if log_lines:
        existing_lines = task.get_recent_log_lines()
        existing_lines.extend(str(line) for line in log_lines)
        task.set_recent_log_lines(existing_lines[-BATCH_SUBSCRIPTION_LOG_RETENTION_LIMIT:])
    task.completed_at = _utcnow()
    task.updated_at = task.completed_at
    db.commit()
    db.refresh(task)
    return task


def reconcile_abandoned_batch_subscription_tasks(db: Session) -> int:
    abandoned_tasks = (
        db.query(BatchSubscriptionTask)
        .filter(BatchSubscriptionTask.task_type == BATCH_SUBSCRIPTION_TASK_TYPE)
        .filter(BatchSubscriptionTask.status.in_(BATCH_SUBSCRIPTION_ACTIVE_STATUSES))
        .all()
    )
    reconciled_count = 0
    for task in abandoned_tasks:
        task.current_account = None
        _trim_batch_subscription_task_logs(task)
        mark_batch_subscription_task_terminal(db, task.id, status="interrupted")
        reconciled_count += 1
    return reconciled_count


# ============================================================================
# 账户 CRUD
# ============================================================================

def create_account(
    db: Session,
    email: str,
    email_service: str,
    password: Optional[str] = None,
    client_id: Optional[str] = None,
    session_token: Optional[str] = None,
    email_service_id: Optional[str] = None,
    account_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    access_token: Optional[str] = None,
    refresh_token: Optional[str] = None,
    id_token: Optional[str] = None,
    proxy_used: Optional[str] = None,
    expires_at: Optional['datetime'] = None,
    extra_data: Optional[Dict[str, Any]] = None,
    status: Optional[str] = None,
    source: Optional[str] = None,
    platform_source: Optional[str] = None,
    last_upload_target: Optional[str] = None,
) -> Account:
    """创建新账户"""
    db_account = Account(
        email=email,
        password=password,
        client_id=client_id,
        session_token=session_token,
        email_service=email_service,
        email_service_id=email_service_id,
        account_id=account_id,
        workspace_id=workspace_id,
        access_token=access_token,
        refresh_token=refresh_token,
        id_token=id_token,
        proxy_used=proxy_used,
        expires_at=expires_at,
        extra_data=extra_data or {},
        status=status or 'active',
        source=source or 'register',
        platform_source=platform_source,
        last_upload_target=last_upload_target,
        registered_at=datetime.utcnow()
    )
    db.add(db_account)
    db.commit()
    db.refresh(db_account)
    return db_account


def get_account_by_id(db: Session, account_id: int) -> Optional[Account]:
    """根据 ID 获取账户"""
    return db.query(Account).filter(Account.id == account_id).first()


def get_account_by_email(db: Session, email: str) -> Optional[Account]:
    """根据邮箱获取账户"""
    return db.query(Account).filter(Account.email == email).first()


def get_accounts(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    email_service: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None
) -> List[Account]:
    """获取账户列表（支持分页、筛选）"""
    query = db.query(Account)

    if email_service:
        query = query.filter(Account.email_service == email_service)

    if status:
        query = query.filter(Account.status == status)

    if search:
        search_filter = or_(
            Account.email.ilike(f"%{search}%"),
            Account.account_id.ilike(f"%{search}%"),
            Account.workspace_id.ilike(f"%{search}%")
        )
        query = query.filter(search_filter)

    query = query.order_by(desc(Account.created_at)).offset(skip).limit(limit)
    return query.all()


def update_account(
    db: Session,
    account_id: int,
    **kwargs
) -> Optional[Account]:
    """更新账户信息"""
    db_account = get_account_by_id(db, account_id)
    if not db_account:
        return None

    for key, value in kwargs.items():
        if hasattr(db_account, key) and value is not None:
            setattr(db_account, key, value)

    db.commit()
    db.refresh(db_account)
    return db_account


def delete_account(db: Session, account_id: int) -> bool:
    """删除账户"""
    db_account = get_account_by_id(db, account_id)
    if not db_account:
        return False

    db.delete(db_account)
    db.commit()
    return True


def delete_accounts_batch(db: Session, account_ids: List[int]) -> int:
    """批量删除账户"""
    result = db.query(Account).filter(Account.id.in_(account_ids)).delete(synchronize_session=False)
    db.commit()
    return result


def get_accounts_count(
    db: Session,
    email_service: Optional[str] = None,
    status: Optional[str] = None
) -> int:
    """获取账户数量"""
    query = db.query(func.count(Account.id))

    if email_service:
        query = query.filter(Account.email_service == email_service)

    if status:
        query = query.filter(Account.status == status)

    return query.scalar()


def _isoformat(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value else None


def _build_subscription_summary(account: Account) -> Dict[str, Any]:
    return {
        "subscription_type": account.subscription_type,
        "subscription_at": _isoformat(account.subscription_at),
        "has_subscription": bool(account.subscription_type),
    }


def _build_quota_summary(inventory: Optional[RemoteAuthInventory]) -> Dict[str, Any]:
    payload = (inventory.payload_json or {}) if inventory else {}
    return {
        "probe_status": inventory.probe_status if inventory else None,
        "slots_used": payload.get("slots_used"),
        "slots_total": payload.get("slots_total"),
    }


def _build_remote_inventory_summary(
    inventory: Optional[RemoteAuthInventory],
    environment_map: Dict[int, CLIProxyAPIEnvironment],
) -> Optional[Dict[str, Any]]:
    if inventory is None:
        return None

    environment = environment_map.get(inventory.environment_id)
    return {
        "environment_id": inventory.environment_id,
        "environment_name": environment.name if environment else None,
        "remote_file_id": inventory.remote_file_id,
        "remote_account_id": inventory.remote_account_id,
        "sync_state": inventory.sync_state,
        "probe_status": inventory.probe_status,
        "disable_source": inventory.disable_source,
        "last_seen_at": _isoformat(inventory.last_seen_at),
        "last_probed_at": _isoformat(inventory.last_probed_at),
    }


def _build_export_status_summary(account: Account) -> Dict[str, Any]:
    return {
        "cpa_uploaded": bool(account.cpa_uploaded),
        "cpa_uploaded_at": _isoformat(account.cpa_uploaded_at),
        "last_upload_target": account.last_upload_target,
    }


def _build_billing_status_summary(account: Account) -> Dict[str, Any]:
    return {
        "subscription_type": account.subscription_type,
        "subscription_at": _isoformat(account.subscription_at),
        "status": "active" if account.subscription_type else "free",
    }


def _empty_recent_task_summary() -> Dict[str, Any]:
    return {
        "task_id": None,
        "status": None,
        "created_at": None,
        "completed_at": None,
    }


def _build_cliproxy_jump_entry(
    inventory: Optional[RemoteAuthInventory],
    run: Optional[MaintenanceRun],
) -> Optional[Dict[str, Any]]:
    if inventory is None and run is None:
        return None

    environment_id = inventory.environment_id if inventory else run.environment_id if run else None
    run_id = run.id if run else None
    href = f"/api/cliproxy-environments/runs/{run_id}" if run_id is not None else None
    return {
        "label": "Open CLIProxy maintenance context",
        "href": href,
        "environment_id": environment_id,
        "run_id": run_id,
    }


def _build_automation_trace_summary(
    account: Account,
    environment: Optional[CLIProxyAPIEnvironment],
    run: Optional[MaintenanceRun],
    recent_task_summary: Dict[str, Any],
) -> Dict[str, Any]:
    run_summary = run.summary_json if run and isinstance(run.summary_json, dict) else {}
    result_summary = run_summary.get("result_summary") or {}
    completed_at = _isoformat(
        environment.last_maintained_at if environment and environment.last_maintained_at else run.completed_at if run and run.completed_at else run.updated_at if run else None
    )

    if run is not None:
        log_excerpt = (
            f"Maintain run {run.status} at {completed_at or 'unknown time'} with "
            f"records={result_summary.get('records', 0)}, "
            f"matches={result_summary.get('matches', 0)}, "
            f"disabled={result_summary.get('disabled', 0)}."
        )
    else:
        log_excerpt = "No maintenance trace available for this account."

    return {
        "source": account.platform_source,
        "batch_target": account.last_upload_target,
        "proxy": account.proxy_used,
        "recent_task_status": recent_task_summary.get("status"),
        "recent_task_label": "No recent account task" if not recent_task_summary.get("status") else recent_task_summary.get("status"),
        "log_excerpt": log_excerpt,
    }


def _get_account_workbench_context(
    db: Session,
    account_ids: List[int],
) -> tuple[
    Dict[int, Account],
    Dict[int, Optional[RemoteAuthInventory]],
    Dict[int, CLIProxyAPIEnvironment],
    Dict[int, MaintenanceRun],
]:
    if not account_ids:
        return {}, {}, {}, {}

    accounts = db.query(Account).filter(Account.id.in_(account_ids)).all()
    account_map = {account.id: account for account in accounts}
    inventory_rows = (
        db.query(RemoteAuthInventory)
        .filter(RemoteAuthInventory.local_account_id.in_(account_ids))
        .order_by(desc(RemoteAuthInventory.last_seen_at), desc(RemoteAuthInventory.id))
        .all()
    )

    latest_inventory_by_account: Dict[int, RemoteAuthInventory] = {}
    environment_ids = set()
    for row in inventory_rows:
        if row.local_account_id is None or row.local_account_id in latest_inventory_by_account:
            continue
        latest_inventory_by_account[row.local_account_id] = row
        environment_ids.add(row.environment_id)

    environment_map: Dict[int, CLIProxyAPIEnvironment] = {}
    if environment_ids:
        environments = db.query(CLIProxyAPIEnvironment).filter(CLIProxyAPIEnvironment.id.in_(environment_ids)).all()
        environment_map = {environment.id: environment for environment in environments}

    latest_runs_by_environment: Dict[int, MaintenanceRun] = {}
    if environment_ids:
        runs = (
            db.query(MaintenanceRun)
            .filter(MaintenanceRun.environment_id.in_(environment_ids))
            .order_by(desc(MaintenanceRun.id))
            .all()
        )
        for run in runs:
            if run.environment_id is None or run.environment_id in latest_runs_by_environment:
                continue
            latest_runs_by_environment[run.environment_id] = run

    latest_inventory_optional = {
        account_id: latest_inventory_by_account.get(account_id) for account_id in account_ids
    }

    return account_map, latest_inventory_optional, environment_map, latest_runs_by_environment


def get_account_workbench_list_summaries(
    db: Session,
    account_ids: List[int],
) -> Dict[int, Dict[str, Any]]:
    account_map, inventory_map, environment_map, latest_runs_by_environment = _get_account_workbench_context(db, account_ids)

    summaries: Dict[int, Dict[str, Any]] = {}
    for account_id, account in account_map.items():
        inventory = inventory_map.get(account_id)
        environment = environment_map.get(inventory.environment_id) if inventory else None
        run = latest_runs_by_environment.get(inventory.environment_id) if inventory else None
        summaries[account_id] = {
            "platform_source": account.platform_source,
            "subscription_summary": _build_subscription_summary(account),
            "quota_summary": _build_quota_summary(inventory),
            "remote_sync_state": inventory.sync_state if inventory else None,
            "remote_environment_name": environment.name if environment else None,
            "last_maintenance_status": run.status if run else None,
            "last_maintenance_at": _isoformat(run.completed_at if run and run.completed_at else run.updated_at if run else None),
            "last_upload_target": account.last_upload_target,
        }

    return summaries


def get_account_workbench_detail_summary(
    db: Session,
    account_id: int,
) -> Dict[str, Any]:
    account_map, inventory_map, environment_map, latest_runs_by_environment = _get_account_workbench_context(db, [account_id])
    account = account_map.get(account_id)
    if account is None:
        return {}

    inventory = inventory_map.get(account_id)
    environment = environment_map.get(inventory.environment_id) if inventory else None
    run = latest_runs_by_environment.get(inventory.environment_id) if inventory else None

    return {
        "platform_source": account.platform_source,
        "subscription_summary": _build_subscription_summary(account),
        "quota_summary": _build_quota_summary(inventory),
        "remote_sync_state": inventory.sync_state if inventory else None,
        "remote_environment_name": environment.name if environment else None,
        "last_maintenance_status": run.status if run else None,
        "last_maintenance_at": _isoformat(run.completed_at if run and run.completed_at else run.updated_at if run else None),
        "last_upload_target": account.last_upload_target,
        "export_status_summary": _build_export_status_summary(account),
        "billing_status_summary": _build_billing_status_summary(account),
        "remote_inventory_summary": _build_remote_inventory_summary(inventory, environment_map),
        "recent_task_summary": _empty_recent_task_summary(),
        "cliproxy_jump_entry": _build_cliproxy_jump_entry(inventory, run),
        "automation_trace_summary": _build_automation_trace_summary(account, environment, run, _empty_recent_task_summary()),
    }


# ============================================================================
# 邮箱服务 CRUD
# ============================================================================

def create_email_service(
    db: Session,
    service_type: str,
    name: str,
    config: Dict[str, Any],
    enabled: bool = True,
    priority: int = 0
) -> EmailService:
    """创建邮箱服务配置"""
    db_service = EmailService(
        service_type=service_type,
        name=name,
        config=config,
        enabled=enabled,
        priority=priority
    )
    db.add(db_service)
    db.commit()
    db.refresh(db_service)
    return db_service


def get_email_service_by_id(db: Session, service_id: int) -> Optional[EmailService]:
    """根据 ID 获取邮箱服务"""
    return db.query(EmailService).filter(EmailService.id == service_id).first()


def get_email_services(
    db: Session,
    service_type: Optional[str] = None,
    enabled: Optional[bool] = None,
    skip: int = 0,
    limit: int = 100
) -> List[EmailService]:
    """获取邮箱服务列表"""
    query = db.query(EmailService)

    if service_type:
        query = query.filter(EmailService.service_type == service_type)

    if enabled is not None:
        query = query.filter(EmailService.enabled == enabled)

    query = query.order_by(
        asc(EmailService.priority),
        desc(EmailService.last_used)
    ).offset(skip).limit(limit)

    return query.all()


def update_email_service(
    db: Session,
    service_id: int,
    **kwargs
) -> Optional[EmailService]:
    """更新邮箱服务配置"""
    db_service = get_email_service_by_id(db, service_id)
    if not db_service:
        return None

    for key, value in kwargs.items():
        if hasattr(db_service, key) and value is not None:
            setattr(db_service, key, value)

    db.commit()
    db.refresh(db_service)
    return db_service


def delete_email_service(db: Session, service_id: int) -> bool:
    """删除邮箱服务配置"""
    db_service = get_email_service_by_id(db, service_id)
    if not db_service:
        return False

    db.delete(db_service)
    db.commit()
    return True


# ============================================================================
# 注册任务 CRUD
# ============================================================================

def create_registration_task(
    db: Session,
    task_uuid: str,
    email_service_id: Optional[int] = None,
    proxy: Optional[str] = None
) -> RegistrationTask:
    """创建注册任务"""
    db_task = RegistrationTask(
        task_uuid=task_uuid,
        email_service_id=email_service_id,
        proxy=proxy,
        status='pending'
    )
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    return db_task


def get_registration_task_by_uuid(db: Session, task_uuid: str) -> Optional[RegistrationTask]:
    """根据 UUID 获取注册任务"""
    return db.query(RegistrationTask).filter(RegistrationTask.task_uuid == task_uuid).first()


def get_registration_tasks(
    db: Session,
    status: Optional[str] = None,
    skip: int = 0,
    limit: int = 100
) -> List[RegistrationTask]:
    """获取注册任务列表"""
    query = db.query(RegistrationTask)

    if status:
        query = query.filter(RegistrationTask.status == status)

    query = query.order_by(desc(RegistrationTask.created_at)).offset(skip).limit(limit)
    return query.all()


def update_registration_task(
    db: Session,
    task_uuid: str,
    **kwargs
) -> Optional[RegistrationTask]:
    """更新注册任务状态"""
    db_task = get_registration_task_by_uuid(db, task_uuid)
    if not db_task:
        return None

    for key, value in kwargs.items():
        if hasattr(db_task, key):
            setattr(db_task, key, value)

    db.commit()
    db.refresh(db_task)
    return db_task


def append_task_log(db: Session, task_uuid: str, log_message: str) -> bool:
    """追加任务日志"""
    db_task = get_registration_task_by_uuid(db, task_uuid)
    if not db_task:
        return False

    if db_task.logs:
        db_task.logs += f"\n{log_message}"
    else:
        db_task.logs = log_message

    db.commit()
    return True


def delete_registration_task(db: Session, task_uuid: str) -> bool:
    """删除注册任务"""
    db_task = get_registration_task_by_uuid(db, task_uuid)
    if not db_task:
        return False

    db.delete(db_task)
    db.commit()
    return True


# 为 API 路由添加别名
get_account = get_account_by_id
get_registration_task = get_registration_task_by_uuid


# ============================================================================
# 设置 CRUD
# ============================================================================

def get_setting(db: Session, key: str) -> Optional[Setting]:
    """获取设置"""
    return db.query(Setting).filter(Setting.key == key).first()


def get_settings_by_category(db: Session, category: str) -> List[Setting]:
    """根据分类获取设置"""
    return db.query(Setting).filter(Setting.category == category).all()


def set_setting(
    db: Session,
    key: str,
    value: str,
    description: Optional[str] = None,
    category: str = 'general'
) -> Setting:
    """设置或更新配置项"""
    db_setting = get_setting(db, key)
    if db_setting:
        db_setting.value = value
        db_setting.description = description or db_setting.description
        db_setting.category = category
        db_setting.updated_at = datetime.utcnow()
    else:
        db_setting = Setting(
            key=key,
            value=value,
            description=description,
            category=category
        )
        db.add(db_setting)

    db.commit()
    db.refresh(db_setting)
    return db_setting


def delete_setting(db: Session, key: str) -> bool:
    """删除设置"""
    db_setting = get_setting(db, key)
    if not db_setting:
        return False

    db.delete(db_setting)
    db.commit()
    return True


# ============================================================================
# 代理 CRUD
# ============================================================================

def create_proxy(
    db: Session,
    name: str,
    type: str,
    host: str,
    port: int,
    username: Optional[str] = None,
    password: Optional[str] = None,
    enabled: bool = True,
    priority: int = 0
) -> Proxy:
    """创建代理配置"""
    db_proxy = Proxy(
        name=name,
        type=type,
        host=host,
        port=port,
        username=username,
        password=password,
        enabled=enabled,
        priority=priority
    )
    db.add(db_proxy)
    db.commit()
    db.refresh(db_proxy)
    return db_proxy


def get_proxy_by_id(db: Session, proxy_id: int) -> Optional[Proxy]:
    """根据 ID 获取代理"""
    return db.query(Proxy).filter(Proxy.id == proxy_id).first()


def get_proxies(
    db: Session,
    enabled: Optional[bool] = None,
    skip: int = 0,
    limit: int = 100
) -> List[Proxy]:
    """获取代理列表"""
    query = db.query(Proxy)

    if enabled is not None:
        query = query.filter(Proxy.enabled == enabled)

    query = query.order_by(desc(Proxy.created_at)).offset(skip).limit(limit)
    return query.all()


def get_enabled_proxies(db: Session) -> List[Proxy]:
    """获取所有启用的代理"""
    return db.query(Proxy).filter(Proxy.enabled == True).all()


def update_proxy(
    db: Session,
    proxy_id: int,
    **kwargs
) -> Optional[Proxy]:
    """更新代理配置"""
    db_proxy = get_proxy_by_id(db, proxy_id)
    if not db_proxy:
        return None

    for key, value in kwargs.items():
        if hasattr(db_proxy, key):
            setattr(db_proxy, key, value)

    db.commit()
    db.refresh(db_proxy)
    return db_proxy


def delete_proxy(db: Session, proxy_id: int) -> bool:
    """删除代理配置"""
    db_proxy = get_proxy_by_id(db, proxy_id)
    if not db_proxy:
        return False

    db.delete(db_proxy)
    db.commit()
    return True


def update_proxy_last_used(db: Session, proxy_id: int) -> bool:
    """更新代理最后使用时间"""
    db_proxy = get_proxy_by_id(db, proxy_id)
    if not db_proxy:
        return False

    db_proxy.last_used = datetime.utcnow()
    db.commit()
    return True


def get_random_proxy(db: Session) -> Optional[Proxy]:
    """随机获取一个启用的代理，优先返回 is_default=True 的代理"""
    import random
    # 优先返回默认代理
    default_proxy = db.query(Proxy).filter(Proxy.enabled == True, Proxy.is_default == True).first()
    if default_proxy:
        return default_proxy
    proxies = get_enabled_proxies(db)
    if not proxies:
        return None
    return random.choice(proxies)


def set_proxy_default(db: Session, proxy_id: int) -> Optional[Proxy]:
    """将指定代理设为默认，同时清除其他代理的默认标记"""
    # 清除所有默认标记
    db.query(Proxy).filter(Proxy.is_default == True).update({"is_default": False})
    # 设置新的默认代理
    proxy = db.query(Proxy).filter(Proxy.id == proxy_id).first()
    if proxy:
        proxy.is_default = True
        db.commit()
        db.refresh(proxy)
    return proxy


def get_proxies_count(db: Session, enabled: Optional[bool] = None) -> int:
    """获取代理数量"""
    query = db.query(func.count(Proxy.id))
    if enabled is not None:
        query = query.filter(Proxy.enabled == enabled)
    return query.scalar()


# ============================================================================
# CPA 服务 CRUD
# ============================================================================

def create_cpa_service(
    db: Session,
    name: str,
    api_url: str,
    api_token: str,
    enabled: bool = True,
    priority: int = 0
) -> CpaService:
    """创建 CPA 服务配置"""
    db_service = CpaService(
        name=name,
        api_url=api_url,
        api_token=api_token,
        enabled=enabled,
        priority=priority
    )
    db.add(db_service)
    db.commit()
    db.refresh(db_service)
    return db_service


def get_cpa_service_by_id(db: Session, service_id: int) -> Optional[CpaService]:
    """根据 ID 获取 CPA 服务"""
    return db.query(CpaService).filter(CpaService.id == service_id).first()


def get_cpa_services(
    db: Session,
    enabled: Optional[bool] = None
) -> List[CpaService]:
    """获取 CPA 服务列表"""
    query = db.query(CpaService)
    if enabled is not None:
        query = query.filter(CpaService.enabled == enabled)
    return query.order_by(asc(CpaService.priority), asc(CpaService.id)).all()


CLIPROXY_CPA_REQUIRED_FIELDS = ("api_url", "api_token")


def get_cpa_service_missing_required_fields(service: CpaService) -> List[str]:
    missing_fields: List[str] = []
    for field_name in CLIPROXY_CPA_REQUIRED_FIELDS:
        value = getattr(service, field_name, None)
        if value is None or not str(value).strip():
            missing_fields.append(field_name)
    return missing_fields


def serialize_cliproxy_selectable_cpa_service(service: CpaService) -> Dict[str, Any]:
    missing_fields = get_cpa_service_missing_required_fields(service)
    config_complete = len(missing_fields) == 0
    reason = None if config_complete else "config incomplete"
    return {
        "id": service.id,
        "name": service.name,
        "enabled": bool(service.enabled),
        "priority": service.priority,
        "config_status": "ready" if config_complete else "config incomplete",
        "missing_required_fields": missing_fields,
        "action_state": {
            "test_connection": {"enabled": config_complete, "reason": reason},
            "scan": {"enabled": config_complete, "reason": reason},
            "maintain": {"enabled": config_complete, "reason": reason},
        },
    }


def get_cliproxy_selectable_cpa_services(db: Session) -> List[Dict[str, Any]]:
    services = get_cpa_services(db, enabled=True)
    return [serialize_cliproxy_selectable_cpa_service(service) for service in services]


def get_cliproxy_environment_for_cpa_service(db: Session, service_id: int) -> Optional[CLIProxyAPIEnvironment]:
    return (
        db.query(CLIProxyAPIEnvironment)
        .filter(CLIProxyAPIEnvironment.provider == "cpa_service")
        .filter(CLIProxyAPIEnvironment.provider_scope == str(service_id))
        .order_by(desc(CLIProxyAPIEnvironment.id))
        .first()
    )


def ensure_cliproxy_environment_for_cpa_service(db: Session, service: CpaService) -> CLIProxyAPIEnvironment:
    environment = get_cliproxy_environment_for_cpa_service(db, service.id)
    environment_name = f"cpa-service-{service.id}-{service.name}"
    if environment is None:
        return create_cliproxy_environment(
            db,
            name=environment_name,
            base_url=service.api_url,
            token=service.api_token,
            target_type="cpa",
            provider="cpa_service",
            provider_scope=str(service.id),
            enabled=service.enabled,
            notes=f"Auto-managed from CPA service {service.id}",
        )

    return update_cliproxy_environment(
        db,
        environment.id,
        name=environment_name,
        base_url=service.api_url,
        token=service.api_token,
        target_type="cpa",
        provider="cpa_service",
        provider_scope=str(service.id),
        enabled=service.enabled,
        notes=f"Auto-managed from CPA service {service.id}",
    )


def update_cpa_service(
    db: Session,
    service_id: int,
    **kwargs
) -> Optional[CpaService]:
    """更新 CPA 服务配置"""
    db_service = get_cpa_service_by_id(db, service_id)
    if not db_service:
        return None
    for key, value in kwargs.items():
        if hasattr(db_service, key):
            setattr(db_service, key, value)
    db.commit()
    db.refresh(db_service)
    return db_service


def delete_cpa_service(db: Session, service_id: int) -> bool:
    """删除 CPA 服务配置"""
    db_service = get_cpa_service_by_id(db, service_id)
    if not db_service:
        return False
    db.delete(db_service)
    db.commit()
    return True


# ============================================================================
# Sub2API 服务 CRUD
# ============================================================================

def create_sub2api_service(
    db: Session,
    name: str,
    api_url: str,
    api_key: str,
    target_type: str = "sub2api",
    enabled: bool = True,
    priority: int = 0
) -> Sub2ApiService:
    """创建 Sub2API 服务配置"""
    svc = Sub2ApiService(
        name=name,
        api_url=api_url,
        api_key=api_key,
        target_type=target_type,
        enabled=enabled,
        priority=priority,
    )
    db.add(svc)
    db.commit()
    db.refresh(svc)
    return svc


def get_sub2api_service_by_id(db: Session, service_id: int) -> Optional[Sub2ApiService]:
    """按 ID 获取 Sub2API 服务"""
    return db.query(Sub2ApiService).filter(Sub2ApiService.id == service_id).first()


def get_sub2api_services(
    db: Session,
    enabled: Optional[bool] = None
) -> List[Sub2ApiService]:
    """获取 Sub2API 服务列表"""
    query = db.query(Sub2ApiService)
    if enabled is not None:
        query = query.filter(Sub2ApiService.enabled == enabled)
    return query.order_by(asc(Sub2ApiService.priority), asc(Sub2ApiService.id)).all()


def update_sub2api_service(db: Session, service_id: int, **kwargs) -> Optional[Sub2ApiService]:
    """更新 Sub2API 服务配置"""
    svc = get_sub2api_service_by_id(db, service_id)
    if not svc:
        return None
    for key, value in kwargs.items():
        setattr(svc, key, value)
    db.commit()
    db.refresh(svc)
    return svc


def delete_sub2api_service(db: Session, service_id: int) -> bool:
    """删除 Sub2API 服务配置"""
    svc = get_sub2api_service_by_id(db, service_id)
    if not svc:
        return False
    db.delete(svc)
    db.commit()
    return True


# ============================================================================
# Team Manager 服务 CRUD
# ============================================================================

def create_tm_service(
    db: Session,
    name: str,
    api_url: str,
    api_key: str,
    enabled: bool = True,
    priority: int = 0,
):
    """创建 Team Manager 服务配置"""
    from .models import TeamManagerService
    svc = TeamManagerService(
        name=name,
        api_url=api_url,
        api_key=api_key,
        enabled=enabled,
        priority=priority,
    )
    db.add(svc)
    db.commit()
    db.refresh(svc)
    return svc


def get_tm_service_by_id(db: Session, service_id: int):
    """按 ID 获取 Team Manager 服务"""
    from .models import TeamManagerService
    return db.query(TeamManagerService).filter(TeamManagerService.id == service_id).first()


def get_tm_services(db: Session, enabled=None):
    """获取 Team Manager 服务列表"""
    from .models import TeamManagerService
    q = db.query(TeamManagerService)
    if enabled is not None:
        q = q.filter(TeamManagerService.enabled == enabled)
    return q.order_by(TeamManagerService.priority.asc(), TeamManagerService.id.asc()).all()


def update_tm_service(db: Session, service_id: int, **kwargs):
    """更新 Team Manager 服务配置"""
    svc = get_tm_service_by_id(db, service_id)
    if not svc:
        return None
    for k, v in kwargs.items():
        setattr(svc, k, v)
    db.commit()
    db.refresh(svc)
    return svc


def delete_tm_service(db: Session, service_id: int) -> bool:
    """删除 Team Manager 服务配置"""
    svc = get_tm_service_by_id(db, service_id)
    if not svc:
        return False
    db.delete(svc)
    db.commit()
    return True


# ============================================================================
# CLIProxy 环境 / 维护 / 审计 CRUD
# ============================================================================

MAINTENANCE_RUN_TYPES = {"scan", "maintain", "refill"}
MAINTENANCE_RUN_IN_FLIGHT_STATUSES = {"queued", "running", "cancelling"}
MAINTENANCE_RUN_METADATA_FIELDS = {"current_stage", "progress_percent", "cancellable", "idempotency_key", "request"}
CLIPROXY_AGGREGATE_TASK_STATUSES = {"queued", "running", "completed", "failed", "cancelled", "interrupted"}
CLIPROXY_AGGREGATE_ACTIVE_STATUSES = {"queued", "running"}
CLIPROXY_AGGREGATE_KINDS = {"cliproxy_aggregate"}
REFILL_RESERVED_V1_ERROR = "CLIProxy refill is reserved in v1 and not enabled"


def _validate_maintenance_run_type_enabled(run_type: str) -> None:
    if run_type not in MAINTENANCE_RUN_TYPES:
        raise ValueError(f"unsupported maintenance run type: {run_type}")
    if run_type == "refill":
        raise ValueError(REFILL_RESERVED_V1_ERROR)


def _merge_maintenance_summary(
    summary_json: Optional[Dict[str, Any]],
    **kwargs,
) -> Optional[Dict[str, Any]]:
    merged = dict(summary_json or {})
    touched = bool(summary_json)
    for key in MAINTENANCE_RUN_METADATA_FIELDS:
        if key in kwargs:
            merged[key] = kwargs.pop(key)
            touched = True
    return merged if touched else summary_json


def normalize_cliproxy_service_ids(service_ids: Iterable[int]) -> List[int]:
    return sorted({int(service_id) for service_id in service_ids})


def build_cliproxy_aggregate_scope_key(*, run_type: str, service_ids: Iterable[int]) -> str:
    normalized_service_ids = normalize_cliproxy_service_ids(service_ids)
    return f"{run_type}:{','.join(str(service_id) for service_id in normalized_service_ids)}"


def build_cliproxy_aggregate_summary(*, run_type: str, services: List[Dict[str, Any]], request_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    normalized_service_ids = normalize_cliproxy_service_ids(item["service_id"] for item in services)
    aggregate_key = build_cliproxy_aggregate_scope_key(run_type=run_type, service_ids=normalized_service_ids)
    return {
        "request": dict(request_data or {}),
        "owner_session_id": None,
        "aggregate_key": aggregate_key,
        "service_ids": normalized_service_ids,
        "current_stage": "queued",
        "progress_percent": 0,
        "cancellable": False,
        "task_id": None,
        "status": "queued",
        "run_type": run_type,
        "service_total": len(services),
        "service_completed": 0,
        "known_record_total": None,
        "processed_record_total": 0,
        "services": services,
        "grouped_logs": {str(item["service_id"]): [] for item in services},
        "grouped_results": {str(item["service_id"]): {} for item in services},
    }


def _normalize_cliproxy_grouped_service_data(
    services: List[Dict[str, Any]],
    grouped_data: Optional[Dict[str, Any]],
    default_factory,
) -> Dict[str, Any]:
    normalized = {str(item["service_id"]): default_factory() for item in services}
    for key, value in dict(grouped_data or {}).items():
        if key in normalized:
            normalized[key] = value
    return normalized


def serialize_cliproxy_aggregate_task(run: MaintenanceRun) -> Dict[str, Any]:
    summary = dict(run.summary_json or {})
    services = list(summary.get("services") or [])
    grouped_logs = _normalize_cliproxy_grouped_service_data(services, summary.get("grouped_logs"), list)
    grouped_results = _normalize_cliproxy_grouped_service_data(services, summary.get("grouped_results"), dict)
    return {
        "task_id": str(run.id),
        "status": run.status,
        "run_type": run.run_type,
        "current_stage": summary.get("current_stage"),
        "cancellable": bool(summary.get("cancellable", False)),
        "service_total": int(summary.get("service_total") or len(services)),
        "service_completed": int(summary.get("service_completed") or 0),
        "known_record_total": summary.get("known_record_total"),
        "processed_record_total": int(summary.get("processed_record_total") or 0),
        "progress_percent": int(summary.get("progress_percent") or 0),
        "services": services,
        "grouped_logs": grouped_logs,
        "grouped_results": grouped_results,
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "updated_at": run.updated_at.isoformat() if run.updated_at else None,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "error_message": run.error_message,
    }


def calculate_cliproxy_child_progress_percent(service: Dict[str, Any]) -> int:
    status = str(service.get("status") or "")
    known_record_total = service.get("known_record_total")
    processed_count = max(int(service.get("processed_count") or 0), 0)
    if status == "completed":
        return 100
    if status in {"failed", "interrupted", "cancelled"}:
        return 100
    if known_record_total is None:
        stage = str(service.get("current_stage") or "queued")
        if stage == "queued":
            return 0
        if stage == "fetching_inventory":
            return 10
        if stage == "running":
            return 15
        return 20
    total = max(int(known_record_total or 0), 0)
    if total <= 0:
        return 100 if status == "completed" else 20
    return min(100, round((processed_count * 100) / total))


def _calculate_cliproxy_parent_progress(services: List[Dict[str, Any]]) -> int:
    if not services:
        return 0
    return round(sum(calculate_cliproxy_child_progress_percent(item) for item in services) / len(services))


def _build_cliproxy_grouped_results_for_service(service: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "records": int(service.get("processed_count") or 0),
        "success_count": int(service.get("success_count") or 0),
        "failure_count": int(service.get("failure_count") or 0),
        "status": service.get("status"),
        "last_error": service.get("last_error"),
    }


def update_cliproxy_aggregate_service(
    db: Session,
    task_id: int,
    *,
    service_id: int,
    status: Optional[str] = None,
    known_record_total: Optional[int] = None,
    processed_count: Optional[int] = None,
    success_count: Optional[int] = None,
    failure_count: Optional[int] = None,
    current_stage: Optional[str] = None,
    last_error: Optional[str] = None,
    log_lines: Optional[Iterable[str]] = None,
    result_summary: Optional[Dict[str, Any]] = None,
) -> Optional[MaintenanceRun]:
    run = get_maintenance_run_by_id(db, task_id)
    if run is None or not is_cliproxy_aggregate_task(run):
        return None

    summary = dict(run.summary_json or {})
    services = [dict(item) for item in list(summary.get("services") or [])]
    grouped_logs = _normalize_cliproxy_grouped_service_data(services, summary.get("grouped_logs"), list)
    grouped_results = _normalize_cliproxy_grouped_service_data(services, summary.get("grouped_results"), dict)

    updated_service = None
    for item in services:
        if int(item.get("service_id") or 0) != int(service_id):
            continue
        if status is not None:
            item["status"] = status
        if known_record_total is not None:
            item["known_record_total"] = known_record_total
        if processed_count is not None:
            item["processed_count"] = processed_count
        if success_count is not None:
            item["success_count"] = success_count
        if failure_count is not None:
            item["failure_count"] = failure_count
        if current_stage is not None:
            item["current_stage"] = current_stage
        if last_error is not None or status == "completed":
            item["last_error"] = last_error
        updated_service = item
        break

    if updated_service is None:
        return run

    service_key = str(int(service_id))
    if log_lines:
        grouped_logs[service_key] = list(grouped_logs.get(service_key) or []) + [str(line) for line in log_lines]
    grouped_results[service_key] = dict(result_summary or _build_cliproxy_grouped_results_for_service(updated_service))

    service_total = len(services)
    service_completed = sum(1 for item in services if item.get("status") in {"completed", "failed", "cancelled", "interrupted"})
    known_totals = [int(item.get("known_record_total") or 0) for item in services if item.get("known_record_total") is not None]
    summary["services"] = services
    summary["grouped_logs"] = grouped_logs
    summary["grouped_results"] = grouped_results
    summary["service_total"] = service_total
    summary["service_completed"] = service_completed
    summary["known_record_total"] = sum(known_totals) if len(known_totals) == service_total else None
    summary["processed_record_total"] = sum(int(item.get("processed_count") or 0) for item in services)
    summary["progress_percent"] = _calculate_cliproxy_parent_progress(services)
    summary["current_stage"] = current_stage or summary.get("current_stage") or "running"
    summary["status"] = run.status

    run.summary_json = summary
    run.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(run)
    return run


def finalize_cliproxy_aggregate_task(
    db: Session,
    task_id: int,
    *,
    status: str,
    current_stage: Optional[str] = None,
    error_message: Optional[str] = None,
) -> Optional[MaintenanceRun]:
    run = get_maintenance_run_by_id(db, task_id)
    if run is None or not is_cliproxy_aggregate_task(run):
        return None
    summary = dict(run.summary_json or {})
    services = [dict(item) for item in list(summary.get("services") or [])]
    if status in {"failed", "interrupted", "cancelled"}:
        for item in services:
            if item.get("status") in CLIPROXY_AGGREGATE_ACTIVE_STATUSES:
                item["status"] = status
                item["current_stage"] = current_stage or status
    summary["services"] = services
    summary["service_total"] = len(services)
    summary["service_completed"] = sum(1 for item in services if item.get("status") in {"completed", "failed", "cancelled", "interrupted"})
    known_totals = [int(item.get("known_record_total") or 0) for item in services if item.get("known_record_total") is not None]
    summary["known_record_total"] = sum(known_totals) if len(known_totals) == len(services) else None
    summary["processed_record_total"] = sum(int(item.get("processed_count") or 0) for item in services)
    summary["progress_percent"] = 100 if status in {"completed", "failed", "cancelled", "interrupted"} else _calculate_cliproxy_parent_progress(services)
    summary["current_stage"] = current_stage or status
    summary["status"] = status
    summary["cancellable"] = False
    run.summary_json = summary
    run.status = status
    run.error_message = error_message
    run.completed_at = datetime.utcnow()
    run.updated_at = run.completed_at
    db.commit()
    db.refresh(run)
    return run


def is_cliproxy_aggregate_task(run: MaintenanceRun) -> bool:
    if run.aggregate_kind in CLIPROXY_AGGREGATE_KINDS:
        return True
    summary = run.summary_json or {}
    return bool(summary.get("aggregate_key") or summary.get("service_total") or summary.get("services"))


def get_cliproxy_aggregate_task_owner_session_id(run: MaintenanceRun) -> Optional[str]:
    owner_session_id = run.owner_session_id
    if owner_session_id:
        return _normalize_cliproxy_owner_session_id(owner_session_id)
    summary = run.summary_json or {}
    return _normalize_cliproxy_owner_session_id(summary.get("owner_session_id"))


def get_cliproxy_aggregate_task_scope_key(run: MaintenanceRun) -> Optional[str]:
    if run.aggregate_scope_key:
        return run.aggregate_scope_key
    summary = run.summary_json or {}
    return summary.get("aggregate_key")


def _normalize_cliproxy_owner_session_id(session_id: Optional[str]) -> Optional[str]:
    if not session_id:
        return None
    try:
        from ..web.auth import parse_session_cookie

        return parse_session_cookie(session_id) or session_id
    except Exception:
        return session_id


def create_cliproxy_environment(
    db: Session,
    name: str,
    base_url: str,
    target_type: str,
    provider: str,
    token: Optional[str] = None,
    provider_scope: Optional[str] = None,
    target_scope: Optional[str] = None,
    scope_rules_json: Optional[Dict[str, Any]] = None,
    enabled: bool = True,
    is_default: bool = False,
    notes: Optional[str] = None,
) -> CLIProxyAPIEnvironment:
    if is_default:
        db.query(CLIProxyAPIEnvironment).filter(CLIProxyAPIEnvironment.is_default == True).update({"is_default": False})

    environment = CLIProxyAPIEnvironment(
        name=name,
        base_url=base_url,
        target_type=target_type,
        provider=provider,
        provider_scope=provider_scope,
        target_scope=target_scope,
        scope_rules_json=scope_rules_json,
        enabled=enabled,
        is_default=is_default,
        notes=notes,
    )
    if token is not None:
        environment.set_token(token)

    db.add(environment)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise
    db.refresh(environment)
    return environment


def get_cliproxy_environment_by_id(db: Session, environment_id: int) -> Optional[CLIProxyAPIEnvironment]:
    return db.query(CLIProxyAPIEnvironment).filter(CLIProxyAPIEnvironment.id == environment_id).first()


def get_cliproxy_environments(db: Session, enabled: Optional[bool] = None) -> List[CLIProxyAPIEnvironment]:
    query = db.query(CLIProxyAPIEnvironment)
    if enabled is not None:
        query = query.filter(CLIProxyAPIEnvironment.enabled == enabled)
    return query.order_by(asc(CLIProxyAPIEnvironment.name), asc(CLIProxyAPIEnvironment.id)).all()


def update_cliproxy_environment(db: Session, environment_id: int, **kwargs) -> Optional[CLIProxyAPIEnvironment]:
    environment = get_cliproxy_environment_by_id(db, environment_id)
    if not environment:
        return None

    token_present = "token" in kwargs
    token = kwargs.pop("token") if token_present else None
    is_default = kwargs.get("is_default")

    if is_default:
        db.query(CLIProxyAPIEnvironment).filter(
            CLIProxyAPIEnvironment.is_default == True,
            CLIProxyAPIEnvironment.id != environment_id,
        ).update({"is_default": False})

    for key, value in kwargs.items():
        if hasattr(environment, key):
            setattr(environment, key, value)

    if token_present:
        environment.set_token(token or "")

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise
    db.refresh(environment)
    return environment


def create_maintenance_run(
    db: Session,
    run_type: str,
    environment_id: Optional[int] = None,
    status: str = "pending",
    summary_json: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None,
    owner_session_id: Optional[str] = None,
    aggregate_scope_key: Optional[str] = None,
    aggregate_kind: Optional[str] = None,
) -> MaintenanceRun:
    _validate_maintenance_run_type_enabled(run_type)

    summary_json = _merge_maintenance_summary(summary_json)

    run = MaintenanceRun(
        run_type=run_type,
        environment_id=environment_id,
        owner_session_id=owner_session_id,
        aggregate_scope_key=aggregate_scope_key,
        aggregate_kind=aggregate_kind,
        status=status,
        summary_json=summary_json,
        error_message=error_message,
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def update_maintenance_run(db: Session, run_id: int, **kwargs) -> Optional[MaintenanceRun]:
    run = db.query(MaintenanceRun).filter(MaintenanceRun.id == run_id).first()
    if not run:
        return None
    explicit_summary = kwargs.pop("summary_json", None)
    summary_base = dict(run.summary_json or {})
    if explicit_summary:
        summary_base.update(explicit_summary)
    run.summary_json = _merge_maintenance_summary(summary_base or None, **kwargs)
    for key, value in kwargs.items():
        if hasattr(run, key):
            setattr(run, key, value)
    db.commit()
    db.refresh(run)
    return run


def get_maintenance_run_by_id(db: Session, run_id: int) -> Optional[MaintenanceRun]:
    return db.query(MaintenanceRun).filter(MaintenanceRun.id == run_id).first()


def get_maintenance_runs(
    db: Session,
    environment_id: Optional[int] = None,
    run_type: Optional[str] = None,
) -> List[MaintenanceRun]:
    query = db.query(MaintenanceRun)
    if environment_id is not None:
        query = query.filter(MaintenanceRun.environment_id == environment_id)
    if run_type is not None:
        query = query.filter(MaintenanceRun.run_type == run_type)
    return query.order_by(desc(MaintenanceRun.id)).all()


def get_in_flight_maintenance_run(db: Session, environment_id: int) -> Optional[MaintenanceRun]:
    return (
        db.query(MaintenanceRun)
        .filter(MaintenanceRun.environment_id == environment_id)
        .filter(MaintenanceRun.status.in_(MAINTENANCE_RUN_IN_FLIGHT_STATUSES))
        .order_by(desc(MaintenanceRun.id))
        .first()
    )


def get_cliproxy_aggregate_task_by_id(
    db: Session,
    task_id: int,
    *,
    owner_session_id: Optional[str] = None,
) -> Optional[MaintenanceRun]:
    run = db.query(MaintenanceRun).filter(MaintenanceRun.id == task_id).first()
    if run is None or not is_cliproxy_aggregate_task(run):
        return None
    if owner_session_id is not None and get_cliproxy_aggregate_task_owner_session_id(run) != owner_session_id:
        return None
    return run


def get_latest_active_cliproxy_aggregate_task(
    db: Session,
    *,
    owner_session_id: str,
    run_type: Optional[str] = None,
    aggregate_scope_key: Optional[str] = None,
) -> Optional[MaintenanceRun]:
    query = db.query(MaintenanceRun).filter(MaintenanceRun.status.in_(CLIPROXY_AGGREGATE_ACTIVE_STATUSES))
    if run_type is not None:
        query = query.filter(MaintenanceRun.run_type == run_type)
    runs = query.order_by(desc(MaintenanceRun.id)).all()
    for run in runs:
        if not is_cliproxy_aggregate_task(run):
            continue
        if get_cliproxy_aggregate_task_owner_session_id(run) != owner_session_id:
            continue
        if aggregate_scope_key is not None and get_cliproxy_aggregate_task_scope_key(run) != aggregate_scope_key:
            continue
        return run
    return None


def create_or_reuse_cliproxy_aggregate_task(
    db: Session,
    *,
    owner_session_id: str,
    run_type: str,
    service_ids: Iterable[int],
    services: List[Dict[str, Any]],
    request_data: Optional[Dict[str, Any]] = None,
) -> tuple[MaintenanceRun, bool]:
    normalized_service_ids = normalize_cliproxy_service_ids(service_ids)
    if not normalized_service_ids:
        raise ValueError("service_ids must not be empty")

    aggregate_scope_key = build_cliproxy_aggregate_scope_key(run_type=run_type, service_ids=normalized_service_ids)
    begin_immediate_transaction(db)

    existing = get_latest_active_cliproxy_aggregate_task(
        db,
        owner_session_id=owner_session_id,
        run_type=run_type,
        aggregate_scope_key=aggregate_scope_key,
    )
    if existing is not None:
        db.commit()
        db.refresh(existing)
        return existing, False

    conflicting_run_type = "maintain" if run_type == "scan" else "scan"
    conflicting_scope_key = build_cliproxy_aggregate_scope_key(run_type=conflicting_run_type, service_ids=normalized_service_ids)
    conflict = get_latest_active_cliproxy_aggregate_task(
        db,
        owner_session_id=owner_session_id,
        run_type=conflicting_run_type,
        aggregate_scope_key=conflicting_scope_key,
    )
    if conflict is not None:
        db.commit()
        db.refresh(conflict)
        raise ValueError(
            json.dumps(
                {
                    "code": "cliproxy_aggregate_conflict",
                    "message": "CLIProxy aggregate task already active for this service set",
                    "active_run_type": conflict.run_type,
                    "requested_run_type": run_type,
                    "aggregate_key": get_cliproxy_aggregate_task_scope_key(conflict),
                    "service_ids": normalized_service_ids,
                },
                sort_keys=True,
            )
        )

    run = create_maintenance_run(
        db,
        run_type=run_type,
        environment_id=None,
        status="queued",
        owner_session_id=owner_session_id,
        aggregate_scope_key=aggregate_scope_key,
        aggregate_kind="cliproxy_aggregate",
        summary_json=build_cliproxy_aggregate_summary(
            run_type=run_type,
            services=services,
            request_data=request_data,
        ),
    )
    summary = dict(run.summary_json or {})
    summary["task_id"] = str(run.id)
    summary["owner_session_id"] = owner_session_id
    summary["aggregate_key"] = aggregate_scope_key
    summary["service_ids"] = normalized_service_ids
    run.summary_json = summary
    db.commit()
    db.refresh(run)
    return run, True


def reconcile_abandoned_cliproxy_aggregate_tasks(db: Session) -> int:
    runs = db.query(MaintenanceRun).filter(MaintenanceRun.status.in_(CLIPROXY_AGGREGATE_ACTIVE_STATUSES)).all()
    reconciled_count = 0
    for run in runs:
        if not is_cliproxy_aggregate_task(run):
            continue
        summary = dict(run.summary_json or {})
        services = []
        for service in list(summary.get("services") or []):
            item = dict(service)
            item["status"] = "interrupted"
            item["current_stage"] = "interrupted"
            services.append(item)
        summary["services"] = services
        summary["status"] = "interrupted"
        summary["cancellable"] = False
        summary["owner_session_id"] = get_cliproxy_aggregate_task_owner_session_id(run)
        summary["aggregate_key"] = get_cliproxy_aggregate_task_scope_key(run)
        summary["service_ids"] = normalize_cliproxy_service_ids(item["service_id"] for item in services)
        summary["grouped_logs"] = _normalize_cliproxy_grouped_service_data(services, summary.get("grouped_logs"), list)
        summary["grouped_results"] = _normalize_cliproxy_grouped_service_data(services, summary.get("grouped_results"), dict)
        run.summary_json = summary
        run.aggregate_kind = "cliproxy_aggregate"
        run.owner_session_id = get_cliproxy_aggregate_task_owner_session_id(run)
        run.aggregate_scope_key = get_cliproxy_aggregate_task_scope_key(run)
        run.status = "interrupted"
        run.completed_at = run.completed_at or datetime.utcnow()
        reconciled_count += 1
    if reconciled_count:
        db.commit()
    return reconciled_count


def begin_immediate_transaction(db: Session) -> None:
    bind = db.get_bind()
    if bind is not None and bind.dialect.name == "sqlite":
        db.execute(text("BEGIN IMMEDIATE"))


def create_maintenance_run_if_available(
    db: Session,
    run_type: str,
    environment_id: int,
    request_data: Dict[str, Any],
) -> tuple[MaintenanceRun, bool]:
    _validate_maintenance_run_type_enabled(run_type)
    idempotency_key = request_data.get("idempotency_key")
    begin_immediate_transaction(db)

    if idempotency_key:
        replay = get_recent_idempotent_maintenance_run(
            db,
            environment_id=environment_id,
            run_type=run_type,
            idempotency_key=idempotency_key,
            request=request_data,
        )
        if replay is not None:
            db.commit()
            db.refresh(replay)
            return replay, False

    in_flight = get_in_flight_maintenance_run(db, environment_id)
    if in_flight is not None:
        db.commit()
        db.refresh(in_flight)
        return in_flight, False

    run = MaintenanceRun(
        run_type=run_type,
        environment_id=environment_id,
        status="queued",
        summary_json=_merge_maintenance_summary(
            {
                "request": request_data,
                "idempotency_key": idempotency_key,
                "current_stage": "queued",
                "progress_percent": 0,
                "cancellable": True,
            }
        ),
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run, True


def get_recent_idempotent_maintenance_run(
    db: Session,
    environment_id: int,
    run_type: str,
    idempotency_key: str,
    request: Dict[str, Any],
    now: Optional[datetime] = None,
    window_minutes: int = 10,
) -> Optional[MaintenanceRun]:
    cutoff = (now or datetime.utcnow()) - timedelta(minutes=window_minutes)
    runs = (
        db.query(MaintenanceRun)
        .filter(MaintenanceRun.environment_id == environment_id)
        .filter(MaintenanceRun.run_type == run_type)
        .filter(MaintenanceRun.created_at >= cutoff)
        .order_by(desc(MaintenanceRun.id))
        .all()
    )
    for run in runs:
        summary = run.summary_json or {}
        existing_key = summary.get("idempotency_key")
        if existing_key is None:
            existing_key = (summary.get("request") or {}).get("idempotency_key")
        if existing_key != idempotency_key:
            continue
        if summary.get("request") != request:
            continue
        return run
    return None


def create_maintenance_action_log(
    db: Session,
    run_id: int,
    action_type: str,
    status: str = "pending",
    environment_id: Optional[int] = None,
    remote_file_id: Optional[str] = None,
    message: Optional[str] = None,
    details_json: Optional[Dict[str, Any]] = None,
) -> MaintenanceActionLog:
    action_log = MaintenanceActionLog(
        run_id=run_id,
        environment_id=environment_id,
        action_type=action_type,
        status=status,
        remote_file_id=remote_file_id,
        message=message,
        details_json=details_json,
    )
    db.add(action_log)
    db.commit()
    db.refresh(action_log)
    if action_type in {"disable", "reenable"}:
        write_audit_log(
            db,
            event_type="maintain_action",
            actor="webui",
            environment_id=environment_id,
            run_id=run_id,
            message=message,
            details_json={
                "resource": "action_log",
                "resource_id": action_log.id,
                "action_type": action_type,
                "status": status,
                "remote_file_id": remote_file_id,
                "details_json": details_json,
            },
        )
    return action_log


def get_maintenance_action_log_by_id(db: Session, action_log_id: int) -> Optional[MaintenanceActionLog]:
    return db.query(MaintenanceActionLog).filter(MaintenanceActionLog.id == action_log_id).first()


def get_maintenance_action_logs(
    db: Session,
    run_id: Optional[int] = None,
    environment_id: Optional[int] = None,
) -> List[MaintenanceActionLog]:
    query = db.query(MaintenanceActionLog)
    if run_id is not None:
        query = query.filter(MaintenanceActionLog.run_id == run_id)
    if environment_id is not None:
        query = query.filter(MaintenanceActionLog.environment_id == environment_id)
    return query.order_by(asc(MaintenanceActionLog.id)).all()


def update_maintenance_action_log(db: Session, action_log_id: int, **kwargs) -> Optional[MaintenanceActionLog]:
    action_log = get_maintenance_action_log_by_id(db, action_log_id)
    if not action_log:
        return None
    for key, value in kwargs.items():
        if hasattr(action_log, key):
            setattr(action_log, key, value)
    db.commit()
    db.refresh(action_log)
    return action_log


def write_audit_log(
    db: Session,
    event_type: str,
    actor: str = "system",
    environment_id: Optional[int] = None,
    run_id: Optional[int] = None,
    message: Optional[str] = None,
    details_json: Optional[Dict[str, Any]] = None,
) -> AuditLog:
    audit_log = AuditLog(
        event_type=event_type,
        actor=actor,
        environment_id=environment_id,
        run_id=run_id,
        message=message,
        details_json=details_json,
    )
    db.add(audit_log)
    db.commit()
    db.refresh(audit_log)
    return audit_log


def get_remote_auth_inventory(
    db: Session,
    environment_id: Optional[int] = None,
) -> List[RemoteAuthInventory]:
    query = db.query(RemoteAuthInventory)
    if environment_id is not None:
        query = query.filter(RemoteAuthInventory.environment_id == environment_id)
    return query.order_by(asc(RemoteAuthInventory.id)).all()


def get_audit_logs(
    db: Session,
    environment_id: Optional[int] = None,
    run_id: Optional[int] = None,
    event_type: Optional[str] = None,
    resource: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> List[AuditLog]:
    query = db.query(AuditLog)
    if environment_id is not None:
        query = query.filter(AuditLog.environment_id == environment_id)
    if run_id is not None:
        query = query.filter(AuditLog.run_id == run_id)
    if event_type is not None:
        query = query.filter(AuditLog.event_type == event_type)
    if start_time is not None:
        query = query.filter(AuditLog.created_at >= start_time)
    if end_time is not None:
        query = query.filter(AuditLog.created_at <= end_time)
    rows = query.order_by(desc(AuditLog.id)).all()
    if resource is None:
        return rows
    return [row for row in rows if (row.details_json or {}).get("resource") == resource]


def upsert_remote_auth_inventory(
    db: Session,
    environment_id: int,
    remote_file_id: str,
    email: Optional[str] = None,
    payload_json: Optional[Dict[str, Any]] = None,
    remote_account_id: Optional[str] = None,
    local_account_id: Optional[int] = None,
    last_seen_at: Optional[datetime] = None,
    last_probed_at: Optional[datetime] = None,
    sync_state: Optional[str] = None,
    probe_status: Optional[str] = None,
    disable_source: Optional[str] = None,
) -> RemoteAuthInventory:
    inventory = db.query(RemoteAuthInventory).filter(
        RemoteAuthInventory.environment_id == environment_id,
        RemoteAuthInventory.remote_file_id == remote_file_id,
    ).first()

    effective_last_seen_at = last_seen_at or datetime.utcnow()

    if inventory is None:
        inventory = RemoteAuthInventory(
            environment_id=environment_id,
            remote_file_id=remote_file_id,
            email=email,
            remote_account_id=remote_account_id,
            local_account_id=local_account_id,
            payload_json=payload_json,
            last_seen_at=effective_last_seen_at,
            last_probed_at=last_probed_at,
            sync_state=sync_state or "unlinked",
            probe_status=probe_status or "unknown",
            disable_source=disable_source,
        )
        db.add(inventory)
    else:
        inventory.email = email
        inventory.remote_account_id = remote_account_id
        inventory.local_account_id = local_account_id
        inventory.payload_json = payload_json
        inventory.last_seen_at = effective_last_seen_at
        inventory.last_probed_at = last_probed_at
        if sync_state is not None:
            inventory.sync_state = sync_state
        if probe_status is not None:
            inventory.probe_status = probe_status
        if disable_source is not None:
            inventory.disable_source = disable_source

    db.flush()
    return inventory
