import logging
import threading
from datetime import datetime
from typing import Iterable, Optional

from ...database.session import get_db
from ...database import crud

logger = logging.getLogger(__name__)


def start_batch_subscription_check_task(
    task_id: int,
    *,
    check_subscription_status_fn,
) -> threading.Thread:
    thread = threading.Thread(
        target=_run_batch_subscription_check_task,
        args=(task_id, check_subscription_status_fn),
        daemon=True,
        name=f"batch-subscription-check-{task_id}",
    )
    thread.start()
    return thread


def _run_batch_subscription_check_task(task_id: int, check_subscription_status_fn) -> None:
    try:
        with get_db() as db:
            task = crud.start_batch_subscription_task(db, task_id)
            if task is None or task.status != "running":
                return
            request_payload = dict(task.request_payload or {})
            account_ids = [int(account_id) for account_id in request_payload.get("ids") or []]
            proxy = task.proxy

        processed_count = 0
        success_count = 0
        failure_count = 0

        for account_id in account_ids:
            with get_db() as db:
                account = crud.get_account_by_id(db, account_id)
                if account is None:
                    failure_count += 1
                    processed_count += 1
                    crud.update_batch_subscription_task_progress(
                        db,
                        task_id,
                        processed_count=processed_count,
                        success_count=success_count,
                        failure_count=failure_count,
                        current_account=str(account_id),
                        log_lines=[
                            f"checking account {account_id}",
                            f"account {account_id} subscription check failed: account not found",
                        ],
                    )
                    continue

                account_label = str(account.id)
                try:
                    subscription_type = check_subscription_status_fn(account, proxy=proxy)
                    account.subscription_type = None if subscription_type == "free" else subscription_type
                    account.subscription_at = datetime.utcnow() if subscription_type != "free" else None
                    db.commit()
                    success_count += 1
                    log_lines = [
                        f"checking account {account_label}",
                        f"account {account_label} subscription updated to {subscription_type}",
                    ]
                except Exception as exc:
                    db.rollback()
                    failure_count += 1
                    log_lines = [
                        f"checking account {account_label}",
                        f"account {account_label} subscription check failed: {exc}",
                    ]

                processed_count += 1
                crud.update_batch_subscription_task_progress(
                    db,
                    task_id,
                    processed_count=processed_count,
                    success_count=success_count,
                    failure_count=failure_count,
                    current_account=account_label,
                    log_lines=log_lines,
                )

        with get_db() as db:
            crud.finalize_batch_subscription_task(
                db,
                task_id,
                status="completed",
                current_account=None,
                log_lines=["batch subscription task completed"],
            )
    except Exception as exc:
        logger.exception("batch subscription task %s failed", task_id)
        with get_db() as db:
            crud.finalize_batch_subscription_task(
                db,
                task_id,
                status="failed",
                current_account=None,
                log_lines=[f"batch subscription task failed: {exc}"],
            )
