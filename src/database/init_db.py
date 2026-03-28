"""
数据库初始化和初始化数据
"""

import hashlib
import json

from contextlib import contextmanager

from sqlalchemy import inspect, text
from sqlalchemy.exc import NoSuchTableError

from .session import init_database
from .models import Base, normalize_temp_mail_config


@contextmanager
def _get_managed_db_session(db_manager):
    db_resource = db_manager.get_db()

    if hasattr(db_resource, "__enter__") and hasattr(db_resource, "__exit__"):
        with db_resource as db:
            yield db
        return

    generator = iter(db_resource)
    db = next(generator)
    try:
        yield db
    finally:
        try:
            next(generator)
        except StopIteration:
            pass
        close = getattr(db, "close", None)
        if callable(close):
            close()


def _backfill_sub2api_service_target_type(db_manager):
    """为旧数据库补充 sub2api_services.target_type 字段。"""
    try:
        inspector = inspect(db_manager.engine)
        columns = inspector.get_columns("sub2api_services")
    except NoSuchTableError:
        return

    column_names = {column["name"] for column in columns}
    if "target_type" in column_names:
        return

    with _get_managed_db_session(db_manager) as db:
        db.execute(
            text(
                "ALTER TABLE sub2api_services "
                "ADD COLUMN target_type VARCHAR(20) NOT NULL DEFAULT 'sub2api'"
            )
        )
        db.commit()


def _backfill_cliproxy_environment_scope_columns(db_manager):
    """为旧数据库补充 cliproxy_environments 范围字段。"""
    try:
        inspector = inspect(db_manager.engine)
        columns = inspector.get_columns("cliproxy_environments")
    except NoSuchTableError:
        return

    column_names = {column["name"] for column in columns}
    statements = []
    if "provider_scope" not in column_names:
        statements.append("ALTER TABLE cliproxy_environments ADD COLUMN provider_scope VARCHAR(100)")
    if "target_scope" not in column_names:
        statements.append("ALTER TABLE cliproxy_environments ADD COLUMN target_scope VARCHAR(100)")
    if "scope_rules_json" not in column_names:
        statements.append("ALTER TABLE cliproxy_environments ADD COLUMN scope_rules_json TEXT")

    if not statements:
        return

    with _get_managed_db_session(db_manager) as db:
        for statement in statements:
            db.execute(text(statement))
        db.commit()


def _backfill_maintenance_run_aggregate_columns(db_manager):
    """为旧数据库补充 CLIProxy 聚合任务字段。"""
    try:
        inspector = inspect(db_manager.engine)
        columns = inspector.get_columns("maintenance_runs")
    except NoSuchTableError:
        return

    column_names = {column["name"] for column in columns}
    statements = []
    if "owner_session_id" not in column_names:
        statements.append("ALTER TABLE maintenance_runs ADD COLUMN owner_session_id VARCHAR(255)")
    if "aggregate_scope_key" not in column_names:
        statements.append("ALTER TABLE maintenance_runs ADD COLUMN aggregate_scope_key VARCHAR(255)")
    if "aggregate_kind" not in column_names:
        statements.append("ALTER TABLE maintenance_runs ADD COLUMN aggregate_kind VARCHAR(32)")

    if not statements:
        return

    with _get_managed_db_session(db_manager) as db:
        for statement in statements:
            db.execute(text(statement))
        db.commit()


def _backfill_account_traceability_columns(db_manager):
    """为旧数据库补充 accounts 追踪字段。"""
    try:
        inspector = inspect(db_manager.engine)
        columns = inspector.get_columns("accounts")
    except NoSuchTableError:
        return

    column_names = {column["name"] for column in columns}
    statements = []
    if "platform_source" not in column_names:
        statements.append("ALTER TABLE accounts ADD COLUMN platform_source VARCHAR(50)")
    if "last_upload_target" not in column_names:
        statements.append("ALTER TABLE accounts ADD COLUMN last_upload_target VARCHAR(20)")

    if not statements:
        return

    with _get_managed_db_session(db_manager) as db:
        for statement in statements:
            db.execute(text(statement))
        db.commit()


def _backfill_batch_subscription_task_columns(db_manager):
    """确保批量订阅任务表存在最小字段集。"""
    try:
        inspector = inspect(db_manager.engine)
        columns = inspector.get_columns("batch_subscription_tasks")
    except NoSuchTableError:
        return

    column_names = {column["name"] for column in columns}
    statements = []
    if "owner_key" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN owner_key VARCHAR(255) NOT NULL DEFAULT 'anonymous'")
    if "session_id" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN session_id VARCHAR(255)")
    if "scope_key" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN scope_key VARCHAR(64) NOT NULL DEFAULT ''")
    if "active_scope_key" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN active_scope_key VARCHAR(64)")
    if "status" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN status VARCHAR(20) NOT NULL DEFAULT 'queued'")
    if "total_count" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN total_count INTEGER NOT NULL DEFAULT 0")
    if "processed_count" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN processed_count INTEGER NOT NULL DEFAULT 0")
    if "success_count" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN success_count INTEGER NOT NULL DEFAULT 0")
    if "failure_count" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0")
    if "current_account" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN current_account VARCHAR(255)")
    if "request_payload" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN request_payload TEXT")
    if "recent_logs" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN recent_logs TEXT")
    if "proxy" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN proxy VARCHAR(255)")
    if "started_at" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN started_at DATETIME")
    if "completed_at" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN completed_at DATETIME")
    if "updated_at" not in column_names:
        statements.append("ALTER TABLE batch_subscription_tasks ADD COLUMN updated_at DATETIME")

    with _get_managed_db_session(db_manager) as db:
        for statement in statements:
            db.execute(text(statement))
        legacy_rows = db.execute(
            text(
                "SELECT id, owner_key, scope_key, active_scope_key, status, request_payload, proxy "
                "FROM batch_subscription_tasks ORDER BY id"
            )
        ).fetchall()
        active_statuses = {"queued", "running"}
        terminal_statuses = {"completed", "failed", "cancelled", "interrupted"}
        reservation_counts = {}

        for row in legacy_rows:
            scope_key = (row.scope_key or "").strip()
            request_payload = None
            if row.request_payload:
                try:
                    request_payload = json.loads(row.request_payload)
                except (TypeError, json.JSONDecodeError):
                    request_payload = None

            if not scope_key:
                if isinstance(request_payload, dict):
                    ids = request_payload.get("ids") or []
                    normalized_ids = sorted({int(account_id) for account_id in ids}) if isinstance(ids, list) else []
                    normalized_scope = {
                        "kind": "filter_snapshot" if request_payload.get("select_all") else "account_ids",
                        "account_ids": normalized_ids,
                        "status_filter": request_payload.get("status_filter") or "",
                        "email_service_filter": request_payload.get("email_service_filter") or "",
                        "search_filter": request_payload.get("search_filter") or "",
                        "proxy": request_payload.get("proxy") or row.proxy or "",
                    }
                    serialized = json.dumps(normalized_scope, sort_keys=True, separators=(",", ":"))
                    scope_key = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
                else:
                    scope_key = hashlib.sha256(f"legacy:{row.owner_key}:{row.id}".encode("utf-8")).hexdigest()

                db.execute(
                    text("UPDATE batch_subscription_tasks SET scope_key = :scope_key WHERE id = :task_id"),
                    {"scope_key": scope_key, "task_id": row.id},
                )

            if row.status in active_statuses:
                reservation = scope_key
                reservation_key = (row.owner_key, reservation)
                reservation_counts[reservation_key] = reservation_counts.get(reservation_key, 0) + 1
                if reservation_counts[reservation_key] > 1:
                    reservation = hashlib.sha256(f"{reservation}:legacy:{row.id}".encode("utf-8")).hexdigest()
                db.execute(
                    text("UPDATE batch_subscription_tasks SET active_scope_key = :active_scope_key WHERE id = :task_id"),
                    {"active_scope_key": reservation, "task_id": row.id},
                )
            elif row.status in terminal_statuses:
                db.execute(
                    text("UPDATE batch_subscription_tasks SET active_scope_key = NULL WHERE id = :task_id"),
                    {"task_id": row.id},
                )
        db.execute(
            text(
                "UPDATE batch_subscription_tasks "
                "SET active_scope_key = scope_key "
                "WHERE status IN ('queued', 'running') "
                "AND (active_scope_key IS NULL OR active_scope_key = '')"
            )
        )
        db.execute(
            text(
                "UPDATE batch_subscription_tasks "
                "SET active_scope_key = NULL "
                "WHERE status IN ('completed', 'failed', 'cancelled', 'interrupted')"
            )
        )
        db.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_batch_subscription_tasks_owner_active_scope "
                "ON batch_subscription_tasks (owner_key, active_scope_key)"
            )
        )
        db.commit()


def _backfill_temp_mail_domains(db_manager):
    """将旧 TempMail 单域名配置懒升级为 canonical domains 形态。"""
    try:
        inspector = inspect(db_manager.engine)
        inspector.get_columns("email_services")
    except NoSuchTableError:
        return

    with _get_managed_db_session(db_manager) as db:
        rows = db.execute(
            text(
                "SELECT id, config FROM email_services WHERE service_type = 'temp_mail' ORDER BY id"
            )
        ).fetchall()

        for row in rows:
            if not row.config:
                continue
            try:
                config = json.loads(row.config)
            except (TypeError, json.JSONDecodeError):
                continue

            try:
                normalized = normalize_temp_mail_config(config)
            except ValueError:
                continue

            if normalized != config:
                db.execute(
                    text("UPDATE email_services SET config = :config WHERE id = :service_id"),
                    {
                        "config": json.dumps(normalized, ensure_ascii=False),
                        "service_id": row.id,
                    },
                )

        db.commit()


def _backfill_cpa_workbench_task_table(db_manager):
    """确保 CPA 工作台任务表存在。"""
    try:
        inspector = inspect(db_manager.engine)
        inspector.get_columns("cpa_workbench_tasks")
        return
    except NoSuchTableError:
        pass

    with _get_managed_db_session(db_manager) as db:
        db.execute(
            text(
                "CREATE TABLE cpa_workbench_tasks ("
                "id INTEGER NOT NULL PRIMARY KEY, "
                "task_type VARCHAR(32) NOT NULL, "
                "owner_session_id VARCHAR(255) NOT NULL, "
                "scope_key VARCHAR(255) NOT NULL, "
                "status VARCHAR(20) NOT NULL DEFAULT 'queued', "
                "total_count INTEGER NOT NULL DEFAULT 0, "
                "processed_count INTEGER NOT NULL DEFAULT 0, "
                "current_item VARCHAR(255), "
                "log_lines TEXT, "
                "stats_json TEXT, "
                "created_at DATETIME, "
                "started_at DATETIME, "
                "completed_at DATETIME, "
                "updated_at DATETIME"
                ")"
            )
        )
        db.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_cpa_workbench_tasks_task_type "
                "ON cpa_workbench_tasks (task_type)"
            )
        )
        db.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_cpa_workbench_tasks_owner_session_id "
                "ON cpa_workbench_tasks (owner_session_id)"
            )
        )
        db.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_cpa_workbench_tasks_scope_key "
                "ON cpa_workbench_tasks (scope_key)"
            )
        )
        db.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_cpa_workbench_tasks_status "
                "ON cpa_workbench_tasks (status)"
            )
        )
        db.commit()


def initialize_database(database_url: str = None):
    """
    初始化数据库
    创建所有表并设置默认配置
    """
    # 初始化数据库连接和表
    db_manager = init_database(database_url)

    # 创建表
    db_manager.create_tables()

    # 兼容旧库结构
    _backfill_sub2api_service_target_type(db_manager)
    _backfill_cliproxy_environment_scope_columns(db_manager)
    _backfill_maintenance_run_aggregate_columns(db_manager)
    _backfill_account_traceability_columns(db_manager)
    _backfill_batch_subscription_task_columns(db_manager)
    _backfill_temp_mail_domains(db_manager)
    _backfill_cpa_workbench_task_table(db_manager)

    # 初始化默认设置（从 settings 模块导入以避免循环导入）
    from ..config.settings import init_default_settings
    init_default_settings()

    return db_manager


def reset_database(database_url: str = None):
    """
    重置数据库（删除所有表并重新创建）
    警告：会丢失所有数据！
    """
    db_manager = init_database(database_url)

    # 删除所有表
    db_manager.drop_tables()
    print("已删除所有表")

    # 重新创建所有表
    db_manager.create_tables()
    print("已重新创建所有表")

    # 初始化默认设置
    from ..config.settings import init_default_settings
    init_default_settings()

    print("数据库重置完成")
    return db_manager


def check_database_connection(database_url: str = None) -> bool:
    """
    检查数据库连接是否正常
    """
    try:
        db_manager = init_database(database_url)
        with _get_managed_db_session(db_manager) as db:
            # 尝试执行一个简单的查询
            db.execute("SELECT 1")
        print("数据库连接正常")
        return True
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return False


if __name__ == "__main__":
    # 当直接运行此脚本时，初始化数据库
    import argparse

    parser = argparse.ArgumentParser(description="数据库初始化脚本")
    parser.add_argument("--reset", action="store_true", help="重置数据库（删除所有数据）")
    parser.add_argument("--check", action="store_true", help="检查数据库连接")
    parser.add_argument("--url", help="数据库连接字符串")

    args = parser.parse_args()

    if args.check:
        check_database_connection(args.url)
    elif args.reset:
        confirm = input("警告：这将删除所有数据！确认重置？(y/N): ")
        if confirm.lower() == 'y':
            reset_database(args.url)
        else:
            print("操作已取消")
    else:
        initialize_database(args.url)
