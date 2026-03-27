"""
数据库初始化和初始化数据
"""

from contextlib import contextmanager

from sqlalchemy import inspect, text
from sqlalchemy.exc import NoSuchTableError

from .session import init_database
from .models import Base


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
    _backfill_account_traceability_columns(db_manager)

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
