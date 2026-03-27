from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import NoSuchTableError

from src.database import init_db


class FakeSession:
    def __init__(self):
        self.statements = []
        self.committed = False

    def execute(self, statement):
        self.statements.append(str(statement))
        return []

    def commit(self):
        self.committed = True


class FakeDbManager:
    def __init__(self, dialect_name):
        self.engine = SimpleNamespace(dialect=SimpleNamespace(name=dialect_name))
        self.session = FakeSession()

    @contextmanager
    def get_db(self):
        yield self.session


class FakeGeneratorDbManager:
    def __init__(self, dialect_name):
        self.engine = SimpleNamespace(dialect=SimpleNamespace(name=dialect_name))
        self.session = FakeSession()

    def get_db(self):
        yield self.session


def test_backfill_sub2api_target_type_uses_backend_aware_column_lookup_for_postgresql(monkeypatch):
    manager = FakeDbManager("postgresql")

    class FakeInspector:
        def get_columns(self, table_name):
            assert table_name == "sub2api_services"
            return [
                {"name": "id"},
                {"name": "name"},
                {"name": "api_url"},
                {"name": "api_key"},
            ]

    monkeypatch.setattr(init_db, "inspect", lambda engine: FakeInspector())

    init_db._backfill_sub2api_service_target_type(manager)

    assert manager.session.committed is True
    assert any(
        "ALTER TABLE sub2api_services ADD COLUMN target_type VARCHAR(20) NOT NULL DEFAULT 'sub2api'" in statement
        for statement in manager.session.statements
    )


def test_backfill_sub2api_target_type_skips_when_column_already_exists(monkeypatch):
    manager = FakeDbManager("postgresql")

    class FakeInspector:
        def get_columns(self, table_name):
            assert table_name == "sub2api_services"
            return [
                {"name": "id"},
                {"name": "target_type"},
            ]

    monkeypatch.setattr(init_db, "inspect", lambda engine: FakeInspector())

    init_db._backfill_sub2api_service_target_type(manager)

    assert manager.session.committed is False
    assert manager.session.statements == []


def test_backfill_sub2api_target_type_skips_when_table_does_not_exist(monkeypatch):
    manager = FakeDbManager("postgresql")

    class MissingTableInspector:
        def get_columns(self, table_name):
            raise NoSuchTableError(table_name)

    monkeypatch.setattr(init_db, "inspect", lambda engine: MissingTableInspector())

    init_db._backfill_sub2api_service_target_type(manager)

    assert manager.session.committed is False
    assert manager.session.statements == []


def test_backfill_sub2api_target_type_surfaces_non_sqlite_alter_failures(monkeypatch):
    manager = FakeDbManager("postgresql")

    class FakeInspector:
        def get_columns(self, table_name):
            return [{"name": "id"}]

    def broken_execute(statement):
        raise RuntimeError("permission denied for alter table")

    manager.session.execute = broken_execute
    monkeypatch.setattr(init_db, "inspect", lambda engine: FakeInspector())

    with pytest.raises(RuntimeError, match="permission denied"):
        init_db._backfill_sub2api_service_target_type(manager)


def test_backfill_account_traceability_columns_adds_missing_columns(monkeypatch):
    manager = FakeDbManager("postgresql")

    class FakeInspector:
        def get_columns(self, table_name):
            assert table_name == "accounts"
            return [
                {"name": "id"},
                {"name": "email"},
            ]

    monkeypatch.setattr(init_db, "inspect", lambda engine: FakeInspector())

    init_db._backfill_account_traceability_columns(manager)

    assert manager.session.committed is True
    assert any(
        "ALTER TABLE accounts ADD COLUMN platform_source VARCHAR(50)" in statement
        for statement in manager.session.statements
    )
    assert any(
        "ALTER TABLE accounts ADD COLUMN last_upload_target VARCHAR(20)" in statement
        for statement in manager.session.statements
    )


def test_backfill_account_traceability_columns_supports_generator_get_db(monkeypatch):
    manager = FakeGeneratorDbManager("postgresql")

    class FakeInspector:
        def get_columns(self, table_name):
            assert table_name == "accounts"
            return [
                {"name": "id"},
                {"name": "email"},
            ]

    monkeypatch.setattr(init_db, "inspect", lambda engine: FakeInspector())

    init_db._backfill_account_traceability_columns(manager)

    assert manager.session.committed is True
    assert any(
        "ALTER TABLE accounts ADD COLUMN platform_source VARCHAR(50)" in statement
        for statement in manager.session.statements
    )
