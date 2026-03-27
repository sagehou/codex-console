from __future__ import annotations

from datetime import datetime, timedelta

from src.core.cliproxy.maintenance import CLIProxyMaintenanceEngine
from src.database import crud
from src.database.models import Base, CLIProxyAPIEnvironment, MaintenanceActionLog, RemoteAuthInventory
from src.database.session import DatabaseSessionManager


class FakeCLIProxyAPIClient:
    def __init__(self, inventory_records=None, probe_results=None):
        self.inventory_records = inventory_records or []
        self.probe_results = probe_results or {}
        self.disable_calls = []
        self.reenable_calls = []

    def fetch_inventory(self):
        return list(self.inventory_records)

    def probe_usage(self, remote_file_id):
        return dict(self.probe_results[remote_file_id])

    def disable_auth(self, remote_file_id):
        self.disable_calls.append(remote_file_id)
        return {"ok": True, "remote_file_id": remote_file_id}

    def reenable_auth(self, remote_file_id):
        self.reenable_calls.append(remote_file_id)
        return {"ok": True, "remote_file_id": remote_file_id}


class ExplodingCLIProxyAPIClient(FakeCLIProxyAPIClient):
    def __init__(self, *args, explode_on=None, exception=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.explode_on = explode_on
        self.exception = exception or RuntimeError("boom")

    def fetch_inventory(self):
        if self.explode_on == "fetch_inventory":
            raise self.exception
        return super().fetch_inventory()

    def probe_usage(self, remote_file_id):
        if self.explode_on == "probe_usage":
            raise self.exception
        return super().probe_usage(remote_file_id)

    def disable_auth(self, remote_file_id):
        if self.explode_on == "disable_auth":
            raise self.exception
        return super().disable_auth(remote_file_id)

    def reenable_auth(self, remote_file_id):
        if self.explode_on == "reenable_auth":
            raise self.exception
        return super().reenable_auth(remote_file_id)


class CancellationAwareCLIProxyAPIClient(FakeCLIProxyAPIClient):
    def __init__(self, inventory_records=None, probe_results=None, cancel_on_probe_call=None):
        super().__init__(inventory_records=inventory_records, probe_results=probe_results)
        self.cancel_on_probe_call = cancel_on_probe_call
        self.probe_calls = 0
        self.cancel_callback = None

    def probe_usage(self, remote_file_id):
        self.probe_calls += 1
        if self.cancel_callback is not None and self.cancel_on_probe_call == self.probe_calls:
            self.cancel_callback()
        return super().probe_usage(remote_file_id)


def make_db(tmp_path, name="cliproxy_maintenance.db"):
    db_path = tmp_path / name
    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)
    return manager


def make_environment(db):
    environment = CLIProxyAPIEnvironment(
        name="primary",
        base_url="https://cliproxy.example.com",
        target_type="cpa",
        provider="cloudmail",
    )
    db.add(environment)
    db.flush()
    return environment


def test_scan_classifies_and_persists_inventory_updates(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        account = crud.create_account(
            db,
            email="linked@example.com",
            email_service="tempmail",
            account_id="acct-1",
        )
        assert account.email == "linked@example.com"
        linked_seed = crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="seed-file",
            email="linked@example.com",
            remote_account_id="acct-1",
            local_account_id=account.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        assert linked_seed.local_account_id == account.id
        seen_before = datetime.utcnow() - timedelta(days=1)
        inventory = RemoteAuthInventory(
            environment_id=environment.id,
            remote_file_id="file-1",
            email="old@example.com",
            payload_json={"stale": True},
            last_seen_at=seen_before,
        )
        db.add(inventory)
        db.flush()
        inventory_id = inventory.id

        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "linked@example.com", "remote_account_id": "acct-1"},
                {"remote_file_id": "file-2", "remote_email": "quota@example.com", "remote_account_id": "acct-2"},
                {"remote_file_id": "file-3", "remote_email": "recover@example.com", "remote_account_id": "acct-3"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
                "file-2": {"quota_limited": True},
                "file-3": {"status": "ok"},
            },
        )

        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.scan(environment.id)

        assert [item["classification"] for item in result["records"]] == [
            "unauthorized_401",
            "quota_limited",
            "recovered",
        ]
        persisted = db.get(RemoteAuthInventory, inventory_id)
        assert persisted is not None
        assert persisted.remote_file_id == "file-1"
        assert persisted.email == "linked@example.com"
        assert persisted.remote_account_id == "acct-1"
        assert persisted.sync_state == "linked"
        assert persisted.probe_status == "unauthorized_401"
        assert persisted.last_seen_at >= seen_before
        assert persisted.last_probed_at is not None


def test_maintain_dry_run_records_actions_without_remote_state_change(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        account = crud.create_account(db, email="disable@example.com", email_service="tempmail")
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="seed-disable",
            email="disable@example.com",
            local_account_id=account.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "disable@example.com"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.maintain(environment.id, dry_run=True)

        logs = db.query(MaintenanceActionLog).order_by(MaintenanceActionLog.id).all()
        assert result["records"][0]["decision"] == "disable"
        assert result["records"][0]["dry_run"] is True
        assert client.disable_calls == []
        assert client.reenable_calls == []
        assert logs[-1].action_type == "disable"
        assert logs[-1].status == "dry_run"


def test_maintain_only_allows_disable_and_reenable_in_v1(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "missing@example.com"},
                {"remote_file_id": "file-2", "remote_email": "conflict@example.com"},
            ],
            probe_results={
                "file-1": {"status": "probe_failed", "error": "timeout"},
                "file-2": {"status_code": 403},
            },
        )
        crud.create_account(db, email="conflict@example.com", email_service="tempmail", account_id="acct-a")
        crud.create_account(db, email="other@example.com", email_service="tempmail", account_id="acct-b")
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.maintain(environment.id)

        assert {item["decision"] for item in result["records"]} <= {"disable", "reenable", "log_only"}
        assert all(item["decision"] != "delete" for item in result["records"])


def test_matching_uses_remote_email_then_remote_account_id(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        email_match = crud.create_account(
            db,
            email="match@example.com",
            email_service="tempmail",
            account_id="acct-email",
        )
        account_match = crud.create_account(
            db,
            email="other@example.com",
            email_service="tempmail",
            account_id="acct-remote",
        )
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="seed-email",
            email="match@example.com",
            remote_account_id="acct-email",
            local_account_id=email_match.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="seed-account",
            email="other@example.com",
            remote_account_id="acct-remote",
            local_account_id=account_match.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "match@example.com", "remote_account_id": "wrong-id"},
                {"remote_file_id": "file-2", "remote_email": "missing@example.com", "remote_account_id": "acct-remote"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
                "file-2": {"status_code": 401},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.scan(environment.id)

        first, second = result["records"]
        assert first["match"]["outcome"] == "linked"
        assert first["match"]["account_id"] == email_match.id
        assert first["match"]["strategy"] == "remote_email"
        assert second["match"]["outcome"] == "linked"
        assert second["match"]["account_id"] == account_match.id
        assert second["match"]["strategy"] == "remote_account_id"


def test_matching_marks_conflict_and_missing_local(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        first = crud.create_account(db, email="shared@example.com", email_service="tempmail", account_id="acct-1")
        second = crud.create_account(db, email="other@example.com", email_service="tempmail", account_id="acct-1")
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="seed-conflict",
            email="shared@example.com",
            remote_account_id="acct-1",
            local_account_id=first.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="seed-conflict-2",
            email="other@example.com",
            remote_account_id="acct-1",
            local_account_id=second.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "shared@example.com"},
                {"remote_file_id": "file-2", "remote_email": "missing@example.com", "remote_account_id": "none"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
                "file-2": {"status_code": 401},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.scan(environment.id)

        assert result["records"][0]["match"]["outcome"] == "conflict"
        assert result["records"][1]["match"]["outcome"] == "missing_local"


def test_recovered_only_reenables_records_previously_disabled_by_system(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        account = crud.create_account(db, email="recover@example.com", email_service="tempmail")
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="seed-recover",
            email="recover@example.com",
            local_account_id=account.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        first = crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="file-1",
            email="recover@example.com",
            payload_json={"source": "seed"},
            remote_account_id=None,
            local_account_id=account.id,
            sync_state="disabled",
            probe_status="unauthorized_401",
            disable_source="system",
            last_probed_at=datetime.utcnow() - timedelta(hours=2),
        )
        second = crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="file-2",
            email="recover@example.com",
            payload_json={"source": "seed"},
            remote_account_id=None,
            sync_state="linked",
            probe_status="unauthorized_401",
            last_probed_at=datetime.utcnow() - timedelta(hours=2),
        )
        assert first.id != second.id
        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "recover@example.com"},
                {"remote_file_id": "file-2", "remote_email": "recover@example.com"},
            ],
            probe_results={
                "file-1": {"status": "ok"},
                "file-2": {"status": "ok"},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.maintain(environment.id)

        assert client.reenable_calls == ["file-1"]
        by_file = {item["remote_file_id"]: item for item in result["records"]}
        assert by_file["file-1"]["decision"] == "reenable"
        assert by_file["file-2"]["decision"] == "log_only"


def test_probe_failed_conflict_and_missing_local_are_log_only(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        first = crud.create_account(db, email="conflict@example.com", email_service="tempmail", account_id="acct-a")
        second = crud.create_account(db, email="other@example.com", email_service="tempmail", account_id="acct-a")
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="seed-conflict-a",
            email="conflict@example.com",
            remote_account_id="acct-a",
            local_account_id=first.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="seed-conflict-b",
            email="other@example.com",
            remote_account_id="acct-a",
            local_account_id=second.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "probe@example.com"},
                {"remote_file_id": "file-2", "remote_email": "conflict@example.com"},
                {"remote_file_id": "file-3", "remote_email": "missing@example.com"},
            ],
            probe_results={
                "file-1": {"status": "error", "error": "timeout"},
                "file-2": {"status_code": 401},
                "file-3": {"status_code": 401},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.maintain(environment.id)

        assert [item["decision"] for item in result["records"]] == ["log_only", "log_only", "log_only"]
        assert client.disable_calls == []
        assert client.reenable_calls == []


def test_matching_is_environment_scoped(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment_one = CLIProxyAPIEnvironment(
            name="primary-scoped",
            base_url="https://cliproxy.example.com",
            target_type="newApi",
            provider="cloudmail",
            provider_scope="alpha",
            target_scope="workspace-a",
        )
        db.add(environment_one)
        db.flush()
        environment_two = CLIProxyAPIEnvironment(
            name="secondary",
            base_url="https://cliproxy-2.example.com",
            target_type="newApi",
            provider="cloudmail",
            provider_scope="beta",
            target_scope="workspace-b",
        )
        db.add(environment_two)
        db.flush()

        account = crud.create_account(
            db,
            email="scoped@example.com",
            email_service="tempmail",
            account_id="acct-scoped",
            platform_source="cloudmail",
            last_upload_target="newApi",
            extra_data={"provider_scope": "beta", "target_scope": "workspace-b"},
        )
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment_two.id,
            remote_file_id="other-env-file",
            email="scoped@example.com",
            remote_account_id="acct-scoped",
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )

        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "scoped@example.com", "remote_account_id": "acct-scoped"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.scan(environment_one.id)

        assert account.email == "scoped@example.com"
        assert result["records"][0]["match"]["outcome"] == "missing_local"


def test_recovered_only_reenables_when_previously_system_disabled(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        crud.create_account(db, email="recover@example.com", email_service="tempmail")
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="file-1",
            email="recover@example.com",
            payload_json={"source": "seed"},
            sync_state="disabled",
            probe_status="unauthorized_401",
            disable_source="manual",
            last_probed_at=datetime.utcnow() - timedelta(hours=2),
        )

        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "recover@example.com"},
            ],
            probe_results={
                "file-1": {"status": "ok"},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.maintain(environment.id)

        assert client.reenable_calls == []
        assert result["records"][0]["decision"] == "log_only"


def test_recovered_non_eligible_cases_remain_log_only(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        crud.create_account(db, email="recover@example.com", email_service="tempmail")
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="file-1",
            email="recover@example.com",
            payload_json={"source": "seed"},
            sync_state="disabled",
            probe_status="unauthorized_401",
            disable_source="manual",
            last_probed_at=datetime.utcnow() - timedelta(hours=2),
        )
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="file-2",
            email="recover@example.com",
            payload_json={"source": "seed"},
            sync_state="linked",
            probe_status="unauthorized_401",
            disable_source="system",
            last_probed_at=datetime.utcnow() - timedelta(hours=2),
        )

        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "recover@example.com"},
                {"remote_file_id": "file-2", "remote_email": "recover@example.com"},
            ],
            probe_results={
                "file-1": {"status": "ok"},
                "file-2": {"status": "ok"},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.maintain(environment.id)

        assert [item["decision"] for item in result["records"]] == ["log_only", "log_only"]
        assert client.reenable_calls == []


def test_recovered_system_disabled_missing_local_remains_log_only(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="file-1",
            email="recover@example.com",
            payload_json={"source": "seed"},
            sync_state="disabled",
            probe_status="unauthorized_401",
            disable_source="system",
            last_probed_at=datetime.utcnow() - timedelta(hours=2),
        )

        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "recover@example.com"},
            ],
            probe_results={
                "file-1": {"status": "ok"},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.maintain(environment.id)

        assert result["records"][0]["match"]["outcome"] == "missing_local"
        assert result["records"][0]["decision"] == "log_only"
        assert client.reenable_calls == []


def test_recovered_system_disabled_conflict_remains_log_only(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        first = crud.create_account(db, email="recover@example.com", email_service="tempmail", account_id="acct-x")
        second = crud.create_account(db, email="other@example.com", email_service="tempmail", account_id="acct-x")
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="seed-1",
            email="recover@example.com",
            remote_account_id="acct-x",
            local_account_id=first.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="seed-2",
            email="other@example.com",
            remote_account_id="acct-x",
            local_account_id=second.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="file-1",
            email="recover@example.com",
            remote_account_id="acct-x",
            local_account_id=first.id,
            payload_json={"source": "seed"},
            sync_state="disabled",
            probe_status="unauthorized_401",
            disable_source="system",
            last_probed_at=datetime.utcnow() - timedelta(hours=2),
        )

        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "recover@example.com", "remote_account_id": "acct-x"},
            ],
            probe_results={
                "file-1": {"status": "ok"},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.maintain(environment.id)

        assert result["records"][0]["match"]["outcome"] == "conflict"
        assert result["records"][0]["decision"] == "log_only"
        assert client.reenable_calls == []


def test_matching_shared_account_id_in_other_environment_does_not_force_conflict(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment_one = CLIProxyAPIEnvironment(
            name="primary-shared-account",
            base_url="https://cliproxy.example.com",
            target_type="newApi",
            provider="cloudmail",
            provider_scope="alpha",
            target_scope="workspace-a",
        )
        db.add(environment_one)
        db.flush()
        environment_two = CLIProxyAPIEnvironment(
            name="secondary-shared-account",
            base_url="https://cliproxy-2.example.com",
            target_type="newApi",
            provider="cloudmail",
            provider_scope="beta",
            target_scope="workspace-b",
        )
        db.add(environment_two)
        db.flush()

        linked_account = crud.create_account(
            db,
            email="linked@example.com",
            email_service="tempmail",
            account_id="acct-shared",
            platform_source="cloudmail",
            last_upload_target="newApi",
            extra_data={"provider_scope": "alpha", "target_scope": "workspace-a"},
        )
        other_env_account = crud.create_account(
            db,
            email="other-env@example.com",
            email_service="tempmail",
            account_id="acct-shared",
            platform_source="cloudmail",
            last_upload_target="newApi",
            extra_data={"provider_scope": "beta", "target_scope": "workspace-b"},
        )
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment_one.id,
            remote_file_id="seed-current-env",
            email="linked@example.com",
            remote_account_id="acct-shared",
            local_account_id=linked_account.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment_two.id,
            remote_file_id="seed-other-env",
            email="other-env@example.com",
            remote_account_id="acct-shared",
            local_account_id=other_env_account.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )

        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "linked@example.com", "remote_account_id": "acct-shared"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.scan(environment_one.id)

        assert result["records"][0]["match"]["outcome"] == "linked"
        assert result["records"][0]["match"]["account_id"] == linked_account.id


def test_maintain_dry_run_does_not_mutate_disable_or_reenable_state(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        disable_account = crud.create_account(db, email="disable@example.com", email_service="tempmail")
        recover_account = crud.create_account(db, email="recover@example.com", email_service="tempmail")
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="seed-disable",
            email="disable@example.com",
            local_account_id=disable_account.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        disable_target = crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="file-disable",
            email="disable@example.com",
            local_account_id=disable_account.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="seed-reenable",
            email="recover@example.com",
            local_account_id=recover_account.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        reenable_target = crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="file-reenable",
            email="recover@example.com",
            local_account_id=recover_account.id,
            payload_json={"seed": True},
            sync_state="disabled",
            probe_status="unauthorized_401",
            disable_source="system",
        )

        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-disable", "remote_email": "disable@example.com"},
                {"remote_file_id": "file-reenable", "remote_email": "recover@example.com"},
            ],
            probe_results={
                "file-disable": {"status_code": 401},
                "file-reenable": {"status": "ok"},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.maintain(environment.id, dry_run=True)

        disable_row = db.get(RemoteAuthInventory, disable_target.id)
        reenable_row = db.get(RemoteAuthInventory, reenable_target.id)
        assert [item["decision"] for item in result["records"]] == ["disable", "reenable"]
        assert disable_row.sync_state == "linked"
        assert disable_row.disable_source is None
        assert reenable_row.sync_state == "disabled"
        assert reenable_row.disable_source == "system"


def test_maintain_client_errors_mark_run_failed(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        account = crud.create_account(db, email="disable@example.com", email_service="tempmail")
        crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="seed-disable",
            email="disable@example.com",
            local_account_id=account.id,
            payload_json={"seed": True},
            sync_state="linked",
            probe_status="unknown",
        )
        client = ExplodingCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "disable@example.com"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
            },
            explode_on="disable_auth",
            exception=RuntimeError("disable failed"),
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        try:
            engine.maintain(environment.id)
        except RuntimeError as exc:
            assert str(exc) == "disable failed"
        else:
            raise AssertionError("expected maintain to raise")

        run = db.query(crud.MaintenanceRun).order_by(crud.MaintenanceRun.id.desc()).first()
        logs = db.query(MaintenanceActionLog).order_by(MaintenanceActionLog.id.asc()).all()
        assert run is not None
        assert run.status == "failed"
        assert run.completed_at is not None
        assert run.error_message == "disable failed"
        assert logs[-1].status == "failed"
        assert logs[-1].action_type == "disable"
        assert logs[-1].remote_file_id == "file-1"


def test_first_seen_remote_record_matches_local_account_without_seeded_inventory(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = CLIProxyAPIEnvironment(
            name="scoped-first-seen",
            base_url="https://cliproxy.example.com",
            target_type="newApi",
            provider="cloudmail",
        )
        db.add(environment)
        db.flush()
        account = crud.create_account(
            db,
            email="first@example.com",
            email_service="tempmail",
            account_id="acct-first",
            platform_source="cloudmail",
            last_upload_target="newApi",
        )

        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "first@example.com", "remote_account_id": "acct-first"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.scan(environment.id)

        assert result["records"][0]["match"]["outcome"] == "linked"
        assert result["records"][0]["match"]["account_id"] == account.id
        assert result["records"][0]["match"]["strategy"] == "remote_email"


def test_scope_rules_include_and_exclude_candidates(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = CLIProxyAPIEnvironment(
            name="scope-rules",
            base_url="https://cliproxy.example.com",
            target_type="newApi",
            provider="cloudmail",
            provider_scope="alpha",
            target_scope="workspace-a",
            scope_rules_json={
                "include": {"email_service": ["tempmail"]},
                "exclude": {"email": ["excluded@example.com"]},
            },
        )
        db.add(environment)
        db.flush()
        included = crud.create_account(
            db,
            email="included@example.com",
            email_service="tempmail",
            account_id="acct-included",
            platform_source="cloudmail",
            last_upload_target="newApi",
            extra_data={"provider_scope": "alpha", "target_scope": "workspace-a"},
        )
        crud.create_account(
            db,
            email="excluded@example.com",
            email_service="tempmail",
            account_id="acct-excluded",
            platform_source="cloudmail",
            last_upload_target="newApi",
            extra_data={"provider_scope": "alpha", "target_scope": "workspace-a"},
        )
        crud.create_account(
            db,
            email="wrong-service@example.com",
            email_service="outlook",
            account_id="acct-service",
            platform_source="cloudmail",
            last_upload_target="newApi",
            extra_data={"provider_scope": "alpha", "target_scope": "workspace-a"},
        )
        crud.create_account(
            db,
            email="wrong-provider@example.com",
            email_service="tempmail",
            account_id="acct-provider",
            platform_source="othermail",
            last_upload_target="newApi",
            extra_data={"provider_scope": "alpha", "target_scope": "workspace-a"},
        )

        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "included@example.com", "remote_account_id": "acct-included"},
                {"remote_file_id": "file-2", "remote_email": "excluded@example.com", "remote_account_id": "acct-excluded"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
                "file-2": {"status_code": 401},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.scan(environment.id)

        by_file = {item["remote_file_id"]: item for item in result["records"]}
        assert by_file["file-1"]["match"]["outcome"] == "linked"
        assert by_file["file-1"]["match"]["account_id"] == included.id
        assert by_file["file-2"]["match"]["outcome"] == "missing_local"


def test_other_environment_data_does_not_interfere_with_current_environment_matching(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        current = CLIProxyAPIEnvironment(
            name="current-env",
            base_url="https://cliproxy.example.com",
            target_type="newApi",
            provider="cloudmail",
            provider_scope="alpha",
            target_scope="workspace-a",
        )
        other = CLIProxyAPIEnvironment(
            name="other-env",
            base_url="https://cliproxy-2.example.com",
            target_type="sub2api",
            provider="cloudmail",
            provider_scope="beta",
            target_scope="workspace-b",
        )
        db.add(current)
        db.add(other)
        db.flush()

        current_account = crud.create_account(
            db,
            email="same@example.com",
            email_service="tempmail",
            account_id="acct-same",
            platform_source="cloudmail",
            last_upload_target="newApi",
            extra_data={"provider_scope": "alpha", "target_scope": "workspace-a"},
        )
        crud.create_account(
            db,
            email="other@example.com",
            email_service="tempmail",
            account_id="acct-same",
            platform_source="cloudmail",
            last_upload_target="sub2api",
            extra_data={"provider_scope": "beta", "target_scope": "workspace-b"},
        )

        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "same@example.com", "remote_account_id": "acct-same"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.scan(current.id)

        assert result["records"][0]["match"]["outcome"] == "linked"
        assert result["records"][0]["match"]["account_id"] == current_account.id


def test_scan_preserves_existing_system_disabled_state(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        account = crud.create_account(db, email="disabled@example.com", email_service="tempmail")
        preserved = crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="file-1",
            email="disabled@example.com",
            local_account_id=account.id,
            payload_json={"seed": True},
            sync_state="disabled",
            probe_status="unauthorized_401",
            disable_source="system",
        )

        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "disabled@example.com"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.scan(environment.id)

        refreshed = db.get(RemoteAuthInventory, preserved.id)
        assert result["records"][0]["match"]["outcome"] == "linked"
        assert refreshed.sync_state == "disabled"
        assert refreshed.disable_source == "system"


def test_maintain_does_not_disable_again_when_already_system_disabled(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        account = crud.create_account(db, email="disabled@example.com", email_service="tempmail")
        existing = crud.upsert_remote_auth_inventory(
            db,
            environment_id=environment.id,
            remote_file_id="file-1",
            email="disabled@example.com",
            local_account_id=account.id,
            payload_json={"seed": True},
            sync_state="disabled",
            probe_status="unauthorized_401",
            disable_source="system",
        )

        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "disabled@example.com"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.maintain(environment.id)

        refreshed = db.get(RemoteAuthInventory, existing.id)
        assert result["records"][0]["decision"] == "log_only"
        assert client.disable_calls == []
        assert refreshed.sync_state == "disabled"
        assert refreshed.disable_source == "system"


def test_scan_executes_existing_queued_run_without_creating_second_row(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        queued = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment.id,
            status="queued",
            summary_json={"current_stage": "queued", "progress_percent": 0, "cancellable": True},
        )
        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "one@example.com", "remote_account_id": "acct-1"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        result = engine.scan(environment.id, run_id=queued.id)

        runs = db.query(crud.MaintenanceRun).order_by(crud.MaintenanceRun.id.asc()).all()
        assert result["run_id"] == queued.id
        assert len(runs) == 1
        assert runs[0].id == queued.id
        assert runs[0].status == "completed"


def test_scan_terminal_update_preserves_progress_fields(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        queued = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment.id,
            status="queued",
            summary_json={"current_stage": "queued", "progress_percent": 0, "cancellable": True},
        )
        client = FakeCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "one@example.com", "remote_account_id": "acct-1"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
            },
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)

        engine.scan(environment.id, run_id=queued.id)

        refreshed = db.get(crud.MaintenanceRun, queued.id)
        assert refreshed is not None
        assert refreshed.summary_json["current_stage"] == "completed"
        assert refreshed.summary_json["progress_percent"] == 100
        assert refreshed.summary_json["cancellable"] is False


def test_running_run_cancel_request_finalizes_as_cancelled_when_polled(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        queued = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment.id,
            status="queued",
            summary_json={"current_stage": "queued", "progress_percent": 0, "cancellable": True},
        )
        client = CancellationAwareCLIProxyAPIClient(
            inventory_records=[
                {"remote_file_id": "file-1", "remote_email": "one@example.com", "remote_account_id": "acct-1"},
                {"remote_file_id": "file-2", "remote_email": "two@example.com", "remote_account_id": "acct-2"},
            ],
            probe_results={
                "file-1": {"status_code": 401},
                "file-2": {"status_code": 401},
            },
            cancel_on_probe_call=1,
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=client)
        client.cancel_callback = lambda: engine.cancel(queued.id)

        result = engine.scan(environment.id, run_id=queued.id)

        refreshed = db.get(crud.MaintenanceRun, queued.id)
        assert result["run_id"] == queued.id
        assert result["status"] == "cancelled"
        assert refreshed is not None
        assert refreshed.status == "cancelled"
        assert refreshed.summary_json["current_stage"] == "cancelled"
        assert refreshed.summary_json["cancellable"] is False


def test_cancelled_run_id_execution_only_allows_queued_runs(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        queued = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment.id,
            status="queued",
            summary_json={"current_stage": "queued", "progress_percent": 0, "cancellable": True},
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=FakeCLIProxyAPIClient())
        engine.cancel(queued.id)

        try:
            engine.scan(environment.id, run_id=queued.id)
        except ValueError as exc:
            assert str(exc) == f"CLIProxy maintenance run {queued.id} is not queued"
        else:
            raise AssertionError("expected queued-only execution guard")


def test_run_id_execution_only_transitions_from_queued_to_running(tmp_path):
    manager = make_db(tmp_path)

    with manager.session_scope() as db:
        environment = make_environment(db)
        running = crud.create_maintenance_run(
            db,
            run_type="scan",
            environment_id=environment.id,
            status="running",
            summary_json={"current_stage": "probing", "progress_percent": 25, "cancellable": True},
        )
        engine = CLIProxyMaintenanceEngine(db=db, client=FakeCLIProxyAPIClient())

        try:
            engine.scan(environment.id, run_id=running.id)
        except ValueError as exc:
            assert str(exc) == f"CLIProxy maintenance run {running.id} is not queued"
        else:
            raise AssertionError("expected queued-only execution guard")
