"""CLIProxy maintenance orchestration."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from ...database import crud
from ...database.models import CLIProxyAPIEnvironment, RemoteAuthInventory
from .client import CLIProxyAPIClient
from .matching import match_remote_record


def _classify_probe(probe_result: Dict[str, Any]) -> str:
    status_code = probe_result.get("status_code")
    status = probe_result.get("status")
    if status_code == 401:
        return "unauthorized_401"
    if probe_result.get("quota_limited") is True:
        return "quota_limited"
    if status in {"ok", "healthy", "success"}:
        return "recovered"
    return "probe_failed"


def _safe_error_summary(exc: Exception) -> str:
    message = str(exc).strip()
    return message or exc.__class__.__name__


class CLIProxyMaintenanceEngine:
    def __init__(self, db: Session, client: Optional[CLIProxyAPIClient] = None):
        self.db = db
        self.client = client

    def _get_environment(self, environment_id: int) -> CLIProxyAPIEnvironment:
        environment = crud.get_cliproxy_environment_by_id(self.db, environment_id)
        if environment is None:
            raise ValueError(f"CLIProxy environment {environment_id} not found")
        return environment

    def _resolve_client(self, environment: CLIProxyAPIEnvironment) -> CLIProxyAPIClient:
        if self.client is not None:
            return self.client
        return CLIProxyAPIClient(base_url=environment.base_url, token=environment.get_token())

    def _sync_state_for_match(self, match: Dict[str, Any]) -> str:
        outcome = match["outcome"]
        if outcome == "linked":
            return "linked"
        return outcome

    def _record_scan_row(
        self,
        environment_id: int,
        remote_record: Dict[str, Any],
        match: Dict[str, Any],
        classification: str,
        probed_at: datetime,
        sync_state: Optional[str] = None,
        disable_source: Optional[str] = None,
    ) -> RemoteAuthInventory:
        return crud.upsert_remote_auth_inventory(
            self.db,
            environment_id=environment_id,
            remote_file_id=remote_record["remote_file_id"],
            email=remote_record.get("remote_email") or remote_record.get("email"),
            remote_account_id=remote_record.get("remote_account_id"),
            local_account_id=match.get("account_id"),
            payload_json=remote_record,
            last_seen_at=probed_at,
            last_probed_at=probed_at,
            sync_state=sync_state if sync_state is not None else self._sync_state_for_match(match),
            probe_status=classification,
            disable_source=disable_source,
        )

    def _get_run(self, run_id: int, run_type: str, environment_id: int):
        run = crud.get_maintenance_run_by_id(self.db, run_id)
        if run is None:
            raise ValueError(f"CLIProxy maintenance run {run_id} not found")
        if run.run_type != run_type or run.environment_id != environment_id:
            raise ValueError(f"CLIProxy maintenance run {run_id} does not match requested execution")
        return run

    def _start_queued_run(self, run_id: int, run_type: str, environment_id: int):
        run = self._get_run(run_id, run_type, environment_id)
        if run.status != "queued":
            raise ValueError(f"CLIProxy maintenance run {run_id} is not queued")
        updated = self._update_run_progress(run.id, stage="running", progress_percent=1, cancellable=True, status="running")
        assert updated is not None
        return updated

    def _update_run_progress(self, run_id: int, stage: str, progress_percent: int, cancellable: bool, **kwargs):
        return crud.update_maintenance_run(
            self.db,
            run_id,
            current_stage=stage,
            progress_percent=progress_percent,
            cancellable=cancellable,
            **kwargs,
        )

    def _finalize_cancelled(self, run_id: int):
        updated = self._update_run_progress(
            run_id,
            stage="cancelled",
            progress_percent=100,
            cancellable=False,
            status="cancelled",
            completed_at=datetime.utcnow(),
        )
        assert updated is not None
        return updated

    def _poll_cancelled(self, run_id: int) -> bool:
        run = crud.get_maintenance_run_by_id(self.db, run_id)
        if run is None:
            raise ValueError(f"CLIProxy maintenance run {run_id} not found")
        if run.status != "cancelling":
            return False
        self._finalize_cancelled(run_id)
        return True

    def _mark_run_failed(
        self,
        run_id: int,
        error: Exception,
        action_type: str,
        environment_id: int,
        remote_file_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        error_message = _safe_error_summary(error)
        crud.update_maintenance_run(
            self.db,
            run_id,
            status="failed",
            completed_at=datetime.utcnow(),
            error_message=error_message,
            current_stage="failed",
            progress_percent=100,
            cancellable=False,
            summary_json={"error": error_message},
        )
        crud.create_maintenance_action_log(
            self.db,
            run_id=run_id,
            environment_id=environment_id,
            action_type=action_type,
            status="failed",
            remote_file_id=remote_file_id,
            message=error_message,
            details_json=details or {"error": error_message},
        )

    def cancel(self, run_id: int) -> Dict[str, Any]:
        run = crud.get_maintenance_run_by_id(self.db, run_id)
        if run is None:
            raise ValueError(f"CLIProxy maintenance run {run_id} not found")

        summary = dict(run.summary_json or {})
        if not summary.get("cancellable"):
            raise ValueError(f"CLIProxy maintenance run {run_id} is not cancellable")

        if run.status == "queued":
            updated = crud.update_maintenance_run(
                self.db,
                run_id,
                status="cancelled",
                completed_at=datetime.utcnow(),
                current_stage="cancelled",
                progress_percent=summary.get("progress_percent", 0),
                cancellable=False,
            )
        elif run.status == "running":
            updated = crud.update_maintenance_run(
                self.db,
                run_id,
                status="cancelling",
                current_stage="cancelling",
                progress_percent=summary.get("progress_percent", 0),
                cancellable=True,
            )
        else:
            raise ValueError(f"CLIProxy maintenance run {run_id} is not cancellable")
        assert updated is not None
        return {"run_id": updated.id, "status": updated.status}

    def scan(self, environment_id: int, run_id: Optional[int] = None) -> Dict[str, Any]:
        environment = self._get_environment(environment_id)
        client = self._resolve_client(environment)
        if run_id is None:
            run = crud.create_maintenance_run(
                self.db,
                run_type="scan",
                environment_id=environment_id,
                status="running",
                summary_json={"current_stage": "running", "progress_percent": 0, "cancellable": True},
            )
        else:
            run = self._start_queued_run(run_id, "scan", environment_id)

        records: List[Dict[str, Any]] = []
        run_failed = False
        now = datetime.utcnow()
        try:
            inventory_records = list(client.fetch_inventory())
            total_records = max(len(inventory_records), 1)
            for index, remote_record in enumerate(inventory_records, start=1):
                if self._poll_cancelled(run.id):
                    return {"run_id": run.id, "records": records, "status": "cancelled"}
                try:
                    existing = self.db.query(RemoteAuthInventory).filter(
                        RemoteAuthInventory.environment_id == environment_id,
                        RemoteAuthInventory.remote_file_id == remote_record["remote_file_id"],
                    ).first()
                    probe_result = client.probe_usage(remote_record["remote_file_id"])
                    match = match_remote_record(self.db, environment_id, remote_record)
                    classification = _classify_probe(probe_result)
                    persisted_sync_state = self._sync_state_for_match(match)
                    persisted_disable_source = existing.disable_source if existing is not None else None
                    if (
                        existing is not None
                        and existing.sync_state == "disabled"
                        and existing.disable_source == "system"
                    ):
                        persisted_sync_state = "disabled"
                        persisted_disable_source = "system"
                    persisted = self._record_scan_row(
                        environment_id,
                        remote_record,
                        match,
                        classification,
                        now,
                        sync_state=persisted_sync_state,
                        disable_source=persisted_disable_source,
                    )
                    records.append(
                        {
                            "remote_file_id": remote_record["remote_file_id"],
                            "classification": classification,
                            "match": match,
                            "inventory_id": persisted.id,
                        }
                    )
                    crud.create_maintenance_action_log(
                        self.db,
                        run_id=run.id,
                        environment_id=environment_id,
                        action_type="scan_record",
                        status="logged",
                        remote_file_id=remote_record["remote_file_id"],
                        message=f"scan classified as {classification}",
                        details_json={"classification": classification, "match": match},
                    )
                    progress = min(99, int(index * 100 / total_records))
                    self._update_run_progress(run.id, stage="probing", progress_percent=progress, cancellable=True)
                except Exception as exc:
                    self._mark_run_failed(
                        run.id,
                        exc,
                        action_type="scan_record",
                        environment_id=environment_id,
                        remote_file_id=remote_record.get("remote_file_id"),
                    )
                    run_failed = True
                    raise

            if self._poll_cancelled(run.id):
                return {"run_id": run.id, "records": records, "status": "cancelled"}
            crud.update_cliproxy_environment(self.db, environment_id, last_scanned_at=now)
            crud.update_maintenance_run(
                self.db,
                run.id,
                status="completed",
                completed_at=datetime.utcnow(),
                current_stage="completed",
                progress_percent=100,
                cancellable=False,
                summary_json={"records": len(records)},
            )
            return {"run_id": run.id, "records": records}
        except Exception as exc:
            if not run_failed:
                self._mark_run_failed(
                    run.id,
                    exc,
                    action_type="scan",
                    environment_id=environment_id,
                )
            raise

    def _decide_action(self, classification: str, match: Dict[str, Any], existing: Optional[RemoteAuthInventory]) -> str:
        outcome = match["outcome"]
        if (
            classification == "unauthorized_401"
            and outcome == "linked"
            and not (
                existing is not None
                and existing.sync_state == "disabled"
                and existing.disable_source == "system"
            )
        ):
            return "disable"
        if (
            classification == "recovered"
            and outcome == "linked"
            and existing is not None
            and existing.sync_state == "disabled"
            and existing.disable_source == "system"
        ):
            return "reenable"
        if classification == "probe_failed":
            return "log_only"
        if outcome in {"conflict", "missing_local"}:
            return "log_only"
        if classification in {"quota_limited", "recovered"}:
            return "log_only"
        return "log_only"

    def maintain(self, environment_id: int, dry_run: bool = False, run_id: Optional[int] = None) -> Dict[str, Any]:
        environment = self._get_environment(environment_id)
        client = self._resolve_client(environment)
        if run_id is None:
            run = crud.create_maintenance_run(
                self.db,
                run_type="maintain",
                environment_id=environment_id,
                status="running",
                summary_json={"dry_run": dry_run, "current_stage": "running", "progress_percent": 0, "cancellable": True},
            )
        else:
            run = self._start_queued_run(run_id, "maintain", environment_id)

        records: List[Dict[str, Any]] = []
        run_failed = False
        now = datetime.utcnow()
        try:
            inventory_records = list(client.fetch_inventory())
            total_records = max(len(inventory_records), 1)
            for index, remote_record in enumerate(inventory_records, start=1):
                if self._poll_cancelled(run.id):
                    return {"run_id": run.id, "records": records, "dry_run": dry_run, "status": "cancelled"}
                try:
                    decision = None
                    existing = self.db.query(RemoteAuthInventory).filter(
                        RemoteAuthInventory.environment_id == environment_id,
                        RemoteAuthInventory.remote_file_id == remote_record["remote_file_id"],
                    ).first()
                    probe_result = client.probe_usage(remote_record["remote_file_id"])
                    match = match_remote_record(self.db, environment_id, remote_record)
                    classification = _classify_probe(probe_result)
                    decision = self._decide_action(classification, match, existing)

                    persisted_disable_source = existing.disable_source if existing is not None else None
                    persisted_sync_state = existing.sync_state if existing is not None else self._sync_state_for_match(match)
                    if not dry_run:
                        if decision == "disable":
                            persisted_disable_source = "system"
                        elif decision == "reenable":
                            persisted_disable_source = None
                    elif decision not in {"disable", "reenable"}:
                        persisted_sync_state = self._sync_state_for_match(match)

                    persisted = self._record_scan_row(
                        environment_id,
                        remote_record,
                        dict(match),
                        classification,
                        now,
                        sync_state=persisted_sync_state,
                        disable_source=persisted_disable_source,
                    )

                    if not dry_run:
                        if decision == "disable":
                            client.disable_auth(remote_record["remote_file_id"])
                            persisted.sync_state = "disabled"
                            persisted.disable_source = "system"
                            log_status = "completed"
                        elif decision == "reenable":
                            client.reenable_auth(remote_record["remote_file_id"])
                            persisted.sync_state = "linked"
                            persisted.disable_source = None
                            log_status = "completed"
                        else:
                            log_status = "logged"
                    else:
                        log_status = "dry_run" if decision in {"disable", "reenable"} else "logged"

                    self.db.flush()
                    crud.create_maintenance_action_log(
                        self.db,
                        run_id=run.id,
                        environment_id=environment_id,
                        action_type=decision,
                        status=log_status,
                        remote_file_id=remote_record["remote_file_id"],
                        message=f"maintenance decision: {decision}",
                        details_json={
                            "classification": classification,
                            "match": match,
                            "dry_run": dry_run,
                        },
                    )
                    progress = min(99, int(index * 100 / total_records))
                    self._update_run_progress(run.id, stage="executing", progress_percent=progress, cancellable=True)

                    records.append(
                        {
                            "remote_file_id": remote_record["remote_file_id"],
                            "classification": classification,
                            "match": match,
                            "decision": decision,
                            "dry_run": dry_run,
                        }
                    )
                except Exception as exc:
                    self._mark_run_failed(
                        run.id,
                        exc,
                        action_type=decision or "maintain_record",
                        environment_id=environment_id,
                        remote_file_id=remote_record.get("remote_file_id"),
                    )
                    run_failed = True
                    raise

            if self._poll_cancelled(run.id):
                return {"run_id": run.id, "records": records, "dry_run": dry_run, "status": "cancelled"}
            crud.update_cliproxy_environment(self.db, environment_id, last_maintained_at=now)
            crud.update_maintenance_run(
                self.db,
                run.id,
                status="completed",
                completed_at=datetime.utcnow(),
                current_stage="completed",
                progress_percent=100,
                cancellable=False,
                summary_json={"records": len(records), "dry_run": dry_run},
            )
            return {"run_id": run.id, "records": records, "dry_run": dry_run}
        except Exception as exc:
            if not run_failed:
                self._mark_run_failed(
                    run.id,
                    exc,
                    action_type="maintain",
                    environment_id=environment_id,
                )
            raise
