"""Matching helpers for CLIProxy remote inventory."""

from __future__ import annotations

from typing import Any, Dict, List

from sqlalchemy.orm import Session

from ...database.models import Account, CLIProxyAPIEnvironment


def _shared_account_id_conflict(scoped_accounts: List[Account], account: Account) -> List[int]:
    if not account.account_id:
        return []
    matches = [candidate for candidate in scoped_accounts if candidate.account_id == account.account_id]
    if len(matches) > 1:
        return [matched.id for matched in matches]
    return []


def _extra_scope_value(account: Account, key: str) -> Any:
    extra_data = account.extra_data or {}
    return extra_data.get(key)


def _rule_matches(account: Account, field: str, expected: Any) -> bool:
    if hasattr(account, field):
        actual = getattr(account, field)
    else:
        actual = _extra_scope_value(account, field)

    if isinstance(expected, list):
        return actual in expected
    return actual == expected


def _matches_optional_constraint(actual: Any, expected: Any) -> bool:
    if expected in (None, ""):
        return True
    if actual in (None, ""):
        return True
    return actual == expected


def _apply_scope_rules(accounts: List[Account], environment: CLIProxyAPIEnvironment) -> List[Account]:
    scoped = list(accounts)

    if environment.provider:
        scoped = [
            account for account in scoped if _matches_optional_constraint(account.platform_source, environment.provider)
        ]
    if environment.target_type:
        scoped = [
            account for account in scoped if _matches_optional_constraint(account.last_upload_target, environment.target_type)
        ]
    if environment.provider_scope:
        scoped = [
            account
            for account in scoped
            if _matches_optional_constraint(_extra_scope_value(account, "provider_scope"), environment.provider_scope)
        ]
    if environment.target_scope:
        scoped = [
            account
            for account in scoped
            if _matches_optional_constraint(_extra_scope_value(account, "target_scope"), environment.target_scope)
        ]

    rules = environment.scope_rules_json or {}
    include_rules = rules.get("include") or {}
    exclude_rules = rules.get("exclude") or {}

    for field, expected in include_rules.items():
        scoped = [account for account in scoped if _rule_matches(account, field, expected)]

    for field, expected in exclude_rules.items():
        scoped = [account for account in scoped if not _rule_matches(account, field, expected)]

    return sorted(scoped, key=lambda account: account.id)


def match_remote_record(db: Session, environment_id: int, remote_record: Dict[str, Any]) -> Dict[str, Any]:
    remote_email = remote_record.get("remote_email") or remote_record.get("email")
    remote_account_id = remote_record.get("remote_account_id")

    environment = db.query(CLIProxyAPIEnvironment).filter(CLIProxyAPIEnvironment.id == environment_id).first()
    if environment is None:
        raise ValueError(f"CLIProxy environment {environment_id} not found")

    scoped_accounts = _apply_scope_rules(db.query(Account).order_by(Account.id.asc()).all(), environment)

    if remote_email:
        matches = [account for account in scoped_accounts if account.email == remote_email]
        if len(matches) == 1:
            conflicting_ids = _shared_account_id_conflict(scoped_accounts, matches[0])
            if conflicting_ids:
                return {
                    "outcome": "conflict",
                    "strategy": "remote_email",
                    "account_id": None,
                    "matched_ids": conflicting_ids,
                }
            return {
                "outcome": "linked",
                "strategy": "remote_email",
                "account_id": matches[0].id,
                "matched_ids": [matches[0].id],
            }
        if len(matches) > 1:
            return {
                "outcome": "conflict",
                "strategy": "remote_email",
                "account_id": None,
                "matched_ids": [account.id for account in matches],
            }

    if remote_account_id:
        matches = [account for account in scoped_accounts if account.account_id == remote_account_id]
        if len(matches) == 1:
            conflicting_ids = _shared_account_id_conflict(scoped_accounts, matches[0])
            if conflicting_ids:
                return {
                    "outcome": "conflict",
                    "strategy": "remote_account_id",
                    "account_id": None,
                    "matched_ids": conflicting_ids,
                }
            return {
                "outcome": "linked",
                "strategy": "remote_account_id",
                "account_id": matches[0].id,
                "matched_ids": [matches[0].id],
            }
        if len(matches) > 1:
            return {
                "outcome": "conflict",
                "strategy": "remote_account_id",
                "account_id": None,
                "matched_ids": [account.id for account in matches],
            }

    return {
        "outcome": "missing_local",
        "strategy": "remote_email" if remote_email else "remote_account_id",
        "account_id": None,
        "matched_ids": [],
    }
