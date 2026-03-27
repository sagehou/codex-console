from pathlib import Path
import os

import pytest
from cryptography.fernet import Fernet

from src.core.cliproxy import secrets as cliproxy_secrets
from src.core.cliproxy.secrets import decrypt_cliproxy_token, encrypt_cliproxy_token
from src.config import settings as settings_module
from src.config.settings import CLIPROXY_ENCRYPTION_KEY_PLACEHOLDER
from src.database.models import Base, CLIProxyAPIEnvironment
from src.database.session import DatabaseSessionManager


def make_fernet_key() -> str:
    return Fernet.generate_key().decode("ascii")


def test_cliproxy_token_is_not_stored_in_plaintext(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "cliproxy_security_storage.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    token = "cliproxy-secret-token"
    encryption_key = make_fernet_key()
    monkeypatch.setenv("CLIPROXY_ENCRYPTION_KEY", encryption_key)

    with manager.session_scope() as session:
        environment = CLIProxyAPIEnvironment(
            name="primary",
            base_url="https://cliproxy.example.com",
            target_type="cpa",
            provider="cloudmail",
        )
        environment.set_encrypted_token(encrypt_cliproxy_token(token, encryption_key))
        session.add(environment)
        session.flush()
        environment_id = environment.id

    with manager.session_scope() as session:
        reloaded = session.get(CLIProxyAPIEnvironment, environment_id)
        assert reloaded.token_encrypted != token
        assert decrypt_cliproxy_token(reloaded.token_encrypted, encryption_key) == token


def test_environment_responses_never_return_full_token(monkeypatch):
    token = "cliproxy-secret-token"
    encryption_key = make_fernet_key()
    monkeypatch.setenv("CLIPROXY_ENCRYPTION_KEY", encryption_key)
    environment = CLIProxyAPIEnvironment(
        id=7,
        name="primary",
        base_url="https://cliproxy.example.com",
        target_type="cpa",
        provider="cloudmail",
    )
    environment.set_encrypted_token(encrypt_cliproxy_token(token, encryption_key))

    list_payload = environment.to_summary_dict()
    detail_payload = environment.to_detail_dict()

    assert list_payload["has_token"] is True
    assert detail_payload["has_token"] is True
    assert token not in str(list_payload)
    assert token not in str(detail_payload)
    assert "token" not in list_payload
    assert "token" not in detail_payload
    assert detail_payload["token_masked"].startswith("clip")
    assert detail_payload["token_masked"].endswith("oken")


def test_cliproxy_secret_helpers_reject_default_missing_or_non_fernet_encryption_key():
    with pytest.raises(ValueError):
        encrypt_cliproxy_token("cliproxy-secret-token", "")

    with pytest.raises(ValueError):
        encrypt_cliproxy_token(
            "cliproxy-secret-token",
            CLIPROXY_ENCRYPTION_KEY_PLACEHOLDER,
        )

    with pytest.raises(ValueError):
        encrypt_cliproxy_token("cliproxy-secret-token", "not-a-fernet-key")


def test_cliproxy_secret_helpers_share_placeholder_constant_with_settings():
    assert cliproxy_secrets.DEFAULT_ENCRYPTION_KEY == CLIPROXY_ENCRYPTION_KEY_PLACEHOLDER


def test_cliproxy_encryption_key_loads_only_from_environment(monkeypatch):
    env_key = make_fernet_key()
    monkeypatch.setenv("CLIPROXY_ENCRYPTION_KEY", env_key)
    monkeypatch.setattr(settings_module, "get_settings", lambda: (_ for _ in ()).throw(AssertionError("db settings should not be consulted")))

    assert settings_module.get_cliproxy_encryption_key() == env_key


def test_cliproxy_encryption_key_is_not_exposed_in_settings_schema():
    assert "encryption_key" not in settings_module.SETTING_DEFINITIONS
    assert "encryption_key" not in settings_module.Settings.model_fields


def test_cliproxy_decrypt_rejects_tampered_ciphertext():
    encryption_key = make_fernet_key()
    ciphertext = encrypt_cliproxy_token("cliproxy-secret-token", encryption_key)
    tampered = ciphertext[:-2] + ("AA" if ciphertext[-2:] != "AA" else "BB")

    with pytest.raises(ValueError):
        decrypt_cliproxy_token(tampered, encryption_key)


def test_environment_detail_serialization_handles_undecryptable_ciphertext_safely(monkeypatch):
    monkeypatch.setenv("CLIPROXY_ENCRYPTION_KEY", make_fernet_key())
    environment = CLIProxyAPIEnvironment(
        id=7,
        name="primary",
        base_url="https://cliproxy.example.com",
        target_type="cpa",
        provider="cloudmail",
    )
    environment._token_encrypted = "not-valid-ciphertext"

    detail_payload = environment.to_detail_dict()

    assert detail_payload["has_token"] is True
    assert detail_payload["token_masked"] == "[unavailable]"
    assert "not-valid-ciphertext" not in str(detail_payload)
    assert "token" not in detail_payload


def test_environment_set_token_clears_state_for_empty_input(monkeypatch):
    encryption_key = make_fernet_key()
    monkeypatch.setenv("CLIPROXY_ENCRYPTION_KEY", encryption_key)

    environment = CLIProxyAPIEnvironment(
        name="primary",
        base_url="https://cliproxy.example.com",
        target_type="cpa",
        provider="cloudmail",
    )

    environment.set_token("cliproxy-secret-token")
    environment.set_token("")

    assert environment.token_encrypted == ""
    assert environment.has_token is False
    assert environment.get_token() == ""


def test_direct_plaintext_assignment_is_not_persisted_as_plaintext(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "cliproxy_security_direct_assignment.db"
    if db_path.exists():
        db_path.unlink()

    encryption_key = make_fernet_key()
    monkeypatch.setenv("CLIPROXY_ENCRYPTION_KEY", encryption_key)

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    plaintext_token = "cliproxy-secret-token"

    with manager.session_scope() as session:
        environment = CLIProxyAPIEnvironment(
            name="direct-assignment",
            base_url="https://cliproxy.example.com",
            target_type="cpa",
            provider="cloudmail",
        )
        environment.token_encrypted = plaintext_token
        session.add(environment)
        session.flush()
        environment_id = environment.id

    with manager.session_scope() as session:
        reloaded = session.get(CLIProxyAPIEnvironment, environment_id)
        assert reloaded.token_encrypted != plaintext_token
        assert reloaded.get_token() == plaintext_token


def test_direct_plaintext_assignment_with_url_or_spaces_is_not_persisted_as_plaintext(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "cliproxy_security_direct_assignment_url_like.db"
    if db_path.exists():
        db_path.unlink()

    encryption_key = make_fernet_key()
    monkeypatch.setenv("CLIPROXY_ENCRYPTION_KEY", encryption_key)

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    plaintext_token = "Bearer https://cliproxy.example.com token value"

    with manager.session_scope() as session:
        environment = CLIProxyAPIEnvironment(
            name="direct-assignment-url-like",
            base_url="https://cliproxy.example.com",
            target_type="cpa",
            provider="cloudmail",
        )
        environment.token_encrypted = plaintext_token
        session.add(environment)
        session.flush()
        environment_id = environment.id

    with manager.session_scope() as session:
        reloaded = session.get(CLIProxyAPIEnvironment, environment_id)
        assert reloaded.token_encrypted != plaintext_token
        assert reloaded.get_token() == plaintext_token


def test_direct_plaintext_assignment_that_looks_like_fernet_is_not_persisted_as_plaintext(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "cliproxy_security_direct_assignment_fernet_like.db"
    if db_path.exists():
        db_path.unlink()

    encryption_key = make_fernet_key()
    monkeypatch.setenv("CLIPROXY_ENCRYPTION_KEY", encryption_key)

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    plaintext_token = "gAAAAA-looks-like-fernet-but-is-plaintext"

    with manager.session_scope() as session:
        environment = CLIProxyAPIEnvironment(
            name="direct-assignment-fernet-like",
            base_url="https://cliproxy.example.com",
            target_type="cpa",
            provider="cloudmail",
        )
        environment.token_encrypted = plaintext_token
        session.add(environment)
        session.flush()
        environment_id = environment.id

    with manager.session_scope() as session:
        reloaded = session.get(CLIProxyAPIEnvironment, environment_id)
        assert reloaded.token_encrypted != plaintext_token
        assert reloaded.get_token() == plaintext_token


def test_set_encrypted_token_rejects_plaintext_input(monkeypatch):
    encryption_key = make_fernet_key()
    monkeypatch.setenv("CLIPROXY_ENCRYPTION_KEY", encryption_key)

    environment = CLIProxyAPIEnvironment(
        name="reject-plaintext-ciphertext-setter",
        base_url="https://cliproxy.example.com",
        target_type="cpa",
        provider="cloudmail",
    )

    with pytest.raises(ValueError):
        environment.set_encrypted_token("plain-token")

    assert environment.token_encrypted == ""
    assert environment.has_token is False
