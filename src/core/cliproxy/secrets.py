"""CLIProxyAPI token secret helpers."""

try:
    from cryptography.fernet import Fernet, InvalidToken
except ModuleNotFoundError as exc:  # pragma: no cover - exercised in deployment/runtime
    Fernet = None  # type: ignore[assignment]
    InvalidToken = ValueError  # type: ignore[assignment]
    _CRYPTO_IMPORT_ERROR = exc
else:
    _CRYPTO_IMPORT_ERROR = None

from ...config.settings import (
    CLIPROXY_ENCRYPTION_KEY_PLACEHOLDER,
    get_cliproxy_encryption_key,
)


DEFAULT_ENCRYPTION_KEY = CLIPROXY_ENCRYPTION_KEY_PLACEHOLDER
UNAVAILABLE_MASK = "[unavailable]"


def _resolve_encryption_key(encryption_key: str | None = None) -> str:
    effective_key = encryption_key if encryption_key is not None else get_cliproxy_encryption_key()
    if not effective_key or effective_key == DEFAULT_ENCRYPTION_KEY:
        raise ValueError("A non-default CLIProxy encryption key is required")
    return effective_key


def _build_fernet(encryption_key: str | None = None) -> Fernet:
    if Fernet is None:
        raise ValueError("cryptography package is required for CLIProxy token encryption") from _CRYPTO_IMPORT_ERROR
    try:
        return Fernet(_resolve_encryption_key(encryption_key).encode("ascii"))
    except (ValueError, TypeError):
        raise
    except Exception as exc:
        raise ValueError("CLIProxy encryption key must be a valid Fernet key") from exc


def encrypt_cliproxy_token(token: str, encryption_key: str | None = None) -> str:
    if not token:
        return ""

    return _build_fernet(encryption_key).encrypt(token.encode("utf-8")).decode("ascii")


def decrypt_cliproxy_token(token_encrypted: str, encryption_key: str | None = None) -> str:
    if not token_encrypted:
        return ""

    try:
        decrypted = _build_fernet(encryption_key).decrypt(token_encrypted.encode("ascii"))
    except (InvalidToken, ValueError, TypeError):
        raise ValueError("Unable to decrypt CLIProxy token")
    return decrypted.decode("utf-8")


def mask_cliproxy_token(token_encrypted: str, encryption_key: str | None = None) -> str:
    if not token_encrypted:
        return ""

    try:
        token = decrypt_cliproxy_token(token_encrypted, encryption_key)
    except ValueError:
        return UNAVAILABLE_MASK

    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}{'*' * (len(token) - 8)}{token[-4:]}"
