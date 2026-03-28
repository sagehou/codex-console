from pathlib import Path

import pytest

from src.core.http_client import HTTPClient, RequestConfig
from src.database.init_db import initialize_database
from src.database.models import Base, EmailService
from src.database.session import DatabaseSessionManager
from src.services.base import EmailServiceError
from src.services.temp_mail import TempMailService


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers = {}

    def json(self):
        if self._payload is None:
            raise ValueError("no json payload")
        return self._payload


class FakeHTTPClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append({
            "method": method,
            "url": url,
            "kwargs": kwargs,
        })
        if not self.responses:
            raise AssertionError(f"未准备响应: {method} {url}")
        return self.responses.pop(0)


class FakeSession:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append({
            "method": method,
            "url": url,
            "kwargs": kwargs,
        })
        if not self.outcomes:
            raise AssertionError(f"未准备响应: {method} {url}")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_tempmail_parses_comma_separated_domains_into_normalized_list():
    service = TempMailService(
        {
            "base_url": "https://mail.example.com",
            "admin_password": "admin-secret",
            "domain": "alpha.example,beta.example,gamma.example",
        }
    )

    assert service.config["domains"] == ["alpha.example", "beta.example", "gamma.example"]
    assert service.config["domain"] == "alpha.example,beta.example,gamma.example"


def test_tempmail_rejects_invalid_domain_entries():
    with pytest.raises(ValueError, match="bad domain"):
        TempMailService(
            {
                "base_url": "https://mail.example.com",
                "admin_password": "admin-secret",
                "domain": "good.example,bad domain",
            }
        )


def test_tempmail_normalizes_at_prefix_spaces_and_duplicates_in_order():
    service = TempMailService(
        {
            "base_url": "https://mail.example.com",
            "admin_password": "admin-secret",
            "domain": " @alpha.example, beta.example , @alpha.example, , beta.example, gamma.example ",
        }
    )

    assert service.config["domains"] == ["alpha.example", "beta.example", "gamma.example"]
    assert service.config["domain"] == "alpha.example,beta.example,gamma.example"


def test_tempmail_rejects_non_ascii_domain_input():
    with pytest.raises(ValueError, match="ex\u00e4mple.com"):
        TempMailService(
            {
                "base_url": "https://mail.example.com",
                "admin_password": "admin-secret",
                "domain": "ex\u00e4mple.com",
            }
        )


def test_tempmail_rejects_empty_domain_list_after_normalization():
    with pytest.raises(ValueError, match="empty"):
        TempMailService(
            {
                "base_url": "https://mail.example.com",
                "admin_password": "admin-secret",
                "domain": " , @ ,  ",
            }
        )


def test_tempmail_reads_legacy_single_domain_as_domains_list():
    service = TempMailService(
        {
            "base_url": "https://mail.example.com",
            "admin_password": "admin-secret",
            "domain": "legacy.example",
        }
    )

    assert service.config["domains"] == ["legacy.example"]
    assert service.config["domain"] == "legacy.example"


def test_tempmail_v1_read_surfaces_return_domains_and_display_domain_alias():
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "temp_mail_domain_backfill.db"
    if db_path.exists():
        db_path.unlink()

    manager = initialize_database(f"sqlite:///{db_path}")

    with manager.session_scope() as session:
        legacy_service = EmailService(
            service_type="temp_mail",
            name="TempMail Legacy",
            config={
                "base_url": "https://mail.example.com",
                "admin_password": "admin-secret",
                "domain": "legacy.example",
            },
            enabled=True,
            priority=0,
        )
        session.add(legacy_service)
        session.commit()
        session.refresh(legacy_service)

        stored_config = legacy_service.config

    assert stored_config["domains"] == ["legacy.example"]
    assert stored_config["domain"] == "legacy.example"


def test_tempmail_tries_domains_in_order_once_and_succeeds_on_later_domain():
    service = TempMailService(
        {
            "base_url": "https://mail.example.com",
            "admin_password": "admin-secret",
            "domain": "alpha.example,beta.example,gamma.example",
        }
    )
    fake_client = FakeHTTPClient(
        [
            FakeResponse(status_code=503, payload={"error": "alpha unavailable"}),
            FakeResponse(
                payload={
                    "address": "tester@beta.example",
                    "jwt": "beta-jwt",
                    "password": "plain-password",
                }
            ),
        ]
    )
    service.http_client = fake_client

    email_info = service.create_email()

    assert email_info["email"] == "tester@beta.example"
    attempted_domains = [call["kwargs"]["json"]["domain"] for call in fake_client.calls]
    assert attempted_domains == ["alpha.example", "beta.example"]
    assert fake_client.responses == []


def test_tempmail_returns_all_domains_exhausted_error_with_attempt_log():
    service = TempMailService(
        {
            "base_url": "https://mail.example.com",
            "admin_password": "admin-secret",
            "domain": "alpha.example,beta.example",
        }
    )
    fake_client = FakeHTTPClient(
        [
            FakeResponse(status_code=503, payload={"error": "alpha unavailable"}),
            FakeResponse(status_code=429, payload={"error": "beta rate limited"}),
        ]
    )
    service.http_client = fake_client

    with pytest.raises(EmailServiceError, match="all domains exhausted") as exc_info:
        service.create_email()

    message = str(exc_info.value)
    assert "alpha.example" in message
    assert "503" in message
    assert "alpha unavailable" in message
    assert "beta.example" in message
    assert "429" in message
    assert "beta rate limited" in message


def test_tempmail_auth_failure_stops_after_first_domain():
    service = TempMailService(
        {
            "base_url": "https://mail.example.com",
            "admin_password": "admin-secret",
            "domain": "alpha.example,beta.example",
        }
    )
    fake_client = FakeHTTPClient(
        [
            FakeResponse(status_code=401, payload={"error": "invalid admin auth"}),
            FakeResponse(
                payload={
                    "address": "tester@beta.example",
                    "jwt": "beta-jwt",
                    "password": "plain-password",
                }
            ),
        ]
    )
    service.http_client = fake_client

    with pytest.raises(EmailServiceError, match="401"):
        service.create_email()

    attempted_domains = [call["kwargs"]["json"]["domain"] for call in fake_client.calls]
    assert attempted_domains == ["alpha.example"]


def test_tempmail_request_transport_failure_stops_after_first_domain():
    service = TempMailService(
        {
            "base_url": "https://mail.example.com",
            "admin_password": "admin-secret",
            "domain": "alpha.example,beta.example",
        }
    )

    class FailingHTTPClient:
        def __init__(self):
            self.calls = []

        def request(self, method, url, **kwargs):
            self.calls.append({
                "method": method,
                "url": url,
                "kwargs": kwargs,
            })
            raise TimeoutError("request timed out")

    failing_client = FailingHTTPClient()
    service.http_client = failing_client

    with pytest.raises(EmailServiceError, match="request timed out"):
        service.create_email()

    attempted_domains = [call["kwargs"]["json"]["domain"] for call in failing_client.calls]
    assert attempted_domains == ["alpha.example"]


def test_tempmail_malformed_create_response_stops_after_first_domain():
    service = TempMailService(
        {
            "base_url": "https://mail.example.com",
            "admin_password": "admin-secret",
            "domain": "alpha.example,beta.example",
        }
    )
    fake_client = FakeHTTPClient(
        [
            FakeResponse(payload={"jwt": "missing-address", "password": "plain-password"}),
            FakeResponse(
                payload={
                    "address": "tester@beta.example",
                    "jwt": "beta-jwt",
                    "password": "plain-password",
                }
            ),
        ]
    )
    service.http_client = fake_client

    with pytest.raises(EmailServiceError, match="API 返回数据不完整"):
        service.create_email()

    attempted_domains = [call["kwargs"]["json"]["domain"] for call in fake_client.calls]
    assert attempted_domains == ["alpha.example"]


def test_tempmail_503_candidate_does_not_hidden_retry_within_same_domain():
    service = TempMailService(
        {
            "base_url": "https://mail.example.com",
            "admin_password": "admin-secret",
            "domain": "alpha.example,beta.example",
        }
    )
    fake_session = FakeSession(
        [
            FakeResponse(status_code=503, payload={"error": "alpha unavailable"}),
            FakeResponse(
                payload={
                    "address": "tester@beta.example",
                    "jwt": "beta-jwt",
                    "password": "plain-password",
                }
            ),
        ]
    )
    service.http_client = HTTPClient(
        proxy_url=None,
        config=RequestConfig(timeout=30, max_retries=3, retry_delay=0),
        session=fake_session,
    )

    email_info = service.create_email()

    assert email_info["email"] == "tester@beta.example"
    attempted_domains = [call["kwargs"]["json"]["domain"] for call in fake_session.calls]
    assert attempted_domains == ["alpha.example", "beta.example"]


def test_tempmail_transport_error_does_not_hidden_retry_within_same_domain():
    service = TempMailService(
        {
            "base_url": "https://mail.example.com",
            "admin_password": "admin-secret",
            "domain": "alpha.example,beta.example",
        }
    )
    fake_session = FakeSession([TimeoutError("request timed out")])
    service.http_client = HTTPClient(
        proxy_url=None,
        config=RequestConfig(timeout=30, max_retries=3, retry_delay=0),
        session=fake_session,
    )

    with pytest.raises(EmailServiceError, match="request timed out"):
        service.create_email()

    attempted_domains = [call["kwargs"]["json"]["domain"] for call in fake_session.calls]
    assert attempted_domains == ["alpha.example"]
