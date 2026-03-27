from src.core.utils import calculate_sha256
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


def test_create_email_uses_admin_new_address_and_optional_site_password():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "site_password": "site-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "address": "tester@example.com",
                "jwt": "address-jwt",
                "password": "plain-password",
            }
        )
    ])
    service.http_client = fake_client

    email_info = service.create_email()

    assert email_info["email"] == "tester@example.com"
    assert email_info["jwt"] == "address-jwt"
    assert email_info["password"] == "plain-password"

    create_call = fake_client.calls[0]
    assert create_call["method"] == "POST"
    assert create_call["url"] == "https://mail.example.com/admin/new_address"
    assert create_call["kwargs"]["json"]["domain"] == "example.com"
    assert create_call["kwargs"]["headers"]["x-admin-auth"] == "admin-secret"
    assert create_call["kwargs"]["headers"]["x-custom-auth"] == "site-secret"


def test_get_verification_code_reauths_with_address_password_after_401():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "site_password": "site-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(status_code=401, text="token expired"),
        FakeResponse(payload={"jwt": "fresh-jwt", "address": "tester@example.com"}),
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": 1,
                        "source": "OpenAI <noreply@openai.com>",
                        "subject": "Your verification code",
                        "raw": "Subject: Your verification code\n\nYour OpenAI verification code is 654321",
                    }
                ]
            }
        ),
    ])
    service.http_client = fake_client
    service._email_cache["tester@example.com"] = {
        "email": "tester@example.com",
        "jwt": "stale-jwt",
        "password": "plain-password",
    }

    code = service.get_verification_code("tester@example.com", timeout=1)

    assert code == "654321"

    first_mail_call = fake_client.calls[0]
    assert first_mail_call["url"] == "https://mail.example.com/api/mails"
    assert first_mail_call["kwargs"]["headers"]["Authorization"] == "Bearer stale-jwt"
    assert first_mail_call["kwargs"]["headers"]["x-custom-auth"] == "site-secret"

    login_call = fake_client.calls[1]
    assert login_call["url"] == "https://mail.example.com/api/address_login"
    assert login_call["kwargs"]["json"] == {
        "email": "tester@example.com",
        "password": calculate_sha256("plain-password"),
    }
    assert login_call["kwargs"]["headers"]["x-custom-auth"] == "site-secret"

    second_mail_call = fake_client.calls[2]
    assert second_mail_call["kwargs"]["headers"]["Authorization"] == "Bearer fresh-jwt"
    assert second_mail_call["kwargs"]["headers"]["x-custom-auth"] == "site-secret"


def test_get_verification_code_reauths_after_401_without_matching_error_text():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "site_password": "site-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(payload={"jwt": "fresh-jwt", "address": "tester@example.com"}),
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": 1,
                        "source": "OpenAI <noreply@openai.com>",
                        "subject": "Your verification code",
                        "raw": "Subject: Your verification code\n\nYour OpenAI verification code is 123456",
                    }
                ]
            }
        ),
    ])
    service.http_client = fake_client
    service._email_cache["tester@example.com"] = {
        "email": "tester@example.com",
        "jwt": "stale-jwt",
        "password": "plain-password",
    }

    original_make_request = service._make_request
    state = {"raised": False}

    def fake_make_request(method, path, **kwargs):
        if path == "/api/mails" and not state["raised"]:
            state["raised"] = True
            from src.services.base import EmailServiceError

            error = EmailServiceError("expired bearer token")
            error.status_code = 401
            raise error
        return original_make_request(method, path, **kwargs)

    service._make_request = fake_make_request

    code = service.get_verification_code("tester@example.com", timeout=1)

    assert code == "123456"
    assert fake_client.calls[0]["url"] == "https://mail.example.com/api/address_login"
    assert fake_client.calls[1]["kwargs"]["headers"]["Authorization"] == "Bearer fresh-jwt"


def test_get_verification_code_skips_previously_used_code_until_new_code_arrives():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": 1,
                        "source": "OpenAI <noreply@openai.com>",
                        "subject": "Your verification code",
                        "raw": "Subject: Your verification code\n\nYour OpenAI verification code is 111111",
                    }
                ]
            }
        ),
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": 1,
                        "source": "OpenAI <noreply@openai.com>",
                        "subject": "Your verification code",
                        "raw": "Subject: Your verification code\n\nYour OpenAI verification code is 111111",
                    },
                    {
                        "id": 2,
                        "source": "OpenAI <noreply@openai.com>",
                        "subject": "Your verification code",
                        "raw": "Subject: Your verification code\n\nYour OpenAI verification code is 222222",
                    }
                ]
            }
        ),
    ])
    service.http_client = fake_client
    service._email_cache["tester@example.com"] = {
        "email": "tester@example.com",
        "jwt": "fresh-jwt",
        "password": "plain-password",
    }

    first_code = service.get_verification_code("tester@example.com", timeout=1)
    second_code = service.get_verification_code("tester@example.com", timeout=4)

    assert first_code == "111111"
    assert second_code == "222222"


def test_get_verification_code_prefers_semantic_code_over_earlier_unrelated_digits():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": 1,
                        "source": "OpenAI <noreply@openai.com>",
                        "subject": "OpenAI verification",
                        "raw": "Subject: OpenAI verification\n\nReference 036964\nYour verification code is 996777\n",
                    }
                ]
            }
        ),
    ])
    service.http_client = fake_client
    service._email_cache["tester@example.com"] = {
        "email": "tester@example.com",
        "jwt": "fresh-jwt",
        "password": "plain-password",
    }

    code = service.get_verification_code("tester@example.com", timeout=1)

    assert code == "996777"


def test_get_verification_code_matches_code_on_next_line_after_instruction_text():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": 1,
                        "source": "OpenAI <noreply@openai.com>",
                        "subject": "OpenAI verification",
                        "raw": "Subject: OpenAI verification\n\nReference 036964\nEnter this temporary verification code to continue:\n\n996777\n",
                    }
                ]
            }
        ),
    ])
    service.http_client = fake_client
    service._email_cache["tester@example.com"] = {
        "email": "tester@example.com",
        "jwt": "fresh-jwt",
        "password": "plain-password",
    }

    code = service.get_verification_code("tester@example.com", timeout=1)

    assert code == "996777"


def test_get_verification_code_ignores_date_like_digits_before_real_signup_code():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": 1,
                        "source": "OpenAI <noreply@openai.com>",
                        "subject": "OpenAI verification",
                        "raw": "Date: 202603\n\nYour ChatGPT code is 202683\nEnter this temporary verification code to continue:\n202683\n",
                    }
                ]
            }
        ),
    ])
    service.http_client = fake_client
    service._email_cache["tester@example.com"] = {
        "email": "tester@example.com",
        "jwt": "fresh-jwt",
        "password": "plain-password",
    }

    code = service.get_verification_code("tester@example.com", timeout=1)

    assert code == "202683"


def test_get_verification_code_matches_inline_login_code_format():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": 1,
                        "source": "OpenAI <noreply@openai.com>",
                        "subject": "ChatGPT Log-in Code",
                        "raw": "Date: 202603\n\nYour ChatGPT code is 636051\nEnter this temporary verification code to continue: 636051. ChatGPT Log-in Code\n",
                    }
                ]
            }
        ),
    ])
    service.http_client = fake_client
    service._email_cache["tester@example.com"] = {
        "email": "tester@example.com",
        "jwt": "fresh-jwt",
        "password": "plain-password",
    }

    code = service.get_verification_code("tester@example.com", timeout=1)

    assert code == "636051"


def test_extract_mail_fields_uses_mime_date_when_list_timestamp_missing():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })

    parsed = service._extract_mail_fields({
        "id": "mail-1",
        "raw": "Date: Fri, 27 Mar 2026 10:00:20 +0000\nSubject: Your verification code\n\nYour OpenAI verification code is 222222",
    })

    assert parsed["subject"] == "Your verification code"
    assert parsed["body"].endswith("Your OpenAI verification code is 222222")
    assert parsed["timestamp"] == 1774605620.0


def test_delete_email_clears_mailbox_dedupe_state():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    service._email_cache["tester@example.com"] = {
        "email": "tester@example.com",
        "service_id": "tester@example.com",
        "id": "tester@example.com",
    }
    service._used_codes["tester@example.com"] = {"111111"}
    service._used_mail_ids["tester@example.com"] = {"mail-1"}

    deleted = service.delete_email("tester@example.com")

    assert deleted is True
    assert "tester@example.com" not in service._email_cache
    assert "tester@example.com" not in service._used_codes
    assert "tester@example.com" not in service._used_mail_ids


def test_reusing_same_address_can_fetch_valid_otp_after_cleanup():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    service._email_cache["tester@example.com"] = {
        "email": "tester@example.com",
        "service_id": "tester@example.com",
        "id": "tester@example.com",
    }
    service._used_codes["tester@example.com"] = {"111111"}
    service._used_mail_ids["tester@example.com"] = {"mail-1"}
    service.delete_email("tester@example.com")

    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": "mail-1",
                        "source": "OpenAI <noreply@openai.com>",
                        "subject": "Your verification code",
                        "raw": "Subject: Your verification code\n\nYour OpenAI verification code is 111111",
                    }
                ]
            }
        ),
    ])
    service.http_client = fake_client
    service._email_cache["tester@example.com"] = {
        "email": "tester@example.com",
        "jwt": "fresh-jwt",
        "password": "plain-password",
        "service_id": "tester@example.com",
    }

    code = service.get_verification_code("tester@example.com", timeout=1)

    assert code == "111111"
