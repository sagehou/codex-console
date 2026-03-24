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
        "password": "59b3e8d637cf97edbe2384cf59cb7453dfe30789f5c9564ec0f5535614cde35f",
    }
    assert login_call["kwargs"]["headers"]["x-custom-auth"] == "site-secret"

    second_mail_call = fake_client.calls[2]
    assert second_mail_call["kwargs"]["headers"]["Authorization"] == "Bearer fresh-jwt"
    assert second_mail_call["kwargs"]["headers"]["x-custom-auth"] == "site-secret"
