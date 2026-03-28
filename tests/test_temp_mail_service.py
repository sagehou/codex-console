import pytest

from src.services.temp_mail import EmailServiceError, TempMailService


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


def test_default_headers_omit_x_custom_auth_when_site_password_blank():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
        "site_password": "",
    })

    assert "x-custom-auth" not in service._default_headers()


def test_parse_domains_normalizes_comma_separated_values():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": " a.com, b.com ,, c.com ",
    })

    assert service.config["domain"] == "a.com,b.com,c.com"
    assert service._get_domains() == ["a.com", "b.com", "c.com"]


def test_create_email_uses_random_choice_from_normalized_domains(monkeypatch):
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "a.com, b.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(payload={"address": "tester@b.com", "jwt": "jwt-123", "password": "plain-password", "address_id": "addr-1"}),
    ])
    service.http_client = fake_client

    monkeypatch.setattr("src.services.temp_mail.random.choice", lambda values: "b.com")

    result = service.create_email()

    assert result["email"] == "tester@b.com"
    create_call = fake_client.calls[0]
    assert create_call["kwargs"]["json"]["domain"] == "b.com"


def test_init_rejects_empty_domain_list_after_normalization():
    import pytest

    with pytest.raises(ValueError):
        TempMailService({
            "base_url": "https://mail.example.com",
            "admin_password": "admin-secret",
            "domain": " , , ",
        })


def test_get_verification_code_refreshes_jwt_via_address_login_before_api_mails():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
        "site_password": "site-secret",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(payload={"jwt": "fresh-jwt", "address": "tester@example.com"}),
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": "mail-1",
                        "source": "noreply@openai.com",
                        "subject": "OpenAI verification",
                        "raw": "From: OpenAI <noreply@openai.com>\nSubject: OpenAI verification\n\nYour OpenAI verification code is 654321",
                    }
                ],
                "count": 1,
            }
        ),
    ])
    service.http_client = fake_client

    email = "tester@example.com"
    service._email_cache[email] = {
        "email": email,
        "address_id": "addr-1",
        "jwt": "jwt-abc",
        "password": "plain-password",
    }

    code = service.get_verification_code(email=email, timeout=1)

    assert code == "654321"
    assert len(fake_client.calls) == 2
    login_call = fake_client.calls[0]
    assert login_call["url"] == "https://mail.example.com/api/address_login"
    assert login_call["kwargs"]["json"] == {
        "email": "tester@example.com",
        "password": "plain-password",
    }
    mail_call = fake_client.calls[1]
    assert mail_call["url"] == "https://mail.example.com/api/mails"
    assert mail_call["kwargs"]["params"] == {"limit": 20, "offset": 0}
    assert mail_call["kwargs"]["headers"]["Authorization"] == "Bearer fresh-jwt"


def test_get_verification_code_without_cached_jwt_logs_in_with_password():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(payload={"jwt": "fresh-jwt", "address": "nojwt@example.com"}),
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": "mail-1",
                        "source": "noreply@openai.com",
                        "subject": "Code",
                        "text": "123456 is your verification code",
                    }
                ],
                "total": 1,
            }
        ),
    ])
    service.http_client = fake_client
    service._email_cache["nojwt@example.com"] = {
        "email": "nojwt@example.com",
        "password": "plain-password",
    }

    code = service.get_verification_code(email="nojwt@example.com", timeout=1)

    assert code == "123456"
    assert len(fake_client.calls) == 2
    assert fake_client.calls[0]["url"] == "https://mail.example.com/api/address_login"
    assert fake_client.calls[0]["kwargs"]["json"] == {
        "email": "nojwt@example.com",
        "password": "plain-password",
    }
    assert fake_client.calls[1]["url"] == "https://mail.example.com/api/mails"
    assert fake_client.calls[1]["kwargs"]["params"] == {"limit": 20, "offset": 0}
    assert fake_client.calls[1]["kwargs"]["headers"]["Authorization"] == "Bearer fresh-jwt"


def test_get_verification_code_skips_last_used_mail_id_between_calls():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(payload={"jwt": "jwt-fresh-1", "address": "reuse@example.com"}),
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": "mail-1",
                        "source": "noreply@openai.com",
                        "subject": "Code #1",
                        "text": "111111 is your verification code",
                    }
                ],
                "total": 1,
            }
        ),
        FakeResponse(payload={"jwt": "jwt-fresh-2", "address": "reuse@example.com"}),
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": "mail-1",
                        "source": "noreply@openai.com",
                        "subject": "Code #1",
                        "text": "111111 is your verification code",
                    },
                    {
                        "id": "mail-2",
                        "source": "noreply@openai.com",
                        "subject": "Code #2",
                        "text": "222222 is your verification code",
                    },
                ],
                "total": 2,
            }
        ),
    ])
    service.http_client = fake_client
    service._email_cache["reuse@example.com"] = {
        "email": "reuse@example.com",
        "jwt": "jwt-abc",
        "password": "plain-password",
    }

    code_1 = service.get_verification_code(email="reuse@example.com", timeout=1)
    code_2 = service.get_verification_code(email="reuse@example.com", timeout=1)

    assert code_1 == "111111"
    assert code_2 == "222222"


def test_get_verification_code_filters_old_mails_by_otp_sent_at():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    otp_sent_at = 1_700_000_000.0
    fake_client = FakeHTTPClient([
        FakeResponse(payload={"jwt": "fresh-jwt", "address": "filter@example.com"}),
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": "mail-old",
                        "source": "noreply@openai.com",
                        "subject": "Old Code",
                        "text": "333333 is your verification code",
                        "createdAt": otp_sent_at - 30,
                    },
                    {
                        "id": "mail-new",
                        "source": "noreply@openai.com",
                        "subject": "New Code",
                        "text": "444444 is your verification code",
                        "createdAt": otp_sent_at + 5,
                    },
                ],
                "total": 2,
            }
        ),
    ])
    service.http_client = fake_client
    service._email_cache["filter@example.com"] = {
        "email": "filter@example.com",
        "jwt": "jwt-abc",
        "password": "plain-password",
    }

    code = service.get_verification_code(
        email="filter@example.com",
        timeout=1,
        otp_sent_at=otp_sent_at,
    )

    assert code == "444444"


def test_get_verification_code_accepts_mails_key_and_missing_mail_id():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(payload={"jwt": "fresh-jwt", "address": "format@example.com"}),
        FakeResponse(
            payload={
                "mails": [
                    {
                        # 没有 id/mail_id 字段，验证回退 ID 逻辑
                        "source": "noreply@openai.com",
                        "subject": "OpenAI verification",
                        "text": "Your verification code is 987654",
                        "createdAt": "2026-03-23 10:00:00",
                    }
                ],
                "total": 1,
            }
        ),
    ])
    service.http_client = fake_client
    service._email_cache["format@example.com"] = {
        "email": "format@example.com",
        "jwt": "jwt-abc",
        "password": "plain-password",
    }

    code = service.get_verification_code(email="format@example.com", timeout=1)

    assert code == "987654"


def test_get_verification_code_scans_all_results_and_prefers_raw():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(payload={"jwt": "fresh-jwt", "address": "detail@example.com"}),
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": "mail-100",
                        "source": "noreply@openai.com",
                        "subject": "OpenAI marketing",
                        "raw": "From: OpenAI <noreply@openai.com>\nSubject: Product update\n\nThis is not a verification message.",
                    },
                    {
                        "id": "mail-101",
                        "source": "noreply@openai.com",
                        "subject": "OpenAI verification",
                        "text": "Fallback text says 999999",
                        "raw": "From: OpenAI <noreply@openai.com>\nSubject: OpenAI verification\n\nYour OpenAI verification code is 123456",
                    },
                    {
                        "id": "mail-102",
                        "source": "noreply@openai.com",
                        "subject": "OpenAI verification",
                        "text": "Your OpenAI verification code is 654321",
                    }
                ]
            }
        ),
    ])
    service.http_client = fake_client
    service._email_cache["detail@example.com"] = {
        "email": "detail@example.com",
        "jwt": "jwt-abc",
        "password": "plain-password",
    }

    code = service.get_verification_code(email="detail@example.com", timeout=1)

    assert code == "123456"
    assert len(fake_client.calls) == 2
    assert fake_client.calls[0]["url"] == "https://mail.example.com/api/address_login"
    assert fake_client.calls[1]["url"] == "https://mail.example.com/api/mails"


def test_get_verification_code_raises_site_password_config_error_on_401_with_custom_auth():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
        "site_password": "wrong-site-password",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(status_code=401, payload={"error": "x-custom-auth rejected"}),
    ])
    service.http_client = fake_client
    service._email_cache["target@example.com"] = {
        "email": "target@example.com",
        "jwt": "jwt-abc",
        "password": "plain-password",
    }

    with pytest.raises(EmailServiceError, match="site_password"):
        service.get_verification_code(email="target@example.com", timeout=1)


def test_create_email_uses_admin_new_address_contract(monkeypatch):
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
        "site_password": "site-secret",
        "enable_prefix": True,
    })
    fake_client = FakeHTTPClient([
        FakeResponse(payload={
            "address": "abc12x@example.com",
            "jwt": "address-jwt",
            "password": "plain-password",
            "address_id": 1,
        }),
    ])
    service.http_client = fake_client

    choice_values = iter([
        list("abcde"),
        ["1"],
        ["x"],
    ])
    monkeypatch.setattr("src.services.temp_mail.random.choices", lambda population, k: next(choice_values))
    monkeypatch.setattr("src.services.temp_mail.random.randint", lambda a, b: 1)

    result = service.create_email()

    assert result["email"] == "abc12x@example.com"
    call = fake_client.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == "https://mail.example.com/admin/new_address"
    assert call["kwargs"]["headers"]["x-admin-auth"] == "admin-secret"
    assert call["kwargs"]["headers"]["x-custom-auth"] == "site-secret"
    assert call["kwargs"]["json"] == {
        "name": "abcde1x",
        "domain": "example.com",
        "enablePrefix": True,
    }


def test_create_email_fails_when_password_missing_from_response():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
        "site_password": "site-secret",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(payload={"address": "tester@example.com", "jwt": "address-jwt", "password": "", "address_id": 1}),
    ])
    service.http_client = fake_client

    with pytest.raises(EmailServiceError):
        service.create_email()


def test_create_email_fails_when_address_id_missing_from_response():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
        "site_password": "site-secret",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(payload={"address": "tester@example.com", "jwt": "address-jwt", "password": "plain-password"}),
    ])
    service.http_client = fake_client

    with pytest.raises(EmailServiceError, match="address_id"):
        service.create_email()


def test_login_address_posts_plain_password_without_hashing():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "site_password": "site-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(payload={"jwt": "fresh-jwt", "address": "tester@example.com"}),
    ])
    service.http_client = fake_client

    service._login_address("tester@example.com", "plain-password")

    login_call = fake_client.calls[0]
    assert login_call["url"] == "https://mail.example.com/api/address_login"
    assert login_call["kwargs"]["json"] == {
        "email": "tester@example.com",
        "password": "plain-password",
    }


def test_create_email_sends_enable_prefix_and_requires_password():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "site_password": "site-secret",
        "domain": "example.com",
        "enable_prefix": True,
    })
    fake_client = FakeHTTPClient([
        FakeResponse(payload={"address": "tester@example.com", "jwt": "address-jwt", "password": None, "address_id": 1}),
    ])
    service.http_client = fake_client

    with pytest.raises(Exception):
        service.create_email()

    create_call = fake_client.calls[0]
    assert create_call["kwargs"]["json"]["enablePrefix"] is True


def test_get_verification_code_does_not_use_admin_or_user_mail_endpoints():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(payload={"jwt": "fresh-jwt-2", "address": "tester@example.com"}),
        FakeResponse(payload={"results": [], "count": 0}),
    ])
    service.http_client = fake_client
    service._email_cache["tester@example.com"] = {"email": "tester@example.com", "jwt": "fresh-jwt", "password": "plain-password"}

    service.get_verification_code("tester@example.com", timeout=1)

    assert all(
        "/admin/mails" not in call["url"] and "/user_api/mails" not in call["url"]
        for call in fake_client.calls
    )


def test_get_verification_code_uses_shared_cached_password_for_cold_cache_instance():
    creator = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    creator.http_client = FakeHTTPClient([
        FakeResponse(payload={
            "address": "cold@example.com",
            "jwt": "create-jwt",
            "password": "plain-password",
            "address_id": "addr-99",
        }),
    ])
    creator.create_email()

    reader = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(payload={"jwt": "fresh-jwt", "address": "cold@example.com"}),
        FakeResponse(payload={
            "results": [
                {
                    "id": "mail-1",
                    "source": "noreply@openai.com",
                    "subject": "OpenAI verification",
                    "text": "Your verification code is 246810",
                }
            ],
            "count": 1,
        }),
    ])
    reader.http_client = fake_client

    code = reader.get_verification_code(email="cold@example.com", email_id="addr-99", timeout=1)

    assert code == "246810"
    assert fake_client.calls[0]["url"] == "https://mail.example.com/api/address_login"
    assert fake_client.calls[0]["kwargs"]["json"] == {
        "email": "cold@example.com",
        "password": "plain-password",
    }
    assert fake_client.calls[1]["url"] == "https://mail.example.com/api/mails"
    assert fake_client.calls[1]["kwargs"]["headers"]["Authorization"] == "Bearer fresh-jwt"


def test_401_with_custom_auth_and_bearer_error_is_not_mislabeled_as_site_password():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
        "site_password": "site-secret",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(status_code=401, payload={"error": "invalid bearer token"}),
    ])
    service.http_client = fake_client

    with pytest.raises(EmailServiceError, match="invalid bearer token") as exc_info:
        service._make_request(
            "GET",
            "/api/mails",
            params={"limit": 20, "offset": 0},
            headers={"Authorization": "Bearer stale-jwt"},
        )

    assert "site_password" not in str(exc_info.value)


def test_403_with_custom_auth_defaults_to_site_password_misconfiguration():
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
        "site_password": "site-secret",
    })
    fake_client = FakeHTTPClient([
        FakeResponse(status_code=403, payload={"error": "forbidden"}),
    ])
    service.http_client = fake_client

    with pytest.raises(EmailServiceError, match="site_password misconfiguration") as exc_info:
        service._make_request(
            "POST",
            "/api/emails",
            json={"name": "tester", "domain": "example.com"},
        )

    assert "x-custom-auth was rejected" in str(exc_info.value)
