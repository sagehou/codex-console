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
            raise AssertionError(f"unexpected request: {method} {url}")
        return self.responses.pop(0)


class FakeClock:
    def __init__(self, start=1000.0):
        self.now = start
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def _service_with_mailbox(fake_client):
    service = TempMailService({
        "base_url": "https://mail.example.com",
        "admin_password": "admin-secret",
        "domain": "example.com",
    })
    service.http_client = fake_client
    service._email_cache["tester@example.com"] = {
        "email": "tester@example.com",
        "jwt": "fresh-jwt",
        "password": "plain-password",
    }
    return service


def test_get_verification_code_ignores_mail_older_than_otp_sent_at(monkeypatch):
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": "old-mail",
                        "from": "OpenAI <noreply@openai.com>",
                        "subject": "Your verification code",
                        "createdAt": "2026-03-27T10:00:00Z",
                        "raw": "Date: Fri, 27 Mar 2026 10:00:00 +0000\nSubject: Your verification code\n\nYour OpenAI verification code is 111111",
                    },
                    {
                        "id": "new-mail",
                        "from": "OpenAI <noreply@openai.com>",
                        "subject": "Your verification code",
                        "createdAt": "2026-03-27T10:00:20Z",
                        "raw": "Date: Fri, 27 Mar 2026 10:00:20 +0000\nSubject: Your verification code\n\nYour OpenAI verification code is 222222",
                    },
                ]
            }
        )
    ])
    service = _service_with_mailbox(fake_client)

    code = service.get_verification_code(
        "tester@example.com",
        timeout=1,
        otp_sent_at=1774605610.0,
    )

    assert code == "222222"


def test_get_verification_code_normalizes_millisecond_epoch_before_otp_comparison(monkeypatch):
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": "old-mail-ms",
                        "from": "OpenAI <noreply@openai.com>",
                        "subject": "Your verification code",
                        "createdAt": 1774605600000,
                        "raw": "Date: Fri, 27 Mar 2026 10:00:00 +0000\nSubject: Your verification code\n\nYour OpenAI verification code is 111111",
                    },
                    {
                        "id": "new-mail-ms",
                        "from": "OpenAI <noreply@openai.com>",
                        "subject": "Your verification code",
                        "createdAt": 1774605620000,
                        "raw": "Date: Fri, 27 Mar 2026 10:00:20 +0000\nSubject: Your verification code\n\nYour OpenAI verification code is 222222",
                    },
                ]
            }
        )
    ])
    service = _service_with_mailbox(fake_client)

    code = service.get_verification_code(
        "tester@example.com",
        timeout=1,
        otp_sent_at=1774605610.0,
    )

    assert code == "222222"


def test_get_verification_code_dedupes_same_mail_id_across_polls(monkeypatch):
    clock = FakeClock()
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": "mail-1",
                        "from": "OpenAI <noreply@openai.com>",
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
                        "id": "mail-1",
                        "from": "OpenAI <noreply@openai.com>",
                        "subject": "Your verification code",
                        "raw": "Subject: Your verification code\n\nYour OpenAI verification code is 222222",
                    },
                    {
                        "id": "mail-2",
                        "from": "OpenAI <noreply@openai.com>",
                        "subject": "Your verification code",
                        "raw": "Subject: Your verification code\n\nYour OpenAI verification code is 333333",
                    },
                ]
            }
        ),
    ])
    service = _service_with_mailbox(fake_client)

    monkeypatch.setattr("src.services.temp_mail.time.time", clock.time)
    monkeypatch.setattr("src.services.temp_mail.time.sleep", clock.sleep)

    first_code = service.get_verification_code("tester@example.com", timeout=1)
    second_code = service.get_verification_code("tester@example.com", timeout=4)

    assert first_code == "111111"
    assert second_code == "333333"


def test_get_verification_code_allows_unknown_time_only_for_strong_semantic_match_after_wait(monkeypatch):
    clock = FakeClock(start=2000.0)
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": "mail-1",
                        "from": "OpenAI <noreply@openai.com>",
                        "subject": "Security alert",
                        "raw": "Subject: Security alert\n\nYour ChatGPT code is 444444",
                    }
                ]
            }
        ),
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": "mail-1",
                        "from": "OpenAI <noreply@openai.com>",
                        "subject": "Security alert",
                        "raw": "Subject: Security alert\n\nYour ChatGPT code is 444444",
                    }
                ]
            }
        ),
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": "mail-1",
                        "from": "OpenAI <noreply@openai.com>",
                        "subject": "Security alert",
                        "raw": "Subject: Security alert\n\nYour ChatGPT code is 444444",
                    }
                ]
            }
        ),
    ])
    service = _service_with_mailbox(fake_client)

    monkeypatch.setattr("src.services.temp_mail.time.time", clock.time)
    monkeypatch.setattr("src.services.temp_mail.time.sleep", clock.sleep)

    code = service.get_verification_code(
        "tester@example.com",
        timeout=12,
        otp_sent_at=1997.0,
    )

    assert code == "444444"
    assert clock.time() - 1997.0 >= 6


def test_get_verification_code_rejects_unknown_time_plain_digit_fallback_only(monkeypatch):
    clock = FakeClock(start=3000.0)
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "results": [
                    {
                        "id": "mail-1",
                        "from": "OpenAI <noreply@openai.com>",
                        "subject": "Inbox item 555555",
                        "body": "Use code 555555 if prompted.",
                    }
                ]
            }
        ),
        FakeResponse(payload={"results": []}),
        FakeResponse(payload={"results": []}),
        FakeResponse(payload={"results": []}),
    ])
    service = _service_with_mailbox(fake_client)

    monkeypatch.setattr("src.services.temp_mail.time.time", clock.time)
    monkeypatch.setattr("src.services.temp_mail.time.sleep", clock.sleep)

    code = service.get_verification_code(
        "tester@example.com",
        timeout=9,
        otp_sent_at=2995.0,
    )

    assert code is None
