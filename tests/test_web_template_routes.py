from pathlib import Path
import importlib

from fastapi.testclient import TestClient

web_app_module = importlib.import_module("src.web.app")
web_auth_module = importlib.import_module("src.web.auth")
REPO_ROOT = Path(__file__).resolve().parents[1]


def make_dummy_settings(project_root: Path):
    class DummySecret:
        def __init__(self, value):
            self._value = value

        def get_secret_value(self):
            return self._value

    class DummySettings:
        app_name = "test"
        app_version = "1.0"
        debug = False
        database_url = str(project_root / "test.db")
        webui_secret_key = DummySecret("secret")
        webui_access_password = DummySecret("password")

    return DummySettings()


def build_template_test_client(monkeypatch, tmp_path: Path) -> TestClient:
    project_root = tmp_path
    templates_dir = project_root / "templates"
    static_dir = project_root / "static"
    templates_dir.mkdir()
    static_dir.mkdir()

    for name in ["login.html", "index.html", "email_services.html", "settings.html", "payment.html"]:
        (templates_dir / name).write_text("<html><body>ok</body></html>", encoding="utf-8")

    (templates_dir / "accounts.html").write_text(
        (REPO_ROOT / "templates" / "accounts.html").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (templates_dir / "cliproxy.html").write_text(
        (REPO_ROOT / "templates" / "cliproxy.html").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    monkeypatch.setattr(web_app_module, "STATIC_DIR", static_dir)
    monkeypatch.setattr(web_app_module, "TEMPLATES_DIR", templates_dir)
    monkeypatch.setattr(web_app_module, "get_settings", lambda: make_dummy_settings(project_root))
    monkeypatch.setattr(web_auth_module, "get_settings", lambda: make_dummy_settings(project_root))

    app = web_app_module.create_app()
    return TestClient(app)


def test_accounts_page_renders_with_new_template_response_signature(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/accounts"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/accounts", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert 'id="accounts-workbench"' in response.text
    assert 'id="accounts-list-panel"' in response.text
    assert 'id="accounts-bulk-bar"' in response.text
    assert 'id="accounts-selection-banner-anchor"' in response.text
    assert 'id="account-detail-panel"' in response.text
    assert 'data-detail-layer="subscription-quota"' in response.text
    assert 'data-detail-layer="core-ops"' in response.text
    assert 'data-detail-layer="automation-trace"' in response.text
    assert 'data-detail-layer="cliproxy-summary"' in response.text


def test_login_rejects_unsafe_external_next_redirect(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    response = client.post(
        "/login",
        data={"password": "password", "next": "https://evil.example/phish"},
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["location"] == "/"


def test_logout_rejects_unsafe_external_next_redirect(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    response = client.get("/logout?next=https://evil.example/phish", follow_redirects=False)

    assert response.status_code == 302
    assert response.headers["location"] == "/login"


def test_app_cors_configuration_is_not_wildcard_with_credentials(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    cors_middleware = next(mw for mw in client.app.user_middleware if mw.cls.__name__ == "CORSMiddleware")

    assert cors_middleware.kwargs["allow_credentials"] is False
    assert cors_middleware.kwargs["allow_origins"] != ["*"]


def test_payment_page_requires_auth(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    response = client.get("/payment", follow_redirects=False)

    assert response.status_code == 302
    assert response.headers["location"] == "/login?next=/payment"


def test_cliproxy_page_renders_dedicated_management_ui(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/cliproxy"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/cliproxy", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert 'id="cliproxy-environment-list"' in response.text
    assert 'id="cliproxy-environment-form"' in response.text
    assert 'id="cliproxy-test-connection-section"' in response.text
    assert 'id="cliproxy-maintenance-actions-section"' in response.text
    assert 'id="cliproxy-test-connection-btn"' in response.text
    assert 'id="cliproxy-scan-btn"' in response.text
    assert 'id="cliproxy-maintain-btn"' in response.text
    assert 'id="cliproxy-run-history-table"' in response.text
    assert 'id="cliproxy-inventory-table"' in response.text
