from pathlib import Path

from fastapi.testclient import TestClient

import src.web.app as web_app_module


def test_accounts_page_renders_with_new_template_response_signature(monkeypatch, tmp_path):
    project_root = tmp_path
    templates_dir = project_root / "templates"
    static_dir = project_root / "static"
    templates_dir.mkdir()
    static_dir.mkdir()

    for name in ["login.html", "index.html", "accounts.html", "email_services.html", "settings.html", "payment.html"]:
        (templates_dir / name).write_text("<html><body>ok</body></html>", encoding="utf-8")

    monkeypatch.setattr(web_app_module, "STATIC_DIR", static_dir)
    monkeypatch.setattr(web_app_module, "TEMPLATES_DIR", templates_dir)

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

    monkeypatch.setattr(web_app_module, "get_settings", lambda: DummySettings())

    app = web_app_module.create_app()
    client = TestClient(app)

    login_response = client.post("/login", data={"password": "password", "next": "/accounts"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/accounts", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert "ok" in response.text
