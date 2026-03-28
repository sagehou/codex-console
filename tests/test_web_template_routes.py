from pathlib import Path
import importlib

from fastapi.testclient import TestClient
import src.database.session as session_module
from src.database import crud
from src.database.init_db import initialize_database

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

    for name in ["login.html", "settings.html", "payment.html"]:
        (templates_dir / name).write_text("<html><body>ok</body></html>", encoding="utf-8")

    (templates_dir / "index.html").write_text(
        (REPO_ROOT / "templates" / "index.html").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    (templates_dir / "email_services.html").write_text(
        (REPO_ROOT / "templates" / "email_services.html").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    (templates_dir / "accounts.html").write_text(
        (REPO_ROOT / "templates" / "accounts.html").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (templates_dir / "cliproxy.html").write_text(
        (REPO_ROOT / "templates" / "cliproxy.html").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    cpa_template = REPO_ROOT / "templates" / "cpa_workbench.html"
    if cpa_template.exists():
        (templates_dir / "cpa_workbench.html").write_text(
            cpa_template.read_text(encoding="utf-8"),
            encoding="utf-8",
        )

    monkeypatch.setattr(web_app_module, "STATIC_DIR", static_dir)
    monkeypatch.setattr(web_app_module, "TEMPLATES_DIR", templates_dir)
    monkeypatch.setattr(web_app_module, "get_settings", lambda: make_dummy_settings(project_root))
    monkeypatch.setattr(web_auth_module, "get_settings", lambda: make_dummy_settings(project_root))
    monkeypatch.setattr(session_module, "_db_manager", None)
    initialize_database(f"sqlite:///{project_root / 'test.db'}")

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
    assert 'data-detail-layer="cpa-summary"' in response.text


def test_accounts_template_exposes_batch_check_progress_panel_markers(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/accounts"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/accounts", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert 'id="batch-check-task-panel"' in response.text
    assert 'data-task-panel="batch-subscription-check"' in response.text
    assert 'id="batch-check-task-summary"' in response.text
    assert 'id="batch-check-task-total-count"' in response.text
    assert 'id="batch-check-task-processed-count"' in response.text
    assert 'id="batch-check-task-success-count"' in response.text
    assert 'id="batch-check-task-failure-count"' in response.text
    assert 'id="batch-check-task-progress-text"' in response.text
    assert 'class="batch-task-progress-track"' in response.text
    assert 'id="batch-check-task-progress-bar"' in response.text
    assert 'id="batch-check-task-current-account"' in response.text
    assert 'id="batch-check-task-current-account-value"' in response.text
    assert 'id="batch-check-task-log-list"' in response.text
    assert 'id="batch-check-task-log-empty"' in response.text


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
    assert 'id="cliproxy-test-connection-section"' not in response.text
    assert 'id="cliproxy-maintenance-actions-section"' not in response.text
    assert 'id="cliproxy-test-connection-btn"' not in response.text
    assert 'id="cliproxy-scan-btn"' not in response.text
    assert 'id="cliproxy-maintain-btn"' not in response.text
    assert 'id="cliproxy-service-selection-region"' in response.text
    assert 'id="cliproxy-action-region"' in response.text
    assert 'id="cliproxy-run-history-table"' in response.text
    assert 'id="cliproxy-inventory-table"' in response.text


def test_cliproxy_page_exposes_cpa_selection_and_action_regions(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/cliproxy"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/cliproxy", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert 'id="cliproxy-service-selection-region"' in response.text
    assert 'data-selection-mode="multi"' in response.text
    assert 'id="cliproxy-selection-count"' in response.text
    assert 'id="cliproxy-action-region"' in response.text
    assert 'id="cliproxy-bulk-test-connection-btn"' in response.text
    assert 'id="cliproxy-bulk-scan-btn"' in response.text
    assert 'id="cliproxy-bulk-maintain-btn"' in response.text


def test_cliproxy_page_exposes_grouped_progress_and_result_regions(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/cliproxy"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/cliproxy", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert 'id="cliproxy-aggregate-progress-region"' in response.text
    assert 'id="cliproxy-aggregate-progress-bar"' in response.text
    assert 'id="cliproxy-service-progress-list"' in response.text
    assert 'id="cliproxy-grouped-log-region"' in response.text
    assert 'id="cliproxy-grouped-log-list"' in response.text
    assert 'id="cliproxy-grouped-result-region"' in response.text
    assert 'id="cliproxy-grouped-result-list"' in response.text


def test_cliproxy_page_exposes_run_history_inventory_and_audit_regions(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/cliproxy"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/cliproxy", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert 'id="cliproxy-result-run-history-region"' in response.text
    assert 'id="cliproxy-run-history-table"' in response.text
    assert 'id="cliproxy-run-history-body"' in response.text
    assert 'id="cliproxy-result-inventory-region"' in response.text
    assert 'id="cliproxy-inventory-table"' in response.text
    assert 'id="cliproxy-inventory-body"' in response.text
    assert 'id="cliproxy-result-audit-region"' in response.text
    assert 'id="cliproxy-audit-summary-table"' in response.text
    assert 'id="cliproxy-audit-summary-body"' in response.text


def test_cliproxy_nav_entry_remains_visible_even_without_cpa_services(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/cliproxy"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/cliproxy", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert '<a href="/cliproxy" class="nav-link active">CLIProxyAPI</a>' in response.text


def test_cpa_workbench_route_renders(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/cpa"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/cpa", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert '<a href="/cpa" class="nav-link active">CPA管理</a>' in response.text
    assert 'id="cpa-workbench"' in response.text
    assert 'id="cpa-stats-panel"' in response.text
    assert 'id="cpa-task-panel"' in response.text
    assert 'id="cpa-credential-list-panel"' in response.text
    assert 'id="cpa-detail-panel"' in response.text


def test_cpa_nav_entry_is_visible_on_existing_non_cpa_page(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/accounts"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/accounts", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert '<a href="/cpa" class="nav-link">CPA管理</a>' in response.text


def test_cpa_workbench_route_has_layout_skeleton_regions(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/cpa"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/cpa", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert 'id="cpa-shell-top"' in response.text
    assert 'id="cpa-shell-bottom"' in response.text
    assert 'id="cpa-stats-region"' in response.text
    assert 'id="cpa-active-task-region"' in response.text
    assert 'id="cpa-credential-list-region"' in response.text
    assert 'id="cpa-detail-layer-stack"' in response.text
    assert 'id="cpa-bulk-test-connection-btn"' in response.text
    assert 'id="cpa-bulk-scan-btn"' in response.text
    assert 'id="cpa-bulk-action-btn"' in response.text
    assert 'data-detail-layer="credential-identity"' in response.text
    assert 'data-detail-layer="credential-core-status"' in response.text
    assert 'data-detail-layer="credential-quick-actions"' in response.text
    assert 'data-detail-layer="credential-local-account"' in response.text
    assert 'data-detail-layer="credential-recent-logs"' in response.text
    assert 'CLIProxyAPI 管理台' not in response.text


def test_cpa_page_bootstrap_does_not_include_cliproxy_task_shape(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/cpa"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/cpa", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert 'id="cpa-latest-active-task-bootstrap"' in response.text
    assert 'aggregate_key' not in response.text
    assert 'run_type' not in response.text


def test_cpa_page_bootstrap_matches_scoped_cpa_latest_active_task(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/cpa"}, follow_redirects=False)
    auth_cookie = login_response.cookies.get("webui_auth")
    session_cookie = login_response.cookies.get("session_id")
    session_id = session_cookie.split(".", 1)[0]

    with session_module.get_db() as db:
        service = crud.create_cpa_service(
            db,
            name="bootstrap-service",
            api_url="https://bootstrap.example.com",
            api_token="bootstrap-token",
            enabled=True,
            priority=1,
        )
        service_id = service.id
        task = crud.create_cpa_scan_task(db, owner_session_id=session_id, service_ids=[service_id])
        task_id = task.id
        crud.start_cpa_workbench_task(db, task.id)

    response = client.get("/cpa", cookies={"webui_auth": auth_cookie, "session_id": session_cookie})

    assert response.status_code == 200
    assert f'"task_id": "{task_id}"' in response.text
    assert '"type": "scan"' in response.text
    assert f'"service_ids": [{service_id}]' in response.text
    assert 'aggregate_key' not in response.text


def test_cliproxy_page_renders_no_available_cpa_services_state(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/cliproxy"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/cliproxy", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert 'id="cliproxy-empty-state"' in response.text
    assert 'data-empty-state="no-cpa-services"' in response.text
    assert 'id="cliproxy-empty-state-message"' in response.text
    assert 'id="cliproxy-active-task-banner"' in response.text


def test_cliproxy_page_marks_incomplete_cpa_services_and_disables_actions(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/cliproxy"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/cliproxy", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert 'id="cliproxy-cpa-service-contract"' not in response.text
    assert 'id="cliproxy-cpa-service-list"' not in response.text
    assert 'data-service-action="test-connection"' not in response.text
    assert 'data-service-action="scan"' not in response.text
    assert 'data-service-action="maintain"' not in response.text
    assert 'id="cliproxy-service-selection-region"' in response.text
    assert 'id="cliproxy-action-region"' in response.text


def test_cliproxy_js_does_not_reuse_no_services_empty_state_for_incomplete_services():
    script = (REPO_ROOT / "static" / "js" / "cliproxy.js").read_text(encoding="utf-8")

    assert "if (incompleteCount > 0)" not in script
    assert "已发现 ${incompleteCount} 个 CPA 服务配置不完整" not in script
    assert "handleTestConnection" not in script
    assert "handleMaintenanceAction" not in script
    assert "cliproxyElements.testConnectionBtn?.addEventListener" not in script
    assert "cliproxyElements.scanBtn?.addEventListener" not in script
    assert "cliproxyElements.maintainBtn?.addEventListener" not in script


def test_cliproxy_js_run_history_and_inventory_rendering_follow_result_table_contracts():
    script = (REPO_ROOT / "static" / "js" / "cliproxy.js").read_text(encoding="utf-8")

    assert "run.current_stage" in script
    assert "run.result_summary?.records" in script or "run.result_summary && run.result_summary.records" in script
    assert "run.started_at || run.created_at" not in script
    assert "item.service_name || '-'" not in script
    assert "String(item.service_id || '-')" not in script
    assert "item.email || '-'" in script
    assert "item.remote_account_id || '-'" in script


def test_email_services_template_still_exposes_tempmail_domain_field_for_v1_alias(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/email-services"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/email-services", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert 'name="domain"' in response.text


def test_tempmail_settings_form_includes_multi_domain_hint_near_domain_input(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/email-services"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/email-services", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert 'id="tempmail-domain"' in response.text
    assert 'name="domain"' in response.text
    assert 'data-tempmail-domain-row="inline-hint"' in response.text
    assert 'id="tempmail-domain-hint"' in response.text
    assert '支持多个域名，使用英文逗号分隔，例如：a.com,b.com,c.com' in response.text

    row_index = response.text.index('data-tempmail-domain-row="inline-hint"')
    domain_index = response.text.index('id="tempmail-domain"')
    hint_index = response.text.index('支持多个域名，使用英文逗号分隔，例如：a.com,b.com,c.com')
    assert row_index < domain_index < hint_index
    assert hint_index - domain_index < 220


def test_registration_template_includes_tempmail_multi_domain_hint_adjacent_to_input(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert 'id="registration-email-service"' in response.text
    assert 'data-registration-email-row="tempmail-hint"' in response.text
    assert 'id="registration-tempmail-hint"' in response.text
    assert 'TempMail 多域名提示' in response.text
    assert '多个域名请在邮箱服务页使用英文逗号分隔，例如：a.com,b.com,c.com' in response.text

    row_index = response.text.index('data-registration-email-row="tempmail-hint"')
    field_index = response.text.index('id="registration-email-service"')
    hint_index = response.text.index('id="registration-tempmail-hint"')
    assert row_index < field_index < hint_index
    assert hint_index - field_index < 320


def test_registration_first_screen_keeps_primary_controls_visible(monkeypatch, tmp_path):
    client = build_template_test_client(monkeypatch, tmp_path)

    login_response = client.post("/login", data={"password": "password", "next": "/"}, follow_redirects=False)
    cookie = login_response.cookies.get("webui_auth")

    response = client.get("/", cookies={"webui_auth": cookie})

    assert response.status_code == 200
    assert 'id="registration-primary-panel"' in response.text
    assert 'data-primary-controls="registration"' in response.text
    assert 'id="registration-email-service"' in response.text
    assert 'id="registration-mode-controls"' in response.text
    assert 'id="registration-batch-parameters"' in response.text
    assert 'id="registration-auto-upload-controls"' in response.text
    assert 'id="registration-run-monitor"' in response.text
    assert 'data-high-frequency-panel="run-monitor"' in response.text
    assert 'id="task-status-row"' in response.text
    assert 'id="console-log"' in response.text
    assert 'id="auto-upload-cpa"' in response.text
    assert 'id="cpa-service-select-group"' in response.text

    primary_index = response.text.index('id="registration-primary-panel"')
    mode_index = response.text.index('id="registration-mode-controls"')
    batch_index = response.text.index('id="registration-batch-parameters"')
    upload_index = response.text.index('id="registration-auto-upload-controls"')
    monitor_index = response.text.index('id="registration-run-monitor"')

    assert primary_index < mode_index < batch_index < upload_index
    assert primary_index < monitor_index
