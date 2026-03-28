from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_cpa_nav_is_visible_without_accounts_or_services():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")

    assert '<a href="/cpa" class="nav-link active">CPA管理</a>' in template
    assert 'id="cpa-workbench"' in template
    assert 'id="cpa-top-region"' in template
    assert 'id="cpa-bottom-region"' in template
    assert 'id="cpa-detail-panel"' in template


def test_cpa_workbench_renders_no_usable_services_empty_state():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")

    assert 'id="cpa-empty-state"' in template
    assert 'data-empty-state="no-usable-services"' in template
    assert 'id="cpa-empty-state-message"' in template
    assert '请先在设置中配置并启用至少一个 CPA 服务' in template


def test_cpa_workbench_bootstraps_latest_active_task_and_selection_recovery_contracts():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")

    assert 'id="cpa-latest-active-task-bootstrap"' in template
    assert 'id="cpa-selection-recovery-bootstrap"' in template
    assert 'id="cpa-selection-notice"' in template
    assert 'data-notice-kind="selection-recovery"' in template


def test_cpa_workbench_bootstraps_selection_notice_contract_from_route_context():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")

    assert 'id="cpa-selection-notice-bootstrap"' in template
    assert "{{ cpa_selection_notice | tojson if cpa_selection_notice else 'null' }}" in template


def test_cpa_workbench_has_stats_task_list_and_detail_regions():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")

    assert 'id="cpa-shell-top"' in template
    assert 'id="cpa-shell-bottom"' in template
    assert 'id="cpa-stats-region"' in template
    assert 'id="cpa-active-task-region"' in template
    assert 'id="cpa-credential-list-region"' in template
    assert 'id="cpa-detail-layer-stack"' in template
    assert 'data-detail-layer="credential-identity"' in template
    assert 'data-detail-layer="credential-core-status"' in template
    assert 'data-detail-layer="credential-quick-actions"' in template
    assert 'data-detail-layer="credential-local-account"' in template
    assert 'data-detail-layer="credential-recent-logs"' in template
    assert 'CLIProxyAPI 管理台' not in template


def test_cpa_workbench_selector_renders_multi_select_service_controls():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")

    assert 'id="cpa-service-selector"' in template
    assert 'data-selection-mode="multi"' in template
    assert 'id="cpa-service-selector-list"' in template
    assert 'id="cpa-selected-service-count"' in template
    assert 'type="checkbox"' in template
    assert 'src="/static/js/cpa_workbench.js?v={{ static_version }}"' in template


def test_task9_selector_script_uses_summary_and_inventory_endpoints_without_task_panel_mutation():
    script = (REPO_ROOT / "static" / "js" / "cpa_workbench.js").read_text(encoding="utf-8")

    assert "/api/cpa/summary" in script
    assert "/api/cpa/credentials" in script
    assert "inventoryPayload.summary" not in script
    assert "renderTaskScope" not in script
    assert "cpaWorkbenchElements.taskRegion.innerHTML" not in script


def test_cpa_workbench_stats_panel_shows_aggregate_counts():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")

    assert 'id="cpa-stats-region"' in template
    assert 'data-stat-key="total"' in template
    assert 'data-stat-key="valid_count"' in template
    assert 'data-stat-key="expired_count"' in template
    assert 'data-stat-key="quota_count"' in template
    assert 'data-stat-key="error_count"' in template
    assert 'data-stat-key="unknown_count"' in template


def test_cpa_workbench_task_panel_shows_type_totals_current_item_and_logs():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")
    script = (REPO_ROOT / "static" / "js" / "cpa_workbench.js").read_text(encoding="utf-8")

    assert 'id="cpa-active-task-region"' in template
    assert 'data-task-field="type"' in template
    assert 'data-task-field="total"' in template
    assert 'data-task-field="processed"' in template
    assert 'data-task-field="current_item"' in template
    assert 'id="cpa-task-log-list"' in template
    assert 'id="cpa-task-progress-bar"' in template
    assert "/api/cpa/tasks/latest-active?type=scan|action" not in script
    assert "/api/cpa/tasks/latest-active" in script
    assert "/api/cpa/tasks/" in script
    assert "renderTaskPanel" in script


def test_cpa_workbench_task_panel_renders_distinct_action_types_and_scoped_latest_active_requests():
    script = (REPO_ROOT / "static" / "js" / "cpa_workbench.js").read_text(encoding="utf-8")

    assert "删除任务" in script
    assert "禁用任务" in script
    assert "task.stats?.quota_action" in script
    assert "query.set('service_ids'" in script
    assert "/api/cpa/tasks/latest-active?" in script or "new URLSearchParams()" in script


def test_cpa_workbench_task_panel_refresh_preserves_explicit_empty_scope_marker():
    script = (REPO_ROOT / "static" / "js" / "cpa_workbench.js").read_text(encoding="utf-8")

    assert "service_scope" in script
    assert "query.set('service_scope', 'empty')" in script or 'query.set("service_scope", "empty")' in script


def test_cpa_workbench_defaults_show_scan_delete_disable_concurrency_as_two():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")

    assert 'name="scan-concurrency"' in template
    assert 'name="delete-concurrency"' in template
    assert 'name="disable-concurrency"' in template
    assert 'value="2"' in template
    assert 'data-default-concurrency="2"' in template


def test_cpa_workbench_table_shows_remote_credential_rows():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")
    script = (REPO_ROOT / "static" / "js" / "cpa_workbench.js").read_text(encoding="utf-8")

    assert 'id="cpa-credential-table"' in template
    assert 'data-inventory-table="credentials"' in template
    assert 'id="cpa-credential-table-body"' in template
    assert 'data-column="credential_id"' in template
    assert 'data-column="service_name"' in template
    assert 'data-column="status"' in template
    assert 'data-column="quota_status"' in template
    assert 'data-column="last_scanned_at"' in template
    assert 'data-column="local_account_summary"' in template
    assert 'data-column="row_actions"' in template
    assert '<tr data-service-id="${row.service_id}" data-credential-id="${escapeHtml(row.credential_id)}"' in script


def test_selected_credential_updates_detail_context():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")
    script = (REPO_ROOT / "static" / "js" / "cpa_workbench.js").read_text(encoding="utf-8")

    assert 'data-detail-field="credential_id"' in template
    assert 'data-detail-field="service_name"' in template
    assert 'data-detail-field="status"' in template
    assert 'data-detail-field="quota_status"' in template
    assert 'data-detail-field="last_scanned_at"' in template
    assert 'data-detail-field="local_account_summary"' in template
    assert 'selectionRecovery: readBootstrapJson' in script
    assert 'selectedCredentialKey' in script
    assert 'applyRecoveredSelection' in script
    assert 'renderDetailContext' in script
    assert 'handleCredentialRowSelection' in script


def test_recovered_selection_is_consumed_once_then_current_selection_wins():
    script = (REPO_ROOT / "static" / "js" / "cpa_workbench.js").read_text(encoding="utf-8")

    assert 'selectionRecovery = null' in script
    assert 'if (cpaWorkbenchState.selectedCredentialKey && rows.some' in script
    assert 'const recovery = cpaWorkbenchState.selectionRecovery;' in script
    assert 'cpaWorkbenchState.selectedCredentialKey = recoveredKey;' in script


def test_bootstrapped_selection_notice_is_rendered_by_script():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")
    script = (REPO_ROOT / "static" / "js" / "cpa_workbench.js").read_text(encoding="utf-8")

    assert 'id="cpa-selection-notice-bootstrap"' in template
    assert 'id="cpa-selection-notice"' in template
    assert 'selectionNoticeBootstrap' in script
    assert 'selectionNotice: readBootstrapJson' in script
    assert 'renderSelectionNotice' in script
    assert 'cpaWorkbenchElements.selectionNotice' in script
    assert '.classList.remove(\'hidden\')' in script or '.classList.remove("hidden")' in script


def test_cpa_detail_panel_shows_layered_sections():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")

    assert 'data-detail-layer="credential-identity"' in template
    assert 'data-detail-layer="credential-core-status"' in template
    assert 'data-detail-layer="credential-quick-actions"' in template
    assert 'data-detail-layer="credential-local-account"' in template
    assert 'data-detail-layer="credential-recent-logs"' in template
    assert 'data-detail-field="detail_status_summary"' in template
    assert 'data-detail-field="detail_default_action"' in template


def test_cpa_detail_panel_shows_recent_log_excerpt_and_view_logs_action():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")
    script = (REPO_ROOT / "static" / "js" / "cpa_workbench.js").read_text(encoding="utf-8")

    assert 'data-detail-field="recent_log_excerpt"' in template
    assert 'data-detail-action="view-logs"' in template
    assert 'renderRecentLogExcerpt' in script
    assert 'view-logs' in script


def test_cpa_detail_panel_supports_single_item_scan_delete_and_disable_actions():
    template = (REPO_ROOT / "templates" / "cpa_workbench.html").read_text(encoding="utf-8")
    script = (REPO_ROOT / "static" / "js" / "cpa_workbench.js").read_text(encoding="utf-8")

    assert 'data-detail-action="scan-single"' in template
    assert 'data-detail-action="delete-single"' in template
    assert 'data-detail-action="disable-single"' in template
    assert 'handleDetailAction' in script
    assert 'credential_ids' in script
    assert "/api/cpa/scan" in script
    assert "/api/cpa/actions" in script
