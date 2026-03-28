let cliproxyEnvironments = [];
let cliproxyRuns = [];
let cliproxyInventory = [];
let cliproxyAuditSummary = [];
let cliproxyCpaServices = [];
let selectedEnvironmentId = null;
let selectedCpaServiceIds = new Set();
let cliproxyAggregateTask = null;
let cliproxyTaskPollTimer = null;

const cliproxyElements = {
    environmentList: document.getElementById('cliproxy-environment-list'),
    environmentForm: document.getElementById('cliproxy-environment-form'),
    environmentId: document.getElementById('cliproxy-environment-id'),
    name: document.getElementById('cliproxy-name'),
    baseUrl: document.getElementById('cliproxy-base-url'),
    targetType: document.getElementById('cliproxy-target-type'),
    provider: document.getElementById('cliproxy-provider'),
    providerScope: document.getElementById('cliproxy-provider-scope'),
    targetScope: document.getElementById('cliproxy-target-scope'),
    token: document.getElementById('cliproxy-token'),
    scopeRulesJson: document.getElementById('cliproxy-scope-rules-json'),
    notes: document.getElementById('cliproxy-notes'),
    enabled: document.getElementById('cliproxy-enabled'),
    isDefault: document.getElementById('cliproxy-is-default'),
    saveBtn: document.getElementById('cliproxy-save-btn'),
    resetFormBtn: document.getElementById('cliproxy-reset-form-btn'),
    newEnvironmentBtn: document.getElementById('cliproxy-new-environment-btn'),
    refreshEnvironmentsBtn: document.getElementById('cliproxy-refresh-environments-btn'),
    refreshRunsBtn: document.getElementById('cliproxy-refresh-runs-btn'),
    refreshInventoryBtn: document.getElementById('cliproxy-refresh-inventory-btn'),
    totalEnvironments: document.getElementById('cliproxy-total-environments'),
    enabledEnvironments: document.getElementById('cliproxy-enabled-environments'),
    runCount: document.getElementById('cliproxy-run-count'),
    inventoryCount: document.getElementById('cliproxy-inventory-count'),
    runEnvironmentName: document.getElementById('cliproxy-run-environment-name'),
    runHistoryBody: document.getElementById('cliproxy-run-history-body'),
    inventoryBody: document.getElementById('cliproxy-inventory-body'),
    auditSummaryBody: document.getElementById('cliproxy-audit-summary-body'),
    inventorySummaryCount: document.getElementById('cliproxy-inventory-summary-count'),
    emptyState: document.getElementById('cliproxy-empty-state'),
    emptyStateMessage: document.getElementById('cliproxy-empty-state-message'),
    activeTaskBanner: document.getElementById('cliproxy-active-task-banner'),
    activeTaskBannerMessage: document.getElementById('cliproxy-active-task-banner-message'),
    selectionList: document.getElementById('cliproxy-selection-list'),
    selectionEmpty: document.getElementById('cliproxy-selection-empty'),
    selectionCount: document.getElementById('cliproxy-selection-count'),
    bulkActionSummary: document.getElementById('cliproxy-bulk-action-summary'),
    bulkTestConnectionBtn: document.getElementById('cliproxy-bulk-test-connection-btn'),
    bulkScanBtn: document.getElementById('cliproxy-bulk-scan-btn'),
    bulkMaintainBtn: document.getElementById('cliproxy-bulk-maintain-btn'),
    bulkTestResults: document.getElementById('cliproxy-bulk-test-results'),
    aggregateProgressRegion: document.getElementById('cliproxy-aggregate-progress-region'),
    aggregateProgressText: document.getElementById('cliproxy-aggregate-progress-text'),
    aggregateProgressBar: document.getElementById('cliproxy-aggregate-progress-bar'),
    aggregateStatus: document.getElementById('cliproxy-aggregate-status'),
    aggregateServiceCompleted: document.getElementById('cliproxy-aggregate-service-completed'),
    aggregateServiceTotal: document.getElementById('cliproxy-aggregate-service-total'),
    aggregateRecordProgress: document.getElementById('cliproxy-aggregate-record-progress'),
    serviceProgressList: document.getElementById('cliproxy-service-progress-list'),
    groupedLogRegion: document.getElementById('cliproxy-grouped-log-region'),
    groupedLogList: document.getElementById('cliproxy-grouped-log-list'),
    groupedResultRegion: document.getElementById('cliproxy-grouped-result-region'),
    groupedResultList: document.getElementById('cliproxy-grouped-result-list'),
};

document.addEventListener('DOMContentLoaded', () => {
    initCliproxyEventListeners();
    resetCliproxyForm();
    recoverLatestActiveCliproxyTaskFromPage();
    loadCliproxyCpaServices();
    loadCliproxyEnvironments();
    loadCliproxyResultAreas();
});

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text == null ? '' : String(text);
    return div.innerHTML;
}

function getSelectedEnvironment() {
    return cliproxyEnvironments.find(item => item.id === selectedEnvironmentId) || null;
}

function getReadySelectedServiceIds() {
    return [...selectedCpaServiceIds].filter(serviceId => {
        const service = cliproxyCpaServices.find(item => Number(item.id) === Number(serviceId));
        return service && service.config_status === 'ready';
    });
}

function getChildProgress(service) {
    if (service.known_record_total == null) {
        return service.status === 'completed' ? 100 : service.current_stage === 'queued' ? 0 : 20;
    }
    const total = Math.max(Number(service.known_record_total || 0), 0);
    if (!total) return service.status === 'completed' ? 100 : 20;
    return Math.min(100, Math.round((Number(service.processed_count || 0) * 100) / total));
}

function setAggregateTask(task) {
    cliproxyAggregateTask = task || null;
    syncSelectionFromTask();
    renderAggregateTaskState();
    syncTaskPolling();
}

function clearTaskPolling() {
    if (cliproxyTaskPollTimer) {
        window.clearTimeout(cliproxyTaskPollTimer);
        cliproxyTaskPollTimer = null;
    }
}

function syncTaskPolling() {
    clearTaskPolling();
    if (!cliproxyAggregateTask || !['queued', 'running'].includes(cliproxyAggregateTask.status)) return;
    const intervalMs = Number(cliproxyElements.aggregateProgressRegion?.dataset.taskPollIntervalMs || 2000);
    cliproxyTaskPollTimer = window.setTimeout(() => refreshAggregateTaskDetail(), intervalMs);
}

async function refreshAggregateTaskDetail() {
    if (!cliproxyAggregateTask?.task_id) return;
    try {
        const task = await api.get(`/cliproxy/tasks/${cliproxyAggregateTask.task_id}`);
        setAggregateTask(task);
    } catch (error) {
        if (error?.response?.status === 404) {
            setAggregateTask(null);
            return;
        }
        toast.error(error.message || '刷新 CLIProxy 聚合任务失败');
    }
}

function recoverLatestActiveCliproxyTaskFromPage() {
    const taskId = cliproxyElements.activeTaskBanner?.dataset.latestActiveTaskId;
    const status = cliproxyElements.activeTaskBanner?.dataset.latestActiveTaskStatus;
    const runType = cliproxyElements.activeTaskBanner?.dataset.latestActiveTaskType;
    const bootstrapNode = document.getElementById('cliproxy-latest-active-task-bootstrap');
    let bootstrapTask = null;

    if (bootstrapNode?.textContent) {
        try {
            bootstrapTask = JSON.parse(bootstrapNode.textContent);
        } catch (error) {
            bootstrapTask = null;
        }
    }

    if (taskId && status && runType) {
        cliproxyElements.activeTaskBanner?.classList.remove('hidden');
        if (cliproxyElements.activeTaskBannerMessage) {
            cliproxyElements.activeTaskBannerMessage.textContent = `已恢复最近的 CLIProxy ${runType} 任务，当前状态：${status}。`;
        }
    }

    if (bootstrapTask?.task_id) {
        setAggregateTask(bootstrapTask);
        refreshAggregateTaskDetail();
    }
}

function initCliproxyEventListeners() {
    cliproxyElements.environmentForm?.addEventListener('submit', handleCliproxyFormSubmit);
    cliproxyElements.resetFormBtn?.addEventListener('click', resetCliproxyForm);
    cliproxyElements.newEnvironmentBtn?.addEventListener('click', resetCliproxyForm);
    cliproxyElements.refreshEnvironmentsBtn?.addEventListener('click', loadCliproxyEnvironments);
    cliproxyElements.refreshRunsBtn?.addEventListener('click', () => selectedEnvironmentId && loadCliproxyRuns(selectedEnvironmentId));
    cliproxyElements.refreshInventoryBtn?.addEventListener('click', () => selectedEnvironmentId && loadCliproxyInventory(selectedEnvironmentId));
    cliproxyElements.environmentList?.addEventListener('click', handleEnvironmentListClick);
    cliproxyElements.selectionList?.addEventListener('change', handleCpaSelectionChange);
    cliproxyElements.bulkTestConnectionBtn?.addEventListener('click', () => handleBulkAction('test-connection'));
    cliproxyElements.bulkScanBtn?.addEventListener('click', () => handleBulkAction('scan'));
    cliproxyElements.bulkMaintainBtn?.addEventListener('click', () => handleBulkAction('maintain'));
}

async function loadCliproxyCpaServices() {
    try {
        cliproxyCpaServices = await api.get('/cliproxy/cpa-services');
    } catch (error) {
        cliproxyCpaServices = [];
    }
    renderCliproxyCpaSelection();
    updateCliproxyEmptyState();
    updateBulkActionState();
}

async function loadCliproxyResultAreas() {
    await Promise.all([
        loadCliproxyTaskHistory(),
        loadCliproxyInventorySummary(),
        loadCliproxyAuditSummary(),
    ]);
}

async function loadCliproxyTaskHistory() {
    try {
        cliproxyRuns = await api.get('/cliproxy/tasks/history');
    } catch (error) {
        cliproxyRuns = [];
    }
    renderRunTable();
    renderCliproxyStats();
}

async function loadCliproxyInventorySummary() {
    try {
        cliproxyInventory = await api.get('/cliproxy/inventory');
    } catch (error) {
        cliproxyInventory = [];
    }
    renderInventoryTable();
    renderCliproxyStats();
}

async function loadCliproxyAuditSummary() {
    try {
        cliproxyAuditSummary = await api.get('/audit?resource_type=cliproxy');
    } catch (error) {
        cliproxyAuditSummary = [];
    }
    renderAuditSummaryTable();
}

function renderCliproxyCpaSelection() {
    if (!cliproxyElements.selectionList) return;
    if (!cliproxyCpaServices.length) {
        cliproxyElements.selectionList.classList.add('hidden');
        cliproxyElements.selectionEmpty?.classList.remove('hidden');
        cliproxyElements.selectionCount.textContent = '0';
        return;
    }
    cliproxyElements.selectionEmpty?.classList.add('hidden');
    cliproxyElements.selectionList.classList.remove('hidden');
    cliproxyElements.selectionList.innerHTML = cliproxyCpaServices.map(service => {
        const selected = selectedCpaServiceIds.has(service.id);
        const disabled = service.config_status !== 'ready';
        return `
            <label class="cliproxy-select-card ${selected ? 'is-selected' : ''} ${disabled ? 'is-disabled' : ''}" data-service-id="${service.id}" data-service-name="${escapeHtml(service.name)}" data-service-select="true" data-config-status="${escapeHtml(service.config_status || '')}">
                <div class="cliproxy-service-card-header">
                    <div><input type="checkbox" value="${service.id}" ${selected ? 'checked' : ''} ${disabled ? 'disabled' : ''}><strong>${escapeHtml(service.name)}</strong></div>
                    <span class="cliproxy-chip">${disabled ? 'config incomplete' : 'ready'}</span>
                </div>
                <p class="cliproxy-muted">${disabled ? `缺少必填配置：${escapeHtml((service.missing_required_fields || []).join(', '))}` : '已启用，可参与聚合任务与连接测试。'}</p>
            </label>`;
    }).join('');
    cliproxyElements.selectionCount.textContent = String(getReadySelectedServiceIds().length);
}

function handleCpaSelectionChange(event) {
    const checkbox = event.target.closest('input[type="checkbox"]');
    if (!checkbox) return;
    const serviceId = Number(checkbox.value);
    if (checkbox.checked) selectedCpaServiceIds.add(serviceId);
    else selectedCpaServiceIds.delete(serviceId);
    renderCliproxyCpaSelection();
    updateBulkActionState();
}

function syncSelectionFromTask() {
    const services = cliproxyAggregateTask?.services || [];
    if (!services.length) return;
    selectedCpaServiceIds = new Set(services.map(service => Number(service.service_id)));
    renderCliproxyCpaSelection();
    updateBulkActionState();
}

function updateBulkActionState() {
    const count = getReadySelectedServiceIds().length;
    if (cliproxyElements.selectionCount) cliproxyElements.selectionCount.textContent = String(count);
    const disabled = count === 0;
    if (cliproxyElements.bulkActionSummary) {
        cliproxyElements.bulkActionSummary.textContent = disabled
            ? '先选择一个或多个配置完整的 CPA 服务，再执行 test connection / scan / maintain。'
            : `已选择 ${count} 个 CPA 服务，可执行聚合动作。`;
    }
    if (cliproxyElements.bulkTestConnectionBtn) cliproxyElements.bulkTestConnectionBtn.disabled = disabled;
    if (cliproxyElements.bulkScanBtn) cliproxyElements.bulkScanBtn.disabled = disabled;
    if (cliproxyElements.bulkMaintainBtn) cliproxyElements.bulkMaintainBtn.disabled = disabled;
}

async function handleBulkAction(action) {
    const serviceIds = getReadySelectedServiceIds();
    if (!serviceIds.length) return;
    const button = action === 'test-connection' ? cliproxyElements.bulkTestConnectionBtn : action === 'scan' ? cliproxyElements.bulkScanBtn : cliproxyElements.bulkMaintainBtn;
    loading.show(button, action === 'test-connection' ? '检测中...' : action === 'scan' ? '扫描中...' : '维护中...');
    try {
        if (action === 'test-connection') {
            const payload = await api.post('/cliproxy/test-connection', { service_ids: serviceIds });
            renderBulkTestResults(payload.results || []);
            toast.success('CLIProxy 多服务连接测试已完成');
        } else {
            const task = await api.post(`/cliproxy/${action}`, action === 'maintain' ? { service_ids: serviceIds, dry_run: false } : { service_ids: serviceIds });
            setAggregateTask(task);
            toast.success(`CLIProxy ${action} 任务已提交`);
        }
    } catch (error) {
        toast.error(error.message || 'CLIProxy 聚合动作失败');
    } finally {
        loading.hide(button);
    }
}

function renderBulkTestResults(results) {
    if (!cliproxyElements.bulkTestResults) return;
    if (!results.length) {
        cliproxyElements.bulkTestResults.classList.add('hidden');
        cliproxyElements.bulkTestResults.innerHTML = '';
        return;
    }
    cliproxyElements.bulkTestResults.classList.remove('hidden');
    cliproxyElements.bulkTestResults.innerHTML = results.map(item => `
        <div class="cliproxy-grouped-card">
            <strong>${escapeHtml(item.service_name || String(item.service_id))}</strong>
            <div class="cliproxy-grouped-meta">
                <span class="cliproxy-chip"><strong>状态</strong><span>${escapeHtml(item.status || '-')}</span></span>
                <span class="cliproxy-chip"><strong>延迟</strong><span>${format.number(item.latency_ms || 0)} ms</span></span>
                <span class="cliproxy-chip"><strong>错误</strong><span>${escapeHtml(item.error || '-')}</span></span>
            </div>
        </div>`).join('');
}

function renderAggregateTaskState() {
    const task = cliproxyAggregateTask;
    const hasTask = Boolean(task);
    cliproxyElements.aggregateProgressRegion?.classList.toggle('hidden', !hasTask);
    cliproxyElements.groupedLogRegion?.classList.toggle('hidden', !hasTask);
    cliproxyElements.groupedResultRegion?.classList.toggle('hidden', !hasTask);
    if (!hasTask) {
        if (cliproxyElements.serviceProgressList) cliproxyElements.serviceProgressList.innerHTML = '';
        if (cliproxyElements.groupedLogList) cliproxyElements.groupedLogList.innerHTML = '';
        if (cliproxyElements.groupedResultList) cliproxyElements.groupedResultList.innerHTML = '';
        return;
    }
    if (cliproxyElements.aggregateProgressRegion) {
        cliproxyElements.aggregateProgressRegion.dataset.taskId = task.task_id || '';
        cliproxyElements.aggregateProgressRegion.dataset.runType = task.run_type || '';
    }
    if (cliproxyElements.aggregateProgressText) cliproxyElements.aggregateProgressText.textContent = `当前 ${task.run_type} 任务状态：${task.status}。`;
    if (cliproxyElements.aggregateStatus) cliproxyElements.aggregateStatus.textContent = task.status || '-';
    if (cliproxyElements.aggregateServiceCompleted) cliproxyElements.aggregateServiceCompleted.textContent = String(task.service_completed || 0);
    if (cliproxyElements.aggregateServiceTotal) cliproxyElements.aggregateServiceTotal.textContent = String(task.service_total || 0);
    if (cliproxyElements.aggregateRecordProgress) cliproxyElements.aggregateRecordProgress.textContent = String(task.processed_record_total || 0);
    if (cliproxyElements.aggregateProgressBar) cliproxyElements.aggregateProgressBar.style.width = `${Number(task.progress_percent || 0)}%`;
    renderServiceProgressList(task.services || []);
    renderGroupedLogs(task.services || [], task.grouped_logs || {});
    renderGroupedResults(task.services || [], task.grouped_results || {});
}

function renderServiceProgressList(services) {
    if (!cliproxyElements.serviceProgressList) return;
    cliproxyElements.serviceProgressList.innerHTML = services.map(service => `
        <div class="cliproxy-child-card">
            <div class="cliproxy-service-card-header"><strong>${escapeHtml(service.service_name || String(service.service_id))}</strong><span class="cliproxy-chip">${escapeHtml(service.status || '-')}</span></div>
            <div class="cliproxy-child-progress-track"><div class="cliproxy-child-progress-bar" style="width: ${getChildProgress(service)}%;"></div></div>
            <div class="cliproxy-child-meta">
                <span class="cliproxy-chip"><strong>阶段</strong><span>${escapeHtml(service.current_stage || '-')}</span></span>
                <span class="cliproxy-chip"><strong>成功</strong><span>${format.number(service.success_count || 0)}</span></span>
                <span class="cliproxy-chip"><strong>失败</strong><span>${format.number(service.failure_count || 0)}</span></span>
            </div>
        </div>`).join('');
}

function renderGroupedLogs(services, groupedLogs) {
    if (!cliproxyElements.groupedLogList) return;
    cliproxyElements.groupedLogList.innerHTML = services.map(service => {
        const lines = groupedLogs[String(service.service_id)] || [];
        return `
            <div class="cliproxy-grouped-card">
                <strong>${escapeHtml(service.service_name || String(service.service_id))}</strong>
                <ol class="cliproxy-log-lines">${lines.length ? lines.map(line => `<li>${escapeHtml(line)}</li>`).join('') : '<li>暂无日志</li>'}</ol>
            </div>`;
    }).join('');
}

function renderGroupedResults(services, groupedResults) {
    if (!cliproxyElements.groupedResultList) return;
    cliproxyElements.groupedResultList.innerHTML = services.map(service => {
        const result = groupedResults[String(service.service_id)] || {};
        return `
            <div class="cliproxy-grouped-card">
                <strong>${escapeHtml(service.service_name || String(service.service_id))}</strong>
                <div class="cliproxy-grouped-meta">
                    <span class="cliproxy-chip"><strong>records</strong><span>${format.number(result.records || 0)}</span></span>
                    <span class="cliproxy-chip"><strong>success</strong><span>${format.number(result.success_count || 0)}</span></span>
                    <span class="cliproxy-chip"><strong>failure</strong><span>${format.number(result.failure_count || 0)}</span></span>
                    <span class="cliproxy-chip"><strong>status</strong><span>${escapeHtml(result.status || service.status || '-')}</span></span>
                    <span class="cliproxy-chip"><strong>last_error</strong><span>${escapeHtml(result.last_error || '-')}</span></span>
                </div>
            </div>`;
    }).join('');
}

function updateCliproxyEmptyState() {
    if (!cliproxyElements.emptyState || !cliproxyElements.emptyStateMessage) return;
    if (!cliproxyCpaServices.length) {
        cliproxyElements.emptyState.classList.remove('hidden');
        cliproxyElements.emptyStateMessage.textContent = '当前没有可用的 CPA 服务，请先在设置中添加并启用至少一个 CPA 服务后再执行连接测试、扫描或维护。';
        return;
    }
    cliproxyElements.emptyState.classList.add('hidden');
}

async function loadCliproxyEnvironments() {
    try {
        cliproxyEnvironments = await api.get('/cliproxy-environments');
        renderCliproxyEnvironmentList();
        renderCliproxyStats();
        if (!cliproxyEnvironments.length) {
            selectedEnvironmentId = null;
            updateSelectedEnvironmentState();
            renderRunTable();
            renderInventoryTable();
            return;
        }
        const nextEnvironment = cliproxyEnvironments.find(item => item.id === selectedEnvironmentId) || cliproxyEnvironments.find(item => item.is_default) || cliproxyEnvironments[0];
        selectCliproxyEnvironment(nextEnvironment.id);
    } catch (error) {
        if (cliproxyElements.environmentList) cliproxyElements.environmentList.innerHTML = '<div class="cliproxy-empty">加载环境列表失败，请稍后重试。</div>';
    }
}

function renderCliproxyEnvironmentList() {
    if (!cliproxyElements.environmentList) return;
    if (!cliproxyEnvironments.length) {
        cliproxyElements.environmentList.innerHTML = '<div class="cliproxy-empty">还没有配置 CLIProxyAPI 环境，先使用下方表单创建一个。</div>';
        return;
    }
    cliproxyElements.environmentList.innerHTML = cliproxyEnvironments.map(environment => `
        <button type="button" class="cliproxy-environment-item ${environment.id === selectedEnvironmentId ? 'is-active' : ''}" data-environment-id="${environment.id}">
            <div class="cliproxy-toolbar"><h4>${escapeHtml(environment.name || `环境 #${environment.id}`)}</h4><span class="cliproxy-chip"><strong>${environment.enabled ? '启用' : '停用'}</strong><span>${escapeHtml(environment.last_test_status || 'unknown')}</span></span></div>
            <p>${escapeHtml(environment.base_url || '-')}</p>
        </button>`).join('');
}

function renderCliproxyStats() {
    if (cliproxyElements.totalEnvironments) cliproxyElements.totalEnvironments.textContent = String(cliproxyEnvironments.length);
    if (cliproxyElements.enabledEnvironments) cliproxyElements.enabledEnvironments.textContent = String(cliproxyEnvironments.filter(item => item.enabled).length);
    if (cliproxyElements.runCount) cliproxyElements.runCount.textContent = String(cliproxyRuns.length);
    if (cliproxyElements.inventoryCount) cliproxyElements.inventoryCount.textContent = String(cliproxyInventory.length);
    if (cliproxyElements.inventorySummaryCount) cliproxyElements.inventorySummaryCount.textContent = String(cliproxyInventory.length);
}

function handleEnvironmentListClick(event) {
    const button = event.target.closest('[data-environment-id]');
    if (!button) return;
    selectCliproxyEnvironment(Number(button.dataset.environmentId));
}

function selectCliproxyEnvironment(environmentId) {
    selectedEnvironmentId = environmentId;
    renderCliproxyEnvironmentList();
    populateCliproxyForm(getSelectedEnvironment());
    updateSelectedEnvironmentState();
    loadCliproxyRuns(environmentId);
    loadCliproxyInventory(environmentId);
}

function updateSelectedEnvironmentState() {
    const environment = getSelectedEnvironment();
    const hasEnvironment = Boolean(environment);
    if (cliproxyElements.refreshRunsBtn) cliproxyElements.refreshRunsBtn.disabled = !hasEnvironment;
    if (cliproxyElements.refreshInventoryBtn) cliproxyElements.refreshInventoryBtn.disabled = !hasEnvironment;
    if (cliproxyElements.runEnvironmentName) cliproxyElements.runEnvironmentName.textContent = hasEnvironment ? environment.name : '未选择';
}

function populateCliproxyForm(environment) {
    if (!environment) return resetCliproxyForm();
    cliproxyElements.environmentId.value = environment.id;
    cliproxyElements.name.value = environment.name || '';
    cliproxyElements.baseUrl.value = environment.base_url || '';
    cliproxyElements.targetType.value = environment.target_type || '';
    cliproxyElements.provider.value = environment.provider || '';
    cliproxyElements.providerScope.value = environment.provider_scope || '';
    cliproxyElements.targetScope.value = environment.target_scope || '';
    cliproxyElements.token.value = '';
    cliproxyElements.scopeRulesJson.value = environment.scope_rules_json ? JSON.stringify(environment.scope_rules_json, null, 2) : '';
    cliproxyElements.notes.value = environment.notes || '';
    cliproxyElements.enabled.checked = Boolean(environment.enabled);
    cliproxyElements.isDefault.checked = Boolean(environment.is_default);
}

function resetCliproxyForm() {
    cliproxyElements.environmentForm?.reset();
    if (cliproxyElements.environmentId) cliproxyElements.environmentId.value = '';
    if (cliproxyElements.enabled) cliproxyElements.enabled.checked = true;
}

function readScopeRulesJson() {
    const raw = cliproxyElements.scopeRulesJson.value.trim();
    if (!raw) return null;
    return JSON.parse(raw);
}

async function handleCliproxyFormSubmit(event) {
    event.preventDefault();
    let scopeRulesJson = null;
    try {
        scopeRulesJson = readScopeRulesJson();
    } catch (error) {
        toast.error('Scope Rules JSON 不是有效的 JSON');
        return;
    }
    const environmentId = cliproxyElements.environmentId.value.trim();
    const payload = {
        name: cliproxyElements.name.value.trim(),
        base_url: cliproxyElements.baseUrl.value.trim(),
        target_type: cliproxyElements.targetType.value.trim(),
        provider: cliproxyElements.provider.value.trim(),
        provider_scope: cliproxyElements.providerScope.value.trim() || null,
        target_scope: cliproxyElements.targetScope.value.trim() || null,
        scope_rules_json: scopeRulesJson,
        enabled: cliproxyElements.enabled.checked,
        is_default: cliproxyElements.isDefault.checked,
        notes: cliproxyElements.notes.value.trim() || null,
    };
    const token = cliproxyElements.token.value.trim();
    if (!environmentId || token) payload.token = token || null;
    loading.show(cliproxyElements.saveBtn, environmentId ? '保存中...' : '创建中...');
    try {
        if (environmentId) {
            await api.patch(`/cliproxy-environments/${environmentId}`, payload);
            toast.success('CLIProxy 环境已更新');
        } else {
            const created = await api.post('/cliproxy-environments', payload);
            selectedEnvironmentId = created.id;
            toast.success('CLIProxy 环境已创建');
        }
        cliproxyElements.token.value = '';
        await loadCliproxyEnvironments();
    } catch (error) {
        toast.error(error.message || '保存 CLIProxy 环境失败');
    } finally {
        loading.hide(cliproxyElements.saveBtn);
    }
}

async function loadCliproxyRuns(environmentId) {
    try {
        cliproxyRuns = await api.get(`/cliproxy-environments/${environmentId}/runs`);
    } catch (error) {
        cliproxyRuns = [];
    }
    renderRunTable();
    renderCliproxyStats();
}

async function loadCliproxyInventory(environmentId) {
    try {
        cliproxyInventory = await api.get(`/cliproxy-environments/${environmentId}/inventory`);
    } catch (error) {
        cliproxyInventory = [];
    }
    renderInventoryTable();
    renderCliproxyStats();
}

function renderRunTable() {
    if (!cliproxyElements.runHistoryBody) return;
    if (!cliproxyRuns.length) {
        cliproxyElements.runHistoryBody.innerHTML = '<tr><td colspan="7" class="cliproxy-muted">当前还没有可显示的运行记录。</td></tr>';
        return;
    }
    cliproxyElements.runHistoryBody.innerHTML = cliproxyRuns.map(run => `<tr><td class="cliproxy-code">#${escapeHtml(run.task_id || run.id || '-')}</td><td>${escapeHtml(run.type || run.run_type || '-')}</td><td>${escapeHtml(run.status || '-')}</td><td>${escapeHtml(run.current_stage || '-')}</td><td>${format.number(run.result_summary?.records || run.counters?.record_count || 0)}</td><td>${format.date(run.created_at || run.started_at)}</td><td>${format.date(run.completed_at)}</td></tr>`).join('');
}

function renderInventoryTable() {
    if (!cliproxyElements.inventoryBody) return;
    if (!cliproxyInventory.length) {
        cliproxyElements.inventoryBody.innerHTML = '<tr><td colspan="7" class="cliproxy-muted">当前还没有远端库存记录。</td></tr>';
        return;
    }
    cliproxyElements.inventoryBody.innerHTML = cliproxyInventory.map(item => `<tr><td class="cliproxy-code">${escapeHtml(item.remote_file_id || '-')}</td><td>${escapeHtml(item.email || '-')}</td><td>${escapeHtml(item.remote_account_id || '-')}</td><td>${escapeHtml(item.sync_state || '-')}</td><td>${escapeHtml(item.probe_status || '-')}</td><td>${format.date(item.last_seen_at)}</td><td>${format.date(item.last_probed_at)}</td></tr>`).join('');
}

function renderAuditSummaryTable() {
    if (!cliproxyElements.auditSummaryBody) return;
    if (!cliproxyAuditSummary.length) {
        cliproxyElements.auditSummaryBody.innerHTML = '<tr><td colspan="5" class="cliproxy-muted">当前还没有 CLIProxy 审计摘要。</td></tr>';
        return;
    }
    cliproxyElements.auditSummaryBody.innerHTML = cliproxyAuditSummary.map(item => `<tr><td>${format.date(item.timestamp)}</td><td>${escapeHtml(item.event_type || '-')}</td><td>${escapeHtml(item.service_name || (item.service_id != null ? String(item.service_id) : '-'))}</td><td>${escapeHtml(item.status || '-')}</td><td>${escapeHtml(item.message || '-')}</td></tr>`).join('');
}
