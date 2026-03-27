let cliproxyEnvironments = [];
let cliproxyRuns = [];
let cliproxyInventory = [];
let selectedEnvironmentId = null;

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
    testConnectionBtn: document.getElementById('cliproxy-test-connection-btn'),
    scanBtn: document.getElementById('cliproxy-scan-btn'),
    maintainBtn: document.getElementById('cliproxy-maintain-btn'),
    connectionResult: document.getElementById('cliproxy-connection-result'),
    selectionSummary: document.getElementById('cliproxy-selection-summary'),
    selectedDefault: document.getElementById('cliproxy-selected-default'),
    selectedTestStatus: document.getElementById('cliproxy-selected-test-status'),
    selectedScannedAt: document.getElementById('cliproxy-selected-scanned-at'),
    selectedMaintainedAt: document.getElementById('cliproxy-selected-maintained-at'),
    totalEnvironments: document.getElementById('cliproxy-total-environments'),
    enabledEnvironments: document.getElementById('cliproxy-enabled-environments'),
    runCount: document.getElementById('cliproxy-run-count'),
    inventoryCount: document.getElementById('cliproxy-inventory-count'),
    runEnvironmentName: document.getElementById('cliproxy-run-environment-name'),
    runHistoryBody: document.getElementById('cliproxy-run-history-body'),
    inventoryBody: document.getElementById('cliproxy-inventory-body'),
    inventorySummaryCount: document.getElementById('cliproxy-inventory-summary-count'),
};

document.addEventListener('DOMContentLoaded', () => {
    initCliproxyEventListeners();
    resetCliproxyForm();
    loadCliproxyEnvironments();
});

function initCliproxyEventListeners() {
    cliproxyElements.environmentForm?.addEventListener('submit', handleCliproxyFormSubmit);
    cliproxyElements.resetFormBtn?.addEventListener('click', resetCliproxyForm);
    cliproxyElements.newEnvironmentBtn?.addEventListener('click', resetCliproxyForm);
    cliproxyElements.refreshEnvironmentsBtn?.addEventListener('click', loadCliproxyEnvironments);
    cliproxyElements.refreshRunsBtn?.addEventListener('click', () => {
        if (selectedEnvironmentId) loadCliproxyRuns(selectedEnvironmentId);
    });
    cliproxyElements.refreshInventoryBtn?.addEventListener('click', () => {
        if (selectedEnvironmentId) loadCliproxyInventory(selectedEnvironmentId);
    });
    cliproxyElements.testConnectionBtn?.addEventListener('click', handleTestConnection);
    cliproxyElements.scanBtn?.addEventListener('click', () => handleMaintenanceAction('scan'));
    cliproxyElements.maintainBtn?.addEventListener('click', () => handleMaintenanceAction('maintain'));
    cliproxyElements.environmentList?.addEventListener('click', handleEnvironmentListClick);
}

async function loadCliproxyEnvironments() {
    try {
        cliproxyEnvironments = await api.get('/cliproxy-environments');
        renderCliproxyEnvironmentList();
        renderCliproxyStats();

        if (cliproxyEnvironments.length === 0) {
            selectedEnvironmentId = null;
            updateSelectedEnvironmentState();
            renderRunTable();
            renderInventoryTable();
            return;
        }

        const hasSelected = cliproxyEnvironments.some(item => item.id === selectedEnvironmentId);
        const nextEnvironment = hasSelected
            ? cliproxyEnvironments.find(item => item.id === selectedEnvironmentId)
            : cliproxyEnvironments.find(item => item.is_default) || cliproxyEnvironments[0];
        selectCliproxyEnvironment(nextEnvironment.id);
    } catch (error) {
        cliproxyElements.environmentList.innerHTML = '<div class="cliproxy-empty">加载环境列表失败，请稍后重试。</div>';
    }
}

function renderCliproxyEnvironmentList() {
    if (!cliproxyEnvironments.length) {
        cliproxyElements.environmentList.innerHTML = '<div class="cliproxy-empty">还没有配置 CLIProxyAPI 环境，先使用下方表单创建一个。</div>';
        return;
    }

    cliproxyElements.environmentList.innerHTML = cliproxyEnvironments.map(environment => `
        <button type="button" class="cliproxy-environment-item ${environment.id === selectedEnvironmentId ? 'is-active' : ''}" data-environment-id="${environment.id}">
            <div class="cliproxy-toolbar">
                <h4>${escapeHtml(environment.name || `环境 #${environment.id}`)}</h4>
                <span class="cliproxy-chip"><strong>${environment.enabled ? '启用' : '停用'}</strong><span>${environment.last_test_status || 'unknown'}</span></span>
            </div>
            <p>${escapeHtml(environment.base_url || '-')}</p>
            <div class="cliproxy-meta-row">
                <span class="cliproxy-chip"><strong>Target</strong><span>${escapeHtml(environment.target_type || '-')}</span></span>
                <span class="cliproxy-chip"><strong>Provider</strong><span>${escapeHtml(environment.provider || '-')}</span></span>
                <span class="cliproxy-chip"><strong>默认</strong><span>${environment.is_default ? '是' : '否'}</span></span>
            </div>
        </button>
    `).join('');
}

function renderCliproxyStats() {
    cliproxyElements.totalEnvironments.textContent = cliproxyEnvironments.length;
    cliproxyElements.enabledEnvironments.textContent = cliproxyEnvironments.filter(item => item.enabled).length;
    cliproxyElements.runCount.textContent = cliproxyRuns.length;
    cliproxyElements.inventoryCount.textContent = cliproxyInventory.length;
    cliproxyElements.inventorySummaryCount.textContent = cliproxyInventory.length;
}

function handleEnvironmentListClick(event) {
    const button = event.target.closest('[data-environment-id]');
    if (!button) return;
    selectCliproxyEnvironment(Number(button.dataset.environmentId));
}

function selectCliproxyEnvironment(environmentId) {
    selectedEnvironmentId = environmentId;
    const environment = getSelectedEnvironment();
    renderCliproxyEnvironmentList();
    populateCliproxyForm(environment);
    updateSelectedEnvironmentState();
    loadCliproxyRuns(environmentId);
    loadCliproxyInventory(environmentId);
}

function getSelectedEnvironment() {
    return cliproxyEnvironments.find(item => item.id === selectedEnvironmentId) || null;
}

function updateSelectedEnvironmentState() {
    const environment = getSelectedEnvironment();
    const hasEnvironment = Boolean(environment);
    cliproxyElements.testConnectionBtn.disabled = !hasEnvironment;
    cliproxyElements.scanBtn.disabled = !hasEnvironment;
    cliproxyElements.maintainBtn.disabled = !hasEnvironment;
    cliproxyElements.refreshRunsBtn.disabled = !hasEnvironment;
    cliproxyElements.refreshInventoryBtn.disabled = !hasEnvironment;

    cliproxyElements.selectionSummary.textContent = hasEnvironment
        ? `当前环境：${environment.name}，可以执行连接测试、扫描和维护。`
        : '请选择左侧环境以执行连接测试、扫描或维护。';
    cliproxyElements.selectedDefault.textContent = hasEnvironment ? (environment.is_default ? '是' : '否') : '-';
    cliproxyElements.selectedTestStatus.textContent = hasEnvironment ? (environment.last_test_status || 'unknown') : '-';
    cliproxyElements.selectedScannedAt.textContent = hasEnvironment ? format.date(environment.last_scanned_at) : '-';
    cliproxyElements.selectedMaintainedAt.textContent = hasEnvironment ? format.date(environment.last_maintained_at) : '-';
    cliproxyElements.runEnvironmentName.textContent = hasEnvironment ? environment.name : '未选择';
}

function populateCliproxyForm(environment) {
    if (!environment) {
        resetCliproxyForm();
        return;
    }

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
    cliproxyElements.connectionResult.textContent = environment.last_test_error
        ? `最近检测失败：${environment.last_test_error}`
        : '连接检测结果会显示在这里。';
    cliproxyElements.connectionResult.className = `cliproxy-connection-result ${environment.last_test_status === 'ok' ? 'is-ok' : environment.last_test_status === 'error' ? 'is-error' : ''}`;
}

function resetCliproxyForm() {
    cliproxyElements.environmentForm?.reset();
    cliproxyElements.environmentId.value = '';
    cliproxyElements.enabled.checked = true;
    cliproxyElements.connectionResult.textContent = '连接检测结果会显示在这里。';
    cliproxyElements.connectionResult.className = 'cliproxy-connection-result';
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
    if (!environmentId || token) {
        payload.token = token || null;
    }

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

async function handleTestConnection() {
    const environment = getSelectedEnvironment();
    if (!environment) return;

    loading.show(cliproxyElements.testConnectionBtn, '检测中...');
    try {
        const result = await api.post(`/cliproxy-environments/${environment.id}/test-connection`, {});
        cliproxyElements.connectionResult.textContent = result.status === 'ok'
            ? `连接成功，耗时 ${result.latency_ms} ms`
            : `连接失败：${result.error || '未知错误'}`;
        cliproxyElements.connectionResult.className = `cliproxy-connection-result ${result.status === 'ok' ? 'is-ok' : 'is-error'}`;
        toast[result.status === 'ok' ? 'success' : 'error'](cliproxyElements.connectionResult.textContent);
        await loadCliproxyEnvironments();
    } catch (error) {
        toast.error(error.message || '测试连接失败');
    } finally {
        loading.hide(cliproxyElements.testConnectionBtn);
    }
}

async function handleMaintenanceAction(runType) {
    const environment = getSelectedEnvironment();
    if (!environment) return;

    const button = runType === 'scan' ? cliproxyElements.scanBtn : cliproxyElements.maintainBtn;
    const path = runType === 'scan' ? 'scan' : 'maintain';
    const payload = runType === 'maintain' ? { dry_run: false } : {};

    loading.show(button, runType === 'scan' ? '扫描中...' : '维护中...');
    try {
        const run = await api.post(`/cliproxy-environments/${environment.id}/${path}`, payload);
        toast.success(`${runType === 'scan' ? '扫描' : '维护'}任务已提交 #${run.id}`);
        await Promise.all([
            loadCliproxyRuns(environment.id),
            loadCliproxyEnvironments(),
        ]);
    } catch (error) {
        toast.error(error.message || `${runType} 操作失败`);
    } finally {
        loading.hide(button);
    }
}

async function loadCliproxyRuns(environmentId) {
    try {
        cliproxyRuns = await api.get(`/cliproxy-environments/${environmentId}/runs`);
    } catch (error) {
        cliproxyRuns = [];
        toast.error('加载运行历史失败');
    }
    renderRunTable();
    renderCliproxyStats();
}

async function loadCliproxyInventory(environmentId) {
    try {
        cliproxyInventory = await api.get(`/cliproxy-environments/${environmentId}/inventory`);
    } catch (error) {
        cliproxyInventory = [];
        toast.error('加载远端库存失败');
    }
    renderInventoryTable();
    renderCliproxyStats();
}

function renderRunTable() {
    if (!selectedEnvironmentId) {
        cliproxyElements.runHistoryBody.innerHTML = '<tr><td colspan="7" class="cliproxy-muted">请选择环境查看运行记录。</td></tr>';
        return;
    }

    if (!cliproxyRuns.length) {
        cliproxyElements.runHistoryBody.innerHTML = '<tr><td colspan="7" class="cliproxy-muted">当前环境还没有运行记录。</td></tr>';
        return;
    }

    cliproxyElements.runHistoryBody.innerHTML = cliproxyRuns.map(run => `
        <tr>
            <td class="cliproxy-code">#${run.id}</td>
            <td>${escapeHtml(run.run_type || '-')}</td>
            <td>${escapeHtml(run.status || '-')}</td>
            <td>${escapeHtml(run.current_stage || '-')}</td>
            <td>${format.number(run.counters?.record_count || 0)}</td>
            <td>${format.date(run.created_at)}</td>
            <td>${format.date(run.completed_at)}</td>
        </tr>
    `).join('');
}

function renderInventoryTable() {
    if (!selectedEnvironmentId) {
        cliproxyElements.inventoryBody.innerHTML = '<tr><td colspan="7" class="cliproxy-muted">请选择环境查看库存快照。</td></tr>';
        return;
    }

    if (!cliproxyInventory.length) {
        cliproxyElements.inventoryBody.innerHTML = '<tr><td colspan="7" class="cliproxy-muted">当前环境还没有远端库存记录。</td></tr>';
        return;
    }

    cliproxyElements.inventoryBody.innerHTML = cliproxyInventory.map(item => `
        <tr>
            <td class="cliproxy-code">${escapeHtml(item.remote_file_id || '-')}</td>
            <td>${escapeHtml(item.email || '-')}</td>
            <td>${escapeHtml(item.remote_account_id || '-')}</td>
            <td>${escapeHtml(item.sync_state || '-')}</td>
            <td>${escapeHtml(item.probe_status || '-')}</td>
            <td>${format.date(item.last_seen_at)}</td>
            <td>${format.date(item.last_probed_at)}</td>
        </tr>
    `).join('');
}
