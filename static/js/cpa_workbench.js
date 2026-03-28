const cpaWorkbenchElements = {
    selector: document.getElementById('cpa-service-selector'),
    selectorList: document.getElementById('cpa-service-selector-list'),
    selectedServiceCount: document.getElementById('cpa-selected-service-count'),
    bulkTestConnectionBtn: document.getElementById('cpa-bulk-test-connection-btn'),
    bulkScanBtn: document.getElementById('cpa-bulk-scan-btn'),
    bulkActionBtn: document.getElementById('cpa-bulk-action-btn'),
    statsRegion: document.getElementById('cpa-stats-region'),
    listRegion: document.getElementById('cpa-credential-list-region'),
    credentialTableBody: document.getElementById('cpa-credential-table-body'),
    selectionNotice: document.getElementById('cpa-selection-notice'),
    taskRegion: document.getElementById('cpa-active-task-region'),
    taskProgressBar: document.getElementById('cpa-task-progress-bar'),
    taskLogList: document.getElementById('cpa-task-log-list'),
    latestTaskBootstrap: document.getElementById('cpa-latest-active-task-bootstrap'),
    selectionRecoveryBootstrap: document.getElementById('cpa-selection-recovery-bootstrap'),
    selectionNoticeBootstrap: document.getElementById('cpa-selection-notice-bootstrap')
};

const cpaWorkbenchState = {
    services: [],
    selectedServiceIds: [],
    inventoryRows: [],
    selectedCredentialDetail: null,
    selectedCredentialKey: null,
    selectionRecovery: readBootstrapJson(cpaWorkbenchElements.selectionRecoveryBootstrap),
    selectionNotice: readBootstrapJson(cpaWorkbenchElements.selectionNoticeBootstrap),
    latestTask: readBootstrapJson(cpaWorkbenchElements.latestTaskBootstrap),
    taskPollTimer: null
};

const CPA_TASK_POLL_MS = 3000;

document.addEventListener('DOMContentLoaded', () => {
    if (!cpaWorkbenchElements.selector) {
        return;
    }
    void initializeCpaWorkbenchSelector();
});

async function initializeCpaWorkbenchSelector() {
    try {
        const selectorPayload = await fetchJson('/api/cpa/services');
        cpaWorkbenchState.services = Array.isArray(selectorPayload.services) ? selectorPayload.services : [];
        cpaWorkbenchState.selectedServiceIds = normalizeSelectedServiceIds(
            cpaWorkbenchState.services,
            selectorPayload.selected_service_ids
        );
        if (selectorPayload.latest_active_task) {
            cpaWorkbenchState.latestTask = selectorPayload.latest_active_task;
        }
        renderServiceSelector();
        renderSelectionNotice(cpaWorkbenchState.selectionNotice);
        renderTaskPanel(cpaWorkbenchState.latestTask);
        bindDetailActions();
        await refreshActiveTaskPanel();
        await reloadWorkbenchScope();
    } catch (error) {
        renderSelectorError(error);
    }
}

function normalizeSelectedServiceIds(services, selectedServiceIds) {
    const selectableIds = new Set(
        services.filter((service) => service.selectable).map((service) => service.service_id)
    );
    return (Array.isArray(selectedServiceIds) ? selectedServiceIds : []).filter((serviceId) => selectableIds.has(serviceId));
}

function renderServiceSelector() {
    const { selectorList, selectedServiceCount } = cpaWorkbenchElements;
    selectedServiceCount.textContent = String(cpaWorkbenchState.selectedServiceIds.length);

    if (!selectorList) {
        return;
    }

    if (!cpaWorkbenchState.services.length) {
        selectorList.innerHTML = [
            '<div class="cpa-selector-option is-disabled">',
            '<span>',
            '<span class="cpa-selector-option-title">',
            '<strong>暂无已配置服务</strong>',
            '<span class="cpa-selector-state">empty</span>',
            '</span>',
            '<p>请先在设置中添加并启用至少一个 CPA 服务。</p>',
            '</span>',
            '</div>'
        ].join('');
        return;
    }

    selectorList.innerHTML = cpaWorkbenchState.services.map(renderServiceOption).join('');
    selectorList.querySelectorAll('input[type="checkbox"][data-service-id]').forEach((input) => {
        input.addEventListener('change', handleSelectorChange);
    });
    updatePrimaryActionBarState();
}

function buildCredentialKey(serviceId, credentialId) {
    return `${serviceId}::${credentialId}`;
}

function renderServiceOption(service) {
    const checked = cpaWorkbenchState.selectedServiceIds.includes(service.service_id) ? ' checked' : '';
    const disabled = service.selectable ? '' : ' disabled';
    const optionClass = service.selectable ? 'cpa-selector-option' : 'cpa-selector-option is-disabled';

    return [
        `<label class="${optionClass}">`,
        `<input type="checkbox" data-service-id="${service.service_id}"${checked}${disabled}>`,
        '<span>',
        '<span class="cpa-selector-option-title">',
        `<strong>${escapeHtml(service.service_name)}</strong>`,
        `<span class="cpa-selector-state">${escapeHtml(service.state)}</span>`,
        '</span>',
        `<p>${escapeHtml(service.status_message || '')}</p>`,
        '</span>',
        '</label>'
    ].join('');
}

async function handleSelectorChange() {
    const nextSelectedIds = Array.from(
        cpaWorkbenchElements.selectorList.querySelectorAll('input[type="checkbox"][data-service-id]:checked')
    ).map((input) => Number.parseInt(input.dataset.serviceId, 10)).filter(Number.isInteger);

    cpaWorkbenchState.selectedServiceIds = nextSelectedIds;
    cpaWorkbenchElements.selectedServiceCount.textContent = String(nextSelectedIds.length);
    updatePrimaryActionBarState();
    await reloadWorkbenchScope();
}

function updatePrimaryActionBarState() {
    const hasSelection = cpaWorkbenchState.selectedServiceIds.length > 0;
    if (cpaWorkbenchElements.bulkTestConnectionBtn) {
        cpaWorkbenchElements.bulkTestConnectionBtn.disabled = !hasSelection;
        cpaWorkbenchElements.bulkTestConnectionBtn.onclick = hasSelection ? () => handleBulkTestConnection() : null;
    }
    if (cpaWorkbenchElements.bulkScanBtn) {
        cpaWorkbenchElements.bulkScanBtn.disabled = !hasSelection;
        cpaWorkbenchElements.bulkScanBtn.onclick = hasSelection ? () => handleBulkScan() : null;
    }
    if (cpaWorkbenchElements.bulkActionBtn) {
        cpaWorkbenchElements.bulkActionBtn.disabled = !hasSelection;
        cpaWorkbenchElements.bulkActionBtn.onclick = hasSelection ? () => handleBulkAction() : null;
    }
}

async function handleBulkTestConnection() {
    await submitCpaJsonAction('/api/cpa/test-connection', {
        service_ids: cpaWorkbenchState.selectedServiceIds
    });
}

async function handleBulkScan() {
    await submitCpaJsonAction('/api/cpa/scan', {
        service_ids: cpaWorkbenchState.selectedServiceIds
    });
    await refreshActiveTaskPanel();
}

async function handleBulkAction() {
    await submitCpaJsonAction('/api/cpa/actions', {
        service_ids: cpaWorkbenchState.selectedServiceIds
    });
    await refreshActiveTaskPanel();
}

async function submitCpaJsonAction(url, payload) {
    const response = await fetch(url, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload),
        credentials: 'same-origin'
    });

    if (!response.ok) {
        let message = `Request failed: ${response.status}`;
        try {
            const payload = await response.json();
            if (payload?.detail?.message) {
                message = payload.detail.message;
            } else if (typeof payload?.detail === 'string') {
                message = payload.detail;
            }
        } catch (_) {
            // ignore parse failures
        }
        throw new Error(message);
    }

    return response.json();
}

async function reloadWorkbenchScope() {
    const query = new URLSearchParams();
    if (cpaWorkbenchState.selectedServiceIds.length) {
        query.set('service_ids', cpaWorkbenchState.selectedServiceIds.join(','));
    }

    const requestSuffix = query.toString() ? `?${query.toString()}` : '';
    const summaryPayload = await fetchJson(`/api/cpa/summary${requestSuffix}`);
    const inventoryPayload = await fetchJson(`/api/cpa/credentials${requestSuffix}`);
    renderSummaryRegion(summaryPayload || null);
    cpaWorkbenchState.inventoryRows = Array.isArray(inventoryPayload.rows) ? inventoryPayload.rows : [];
    applyRecoveredSelection(cpaWorkbenchState.inventoryRows);
    renderInventoryRegion(cpaWorkbenchState.inventoryRows);
    await refreshSelectedCredentialDetail();
    await refreshActiveTaskPanel();
}

function renderSummaryRegion(summary) {
    if (!cpaWorkbenchElements.statsRegion) {
        return;
    }

    const metrics = [
        ['总数', summary?.total ?? 0],
        ['可用', summary?.valid_count ?? 0],
        ['401', summary?.expired_count ?? 0],
        ['配额', summary?.quota_count ?? 0],
        ['异常', summary?.error_count ?? 0],
        ['未知', summary?.unknown_count ?? 0]
    ];

    cpaWorkbenchElements.statsRegion.innerHTML = `
        <div class="cpa-stat-grid">
            ${metrics.map(([label, value]) => `
                <div class="stat-card" data-stat-key="${escapeHtml(normalizeStatKey(label))}">
                    <div class="stat-value">${value}</div>
                    <div class="stat-label">${label}</div>
                </div>
            `).join('')}
        </div>
    `;
}

async function refreshActiveTaskPanel() {
    const taskTypes = ['scan', 'action'];
    let latestTask = null;

    for (const taskType of taskTypes) {
        try {
            const query = new URLSearchParams();
            query.set('type', taskType);
            if (cpaWorkbenchState.selectedServiceIds.length) {
                query.set('service_ids', cpaWorkbenchState.selectedServiceIds.join(','));
            } else {
                query.set('service_scope', 'empty');
            }
            const task = await fetchJson(`/api/cpa/tasks/latest-active?${query.toString()}`);
            if (!latestTask || Number.parseInt(task.task_id, 10) > Number.parseInt(latestTask.task_id, 10)) {
                latestTask = task;
            }
        } catch (error) {
            if (!(error instanceof Error) || !error.message.includes('404')) {
                throw error;
            }
        }
    }

    if (latestTask?.task_id) {
        latestTask = await fetchJson(`/api/cpa/tasks/${encodeURIComponent(latestTask.task_id)}`);
    }

    cpaWorkbenchState.latestTask = latestTask;
    renderTaskPanel(latestTask);
    scheduleTaskPanelPolling(latestTask);
}

function scheduleTaskPanelPolling(task) {
    if (cpaWorkbenchState.taskPollTimer) {
        window.clearTimeout(cpaWorkbenchState.taskPollTimer);
        cpaWorkbenchState.taskPollTimer = null;
    }

    if (!task || !['queued', 'running'].includes(task.status)) {
        return;
    }

    cpaWorkbenchState.taskPollTimer = window.setTimeout(() => {
        void refreshActiveTaskPanel();
    }, CPA_TASK_POLL_MS);
}

function renderTaskPanel(task) {
    if (!cpaWorkbenchElements.taskRegion) {
        return;
    }

    const taskData = task || {
        type: '-',
        total: 0,
        processed: 0,
        current_item: '暂无任务',
        progress_percent: 0,
        logs: [],
        stats: {}
    };

    setTaskField('type', formatConcreteTaskType(taskData));
    setTaskField('total', String(taskData.total ?? 0));
    setTaskField('processed', String(taskData.processed ?? 0));
    setTaskField('current_item', taskData.current_item || '暂无任务');

    if (cpaWorkbenchElements.taskProgressBar) {
        cpaWorkbenchElements.taskProgressBar.style.width = `${Math.max(0, Math.min(Number(taskData.progress_percent) || 0, 100))}%`;
    }

    renderTaskLogs(taskData.logs || []);
    updateConcurrencyControls(taskData.stats || {});
}

function setTaskField(fieldName, value) {
    const field = cpaWorkbenchElements.taskRegion?.querySelector(`[data-task-field="${fieldName}"] .cpa-task-field-value`);
    if (field) {
        field.textContent = value;
    }
}

function renderTaskLogs(logs) {
    if (!cpaWorkbenchElements.taskLogList) {
        return;
    }

    if (!logs.length) {
        cpaWorkbenchElements.taskLogList.innerHTML = '<li>暂无任务日志。</li>';
        return;
    }

    cpaWorkbenchElements.taskLogList.innerHTML = logs.map((line) => `<li>${escapeHtml(line)}</li>`).join('');
}

function updateConcurrencyControls(stats) {
    setConcurrencyValue('scan-concurrency', stats.scan_concurrency ?? 2);
    setConcurrencyValue('delete-concurrency', stats.delete_concurrency ?? 2);
    setConcurrencyValue('disable-concurrency', stats.disable_concurrency ?? 2);
}

function setConcurrencyValue(name, fallbackValue) {
    const input = document.querySelector(`input[name="${name}"]`);
    if (!input) {
        return;
    }
    input.value = String(fallbackValue || 2);
}

function renderInventoryRegion(rows) {
    if (!cpaWorkbenchElements.listRegion || !cpaWorkbenchElements.credentialTableBody) {
        return;
    }

    if (!rows.length) {
        cpaWorkbenchElements.credentialTableBody.innerHTML = '<tr data-inventory-scope="empty"><td colspan="7">当前范围内暂无远端凭据。</td></tr>';
        return;
    }

    cpaWorkbenchElements.credentialTableBody.innerHTML = rows.map((row) => {
        const rowKey = buildCredentialKey(row.service_id, row.credential_id);
        const selectedClass = cpaWorkbenchState.selectedCredentialKey === rowKey ? ' class="is-selected"' : '';
        return `
            <tr data-service-id="${row.service_id}" data-credential-id="${escapeHtml(row.credential_id)}"${selectedClass}>
                <td>${escapeHtml(row.credential_id)}</td>
                <td>${escapeHtml(row.service_name)}</td>
                <td>${escapeHtml(row.status)}</td>
                <td>${escapeHtml(row.quota_status)}</td>
                <td>${escapeHtml(formatDetailValue(row.last_scanned_at))}</td>
                <td>${escapeHtml(formatLocalAccountSummary(row.local_account_summary))}</td>
                <td>查看详情</td>
            </tr>
        `;
    }).join('');

    cpaWorkbenchElements.credentialTableBody.querySelectorAll('tr[data-service-id][data-credential-id]').forEach((rowElement) => {
        rowElement.addEventListener('click', handleCredentialRowSelection);
    });
}

function handleCredentialRowSelection(event) {
    const rowElement = event.currentTarget;
    const serviceId = Number.parseInt(rowElement.dataset.serviceId, 10);
    const credentialId = rowElement.dataset.credentialId;
    if (!Number.isInteger(serviceId) || !credentialId) {
        return;
    }
    cpaWorkbenchState.selectedCredentialKey = buildCredentialKey(serviceId, credentialId);
    renderInventoryRegion(cpaWorkbenchState.inventoryRows);
    void refreshSelectedCredentialDetail();
}

function applyRecoveredSelection(rows) {
    if (cpaWorkbenchState.selectedCredentialKey && rows.some(
        (row) => buildCredentialKey(row.service_id, row.credential_id) === cpaWorkbenchState.selectedCredentialKey
    )) {
        return;
    }

    const recovery = cpaWorkbenchState.selectionRecovery;
    if (recovery && recovery.service_id && recovery.credential_id) {
        const recoveredKey = buildCredentialKey(recovery.service_id, recovery.credential_id);
        if (rows.some((row) => buildCredentialKey(row.service_id, row.credential_id) === recoveredKey)) {
            cpaWorkbenchState.selectedCredentialKey = recoveredKey;
            cpaWorkbenchState.selectionRecovery = null;
            return;
        }
    }

    if (rows.length && !rows.some((row) => buildCredentialKey(row.service_id, row.credential_id) === cpaWorkbenchState.selectedCredentialKey)) {
        cpaWorkbenchState.selectedCredentialKey = buildCredentialKey(rows[0].service_id, rows[0].credential_id);
        return;
    }

    if (!rows.length) {
        cpaWorkbenchState.selectedCredentialKey = null;
    }
}

function renderSelectionNotice(notice) {
    if (!cpaWorkbenchElements.selectionNotice) {
        return;
    }

    const messageElement = cpaWorkbenchElements.selectionNotice.querySelector('p');
    if (!notice || !notice.message) {
        cpaWorkbenchElements.selectionNotice.classList.add('hidden');
        if (messageElement) {
            messageElement.textContent = '先前选中的凭据已不在当前视图中。';
        }
        return;
    }

    if (messageElement) {
        messageElement.textContent = notice.message;
    }
    cpaWorkbenchElements.selectionNotice.classList.remove('hidden');
}

function getSelectedCredentialRow() {
    if (!cpaWorkbenchState.selectedCredentialKey) {
        return null;
    }
    return cpaWorkbenchState.inventoryRows.find(
        (row) => buildCredentialKey(row.service_id, row.credential_id) === cpaWorkbenchState.selectedCredentialKey
    ) || null;
}

async function refreshSelectedCredentialDetail() {
    const row = getSelectedCredentialRow();
    if (!row) {
        cpaWorkbenchState.selectedCredentialDetail = null;
        renderDetailContext(null);
        return;
    }

    try {
        const detail = await fetchJson(`/api/cpa/credentials/${encodeURIComponent(row.service_id)}/${encodeURIComponent(row.credential_id)}`);
        cpaWorkbenchState.selectedCredentialDetail = detail;
        renderDetailContext(detail);
    } catch (error) {
        cpaWorkbenchState.selectedCredentialDetail = null;
        renderDetailContext(row);
    }
}

function renderDetailContext(detail) {
    setDetailField('credential_id', detail?.credential_id || '-');
    setDetailField('service_name', detail?.service_name || '-');
    setDetailField('status', detail?.status || '-');
    setDetailField('quota_status', detail?.quota_status || '-');
    setDetailField('last_scanned_at', formatDetailValue(detail?.last_scanned_at));
    setDetailField('local_account_summary', formatLocalAccountSummary(detail?.local_account_summary));
    setDetailField('detail_status_summary', detail?.status_summary || '-');
    setDetailField('detail_default_action', detail?.default_action || '-');
    renderRecentLogExcerpt(detail?.recent_log_excerpt || []);
    setDetailLogLink(detail?.view_logs_target || '#');
}

function setDetailField(fieldName, value) {
    const field = document.querySelector(`[data-detail-field="${fieldName}"]`);
    if (field) {
        field.textContent = value;
    }
}

function renderRecentLogExcerpt(lines) {
    const logList = document.querySelector('[data-detail-field="recent_log_excerpt"]');
    if (!logList) {
        return;
    }

    if (!Array.isArray(lines) || !lines.length) {
        logList.innerHTML = '<li>暂无日志摘录。</li>';
        return;
    }

    logList.innerHTML = lines.map((line) => `<li>${escapeHtml(line)}</li>`).join('');
}

function setDetailLogLink(target) {
    const link = document.querySelector('[data-detail-action="view-logs"]');
    if (!link) {
        return;
    }
    link.setAttribute('href', target || '#');
}

function bindDetailActions() {
    document.querySelectorAll('[data-detail-action]').forEach((element) => {
        element.addEventListener('click', handleDetailAction);
    });
}

async function handleDetailAction(event) {
    const action = event.currentTarget.dataset.detailAction;
    const detail = cpaWorkbenchState.selectedCredentialDetail;
    if (!action || !detail || !detail.service_id || !detail.credential_id) {
        if (action === 'view-logs') {
            event.preventDefault();
        }
        return;
    }

    if (action === 'view-logs') {
        return;
    }

    event.preventDefault();
    if (action === 'scan-single') {
        await fetchJson('/api/cpa/scan', {
            method: 'POST',
            body: JSON.stringify({
                service_ids: [detail.service_id],
                credential_ids: [detail.credential_id],
                concurrency: readConcurrencyValue('scan-concurrency', 2)
            })
        });
    } else if (action === 'delete-single' || action === 'disable-single') {
        await fetchJson('/api/cpa/actions', {
            method: 'POST',
            body: JSON.stringify({
                service_ids: [detail.service_id],
                credential_ids: [detail.credential_id],
                quota_action: action === 'delete-single' ? 'delete' : 'disable',
                delete_concurrency: readConcurrencyValue('delete-concurrency', 2),
                disable_concurrency: readConcurrencyValue('disable-concurrency', 2)
            })
        });
    }
    await refreshActiveTaskPanel();
}

function readConcurrencyValue(name, fallbackValue) {
    const input = document.querySelector(`input[name="${name}"]`);
    const parsed = Number.parseInt(input?.value || '', 10);
    return Number.isInteger(parsed) && parsed > 0 ? parsed : fallbackValue;
}

function formatDetailValue(value) {
    return value || '-';
}

function formatLocalAccountSummary(summary) {
    if (!summary) {
        return '-';
    }
    const pieces = [summary.email, summary.status].filter(Boolean);
    return pieces.length ? pieces.join(' / ') : '-';
}

function renderSelectorError(error) {
    if (cpaWorkbenchElements.selectorList) {
        cpaWorkbenchElements.selectorList.innerHTML = `
            <div class="cpa-selector-option is-disabled">
                <span>
                    <span class="cpa-selector-option-title">
                        <strong>服务加载失败</strong>
                        <span class="cpa-selector-state">error</span>
                    </span>
                    <p>${escapeHtml(error instanceof Error ? error.message : 'Unknown error')}</p>
                </span>
            </div>
        `;
    }
}

function formatTaskType(taskType) {
    if (taskType === 'scan') {
        return '扫描任务';
    }
    if (taskType === 'action') {
        return '操作任务';
    }
    return '-';
}

function formatConcreteTaskType(task) {
    if (!task || !task.type) {
        return '-';
    }
    if (task.type === 'scan') {
        return '扫描任务';
    }
    if (task.type === 'action') {
        if (task.stats?.quota_action === 'delete') {
            return '删除任务';
        }
        if (task.stats?.quota_action === 'disable') {
            return '禁用任务';
        }
    }
    return formatTaskType(task.type);
}

function normalizeStatKey(label) {
    if (label === '总数') {
        return 'total';
    }
    if (label === '可用') {
        return 'valid_count';
    }
    if (label === '401') {
        return 'expired_count';
    }
    if (label === '配额') {
        return 'quota_count';
    }
    if (label === '异常') {
        return 'error_count';
    }
    return 'unknown_count';
}

function readBootstrapJson(element) {
    if (!element || !element.textContent) {
        return null;
    }

    try {
        return JSON.parse(element.textContent);
    } catch (error) {
        return null;
    }
}

async function fetchJson(url, options = null) {
    const response = await fetch(url, {
        method: options?.method || 'GET',
        credentials: 'same-origin',
        headers: {
            Accept: 'application/json',
            ...(options?.body ? { 'Content-Type': 'application/json' } : {})
        },
        body: options?.body || undefined
    });

    if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
    }

    return response.json();
}

function escapeHtml(value) {
    return String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
}
