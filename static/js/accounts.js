/**
 * 账号管理页面 JavaScript
 * 使用 utils.js 中的工具库
 */

// 状态
let currentPage = 1;
let pageSize = 20;
let totalAccounts = 0;
let selectedAccounts = new Set();
let isLoading = false;
let selectAllPages = false;  // 是否选中了全部页
let currentFilters = { status: '', email_service: '', search: '' };  // 当前筛选条件
let currentAccounts = [];
let activeAccountId = null;
let batchCheckTaskState = {
    taskId: null,
    scopeKey: null,
    logOffset: 0,
    pollTimer: null,
    isPolling: false,
    completionRefreshDone: false
};

// DOM 元素
const elements = {
    table: document.getElementById('accounts-table'),
    totalAccounts: document.getElementById('total-accounts'),
    activeAccounts: document.getElementById('active-accounts'),
    expiredAccounts: document.getElementById('expired-accounts'),
    failedAccounts: document.getElementById('failed-accounts'),
    filterStatus: document.getElementById('filter-status'),
    filterService: document.getElementById('filter-service'),
    searchInput: document.getElementById('search-input'),
    refreshBtn: document.getElementById('refresh-btn'),
    batchRefreshBtn: document.getElementById('batch-refresh-btn'),
    batchValidateBtn: document.getElementById('batch-validate-btn'),
    batchUploadBtn: document.getElementById('batch-upload-btn'),
    batchCheckSubBtn: document.getElementById('batch-check-sub-btn'),
    batchDeleteBtn: document.getElementById('batch-delete-btn'),
    exportBtn: document.getElementById('export-btn'),
    exportMenu: document.getElementById('export-menu'),
    selectAll: document.getElementById('select-all'),
    prevPage: document.getElementById('prev-page'),
    nextPage: document.getElementById('next-page'),
    pageInfo: document.getElementById('page-info'),
    closeModal: document.getElementById('close-modal'),
    workbench: document.getElementById('accounts-workbench'),
    listPageSummary: document.getElementById('list-page-summary'),
    selectionBannerAnchor: document.getElementById('accounts-selection-banner-anchor'),
    bulkBar: document.getElementById('accounts-bulk-bar'),
    bulkSelectionCount: document.getElementById('bulk-selection-count'),
    bulkSelectionContext: document.getElementById('bulk-selection-context'),
    detailIdentityTitle: document.getElementById('detail-identity-title'),
    detailIdentitySubtitle: document.getElementById('detail-identity-subtitle'),
    secondaryDetailRegion: document.getElementById('account-secondary-detail-region'),
    detailSubscriptionQuota: document.getElementById('detail-subscription-quota'),
    detailCoreOps: document.getElementById('detail-core-ops'),
    detailAutomationTrace: document.getElementById('detail-automation-trace'),
    detailCliProxySummary: document.getElementById('detail-cliproxy-summary'),
    batchCheckTaskPanel: document.getElementById('batch-check-task-panel'),
    batchCheckTaskStatus: document.getElementById('batch-check-task-status'),
    batchCheckTaskSummary: document.getElementById('batch-check-task-summary'),
    batchCheckTaskTotalCount: document.getElementById('batch-check-task-total-count'),
    batchCheckTaskProcessedCount: document.getElementById('batch-check-task-processed-count'),
    batchCheckTaskSuccessCount: document.getElementById('batch-check-task-success-count'),
    batchCheckTaskFailureCount: document.getElementById('batch-check-task-failure-count'),
    batchCheckTaskProgressBar: document.getElementById('batch-check-task-progress-bar'),
    batchCheckTaskProgressText: document.getElementById('batch-check-task-progress-text'),
    batchCheckTaskCurrentAccount: document.getElementById('batch-check-task-current-account'),
    batchCheckTaskCurrentAccountValue: document.getElementById('batch-check-task-current-account-value'),
    batchCheckTaskLogList: document.getElementById('batch-check-task-log-list'),
    batchCheckTaskLogEmpty: document.getElementById('batch-check-task-log-empty')
};

// 初始化
document.addEventListener('DOMContentLoaded', async () => {
    loadStats();
    initEventListeners();
    updateBatchButtons();  // 初始化按钮状态
    renderSelectAllBanner();
    await loadAccounts();
    await restoreLatestBatchCheckTask();
});

// 事件监听
function initEventListeners() {
    // 筛选
    elements.filterStatus.addEventListener('change', () => {
        currentPage = 1;
        resetSelectAllPages();
        loadAccounts();
    });

    elements.filterService.addEventListener('change', () => {
        currentPage = 1;
        resetSelectAllPages();
        loadAccounts();
    });

    // 搜索（防抖）
    elements.searchInput.addEventListener('input', debounce(() => {
        currentPage = 1;
        resetSelectAllPages();
        loadAccounts();
    }, 300));

    // 快捷键聚焦搜索
    elements.searchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            elements.searchInput.blur();
            elements.searchInput.value = '';
            resetSelectAllPages();
            loadAccounts();
        }
    });

    // 刷新
    elements.refreshBtn.addEventListener('click', () => {
        loadStats();
        loadAccounts();
        toast.info('已刷新');
    });

    // 批量刷新Token
    elements.batchRefreshBtn.addEventListener('click', handleBatchRefresh);

    // 批量验证Token
    elements.batchValidateBtn.addEventListener('click', handleBatchValidate);

    // 批量检测订阅
    elements.batchCheckSubBtn.addEventListener('click', handleBatchCheckSubscription);

    // 上传下拉菜单
    const uploadMenu = document.getElementById('upload-menu');
    elements.batchUploadBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        uploadMenu.classList.toggle('active');
    });
    document.getElementById('batch-upload-cpa-item').addEventListener('click', (e) => { e.preventDefault(); uploadMenu.classList.remove('active'); handleBatchUploadCpa(); });
    document.getElementById('batch-upload-sub2api-item').addEventListener('click', (e) => { e.preventDefault(); uploadMenu.classList.remove('active'); handleBatchUploadSub2Api(); });
    document.getElementById('batch-upload-tm-item').addEventListener('click', (e) => { e.preventDefault(); uploadMenu.classList.remove('active'); handleBatchUploadTm(); });

    // 批量删除
    elements.batchDeleteBtn.addEventListener('click', handleBatchDelete);

    // 全选（当前页）
    elements.selectAll.addEventListener('change', (e) => {
        const checkboxes = elements.table.querySelectorAll('input[type="checkbox"][data-id]');
        checkboxes.forEach(cb => {
            cb.checked = e.target.checked;
            const id = parseInt(cb.dataset.id);
            if (e.target.checked) {
                selectedAccounts.add(id);
            } else {
                selectedAccounts.delete(id);
            }
        });
        if (!e.target.checked) {
            selectAllPages = false;
        }
        updateBatchButtons();
        renderSelectAllBanner();
    });

    // 分页
    elements.prevPage.addEventListener('click', () => {
        if (currentPage > 1 && !isLoading) {
            currentPage--;
            loadAccounts();
        }
    });

    elements.nextPage.addEventListener('click', () => {
        const totalPages = Math.ceil(totalAccounts / pageSize);
        if (currentPage < totalPages && !isLoading) {
            currentPage++;
            loadAccounts();
        }
    });

    // 导出
    elements.exportBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        elements.exportMenu.classList.toggle('active');
    });

    delegate(elements.exportMenu, 'click', '.dropdown-item', (e, target) => {
        e.preventDefault();
        const format = target.dataset.format;
        exportAccounts(format);
        elements.exportMenu.classList.remove('active');
    });

    if (elements.closeModal) {
        elements.closeModal.addEventListener('click', () => {
            activeAccountId = null;
            renderAccounts(currentAccounts);
            renderEmptyDetailPanel();
        });
    }

    // 点击其他地方关闭下拉菜单
    document.addEventListener('click', () => {
        elements.exportMenu.classList.remove('active');
        uploadMenu.classList.remove('active');
        document.querySelectorAll('#accounts-table .dropdown-menu.active').forEach(m => m.classList.remove('active'));
    });
}

// 加载统计信息
async function loadStats() {
    try {
        const data = await api.get('/accounts/stats/summary');

        elements.totalAccounts.textContent = format.number(data.total || 0);
        elements.activeAccounts.textContent = format.number(data.by_status?.active || 0);
        elements.expiredAccounts.textContent = format.number(data.by_status?.expired || 0);
        elements.failedAccounts.textContent = format.number(data.by_status?.failed || 0);

        // 添加动画效果
        animateValue(elements.totalAccounts, data.total || 0);
    } catch (error) {
        console.error('加载统计信息失败:', error);
    }
}

// 数字动画
function animateValue(element, value) {
    element.style.transition = 'transform 0.2s ease';
    element.style.transform = 'scale(1.1)';
    setTimeout(() => {
        element.style.transform = 'scale(1)';
    }, 200);
}

// 加载账号列表
async function loadAccounts() {
    if (isLoading) return;
    isLoading = true;

    // 显示加载状态
    elements.table.innerHTML = `
        <tr>
            <td colspan="9">
                <div class="empty-state">
                    <div class="skeleton skeleton-text" style="width: 60%;"></div>
                    <div class="skeleton skeleton-text" style="width: 80%;"></div>
                    <div class="skeleton skeleton-text" style="width: 40%;"></div>
                </div>
            </td>
        </tr>
    `;

    // 记录当前筛选条件
    currentFilters.status = elements.filterStatus.value;
    currentFilters.email_service = elements.filterService.value;
    currentFilters.search = elements.searchInput.value.trim();

    const params = new URLSearchParams({
        page: currentPage,
        page_size: pageSize,
    });

    if (batchCheckTaskState.taskId && batchCheckTaskState.completionRefreshDone) {
        params.append('refresh_task_id', batchCheckTaskState.taskId);
    }

    if (currentFilters.status) {
        params.append('status', currentFilters.status);
    }

    if (currentFilters.email_service) {
        params.append('email_service', currentFilters.email_service);
    }

    if (currentFilters.search) {
        params.append('search', currentFilters.search);
    }

    try {
        const data = await api.get(`/accounts?${params}`);
        totalAccounts = data.total;
        currentAccounts = data.accounts;
        if (!batchCheckTaskState.taskId && data.latest_batch_check_task?.task_id) {
            batchCheckTaskState.taskId = data.latest_batch_check_task.task_id;
            batchCheckTaskState.scopeKey = data.latest_batch_check_task.scope_key || null;
        }
        if (Array.isArray(data.refreshed_account_ids) && data.refreshed_account_ids.length > 0) {
            batchCheckTaskState.completionRefreshDone = false;
        }
        if (activeAccountId && !currentAccounts.some(account => account.id === activeAccountId)) {
            activeAccountId = null;
        }
        renderAccounts(data.accounts);
        updatePagination();
    } catch (error) {
        console.error('加载账号列表失败:', error);
        elements.table.innerHTML = `
            <tr>
                <td colspan="9">
                    <div class="empty-state">
                        <div class="empty-state-icon">❌</div>
                        <div class="empty-state-title">加载失败</div>
                        <div class="empty-state-description">请检查网络连接后重试</div>
                    </div>
                </td>
            </tr>
        `;
    } finally {
        isLoading = false;
    }
}

// 渲染账号列表
function renderAccounts(accounts) {
    if (elements.listPageSummary) {
        elements.listPageSummary.textContent = `第 ${currentPage} 页`;
    }

    if (accounts.length === 0) {
        elements.table.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📭</div>
                <div class="empty-state-title">暂无数据</div>
                <div class="empty-state-description">没有找到符合条件的账号记录</div>
            </div>
        `;
        renderEmptyDetailPanel();
        return;
    }

    elements.table.innerHTML = accounts.map(account => `
        <article class="account-row-card ${activeAccountId === account.id ? 'is-active' : ''}" data-id="${account.id}">
            <div class="account-select-cell">
                <input type="checkbox" data-id="${account.id}"
                    ${selectedAccounts.has(account.id) ? 'checked' : ''}>
            </div>
            <div class="account-primary">
                <div class="account-title-row">
                    <button type="button" class="account-email-button" data-view-account="${account.id}" title="查看详情">${escapeHtml(account.email)}</button>
                    <button class="btn-copy-icon copy-email-btn" data-email="${escapeHtml(account.email)}" title="复制邮箱">📋</button>
                </div>
                <div class="account-primary-columns">
                    <div class="micro-panel" data-primary-column="status">
                        <h4>状态</h4>
                        <p>${escapeHtml(getStatusText('account', account.status))}；${formatRiskPanel(account)}</p>
                    </div>
                    <div class="micro-panel" data-primary-column="subscription">
                        <h4>订阅</h4>
                        <p>${formatSubscriptionPanel(account)}</p>
                    </div>
                    <div class="micro-panel" data-primary-column="recent-activity">
                        <h4>最近活动</h4>
                        <p>${formatRecentActivityPanel(account)}</p>
                    </div>
                    <div class="micro-panel" data-primary-column="source-target">
                        <h4>Source / Target</h4>
                        <p>${formatSourceTargetPanel(account)}</p>
                    </div>
                </div>
                <div class="account-status-row">
                    <span class="summary-pill"><strong>${escapeHtml(account.platform_source || 'unknown')}</strong><span>source</span></span>
                    <span class="summary-pill"><strong>${escapeHtml(account.last_upload_target || '未设置')}</strong><span>target summary</span></span>
                </div>
            </div>
            <div class="account-actions">
                <button class="btn btn-secondary btn-sm" onclick="viewAccount(${account.id})">详情</button>
                <button class="btn btn-primary btn-sm" onclick="refreshToken(${account.id})">刷新</button>
                <button class="btn btn-secondary btn-sm" onclick="uploadAccount(${account.id})">上传</button>
                <button class="btn btn-secondary btn-sm" onclick="markSubscription(${account.id})">标记订阅</button>
                <button class="btn btn-secondary btn-sm" onclick="checkInboxCode(${account.id})">收件箱</button>
                <div style="display:flex;gap:4px;align-items:center;white-space:nowrap;">
                    <div class="dropdown" style="position:relative;">
                        <button class="btn btn-secondary btn-sm" onclick="event.stopPropagation();toggleMoreMenu(this)">更多</button>
                        <div class="dropdown-menu" style="min-width:100px;">
                            <a href="#" class="dropdown-item" onclick="event.preventDefault();closeMoreMenu(this);refreshToken(${account.id})">刷新</a>
                            <a href="#" class="dropdown-item" onclick="event.preventDefault();closeMoreMenu(this);uploadAccount(${account.id})">上传</a>
                            <a href="#" class="dropdown-item" onclick="event.preventDefault();closeMoreMenu(this);markSubscription(${account.id})">标记</a>
                            <a href="#" class="dropdown-item" onclick="event.preventDefault();closeMoreMenu(this);checkInboxCode(${account.id})">收件箱</a>
                        </div>
                    </div>
                    <button class="btn btn-danger btn-sm" onclick="deleteAccount(${account.id}, '${escapeHtml(account.email)}')">删除</button>
                </div>
            </div>
        </article>
    `).join('');

    // 绑定复选框事件
    elements.table.querySelectorAll('input[type="checkbox"][data-id]').forEach(cb => {
        cb.addEventListener('change', (e) => {
            const id = parseInt(e.target.dataset.id);
            if (e.target.checked) {
                selectedAccounts.add(id);
            } else {
                selectedAccounts.delete(id);
                selectAllPages = false;
            }
            // 同步全选框状态
            const allChecked = elements.table.querySelectorAll('input[type="checkbox"][data-id]');
            const checkedCount = elements.table.querySelectorAll('input[type="checkbox"][data-id]:checked').length;
            elements.selectAll.checked = allChecked.length > 0 && checkedCount === allChecked.length;
            elements.selectAll.indeterminate = checkedCount > 0 && checkedCount < allChecked.length;
            updateBatchButtons();
            renderSelectAllBanner();
        });
    });

    // 绑定复制邮箱按钮
    elements.table.querySelectorAll('.copy-email-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            copyToClipboard(btn.dataset.email);
        });
    });

    elements.table.querySelectorAll('[data-view-account]').forEach(btn => {
        btn.addEventListener('click', () => viewAccount(parseInt(btn.dataset.viewAccount, 10)));
    });

    // 绑定复制密码按钮
    elements.table.querySelectorAll('.copy-pwd-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            copyToClipboard(btn.dataset.pwd);
        });
    });

    // 渲染后同步全选框状态
    const allCbs = elements.table.querySelectorAll('input[type="checkbox"][data-id]');
    const checkedCbs = elements.table.querySelectorAll('input[type="checkbox"][data-id]:checked');
    elements.selectAll.checked = allCbs.length > 0 && checkedCbs.length === allCbs.length;
    elements.selectAll.indeterminate = checkedCbs.length > 0 && checkedCbs.length < allCbs.length;
    renderSelectAllBanner();

    if (activeAccountId) {
        const activeAccount = accounts.find(account => account.id === activeAccountId);
        if (activeAccount) {
            updateDetailIdentity(activeAccount);
        }
    } else {
        renderEmptyDetailPanel();
    }
}

// 切换密码显示
function togglePassword(element, password) {
    if (element.dataset.revealed === 'true') {
        element.textContent = password.substring(0, 4) + '****';
        element.classList.add('password-hidden');
        element.dataset.revealed = 'false';
    } else {
        element.textContent = password;
        element.classList.remove('password-hidden');
        element.dataset.revealed = 'true';
    }
}

// 更新分页
function updatePagination() {
    const totalPages = Math.max(1, Math.ceil(totalAccounts / pageSize));

    elements.prevPage.disabled = currentPage <= 1;
    elements.nextPage.disabled = currentPage >= totalPages;

    elements.pageInfo.textContent = `第 ${currentPage} 页 / 共 ${totalPages} 页`;
    if (elements.listPageSummary) {
        elements.listPageSummary.textContent = `第 ${currentPage} 页 / 共 ${totalPages} 页`;
    }
}

// 重置全选所有页状态
function resetSelectAllPages() {
    selectAllPages = false;
    selectedAccounts.clear();
    updateBatchButtons();
    renderSelectAllBanner();
}

// 构建批量请求体（含 select_all 和筛选参数）
function buildBatchPayload(extraFields = {}) {
    if (selectAllPages) {
        return {
            ids: [],
            select_all: true,
            status_filter: currentFilters.status || null,
            email_service_filter: currentFilters.email_service || null,
            search_filter: currentFilters.search || null,
            ...extraFields
        };
    }
    return { ids: Array.from(selectedAccounts), ...extraFields };
}

// 获取有效选中数量（select_all 时用总数）
function getEffectiveCount() {
    return selectAllPages ? totalAccounts : selectedAccounts.size;
}

// 渲染全选横幅
function renderSelectAllBanner() {
    let banner = document.getElementById('select-all-banner');
    const totalPages = Math.ceil(totalAccounts / pageSize);
    const currentPageSize = elements.table.querySelectorAll('input[type="checkbox"][data-id]').length;
    const checkedOnPage = elements.table.querySelectorAll('input[type="checkbox"][data-id]:checked').length;
    const allPageSelected = currentPageSize > 0 && checkedOnPage === currentPageSize;

    // 只在全选了当前页且有多页时显示横幅
    if (!allPageSelected || totalPages <= 1 || totalAccounts <= pageSize) {
        if (banner) banner.remove();
        return;
    }

    if (!banner) {
        banner = document.createElement('div');
        banner.id = 'select-all-banner';
        banner.style.cssText = 'background:var(--primary-light,#e8f0fe);color:var(--primary-color,#1a73e8);padding:8px 16px;text-align:center;font-size:0.875rem;border-bottom:1px solid var(--border-color);';
        const bannerAnchor = elements.selectionBannerAnchor;
        if (bannerAnchor) {
            bannerAnchor.insertAdjacentElement('afterend', banner);
        }
    }

    if (selectAllPages) {
        banner.innerHTML = `已选中全部 <strong>${totalAccounts}</strong> 条记录。<button onclick="resetSelectAllPages()" style="margin-left:8px;color:var(--primary-color,#1a73e8);background:none;border:none;cursor:pointer;text-decoration:underline;">取消全选</button>`;
    } else {
        banner.innerHTML = `当前页已全选 <strong>${checkedOnPage}</strong> 条。<button onclick="selectAllPagesAction()" style="margin-left:8px;color:var(--primary-color,#1a73e8);background:none;border:none;cursor:pointer;text-decoration:underline;">选择全部 ${totalAccounts} 条</button>`;
    }
}

// 选中所有页
function selectAllPagesAction() {
    selectAllPages = true;
    updateBatchButtons();
    renderSelectAllBanner();
}

// 更新批量操作按钮
function updateBatchButtons() {
    const count = getEffectiveCount();
    elements.batchDeleteBtn.disabled = count === 0;
    elements.batchRefreshBtn.disabled = count === 0;
    elements.batchValidateBtn.disabled = count === 0;
    elements.batchUploadBtn.disabled = count === 0;
    elements.batchCheckSubBtn.disabled = count === 0;
    elements.exportBtn.disabled = count === 0;

    elements.batchDeleteBtn.textContent = count > 0 ? `🗑️ 删除 (${count})` : '🗑️ 批量删除';
    elements.batchRefreshBtn.textContent = count > 0 ? `🔄 刷新 (${count})` : '🔄 刷新Token';
    elements.batchValidateBtn.textContent = count > 0 ? `✅ 验证 (${count})` : '✅ 验证Token';
    elements.batchUploadBtn.textContent = count > 0 ? `☁️ 上传 (${count})` : '☁️ 上传';
    elements.batchCheckSubBtn.textContent = count > 0 ? `🔍 检测 (${count})` : '🔍 检测订阅';

    if (elements.bulkBar && elements.bulkSelectionCount && elements.bulkSelectionContext) {
        elements.bulkBar.classList.toggle('is-visible', count > 0);
        elements.bulkSelectionCount.textContent = count > 0 ? `已选择 ${count} 个账号` : '未选择账号';
        elements.bulkSelectionContext.textContent = selectAllPages
            ? '当前批量操作将应用到全部筛选结果。'
            : '支持刷新、验证、检测订阅、上传、导出和删除。';
    }
}

function isBatchTaskActiveStatus(status) {
    return status === 'queued' || status === 'running';
}

function clearBatchCheckPollTimer() {
    if (batchCheckTaskState.pollTimer) {
        clearTimeout(batchCheckTaskState.pollTimer);
        batchCheckTaskState.pollTimer = null;
    }
}

function showBatchCheckTaskPanel() {
    if (!elements.batchCheckTaskPanel) return;
    elements.batchCheckTaskPanel.hidden = false;
    elements.batchCheckTaskPanel.classList.add('is-visible');
}

function renderBatchCheckLogs(logs = [], append = false) {
    if (!elements.batchCheckTaskLogList || !elements.batchCheckTaskLogEmpty) return;
    if (!append) {
        elements.batchCheckTaskLogList.innerHTML = '';
    }
    logs.forEach((line) => {
        const item = document.createElement('li');
        item.textContent = line;
        elements.batchCheckTaskLogList.appendChild(item);
    });
    const hasLogs = elements.batchCheckTaskLogList.children.length > 0;
    elements.batchCheckTaskLogEmpty.hidden = hasLogs;
    elements.batchCheckTaskLogList.hidden = !hasLogs;
    if (hasLogs) {
        elements.batchCheckTaskLogList.scrollTop = elements.batchCheckTaskLogList.scrollHeight;
    }
}

function renderBatchCheckTaskPanel(task, options = {}) {
    if (!task || !elements.batchCheckTaskPanel) return;
    const appendLogs = Boolean(options.appendLogs);
    const totalCount = Number(task.total_count || 0);
    const processedCount = Number(task.processed_count || 0);
    const successCount = Number(task.success_count || 0);
    const failureCount = Number(task.failure_count || 0);
    const progressPercent = Math.max(0, Math.min(100, Number(task.progress_percent || 0)));
    const currentAccount = task.current_account || '等待任务开始';
    const status = task.status || 'queued';

    batchCheckTaskState.taskId = task.task_id || batchCheckTaskState.taskId;
    if (task.scope_key) {
        batchCheckTaskState.scopeKey = task.scope_key;
    }
    if (typeof task.next_log_offset === 'number') {
        batchCheckTaskState.logOffset = task.next_log_offset;
    }

    showBatchCheckTaskPanel();
    elements.batchCheckTaskStatus.innerHTML = `<strong>${escapeHtml(status)}</strong><span>task status</span>`;
    elements.batchCheckTaskTotalCount.textContent = format.number(totalCount);
    elements.batchCheckTaskProcessedCount.textContent = format.number(processedCount);
    elements.batchCheckTaskSuccessCount.textContent = format.number(successCount);
    elements.batchCheckTaskFailureCount.textContent = format.number(failureCount);
    elements.batchCheckTaskProgressText.textContent = `${progressPercent}%`;
    elements.batchCheckTaskProgressBar.style.width = `${progressPercent}%`;
    elements.batchCheckTaskProgressBar.parentElement?.setAttribute('aria-valuenow', String(progressPercent));
    elements.batchCheckTaskCurrentAccountValue.textContent = currentAccount;
    renderBatchCheckLogs(task.logs || [], appendLogs);
}

async function pollBatchCheckTask(taskId, options = {}) {
    if (!taskId || batchCheckTaskState.isPolling) return;
    const resetLogs = Boolean(options.resetLogs);
    batchCheckTaskState.isPolling = true;
    clearBatchCheckPollTimer();

    try {
        const logOffset = resetLogs ? 0 : batchCheckTaskState.logOffset;
        const payload = await api.get(`/payment/tasks/${taskId}?log_offset=${logOffset}`);
        renderBatchCheckTaskPanel(payload, { appendLogs: !resetLogs && logOffset > 0 });

        if (isBatchTaskActiveStatus(payload.status)) {
            batchCheckTaskState.pollTimer = setTimeout(() => {
                batchCheckTaskState.isPolling = false;
                pollBatchCheckTask(taskId);
            }, 2000);
            return;
        }

        batchCheckTaskState.isPolling = false;
        clearBatchCheckPollTimer();
        if (!batchCheckTaskState.completionRefreshDone && payload.status === 'completed') {
            batchCheckTaskState.completionRefreshDone = true;
            await loadAccounts();
        }
        updateBatchButtons();
    } catch (error) {
        batchCheckTaskState.isPolling = false;
        clearBatchCheckPollTimer();
        toast.error('任务状态查询失败: ' + error.message);
    }
}

async function restoreLatestBatchCheckTask() {
    try {
        let result = null;
        if (batchCheckTaskState.taskId) {
            result = {
                task_id: batchCheckTaskState.taskId,
                scope_key: batchCheckTaskState.scopeKey,
            };
        } else {
            const listPayload = await api.get(`/accounts?page=${currentPage}&page_size=${pageSize}`);
            totalAccounts = listPayload.total;
            currentAccounts = listPayload.accounts;
            renderAccounts(listPayload.accounts);
            updatePagination();
            result = listPayload.latest_batch_check_task;
        }
        if (!result || !result.task_id) {
            return;
        }
        batchCheckTaskState.taskId = result.task_id;
        batchCheckTaskState.scopeKey = result.scope_key || null;
        batchCheckTaskState.completionRefreshDone = false;
        batchCheckTaskState.logOffset = 0;
        await pollBatchCheckTask(result.task_id, { resetLogs: true });
    } catch (error) {
        console.error('恢复批量检测任务失败:', error);
    }
}

// 刷新单个账号Token
async function refreshToken(id) {
    try {
        toast.info('正在刷新Token...');
        const result = await api.post(`/accounts/${id}/refresh`);

        if (result.success) {
            toast.success('Token刷新成功');
            loadAccounts();
        } else {
            toast.error('刷新失败: ' + (result.error || '未知错误'));
        }
    } catch (error) {
        toast.error('刷新失败: ' + error.message);
    }
}

// 批量刷新Token
async function handleBatchRefresh() {
    const count = getEffectiveCount();
    if (count === 0) return;

    const confirmed = await confirm(`确定要刷新选中的 ${count} 个账号的Token吗？`);
    if (!confirmed) return;

    elements.batchRefreshBtn.disabled = true;
    elements.batchRefreshBtn.textContent = '刷新中...';

    try {
        const result = await api.post('/accounts/batch-refresh', buildBatchPayload());
        toast.success(`成功刷新 ${result.success_count} 个，失败 ${result.failed_count} 个`);
        loadAccounts();
    } catch (error) {
        toast.error('批量刷新失败: ' + error.message);
    } finally {
        updateBatchButtons();
    }
}

// 批量验证Token
async function handleBatchValidate() {
    if (getEffectiveCount() === 0) return;

    elements.batchValidateBtn.disabled = true;
    elements.batchValidateBtn.textContent = '验证中...';

    try {
        const result = await api.post('/accounts/batch-validate', buildBatchPayload());
        toast.info(`有效: ${result.valid_count}，无效: ${result.invalid_count}`);
        loadAccounts();
    } catch (error) {
        toast.error('批量验证失败: ' + error.message);
    } finally {
        updateBatchButtons();
    }
}

// 查看账号详情
async function viewAccount(id) {
    try {
        const account = await api.get(`/accounts/${id}`);
        const tokens = await api.get(`/accounts/${id}/tokens`);
        activeAccountId = id;
        renderAccounts(currentAccounts);
        updateDetailIdentity(account);
        renderSecondaryDetailRegion(account, tokens);
        renderSubscriptionQuotaLayer(account);
        renderCoreOpsLayer(account, tokens);
        renderAutomationTraceLayer(account, tokens);
        renderCliProxySummaryLayer(account);
    } catch (error) {
        toast.error('加载账号详情失败: ' + error.message);
    }
}

// 复制邮箱
function copyEmail(email) {
    copyToClipboard(email);
}

// 删除账号
async function deleteAccount(id, email) {
    const confirmed = await confirm(`确定要删除账号 ${email} 吗？此操作不可恢复。`);
    if (!confirmed) return;

    try {
        await api.delete(`/accounts/${id}`);
        toast.success('账号已删除');
        selectedAccounts.delete(id);
        loadStats();
        loadAccounts();
    } catch (error) {
        toast.error('删除失败: ' + error.message);
    }
}

// 批量删除
async function handleBatchDelete() {
    const count = getEffectiveCount();
    if (count === 0) return;

    const confirmed = await confirm(`确定要删除选中的 ${count} 个账号吗？此操作不可恢复。`);
    if (!confirmed) return;

    try {
        const result = await api.post('/accounts/batch-delete', buildBatchPayload());
        toast.success(`成功删除 ${result.deleted_count} 个账号`);
        selectedAccounts.clear();
        selectAllPages = false;
        loadStats();
        loadAccounts();
    } catch (error) {
        toast.error('删除失败: ' + error.message);
    }
}

// 导出账号
async function exportAccounts(format) {
    const count = getEffectiveCount();
    if (count === 0) {
        toast.warning('请先选择要导出的账号');
        return;
    }

    toast.info(`正在导出 ${count} 个账号...`);

    try {
        const response = await fetch('/api/accounts/export/' + format, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(buildBatchPayload())
        });

        if (!response.ok) {
            throw new Error(`导出失败: HTTP ${response.status}`);
        }

        // 获取文件内容
        const blob = await response.blob();

        // 从 Content-Disposition 获取文件名
        const disposition = response.headers.get('Content-Disposition');
        let filename = `accounts_${Date.now()}.${(format === 'cpa' || format === 'sub2api') ? 'json' : format}`;
        if (disposition) {
            const match = disposition.match(/filename=(.+)/);
            if (match) {
                filename = match[1];
            }
        }

        // 创建下载链接
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        a.remove();

        toast.success('导出成功');
    } catch (error) {
        console.error('导出失败:', error);
        toast.error('导出失败: ' + error.message);
    }
}

// HTML 转义
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// ============== CPA 服务选择 ==============

// 弹出 CPA 服务选择框，返回 Promise<{cpa_service_id: number|null}|null>
// null 表示用户取消，{cpa_service_id: null} 表示使用全局配置
function selectCpaService() {
    return new Promise(async (resolve) => {
        const modal = document.getElementById('cpa-service-modal');
        const listEl = document.getElementById('cpa-service-list');
        const closeBtn = document.getElementById('close-cpa-modal');
        const cancelBtn = document.getElementById('cancel-cpa-modal-btn');
        const globalBtn = document.getElementById('cpa-use-global-btn');

        // 加载服务列表
        listEl.innerHTML = '<div style="text-align:center;color:var(--text-muted)">加载中...</div>';
        modal.classList.add('active');

        let services = [];
        try {
            services = await api.get('/cpa-services?enabled=true');
        } catch (e) {
            services = [];
        }

        if (services.length === 0) {
            listEl.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:12px;">暂无已启用的 CPA 服务，将使用全局配置</div>';
        } else {
            listEl.innerHTML = services.map(s => `
                <div class="cpa-service-item" data-id="${s.id}" style="
                    padding: 10px 14px;
                    border: 1px solid var(--border);
                    border-radius: 8px;
                    cursor: pointer;
                    transition: background 0.15s;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                ">
                    <div>
                        <div style="font-weight:500;">${escapeHtml(s.name)}</div>
                        <div style="font-size:0.8rem;color:var(--text-muted);">${escapeHtml(s.api_url)}</div>
                    </div>
                    <span class="badge" style="background:var(--success-color);color:#fff;font-size:0.7rem;padding:2px 8px;border-radius:10px;">选择</span>
                </div>
            `).join('');

            listEl.querySelectorAll('.cpa-service-item').forEach(item => {
                item.addEventListener('mouseenter', () => item.style.background = 'var(--surface-hover)');
                item.addEventListener('mouseleave', () => item.style.background = '');
                item.addEventListener('click', () => {
                    cleanup();
                    resolve({ cpa_service_id: parseInt(item.dataset.id) });
                });
            });
        }

        function cleanup() {
            modal.classList.remove('active');
            closeBtn.removeEventListener('click', onCancel);
            cancelBtn.removeEventListener('click', onCancel);
            globalBtn.removeEventListener('click', onGlobal);
        }
        function onCancel() { cleanup(); resolve(null); }
        function onGlobal() { cleanup(); resolve({ cpa_service_id: null }); }

        closeBtn.addEventListener('click', onCancel);
        cancelBtn.addEventListener('click', onCancel);
        globalBtn.addEventListener('click', onGlobal);
    });
}

// 统一上传入口：弹出目标选择
async function uploadAccount(id) {
    const targets = [
        { label: '☁️ 上传到 CPA', value: 'cpa' },
        { label: '🔗 上传到 Sub2API', value: 'sub2api' },
        { label: '🚀 上传到 Team Manager', value: 'tm' },
    ];

    const choice = await new Promise((resolve) => {
        const modal = document.createElement('div');
        modal.className = 'modal active';
        modal.innerHTML = `
            <div class="modal-content" style="max-width:360px;">
                <div class="modal-header">
                    <h3>☁️ 选择上传目标</h3>
                    <button class="modal-close" id="_upload-close">&times;</button>
                </div>
                <div class="modal-body" style="display:flex;flex-direction:column;gap:8px;">
                    ${targets.map(t => `
                        <button class="btn btn-secondary" data-val="${t.value}" style="text-align:left;">${t.label}</button>
                    `).join('')}
                </div>
            </div>`;
        document.body.appendChild(modal);
        modal.querySelector('#_upload-close').addEventListener('click', () => { modal.remove(); resolve(null); });
        modal.addEventListener('click', (e) => { if (e.target === modal) { modal.remove(); resolve(null); } });
        modal.querySelectorAll('button[data-val]').forEach(btn => {
            btn.addEventListener('click', () => { modal.remove(); resolve(btn.dataset.val); });
        });
    });

    if (!choice) return;
    if (choice === 'cpa') return uploadToCpa(id);
    if (choice === 'sub2api') return uploadToSub2Api(id);
    if (choice === 'tm') return uploadToTm(id);
}

// 上传单个账号到CPA
async function uploadToCpa(id) {
    const choice = await selectCpaService();
    if (choice === null) return;  // 用户取消

    try {
        toast.info('正在上传到CPA...');
        const payload = {};
        if (choice.cpa_service_id != null) payload.cpa_service_id = choice.cpa_service_id;
        const result = await api.post(`/accounts/${id}/upload-cpa`, payload);

        if (result.success) {
            toast.success('上传成功');
            loadAccounts();
        } else {
            toast.error('上传失败: ' + (result.error || '未知错误'));
        }
    } catch (error) {
        toast.error('上传失败: ' + error.message);
    }
}

// 批量上传到CPA
async function handleBatchUploadCpa() {
    const count = getEffectiveCount();
    if (count === 0) return;

    const choice = await selectCpaService();
    if (choice === null) return;  // 用户取消

    const confirmed = await confirm(`确定要将选中的 ${count} 个账号上传到CPA吗？`);
    if (!confirmed) return;

    elements.batchUploadBtn.disabled = true;
    elements.batchUploadBtn.textContent = '上传中...';

    try {
        const payload = buildBatchPayload();
        if (choice.cpa_service_id != null) payload.cpa_service_id = choice.cpa_service_id;
        const result = await api.post('/accounts/batch-upload-cpa', payload);

        let message = `成功: ${result.success_count}`;
        if (result.failed_count > 0) message += `, 失败: ${result.failed_count}`;
        if (result.skipped_count > 0) message += `, 跳过: ${result.skipped_count}`;

        toast.success(message);
        loadAccounts();
    } catch (error) {
        toast.error('批量上传失败: ' + error.message);
    } finally {
        updateBatchButtons();
    }
}

// ============== 订阅状态 ==============

// 手动标记订阅类型
async function markSubscription(id) {
    const type = prompt('请输入订阅类型 (plus / team / free):', 'plus');
    if (!type) return;
    if (!['plus', 'team', 'free'].includes(type.trim().toLowerCase())) {
        toast.error('无效的订阅类型，请输入 plus、team 或 free');
        return;
    }
    try {
        await api.post(`/payment/accounts/${id}/mark-subscription`, {
            subscription_type: type.trim().toLowerCase()
        });
        toast.success('订阅状态已更新');
        loadAccounts();
    } catch (e) {
        toast.error('标记失败: ' + e.message);
    }
}

// 批量检测订阅状态
async function handleBatchCheckSubscription() {
    const count = getEffectiveCount();
    if (count === 0) return;
    const confirmed = await confirm(`确定要检测选中的 ${count} 个账号的订阅状态吗？`);
    if (!confirmed) return;

    elements.batchCheckSubBtn.disabled = true;
    elements.batchCheckSubBtn.textContent = '检测中...';

    try {
        const result = await api.post('/payment/accounts/batch-check-subscription', buildBatchPayload());
        batchCheckTaskState.taskId = result.task_id;
        batchCheckTaskState.scopeKey = result.scope_key || null;
        batchCheckTaskState.logOffset = 0;
        batchCheckTaskState.completionRefreshDone = false;

        renderBatchCheckTaskPanel({
            task_id: result.task_id,
            status: result.status,
            scope_key: result.scope_key,
            total_count: count,
            processed_count: 0,
            success_count: 0,
            failure_count: 0,
            current_account: null,
            progress_percent: 0,
            logs: [result.reused ? '复用已有批量检测任务，继续恢复进度。' : '已创建批量检测任务，正在等待后台执行。'],
            next_log_offset: 1
        }, { appendLogs: false });

        toast.success(result.reused ? '已恢复现有批量检测任务' : '已启动批量检测任务');
        await pollBatchCheckTask(result.task_id, { resetLogs: true });
    } catch (e) {
        toast.error('批量检测失败: ' + e.message);
    } finally {
        updateBatchButtons();
    }
}

// ============== Sub2API 上传 ==============

// 弹出 Sub2API 服务选择框，返回 Promise<{service_id: number|null}|null>
// null 表示用户取消，{service_id: null} 表示自动选择
function selectSub2ApiService() {
    return new Promise(async (resolve) => {
        const modal = document.getElementById('sub2api-service-modal');
        const listEl = document.getElementById('sub2api-service-list');
        const closeBtn = document.getElementById('close-sub2api-modal');
        const cancelBtn = document.getElementById('cancel-sub2api-modal-btn');
        const autoBtn = document.getElementById('sub2api-use-auto-btn');

        listEl.innerHTML = '<div style="text-align:center;color:var(--text-muted)">加载中...</div>';
        modal.classList.add('active');

        let services = [];
        try {
            services = await api.get('/sub2api-services?enabled=true');
        } catch (e) {
            services = [];
        }

        if (services.length === 0) {
            listEl.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:12px;">暂无已启用的 Sub2API 服务，将自动选择第一个</div>';
        } else {
            listEl.innerHTML = services.map(s => `
                <div class="sub2api-service-item" data-id="${s.id}" style="
                    padding: 10px 14px;
                    border: 1px solid var(--border);
                    border-radius: 8px;
                    cursor: pointer;
                    transition: background 0.15s;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                ">
                    <div>
                        <div style="font-weight:500;">${escapeHtml(s.name)}</div>
                        <div style="font-size:0.8rem;color:var(--text-muted);">${escapeHtml(s.api_url)}</div>
                    </div>
                    <span class="badge" style="background:var(--primary);color:#fff;font-size:0.7rem;padding:2px 8px;border-radius:10px;">选择</span>
                </div>
            `).join('');

            listEl.querySelectorAll('.sub2api-service-item').forEach(item => {
                item.addEventListener('mouseenter', () => item.style.background = 'var(--surface-hover)');
                item.addEventListener('mouseleave', () => item.style.background = '');
                item.addEventListener('click', () => {
                    cleanup();
                    resolve({ service_id: parseInt(item.dataset.id) });
                });
            });
        }

        function cleanup() {
            modal.classList.remove('active');
            closeBtn.removeEventListener('click', onCancel);
            cancelBtn.removeEventListener('click', onCancel);
            autoBtn.removeEventListener('click', onAuto);
        }
        function onCancel() { cleanup(); resolve(null); }
        function onAuto() { cleanup(); resolve({ service_id: null }); }

        closeBtn.addEventListener('click', onCancel);
        cancelBtn.addEventListener('click', onCancel);
        autoBtn.addEventListener('click', onAuto);
    });
}

// 批量上传到 Sub2API
async function handleBatchUploadSub2Api() {
    const count = getEffectiveCount();
    if (count === 0) return;

    const choice = await selectSub2ApiService();
    if (choice === null) return;  // 用户取消

    const confirmed = await confirm(`确定要将选中的 ${count} 个账号上传到 Sub2API 吗？`);
    if (!confirmed) return;

    elements.batchUploadBtn.disabled = true;
    elements.batchUploadBtn.textContent = '上传中...';

    try {
        const payload = buildBatchPayload();
        if (choice.service_id != null) payload.service_id = choice.service_id;
        const result = await api.post('/accounts/batch-upload-sub2api', payload);

        let message = `成功: ${result.success_count}`;
        if (result.failed_count > 0) message += `, 失败: ${result.failed_count}`;
        if (result.skipped_count > 0) message += `, 跳过: ${result.skipped_count}`;

        toast.success(message);
        loadAccounts();
    } catch (error) {
        toast.error('批量上传失败: ' + error.message);
    } finally {
        updateBatchButtons();
    }
}

// ============== Team Manager 上传 ==============

// 上传单账号到 Sub2API
async function uploadToSub2Api(id) {
    const choice = await selectSub2ApiService();
    if (choice === null) return;
    try {
        toast.info('正在上传到 Sub2API...');
        const payload = {};
        if (choice.service_id != null) payload.service_id = choice.service_id;
        const result = await api.post(`/accounts/${id}/upload-sub2api`, payload);
        if (result.success) {
            toast.success('上传成功');
            loadAccounts();
        } else {
            toast.error('上传失败: ' + (result.error || result.message || '未知错误'));
        }
    } catch (e) {
        toast.error('上传失败: ' + e.message);
    }
}

// 弹出 Team Manager 服务选择框，返回 Promise<{service_id: number|null}|null>
// null 表示用户取消，{service_id: null} 表示自动选择
function selectTmService() {
    return new Promise(async (resolve) => {
        const modal = document.getElementById('tm-service-modal');
        const listEl = document.getElementById('tm-service-list');
        const closeBtn = document.getElementById('close-tm-modal');
        const cancelBtn = document.getElementById('cancel-tm-modal-btn');
        const autoBtn = document.getElementById('tm-use-auto-btn');

        listEl.innerHTML = '<div style="text-align:center;color:var(--text-muted)">加载中...</div>';
        modal.classList.add('active');

        let services = [];
        try {
            services = await api.get('/tm-services?enabled=true');
        } catch (e) {
            services = [];
        }

        if (services.length === 0) {
            listEl.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:12px;">暂无已启用的 Team Manager 服务，将自动选择第一个</div>';
        } else {
            listEl.innerHTML = services.map(s => `
                <div class="tm-service-item" data-id="${s.id}" style="
                    padding: 10px 14px;
                    border: 1px solid var(--border);
                    border-radius: 8px;
                    cursor: pointer;
                    transition: background 0.15s;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                ">
                    <div>
                        <div style="font-weight:500;">${escapeHtml(s.name)}</div>
                        <div style="font-size:0.8rem;color:var(--text-muted);">${escapeHtml(s.api_url)}</div>
                    </div>
                    <span class="badge" style="background:var(--primary);color:#fff;font-size:0.7rem;padding:2px 8px;border-radius:10px;">选择</span>
                </div>
            `).join('');

            listEl.querySelectorAll('.tm-service-item').forEach(item => {
                item.addEventListener('mouseenter', () => item.style.background = 'var(--surface-hover)');
                item.addEventListener('mouseleave', () => item.style.background = '');
                item.addEventListener('click', () => {
                    cleanup();
                    resolve({ service_id: parseInt(item.dataset.id) });
                });
            });
        }

        function cleanup() {
            modal.classList.remove('active');
            closeBtn.removeEventListener('click', onCancel);
            cancelBtn.removeEventListener('click', onCancel);
            autoBtn.removeEventListener('click', onAuto);
        }
        function onCancel() { cleanup(); resolve(null); }
        function onAuto() { cleanup(); resolve({ service_id: null }); }

        closeBtn.addEventListener('click', onCancel);
        cancelBtn.addEventListener('click', onCancel);
        autoBtn.addEventListener('click', onAuto);
    });
}

// 上传单账号到 Team Manager
async function uploadToTm(id) {
    const choice = await selectTmService();
    if (choice === null) return;
    try {
        toast.info('正在上传到 Team Manager...');
        const payload = {};
        if (choice.service_id != null) payload.service_id = choice.service_id;
        const result = await api.post(`/accounts/${id}/upload-tm`, payload);
        if (result.success) {
            toast.success('上传成功');
        } else {
            toast.error('上传失败: ' + (result.message || '未知错误'));
        }
    } catch (e) {
        toast.error('上传失败: ' + e.message);
    }
}

// 批量上传到 Team Manager
async function handleBatchUploadTm() {
    const count = getEffectiveCount();
    if (count === 0) return;

    const choice = await selectTmService();
    if (choice === null) return;  // 用户取消

    const confirmed = await confirm(`确定要将选中的 ${count} 个账号上传到 Team Manager 吗？`);
    if (!confirmed) return;

    elements.batchUploadBtn.disabled = true;
    elements.batchUploadBtn.textContent = '上传中...';

    try {
        const payload = buildBatchPayload();
        if (choice.service_id != null) payload.service_id = choice.service_id;
        const result = await api.post('/accounts/batch-upload-tm', payload);
        let message = `成功: ${result.success_count}`;
        if (result.failed_count > 0) message += `, 失败: ${result.failed_count}`;
        if (result.skipped_count > 0) message += `, 跳过: ${result.skipped_count}`;
        toast.success(message);
        loadAccounts();
    } catch (e) {
        toast.error('批量上传失败: ' + e.message);
    } finally {
        updateBatchButtons();
    }
}

// 更多菜单切换
function toggleMoreMenu(btn) {
    const menu = btn.nextElementSibling;
    const isActive = menu.classList.contains('active');
    // 关闭所有其他更多菜单
    document.querySelectorAll('.dropdown-menu.active').forEach(m => m.classList.remove('active'));
    if (!isActive) menu.classList.add('active');
}

function closeMoreMenu(el) {
    const menu = el.closest('.dropdown-menu');
    if (menu) menu.classList.remove('active');
}

// 保存账号 Cookies
async function saveCookies(id) {
    const textarea = document.getElementById(`cookies-input-${id}`);
    if (!textarea) return;
    const cookiesValue = textarea.value.trim();
    try {
        await api.patch(`/accounts/${id}`, { cookies: cookiesValue });
        toast.success('Cookies 已保存');
    } catch (e) {
        toast.error('保存 Cookies 失败: ' + e.message);
    }
}

// 查询收件箱验证码
async function checkInboxCode(id) {
    toast.info('正在查询收件箱...');
    try {
        const result = await api.post(`/accounts/${id}/inbox-code`);
        if (result.success) {
            showInboxCodeResult(result.code, result.email);
        } else {
            toast.error('查询失败: ' + (result.error || '未收到验证码'));
        }
    } catch (error) {
        toast.error('查询失败: ' + error.message);
    }
}

function showInboxCodeResult(code, email) {
    elements.detailAutomationTrace.innerHTML = `
        <div class="trace-grid" style="flex-direction:column;">
            <p><strong>${escapeHtml(email)}</strong> 最新验证码</p>
            <pre>${escapeHtml(code)}</pre>
            <div><button class="btn btn-primary btn-sm" onclick="copyToClipboard('${escapeHtml(code)}')">复制验证码</button></div>
        </div>
    `;
}

function updateDetailIdentity(account) {
    elements.detailIdentityTitle.textContent = account.email || '未命名账号';
    elements.detailIdentitySubtitle.textContent = `${getServiceTypeText(account.email_service)} · ${getStatusText('account', account.status)} · ${account.platform_source || 'unknown source'}`;
}

function renderEmptyDetailPanel() {
    if (!elements.detailSubscriptionQuota) return;
    elements.detailIdentityTitle.textContent = '选择一个账号';
    elements.detailIdentitySubtitle.textContent = '从左侧主列表选择账号，查看订阅、额度、远程维护和 CLIProxy 摘要。';
    if (elements.secondaryDetailRegion) {
        elements.secondaryDetailRegion.innerHTML = `
            <h4>低频字段收纳区</h4>
            <div class="detail-empty">账号 ID、邮箱服务、workspace、密码、远程环境、upload target、remote file 和 probe 状态只在这里显示。</div>
        `;
    }
    elements.detailSubscriptionQuota.textContent = '在这里查看订阅类型、额度限制、风险/待处理状态以及最近活动概览。';
    elements.detailCoreOps.textContent = '账号身份头部、核心运维卡片和常用快捷操作会在这里展开。';
    elements.detailAutomationTrace.textContent = '显示 source、batch、proxy、recent tasks 和日志摘录。';
    elements.detailCliProxySummary.textContent = '显示账号侧 CLIProxy 关联环境、远程文件、同步状态，以及进入维护视图的入口。';
}

function renderSecondaryDetailRegion(account, tokens) {
    if (!elements.secondaryDetailRegion) return;
    const remote = account.remote_inventory_summary || {};
    const passwordMarkup = account.password
        ? `<span style="display:inline-flex;align-items:center;gap:4px;">
            <span class="password-hidden" data-pwd="${escapeHtml(account.password)}" onclick="togglePassword(this, this.dataset.pwd)" title="点击查看">${escapeHtml(account.password.substring(0, 4) + '****')}</span>
            <button class="btn-copy-icon copy-pwd-btn" data-pwd="${escapeHtml(account.password)}" title="复制密码">📋</button>
        </span>`
        : '<span>-</span>';

    elements.secondaryDetailRegion.innerHTML = `
        <h4>低频字段收纳区</h4>
        <div class="detail-chip-row">
            <span class="detail-chip" data-detail-only-field="account-id"><strong>${escapeHtml(account.account_id || '-')}</strong><span>account id</span></span>
            <span class="detail-chip" data-detail-only-field="email-service"><strong>${escapeHtml(getServiceTypeText(account.email_service) || '-')}</strong><span>email service</span></span>
            <span class="detail-chip" data-detail-only-field="workspace-id"><strong>${escapeHtml(account.workspace_id || '-')}</strong><span>workspace</span></span>
            <span class="detail-chip" data-detail-only-field="remote-environment"><strong>${escapeHtml(remote.environment_name || account.remote_environment_name || '-')}</strong><span>environment</span></span>
            <span class="detail-chip" data-detail-only-field="upload-target"><strong>${escapeHtml(account.last_upload_target || '-')}</strong><span>upload target</span></span>
            <span class="detail-chip" data-detail-only-field="remote-file"><strong>${escapeHtml(remote.remote_file_id || '-')}</strong><span>remote file</span></span>
            <span class="detail-chip" data-detail-only-field="probe-status"><strong>${escapeHtml(remote.probe_status || account.quota_summary?.probe_status || '-')}</strong><span>probe</span></span>
            <span class="detail-chip" data-detail-only-field="password"><strong>${tokens.access_token ? 'tokens ready' : 'tokens partial'}</strong><span>password below</span></span>
        </div>
        <p>这些字段保留在次级区域，避免主列表首屏过载。</p>
        <div class="password-cell">${passwordMarkup}</div>
    `;
}

function renderSubscriptionQuotaLayer(account) {
    const subscription = account.subscription_summary || {};
    const quota = account.quota_summary || {};
    elements.detailSubscriptionQuota.innerHTML = `
        <div class="detail-chip-row">
            <span class="detail-chip"><strong>${escapeHtml(subscription.subscription_type || 'none')}</strong><span>subscription</span></span>
            <span class="detail-chip"><strong>${quota.slots_used ?? '-'} / ${quota.slots_total ?? '-'}</strong><span>quota</span></span>
            <span class="detail-chip"><strong>${escapeHtml(quota.probe_status || account.remote_sync_state || 'unknown')}</strong><span>risk / pending</span></span>
            <span class="detail-chip"><strong>${escapeHtml(account.last_maintenance_status || 'none')}</strong><span>recent activity</span></span>
        </div>
        <p>订阅时间 ${format.date(subscription.subscription_at) || '-'}；最近维护 ${format.date(account.last_maintenance_at) || '-'}；上传目标 ${escapeHtml(account.last_upload_target || '-')}。</p>
    `;
}

function renderCoreOpsLayer(account, tokens) {
    elements.detailCoreOps.innerHTML = `
        <div class="detail-chip-row">
            <span class="detail-chip"><strong>${escapeHtml(account.client_id || '-')}</strong><span>client</span></span>
            <span class="detail-chip"><strong>${account.export_status_summary?.cpa_uploaded ? 'uploaded' : 'pending'}</strong><span>CPA export</span></span>
        </div>
        <div class="detail-action-grid">
            <button class="btn btn-primary btn-sm" onclick="refreshToken(${account.id})">刷新 Token</button>
            <button class="btn btn-secondary btn-sm" onclick="uploadAccount(${account.id})">上传</button>
            <button class="btn btn-secondary btn-sm" onclick="markSubscription(${account.id})">标记订阅</button>
            <button class="btn btn-secondary btn-sm" onclick="checkInboxCode(${account.id})">收件箱</button>
        </div>
        <p>Access Token：${tokens.access_token ? '已存在' : '缺失'}；Refresh Token：${tokens.refresh_token ? '已存在' : '缺失'}；CPA：${account.export_status_summary?.cpa_uploaded ? '已上传' : '未上传'}。</p>
        <div>
            <textarea id="cookies-input-${account.id}" rows="3" style="width:100%;font-size:0.7rem;font-family:var(--font-mono);background:var(--surface-hover);border:1px solid var(--border);border-radius:12px;padding:8px;color:var(--text-primary);resize:vertical;" placeholder="粘贴完整 cookie 字符串，留空则清除">${escapeHtml(account.cookies || '')}</textarea>
            <button class="btn btn-secondary btn-sm" style="margin-top:8px" onclick="saveCookies(${account.id})">保存 Cookies</button>
        </div>
    `;
}

function renderAutomationTraceLayer(account, tokens) {
    const recentTask = account.recent_task_summary || {};
    const trace = account.automation_trace_summary || {};
    elements.detailAutomationTrace.innerHTML = `
        <div class="trace-grid">
            <span class="detail-chip"><strong>${escapeHtml(trace.source || account.platform_source || '-')}</strong><span>source</span></span>
            <span class="detail-chip"><strong>${escapeHtml(trace.batch_target || account.last_upload_target || '-')}</strong><span>batch target</span></span>
            <span class="detail-chip"><strong>${escapeHtml(trace.proxy || account.proxy_used || 'default')}</strong><span>proxy</span></span>
            <span class="detail-chip"><strong>${escapeHtml(trace.recent_task_status || recentTask.status || 'idle')}</strong><span>recent task</span></span>
        </div>
        <p>${escapeHtml(trace.recent_task_label || 'No recent account task')}；创建于 ${format.date(recentTask.created_at) || '-'}，完成于 ${format.date(recentTask.completed_at) || '-'}，任务 ID ${escapeHtml(String(recentTask.task_id || '-'))}。</p>
        <pre>${escapeHtml(trace.log_excerpt || 'No maintenance trace available for this account.')}</pre>
    `;
}

function renderCliProxySummaryLayer(account) {
    const remote = account.remote_inventory_summary || {};
    const jumpEntry = account.cliproxy_jump_entry || {};
    elements.detailCliProxySummary.innerHTML = `
        <div class="cliproxy-grid">
            <span class="detail-chip"><strong>${escapeHtml(remote.environment_name || account.remote_environment_name || '-')}</strong><span>environment</span></span>
            <span class="detail-chip"><strong>${escapeHtml(remote.remote_file_id || '-')}</strong><span>remote file</span></span>
            <span class="detail-chip"><strong>${escapeHtml(remote.sync_state || account.remote_sync_state || '-')}</strong><span>sync</span></span>
            <span class="detail-chip"><strong>${escapeHtml(remote.probe_status || '-')}</strong><span>probe</span></span>
        </div>
        <p>最近探测 ${format.date(remote.last_probed_at) || '-'}；最近发现 ${format.date(remote.last_seen_at) || '-'}；远程账号 ${escapeHtml(remote.remote_account_id || account.account_id || '-')}。</p>
        <div class="detail-action-grid">
            ${jumpEntry.href
                ? `<a class="btn btn-secondary btn-sm" href="${escapeHtml(jumpEntry.href)}">${escapeHtml(jumpEntry.label || 'Open CLIProxy maintenance context')}</a>`
                : '<span class="detail-chip"><strong>未关联维护运行</strong><span>jump entry</span></span>'}
            <button class="btn btn-ghost btn-sm" onclick="copyToClipboard('${escapeHtml(remote.remote_file_id || '')}')">复制 remote file</button>
        </div>
    `;
}

function formatSubscriptionLabel(account) {
    const subscription = account.subscription_summary || {};
    const quota = account.quota_summary || {};
    const subscriptionType = subscription.subscription_type || account.subscription_type || 'none';
    const slotsUsed = quota.slots_used ?? '-';
    const slotsTotal = quota.slots_total ?? '-';
    return `${subscriptionType} · ${slotsUsed}/${slotsTotal}`;
}

function formatRiskLabel(account) {
    return account.quota_summary?.probe_status || account.remote_sync_state || account.status || 'unknown';
}

function formatRecentActivityLabel(account) {
    return account.last_maintenance_at ? format.date(account.last_maintenance_at) : (format.date(account.last_refresh) || '无记录');
}

function formatRemoteMaintenanceLabel(account) {
    return [account.remote_environment_name || '未关联环境', account.last_maintenance_status || '无维护'].join(' · ');
}

function formatSubscriptionPanel(account) {
    return `订阅 ${escapeHtml(account.subscription_summary?.subscription_type || account.subscription_type || 'none')}，额度 ${account.quota_summary?.slots_used ?? '-'} / ${account.quota_summary?.slots_total ?? '-'}，状态 ${escapeHtml(account.quota_summary?.probe_status || 'unknown')}`;
}

function formatRiskPanel(account) {
    return `风险/待处理 ${escapeHtml(formatRiskLabel(account))}`;
}

function formatRecentActivityPanel(account) {
    return `最近活动 ${escapeHtml(formatRecentActivityLabel(account))}`;
}

function formatRemoteMaintenancePanel(account) {
    return `${escapeHtml(account.remote_environment_name || '未关联环境')} · ${escapeHtml(account.last_maintenance_status || '未维护')} · ${escapeHtml(account.last_upload_target || '未上传')}`;
}

function formatSourceTargetPanel(account) {
    return `${escapeHtml(account.platform_source || 'unknown source')} -> ${escapeHtml(account.last_upload_target || '未设置')}；${escapeHtml(account.last_maintenance_status || 'no maintenance')}`;
}
