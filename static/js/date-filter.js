/**
 * 日期/月份筛选器通用库
 * 支持AJAX和表单提交两种模式
 * 支持日期模式(type="date")和月份模式(type="month")
 *
 * @author Claude Code
 * @version 2.0.0
 * @date 2026-01-02
 */

class DateFilterHelper {
    /**
     * 初始化日期/月份筛选器
     *
     * @param {Object} options - 配置选项
     * @param {string} options.startDateId - 开始日期输入框ID（默认'startDate'）
     * @param {string} options.endDateId - 结束日期输入框ID（默认'endDate'）
     * @param {string} options.defaultRange - 默认日期范围
     *        日期模式: 'current_month', 'last_month', 'last_3_months'
     *        月份模式: 'current_month', 'last_month', 'current_quarter', 'last_quarter', 'last_6_months'
     * @param {Function} options.onDateChange - 日期变化时的回调函数（AJAX模式）
     * @param {boolean} options.autoSubmit - 是否自动提交表单（表单模式）
     * @param {boolean} options.showQuickButtons - 是否显示快捷按钮（默认true）
     * @param {boolean} options.validateDates - 是否启用日期验证（默认true）
     * @param {Function} options.updateCallback - 范围更新时的回调函数
     */
    constructor(options = {}) {
        this.startDateId = options.startDateId || 'startDate';
        this.endDateId = options.endDateId || 'endDate';
        this.defaultRange = options.defaultRange || 'current_month';
        this.onDateChange = options.onDateChange;
        this.autoSubmit = options.autoSubmit || false;
        this.showQuickButtons = options.showQuickButtons !== false;
        this.validateDates = options.validateDates !== false;
        this.updateCallback = options.updateCallback;

        this.startDateInput = null;
        this.endDateInput = null;
        this.isMonthMode = false;  // 是否为月份模式

        this.init();
    }

    /**
     * 初始化日期筛选器
     */
    init() {
        // 获取日期输入框元素
        this.startDateInput = document.getElementById(this.startDateId);
        this.endDateInput = document.getElementById(this.endDateId);

        if (!this.startDateInput || !this.endDateInput) {
            console.error('[DateFilterHelper] 找不到日期输入框元素');
            return;
        }

        // 检测输入框类型（date 或 month）
        this.isMonthMode = this.startDateInput.type === 'month' || this.endDateInput.type === 'month';

        // 如果输入框没有值，设置默认日期
        if (!this.startDateInput.value && !this.endDateInput.value && this.defaultRange) {
            this.setDefaultDates(this.defaultRange);
        }

        // 绑定事件监听器
        this.bindEvents();

        // 创建快捷按钮
        if (this.showQuickButtons) {
            this.createQuickButtons();
        }

        // 初始化时更新范围标签
        this.updateRangeLabel('dateRangeLabel');
    }

    /**
     * 绑定事件监听器
     */
    bindEvents() {
        // 日期变化事件
        const onChange = () => {
            // 日期验证
            if (this.validateDates) {
                this.validateDateRange();
            }

            // 更新范围标签
            this.updateRangeLabel('dateRangeLabel');

            // AJAX模式：调用回调函数
            if (this.onDateChange && typeof this.onDateChange === 'function') {
                this.onDateChange();
            }

            // 表单模式：自动提交
            if (this.autoSubmit) {
                const form = this.startDateInput.closest('form');
                if (form) {
                    form.submit();
                }
            }
        };

        this.startDateInput.addEventListener('change', onChange);
        this.endDateInput.addEventListener('change', onChange);
    }

    /**
     * 创建快捷按钮
     */
    createQuickButtons() {
        const container = document.getElementById('quickButtonsContainer');
        if (!container) return;

        const buttonGroup = document.createElement('div');
        buttonGroup.className = 'btn-group btn-group-sm';
        buttonGroup.setAttribute('role', 'group');

        if (this.isMonthMode) {
            // 月份模式的快捷按钮
            buttonGroup.innerHTML = `
                <button type="button" class="btn btn-outline-secondary" data-range="current_month">本月</button>
                <button type="button" class="btn btn-outline-secondary" data-range="last_month">上月</button>
                <button type="button" class="btn btn-outline-secondary" data-range="current_quarter">本季度</button>
                <button type="button" class="btn btn-outline-secondary" data-range="last_quarter">上季度</button>
                <button type="button" class="btn btn-outline-secondary" data-range="last_6_months">近6个月</button>
            `;
        } else {
            // 日期模式的快捷按钮
            buttonGroup.innerHTML = `
                <button type="button" class="btn btn-outline-secondary" data-range="current_month">本月</button>
                <button type="button" class="btn btn-outline-secondary" data-range="last_month">上月</button>
                <button type="button" class="btn btn-outline-secondary" data-range="last_3_months">最近3个月</button>
            `;
        }

        container.appendChild(buttonGroup);

        // 绑定快捷按钮点击事件
        buttonGroup.querySelectorAll('button').forEach(button => {
            button.addEventListener('click', () => {
                const range = button.getAttribute('data-range');
                this.setDefaultDates(range);

                // 触发change事件
                this.startDateInput.dispatchEvent(new Event('change'));
            });
        });
    }

    /**
     * 设置默认日期范围
     *
     * @param {string} range - 日期范围类型
     */
    setDefaultDates(range) {
        const now = new Date();
        let startDate, endDate;

        if (this.isMonthMode) {
            // 月份模式
            switch (range) {
                case 'current_month':
                    // 当月
                    startDate = this.formatMonth(now);
                    endDate = this.formatMonth(now);
                    break;

                case 'last_month':
                    // 上月
                    const lastMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
                    startDate = this.formatMonth(lastMonth);
                    endDate = this.formatMonth(lastMonth);
                    break;

                case 'current_quarter':
                    // 本季度
                    const currentQuarter = Math.floor(now.getMonth() / 3);
                    const quarterStart = new Date(now.getFullYear(), currentQuarter * 3, 1);
                    const quarterEnd = new Date(now.getFullYear(), currentQuarter * 3 + 2, 1);
                    startDate = this.formatMonth(quarterStart);
                    endDate = this.formatMonth(quarterEnd);
                    break;

                case 'last_quarter':
                    // 上季度
                    const lastQuarter = Math.floor(now.getMonth() / 3) - 1;
                    let qYear = now.getFullYear();
                    let qIndex = lastQuarter;
                    if (qIndex < 0) {
                        qIndex = 3;
                        qYear -= 1;
                    }
                    const lastQStart = new Date(qYear, qIndex * 3, 1);
                    const lastQEnd = new Date(qYear, qIndex * 3 + 2, 1);
                    startDate = this.formatMonth(lastQStart);
                    endDate = this.formatMonth(lastQEnd);
                    break;

                case 'last_6_months':
                    // 近6个月
                    const sixMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 5, 1);
                    startDate = this.formatMonth(sixMonthsAgo);
                    endDate = this.formatMonth(now);
                    break;

                case 'last_3_months':
                    // 近3个月
                    const threeMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 2, 1);
                    startDate = this.formatMonth(threeMonthsAgo);
                    endDate = this.formatMonth(now);
                    break;

                default:
                    return;
            }
        } else {
            // 日期模式
            switch (range) {
                case 'current_month':
                    // 当月：月初至月末
                    startDate = this.getMonthStart(now);
                    endDate = this.getMonthEnd(now);
                    break;

                case 'last_month':
                    // 上月
                    const lastMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
                    startDate = this.getMonthStart(lastMonth);
                    endDate = this.getMonthEnd(lastMonth);
                    break;

                case 'last_3_months':
                    // 最近3个月（90天）
                    endDate = this.formatDate(now);
                    const start = new Date(now);
                    start.setDate(start.getDate() - 90);
                    startDate = this.formatDate(start);
                    break;

                default:
                    return;
            }
        }

        this.startDateInput.value = startDate;
        this.endDateInput.value = endDate;

        // 更新范围标签
        this.updateRangeLabel('dateRangeLabel');
    }

    /**
     * 获取月初日期
     *
     * @param {Date} date - 日期对象
     * @returns {string} YYYY-MM-DD格式的日期字符串
     */
    getMonthStart(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        return `${year}-${month}-01`;
    }

    /**
     * 获取月末日期
     *
     * @param {Date} date - 日期对象
     * @returns {string} YYYY-MM-DD格式的日期字符串
     */
    getMonthEnd(date) {
        const year = date.getFullYear();
        const month = date.getMonth();
        const lastDay = new Date(year, month + 1, 0).getDate();
        const monthStr = String(month + 1).padStart(2, '0');
        const dayStr = String(lastDay).padStart(2, '0');
        return `${year}-${monthStr}-${dayStr}`;
    }

    /**
     * 格式化日期为YYYY-MM-DD
     *
     * @param {Date} date - 日期对象
     * @returns {string} YYYY-MM-DD格式的日期字符串
     */
    formatDate(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    }

    /**
     * 格式化月份为YYYY-MM
     *
     * @param {Date} date - 日期对象
     * @returns {string} YYYY-MM格式的月份字符串
     */
    formatMonth(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        return `${year}-${month}`;
    }

    /**
     * 验证日期范围（结束日期必须≥开始日期）
     *
     * @returns {boolean} 验证是否通过
     */
    validateDateRange() {
        const start = this.startDateInput.value;
        const end = this.endDateInput.value;

        if (start && end && start > end) {
            this.endDateInput.setCustomValidity('结束日期必须大于或等于开始日期');
            this.endDateInput.reportValidity();
            return false;
        } else {
            this.endDateInput.setCustomValidity('');
            return true;
        }
    }

    /**
     * 更新日期范围标签显示
     *
     * @param {string} elementId - 标签元素ID
     */
    updateRangeLabel(elementId) {
        const element = document.getElementById(elementId);
        if (!element) return;

        const start = this.startDateInput.value || '起始';
        const end = this.endDateInput.value || '当前';
        element.textContent = `${start} 至 ${end}`;
    }

    /**
     * 重置为默认日期范围
     */
    reset() {
        if (this.defaultRange) {
            this.setDefaultDates(this.defaultRange);
            this.startDateInput.dispatchEvent(new Event('change'));
        } else {
            this.startDateInput.value = '';
            this.endDateInput.value = '';
            this.updateRangeLabel('dateRangeLabel');
        }
    }

    /**
     * 获取日期参数对象
     *
     * @returns {Object} { start_date, end_date }
     */
    getParams() {
        return {
            start_date: this.startDateInput.value || null,
            end_date: this.endDateInput.value || null
        };
    }

    /**
     * 获取URL查询字符串
     *
     * @returns {string} URL查询字符串（不含?）
     */
    getQueryString() {
        const params = new URLSearchParams();
        const startDate = this.startDateInput.value;
        const endDate = this.endDateInput.value;

        if (startDate) params.append('start_date', startDate);
        if (endDate) params.append('end_date', endDate);

        return params.toString();
    }

    /**
     * 快捷方法：设置为本月
     */
    setThisMonth() {
        this.setDefaultDates('current_month');
    }

    /**
     * 快捷方法：设置为上月
     */
    setLastMonth() {
        this.setDefaultDates('last_month');
    }

    /**
     * 快捷方法：设置为最近3个月
     */
    setLast3Months() {
        this.setDefaultDates('last_3_months');
    }
}

// 导出为全局变量（支持无模块化环境）
if (typeof window !== 'undefined') {
    window.DateFilterHelper = DateFilterHelper;
}
