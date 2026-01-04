#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
公共工具函数模块
提供各 Blueprint 共用的辅助函数
"""
from flask import session, request
from models.database import get_db
from datetime import datetime
import calendar


def current_user_id():
    """
    获取当前登录用户的ID

    Returns:
        int: 用户ID,未登录返回None
    """
    return session.get('user_id')


def current_username():
    """
    获取当前登录用户的用户名

    Returns:
        str: 用户名,未登录返回None
    """
    return session.get('username')


def current_user_role():
    """
    获取当前登录用户的角色

    Returns:
        str: 用户角色 ('admin', 'manager', 'user'),未登录返回None
    """
    return session.get('role')


def is_logged_in():
    """
    检查用户是否已登录

    Returns:
        bool: True表示已登录,False表示未登录
    """
    return session.get('logged_in', False)


def is_admin():
    """
    检查当前用户是否为管理员

    Returns:
        bool: True表示是管理员,False表示不是
    """
    if not is_logged_in():
        return False

    user_id = current_user_id()
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT role FROM users WHERE id = ?", (user_id,))
    row = cur.fetchone()

    return row and row['role'] == 'admin'


def get_user_role():
    """
    获取当前用户的数据库角色

    Returns:
        str: 用户角色,未登录或查询失败返回None
    """
    user_id = current_user_id()
    if not user_id:
        return None

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT role FROM users WHERE id = ?", (user_id,))
    row = cur.fetchone()

    return row['role'] if row else None


def has_permission(required_role):
    """
    检查用户是否具有指定权限

    Args:
        required_role: 需要的角色 ('admin', 'manager', 'user')

    Returns:
        bool: True表示有权限,False表示无权限
    """
    user_role = get_user_role()
    if not user_role:
        return False

    role_hierarchy = {'admin': 3, 'manager': 2, 'user': 1}
    user_level = role_hierarchy.get(user_role, 0)
    required_level = role_hierarchy.get(required_role, 0)

    return user_level >= required_level


def get_user_info(user_id=None):
    """
    获取用户完整信息

    Args:
        user_id: 用户ID,默认为当前登录用户

    Returns:
        dict: 用户信息字典,失败返回None
    """
    if user_id is None:
        user_id = current_user_id()

    if not user_id:
        return None

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT u.id, u.username, u.role, u.created_at,
               d.name as department_name, d.id as department_id
        FROM users u
        LEFT JOIN departments d ON u.department_id = d.id
        WHERE u.id = ?
    """, (user_id,))
    row = cur.fetchone()

    if row:
        return dict(row)
    return None


def format_date(date_str, format_type='display'):
    """
    格式化日期字符串

    Args:
        date_str: 日期字符串
        format_type: 格式类型 ('display', 'database', 'short')

    Returns:
        str: 格式化后的日期字符串
    """
    from datetime import datetime

    if not date_str:
        return ''

    try:
        if isinstance(date_str, str):
            # 尝试多种日期格式解析
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y/%m/%d']:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    continue
            else:
                return date_str
        else:
            dt = date_str

        # 根据类型返回不同格式
        if format_type == 'display':
            return dt.strftime('%Y年%m月%d日 %H:%M')
        elif format_type == 'database':
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        elif format_type == 'short':
            return dt.strftime('%Y-%m-%d')
        else:
            return date_str

    except Exception:
        return date_str


def safe_int(value, default=0):
    """
    安全转换为整数

    Args:
        value: 要转换的值
        default: 转换失败时的默认值

    Returns:
        int: 转换后的整数
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_float(value, default=0.0):
    """
    安全转换为浮点数

    Args:
        value: 要转换的值
        default: 转换失败时的默认值

    Returns:
        float: 转换后的浮点数
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def paginate(query_result, page=1, per_page=20):
    """
    简单的分页功能

    Args:
        query_result: 查询结果列表
        page: 当前页码
        per_page: 每页显示数量

    Returns:
        dict: 包含分页信息的字典
    """
    total = len(query_result)
    start = (page - 1) * per_page
    end = start + per_page

    return {
        'items': query_result[start:end],
        'total': total,
        'page': page,
        'per_page': per_page,
        'pages': (total + per_page - 1) // per_page,
        'has_prev': page > 1,
        'has_next': end < total
    }


def require_user_id():
    """
    获取当前用户ID，未登录则抛出异常

    Returns:
        int: 用户ID

    Raises:
        RuntimeError: 用户未登录时抛出
    """
    uid = current_user_id()
    if not uid:
        raise RuntimeError("No current user id in session")
    return uid


def get_user_department():
    """
    获取当前用户的部门信息

    Returns:
        dict: 包含部门信息的字典，未登录或无部门返回None
    """
    uid = current_user_id()
    if not uid:
        return None

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT u.department_id, u.role, d.name as dept_name, d.level, d.path
        FROM users u
        LEFT JOIN departments d ON u.department_id = d.id
        WHERE u.id = ?
        """,
        (uid,)
    )
    row = cur.fetchone()
    return dict(row) if row else None


def get_accessible_departments(user_dept_info=None):
    """
    获取当前用户可以访问的所有部门

    Args:
        user_dept_info: 用户部门信息，默认自动获取

    Returns:
        list: 可访问的部门列表
    """
    if user_dept_info is None:
        user_dept_info = get_user_department()

    if not user_dept_info:
        return []

    conn = get_db()
    cur = conn.cursor()

    # 管理员可以看到所有部门（即使没有department_id）
    if user_dept_info['role'] == 'admin':
        cur.execute("SELECT id, name, level, path FROM departments ORDER BY level, name")
    else:
        # 普通用户必须有department_id
        if not user_dept_info['department_id']:
            return []

        # 普通用户可以看到自己的部门及所有子部门
        user_path = user_dept_info['path'] or f"/{user_dept_info['department_id']}"
        cur.execute(
            "SELECT id, name, level, path FROM departments WHERE path LIKE ? OR id = ? ORDER BY level, name",
            (f"{user_path}/%", user_dept_info['department_id'])
        )

    rows = cur.fetchall()
    return [dict(row) for row in rows]


def get_accessible_user_ids():
    """
    [已废弃] 获取当前用户可以访问的所有用户ID

    注意：权限系统已改为基于department_id，不再使用user_id过滤数据
    请使用 get_accessible_department_ids() 替代

    Returns:
        list: 可访问的用户ID列表（为兼容性保留）
    """
    import warnings
    warnings.warn(
        "get_accessible_user_ids()已废弃，请使用get_accessible_department_ids()进行权限过滤",
        DeprecationWarning,
        stacklevel=2
    )

    user_dept_info = get_user_department()
    accessible_depts = get_accessible_departments(user_dept_info)

    if not accessible_depts:
        return [current_user_id()]  # 回退到仅当前用户

    dept_ids = [dept['id'] for dept in accessible_depts] + [user_dept_info['department_id']]

    conn = get_db()
    cur = conn.cursor()
    placeholders = ','.join('?' * len(dept_ids))
    cur.execute(
        f"SELECT id FROM users WHERE department_id IN ({placeholders}) OR id = ?",
        dept_ids + [current_user_id()]
    )
    user_ids = [row[0] for row in cur.fetchall()]

    return user_ids if user_ids else [current_user_id()]


def get_accessible_department_ids(user_dept_info=None):
    """
    获取当前用户可以访问的所有部门ID列表

    Args:
        user_dept_info: 用户部门信息，默认自动获取

    Returns:
        list: 可访问的部门ID列表
    """
    accessible_depts = get_accessible_departments(user_dept_info)
    return [dept['id'] for dept in accessible_depts] if accessible_depts else []


def validate_employee_access(emp_no):
    """
    检查当前用户是否可以访问指定员工

    Args:
        emp_no: 员工工号

    Returns:
        bool: True表示可以访问，False表示无权访问
    """
    if not emp_no:
        return False

    # 管理员可以访问所有员工
    if current_user_role() == 'admin':
        return True

    # 获取员工所属部门
    emp_dept_id = get_employee_department_id(emp_no)
    if emp_dept_id is None:
        return False

    # 获取当前用户可访问的部门列表
    accessible_dept_ids = get_accessible_department_ids()

    return emp_dept_id in accessible_dept_ids


def get_employee_department_id(emp_no):
    """
    获取指定员工的部门ID

    Args:
        emp_no: 员工工号

    Returns:
        int: 部门ID，员工不存在返回None
    """
    if not emp_no:
        return None

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT department_id FROM employees WHERE emp_no = ?", (emp_no,))
    row = cur.fetchone()

    return row[0] if row else None


def calculate_years_from_date(date_str):
    """
    计算从指定日期到当前的年限（保留1位小数）

    Args:
        date_str: 日期字符串（格式：YYYY-MM-DD）

    Returns:
        float: 年限（保留1位小数），日期无效返回None
    """
    if not date_str:
        return None

    try:
        from datetime import datetime
        # 解析日期
        if isinstance(date_str, str):
            for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y-%m-%d %H:%M:%S']:
                try:
                    start_date = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    continue
            else:
                return None
        else:
            start_date = date_str

        # 计算年限
        current_date = datetime.now()
        days_diff = (current_date - start_date).days
        years = days_diff / 365.25  # 考虑闰年

        return round(years, 1)

    except Exception:
        return None


def log_import_operation(module, operation, file_name=None, total_rows=0,
                         success_rows=0, failed_rows=0, skipped_rows=0,
                         error_message=None, import_details=None):
    """
    记录数据导入操作日志

    Args:
        module: 模块名称 (personnel/performance/training/safety)
        operation: 操作类型 (import/batch_import)
        file_name: 导入文件名
        total_rows: 总行数
        success_rows: 成功导入行数
        failed_rows: 失败行数
        skipped_rows: 跳过行数（权限不足等）
        error_message: 错误信息
        import_details: 导入详情（可以是字典，会自动转JSON）

    Returns:
        int: 日志记录ID，失败返回None
    """
    from flask import session, request
    import json

    try:
        user_id = session.get('user_id')
        if not user_id:
            return None

        conn = get_db()
        cur = conn.cursor()

        # 获取用户信息
        cur.execute("""
            SELECT u.username, u.role, u.department_id, d.name as department_name
            FROM users u
            LEFT JOIN departments d ON u.department_id = d.id
            WHERE u.id = ?
        """, (user_id,))
        user_info = cur.fetchone()

        if not user_info:
            return None

        # 获取IP地址
        ip_address = request.remote_addr if request else None

        # 转换导入详情为JSON
        details_json = None
        if import_details:
            if isinstance(import_details, dict):
                details_json = json.dumps(import_details, ensure_ascii=False)
            else:
                details_json = str(import_details)

        # 插入日志记录
        cur.execute("""
            INSERT INTO import_logs (
                module, operation, user_id, username, user_role,
                department_id, department_name, file_name,
                total_rows, success_rows, failed_rows, skipped_rows,
                error_message, import_details, ip_address
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            module, operation, user_id, user_info['username'], user_info['role'],
            user_info['department_id'], user_info['department_name'], file_name,
            total_rows, success_rows, failed_rows, skipped_rows,
            error_message, details_json, ip_address
        ))

        conn.commit()
        return cur.lastrowid

    except Exception as e:
        print(f"记录导入日志失败: {e}")
        return None


def build_department_filter(table_alias=None):
    """
    构建基于部门的SQL过滤条件

    Args:
        table_alias: 表别名（如 'e', 'p', 'tr'），用于构建 JOIN 子句

    Returns:
        tuple: (WHERE条件, JOIN子句, 参数列表)

    示例:
        # 对于employees表（已有department_id）
        where_clause, join_clause, params = build_department_filter()
        # where_clause = "department_id IN (?,?,?)"
        # join_clause = ""
        # params = [1, 2, 3]

        # 对于performance_records表（通过emp_no关联employees）
        where_clause, join_clause, params = build_department_filter('pr')
        # where_clause = "e.department_id IN (?,?,?)"
        # join_clause = "LEFT JOIN employees e ON pr.emp_no = e.emp_no"
        # params = [1, 2, 3]
    """
    user_dept_info = get_user_department()

    # 管理员无需过滤
    if user_dept_info and user_dept_info['role'] == 'admin':
        return "1=1", "", []

    # 获取可访问部门
    dept_ids = get_accessible_department_ids(user_dept_info)

    if not dept_ids:
        # 无可访问部门，返回空结果条件
        return "1=0", "", []

    placeholders = ','.join('?' * len(dept_ids))

    # 根据是否有表别名决定JOIN和WHERE
    if table_alias:
        # 需要JOIN employees表
        join_clause = f"LEFT JOIN employees e ON {table_alias}.emp_no = e.emp_no"
        where_clause = f"e.department_id IN ({placeholders})"
    else:
        # 直接查询employees表或已有department_id的表
        join_clause = ""
        where_clause = f"department_id IN ({placeholders})"

    return where_clause, join_clause, dept_ids


# ==================== 日期筛选工具函数 ====================

def parse_date_filters(default_range='current_month'):
    """
    解析URL参数中的日期筛选条件

    Args:
        default_range: 默认日期范围
            - 'current_month': 当月（月初至月末）
            - 'last_month': 上月
            - 'last_3_months': 最近3个月
            - None: 不设默认值，返回空

    Returns:
        tuple: (start_date, end_date) - YYYY-MM-DD格式字符串或None

    示例:
        # 使用默认当月范围
        start_date, end_date = parse_date_filters()  # ('2026-01-01', '2026-01-31')

        # 不设默认值（保持URL参数）
        start_date, end_date = parse_date_filters(None)  # (None, None) 或用户输入的值
    """
    from datetime import datetime, timedelta
    import calendar

    # 尝试从请求参数获取日期
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()

    # 如果用户已提供日期，直接返回
    if start_date or end_date:
        return (start_date or None, end_date or None)

    # 如果没有指定默认范围，返回空
    if default_range is None:
        return (None, None)

    # 根据默认范围计算日期
    now = datetime.now()

    if default_range == 'current_month':
        # 当月：月初至月末
        first_day = datetime(now.year, now.month, 1)
        last_day_num = calendar.monthrange(now.year, now.month)[1]
        last_day = datetime(now.year, now.month, last_day_num)
        return (first_day.strftime('%Y-%m-%d'), last_day.strftime('%Y-%m-%d'))

    elif default_range == 'last_month':
        # 上月：上月月初至上月月末
        first_day_this_month = datetime(now.year, now.month, 1)
        last_day_last_month = first_day_this_month - timedelta(days=1)
        first_day_last_month = datetime(last_day_last_month.year, last_day_last_month.month, 1)
        return (first_day_last_month.strftime('%Y-%m-%d'), last_day_last_month.strftime('%Y-%m-%d'))

    elif default_range == 'last_3_months':
        # 最近3个月：90天前至今天
        end_date = now
        start_date = now - timedelta(days=90)
        return (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

    # 未知的default_range，返回空
    return (None, None)


def build_date_filter_sql(date_column, start_date=None, end_date=None):
    """
    构建日期筛选的SQL条件和参数

    Args:
        date_column: 日期字段名（如'training_date', 'tr.training_date'）
        start_date: 开始日期（YYYY-MM-DD格式）
        end_date: 结束日期（YYYY-MM-DD格式）

    Returns:
        tuple: (conditions, params)
            - conditions: SQL条件列表（可直接用AND连接）
            - params: 参数列表（用于参数化查询）

    示例:
        # 基础用法
        conditions, params = build_date_filter_sql('training_date', '2025-01-01', '2025-01-31')
        # conditions = ['training_date >= ?', 'training_date <= ?']
        # params = ['2025-01-01', '2025-01-31']

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        cursor.execute(f"SELECT * FROM records WHERE {where_clause}", params)

        # 结合其他条件
        all_conditions = []
        all_params = []

        date_conditions, date_params = build_date_filter_sql('tr.training_date', start_date, end_date)
        all_conditions.extend(date_conditions)
        all_params.extend(date_params)

        if name_filter:
            all_conditions.append("name LIKE ?")
            all_params.append(f"%{name_filter}%")

        where_clause = " AND ".join(all_conditions) if all_conditions else "1=1"
        cursor.execute(f"SELECT * FROM table WHERE {where_clause}", all_params)
    """
    conditions = []
    params = []

    if start_date:
        conditions.append(f"{date_column} >= ?")
        params.append(start_date)

    if end_date:
        conditions.append(f"{date_column} <= ?")
        params.append(end_date)

    return (conditions, params)
