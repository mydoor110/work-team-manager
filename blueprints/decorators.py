#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
公共装饰器模块
提供认证、授权等通用装饰器
"""
from functools import wraps
from flask import session, redirect, url_for, flash, request


def login_required(f):
    """
    登录验证装饰器

    用于需要用户登录才能访问的路由
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            flash('请先登录', 'warning')
            # 保存原始请求路径,登录后跳转回来
            next_url = request.path
            return redirect(url_for('auth.login', next=next_url))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    """
    管理员权限验证装饰器

    用于需要管理员权限才能访问的路由
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 先检查是否登录
        if not session.get('logged_in'):
            flash('请先登录', 'warning')
            return redirect(url_for('auth.login', next=request.path))

        # 检查管理员权限
        from models.database import get_db

        user_id = session.get('user_id')
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT role FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()

        if not row or row['role'] != 'admin':
            flash('需要管理员权限才能访问此功能', 'danger')
            return redirect(url_for('performance.index'))

        return f(*args, **kwargs)
    return decorated_function


def manager_required(f):
    """
    部门管理员权限验证装饰器

    要求用户角色为 manager 或 admin
    用于需要管理权限的操作（导入、修改、删除等）
    普通用户（user）只有查看权限
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 先检查是否登录
        if not session.get('logged_in'):
            flash('请先登录', 'warning')
            return redirect(url_for('auth.login', next=request.path))

        # 检查管理员权限
        from models.database import get_db

        user_id = session.get('user_id')
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT role FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()

        if not row or row['role'] not in ['admin', 'manager']:
            flash('需要部门管理员或管理员权限才能执行此操作', 'danger')
            return redirect(url_for('performance.index'))

        return f(*args, **kwargs)
    return decorated_function


def role_required(required_role):
    """
    角色权限验证装饰器工厂

    Args:
        required_role: 需要的角色 ('admin', 'manager', 'user')

    Returns:
        装饰器函数
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # 检查是否登录
            if not session.get('logged_in'):
                flash('请先登录', 'warning')
                return redirect(url_for('auth.login', next=request.path))

            # 检查角色权限
            from models.database import get_db

            role_hierarchy = {'admin': 3, 'manager': 2, 'user': 1}
            required_level = role_hierarchy.get(required_role, 0)

            user_id = session.get('user_id')
            conn = get_db()
            cur = conn.cursor()
            cur.execute("SELECT role FROM users WHERE id = ?", (user_id,))
            row = cur.fetchone()

            if not row:
                flash('用户信息异常', 'danger')
                return redirect(url_for('auth.login'))

            user_level = role_hierarchy.get(row['role'], 0)

            if user_level < required_level:
                role_names = {
                    'admin': '管理员',
                    'manager': '管理员或部门经理',
                    'user': '登录用户'
                }
                flash(f'需要{role_names.get(required_role, required_role)}权限', 'danger')
                return redirect(url_for('performance.index'))

            return f(*args, **kwargs)
        return decorated_function
    return decorator
