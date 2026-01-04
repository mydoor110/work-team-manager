#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
认证授权模块
负责用户登录、登出、密码管理
"""
from flask import Blueprint, render_template, render_template_string, request, session, redirect, url_for, flash
from werkzeug.security import check_password_hash, generate_password_hash
from models.database import get_db
from utils.validators import FormValidator
from utils.logger import AuditLogger, SecurityLogger
from .decorators import login_required
from .helpers import current_user_id

# 创建 Blueprint
auth_bp = Blueprint('auth', __name__)

# 应用标题（从环境变量或配置中获取）
APP_TITLE = "绩效汇总 · 简易版"


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """
    用户登录

    GET: 显示登录页面
    POST: 处理登录请求
    """
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()

        # 基础验证
        if not username or not password:
            flash('请输入账号和密码', 'warning')
            return render_template('login.html', title='登录 | ' + APP_TITLE)

        # 查询用户
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT id, password_hash, role FROM users WHERE username=?", (username,))
        row = cur.fetchone()

        # 验证密码
        if row and check_password_hash(row['password_hash'], password):
            # 清除旧session
            session.clear()

            # 设置新session
            session['logged_in'] = True
            session['user_id'] = row['id']
            session['username'] = username
            session['role'] = row['role'] if row['role'] else 'user'
            session.permanent = True

            # 记录审计日志
            try:
                AuditLogger.login(username, success=True)
            except:
                pass  # 日志失败不影响登录

            flash('登录成功', 'success')

            # 跳转到原始请求页面或首页
            next_url = request.args.get('next') or url_for('performance.index')
            return redirect(next_url)
        else:
            # 记录安全日志
            try:
                SecurityLogger.failed_login(username, '账号或密码不正确')
            except:
                pass

            flash('账号或密码不正确', 'danger')

    return render_template('login.html', title='登录 | ' + APP_TITLE)


@auth_bp.route('/logout')
def logout():
    """
    用户登出
    """
    username = session.get('username', 'unknown')

    # 记录审计日志
    try:
        AuditLogger.logout(username)
    except:
        pass

    # 清除session
    session.clear()
    flash('已退出登录', 'info')

    return redirect(url_for('auth.login'))


@auth_bp.route('/change_password', methods=['GET', 'POST'])
@login_required
def change_password():
    """
    修改密码

    GET: 显示修改密码页面
    POST: 处理密码修改请求
    """
    user_id = current_user_id()

    if request.method == 'POST':
        old_password = request.form.get('old_password', '').strip()
        new_password = request.form.get('new_password', '').strip()

        # 基础验证
        if not old_password or not new_password:
            flash('请输入旧密码和新密码', 'warning')
            return render_template_string(CHANGE_PASSWORD_TEMPLATE, title='修改密码 | ' + APP_TITLE)

        # 验证旧密码
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT password_hash FROM users WHERE id=?", (user_id,))
        row = cur.fetchone()

        if row and check_password_hash(row['password_hash'], old_password):
            # 更新密码
            new_hash = generate_password_hash(new_password)
            cur.execute("UPDATE users SET password_hash=? WHERE id=?", (new_hash, user_id))
            conn.commit()

            # 记录审计日志
            try:
                AuditLogger.update('user', user_id, {'action': 'password_changed'})
            except:
                pass

            flash('密码修改成功，请重新登录', 'success')

            # 清除session，要求重新登录
            session.clear()
            return redirect(url_for('auth.login'))
        else:
            flash('旧密码错误', 'danger')

    return render_template_string(CHANGE_PASSWORD_TEMPLATE, title='修改密码 | ' + APP_TITLE)


# 修改密码页面模板
CHANGE_PASSWORD_TEMPLATE = """
{% extends "base.html" %}
{% block content %}
<div class="row justify-content-center">
  <div class="col-md-4">
    <div class="card shadow">
      <div class="card-body">
        <h5 class="card-title mb-3">修改密码</h5>
        <form method="post">
          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
          <div class="mb-3">
            <label class="form-label">旧密码</label>
            <input class="form-control" type="password" name="old_password" required>
          </div>
          <div class="mb-3">
            <label class="form-label">新密码</label>
            <input class="form-control" type="password" name="new_password" required minlength="6">
            <div class="form-text">密码长度至少6位</div>
          </div>
          <button class="btn btn-primary w-100" type="submit">修改</button>
          <a class="btn btn-link w-100 mt-2" href="{{ url_for('performance.index') }}">返回</a>
        </form>
      </div>
    </div>
  </div>
</div>
{% endblock %}
"""
