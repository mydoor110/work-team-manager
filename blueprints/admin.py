#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理模块
负责用户管理、备份管理等系统级功能
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file
from werkzeug.security import generate_password_hash
import sqlite3
import json
from datetime import datetime, timedelta
from models.database import get_db
from utils.backup import BackupManager, get_backup_statistics
from .decorators import admin_required
from openpyxl import Workbook
import os
from config.settings import EXPORT_DIR

# 创建 Blueprint
admin_bp = Blueprint('admin', __name__, url_prefix='/admin')
APP_TITLE = "绩效汇总 · 简易版"

# ========== 用户管理 ==========

@admin_bp.route('/users', methods=['GET', 'POST'])
@admin_required
def users():
    """用户管理页面"""
    conn = get_db()
    cur = conn.cursor()
    # action可能来自URL参数(GET)或表单数据(POST)
    action = request.args.get('action') or request.form.get('action')

    if request.method == 'POST' and not action:
        # 创建用户
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        department_id = request.form.get('department_id')
        role = request.form.get('role', 'user')

        if not username or not password:
            flash('请输入用户名和密码', 'warning')
        else:
            try:
                department_id = int(department_id) if department_id else None
                cur.execute(
                    "INSERT INTO users(username, password_hash, department_id, role) VALUES (?, ?, ?, ?)",
                    (username, generate_password_hash(password), department_id, role)
                )
                new_user_id = cur.lastrowid

                # 如果角色是部门管理员且指定了部门,同步更新部门的负责人
                if role == 'manager' and department_id:
                    cur.execute(
                        "UPDATE departments SET manager_user_id = ? WHERE id = ?",
                        (new_user_id, department_id)
                    )

                conn.commit()
                flash('用户已创建', 'success')
            except sqlite3.IntegrityError:
                flash('用户名已存在', 'danger')
            except Exception as e:
                flash(f'创建失败: {e}', 'danger')

    # 处理其他操作
    if action == 'edit_user':
        user_id = request.form.get('user_id', type=int)
        username = request.form.get('username', '').strip()
        department_id = request.form.get('department_id')
        role = request.form.get('role', 'user')
        if username:
            try:
                # 获取用户的旧信息
                cur.execute("SELECT department_id, role FROM users WHERE id=?", (user_id,))
                old_info = cur.fetchone()
                old_dept_id = old_info['department_id'] if old_info else None
                old_role = old_info['role'] if old_info else None

                department_id = int(department_id) if department_id else None
                cur.execute("UPDATE users SET username=?, department_id=?, role=? WHERE id=?",
                          (username, department_id, role, user_id))

                # 双向同步部门负责人
                # 1. 如果从manager变为非manager,清除原部门的负责人设置
                if old_role == 'manager' and role != 'manager' and old_dept_id:
                    cur.execute(
                        "UPDATE departments SET manager_user_id = NULL WHERE id = ? AND manager_user_id = ?",
                        (old_dept_id, user_id)
                    )

                # 2. 如果用户换了部门且之前是manager,清除旧部门的负责人
                if old_role == 'manager' and old_dept_id and old_dept_id != department_id:
                    cur.execute(
                        "UPDATE departments SET manager_user_id = NULL WHERE id = ? AND manager_user_id = ?",
                        (old_dept_id, user_id)
                    )

                # 3. 如果新角色是manager且有部门,设置为该部门的负责人
                if role == 'manager' and department_id:
                    cur.execute(
                        "UPDATE departments SET manager_user_id = ? WHERE id = ?",
                        (user_id, department_id)
                    )

                conn.commit()
                flash('用户信息更新成功', 'success')
            except sqlite3.IntegrityError:
                flash('用户名已存在', 'danger')
        return redirect(url_for('admin.users'))

    elif action == 'reset':
        user_id = request.args.get('id', type=int)
        new_pw = request.args.get('newpw', default='123456')
        if user_id:
            cur.execute("UPDATE users SET password_hash=? WHERE id=?",
                       (generate_password_hash(new_pw), user_id))
            conn.commit()
            flash(f'已重置用户 {user_id} 密码', 'success')
        return redirect(url_for('admin.users'))

    elif action == 'delete':
        user_id = request.args.get('id', type=int)
        if user_id == 1:
            flash('管理员不可删除', 'warning')
        elif user_id:
            cur.execute("DELETE FROM users WHERE id=?", (user_id,))
            conn.commit()
            flash(f'已删除用户 {user_id}', 'success')
        return redirect(url_for('admin.users'))

    # 获取用户列表
    cur.execute("""
        SELECT u.id, u.username, u.created_at, u.role,
               u.department_id,
               d.name as department_name
        FROM users u
        LEFT JOIN departments d ON u.department_id = d.id
        ORDER BY u.id
    """)
    users_list = cur.fetchall()

    # 获取部门列表
    cur.execute("SELECT id, name FROM departments ORDER BY name")
    departments_list = cur.fetchall()

    return render_template('admin_users.html', title='用户管理 | ' + APP_TITLE,
                         users=users_list, departments=departments_list)

# ========== 备份管理 ==========

@admin_bp.route('/backups')
@admin_required
def backups():
    """备份管理页面"""
    try:
        manager = BackupManager()
        backups_list = manager.list_backups()
        stats = get_backup_statistics()
        return render_template('backup_management.html', title='备份管理 | ' + APP_TITLE,
                             backups=backups_list, stats=stats)
    except Exception as e:
        flash(f'加载备份列表失败: {e}', 'danger')
        return redirect(url_for('performance.index'))

@admin_bp.route('/backups/create', methods=['POST'])
@admin_required
def create_backup():
    """创建备份"""
    try:
        description = request.form.get('description', '').strip()
        backup_type = request.form.get('backup_type', 'full')
        manager = BackupManager()
        backup_info = manager.create_backup(backup_type=backup_type, description=description)
        flash(f'备份创建成功: {backup_info["name"]}', 'success')
        return jsonify({'success': True, 'backup': backup_info})
    except Exception as e:
        flash(f'备份创建失败: {e}', 'danger')
        return jsonify({'success': False, 'error': str(e)}), 500

@admin_bp.route('/backups/restore', methods=['POST'])
@admin_required
def restore_backup():
    """恢复备份"""
    try:
        backup_name = request.form.get('backup_name')
        restore_database = request.form.get('restore_database') == 'true'
        restore_config = request.form.get('restore_config') == 'true'
        restore_uploads = request.form.get('restore_uploads') == 'true'
        if not backup_name:
            return jsonify({'success': False, 'error': '未指定备份文件'}), 400
        manager = BackupManager()
        restore_info = manager.restore_backup(backup_name, restore_database, restore_config, restore_uploads)
        flash(f'备份恢复成功: {len(restore_info["restored_files"])} 个文件已恢复', 'success')
        return jsonify({'success': True, 'restore_info': restore_info})
    except Exception as e:
        flash(f'备份恢复失败: {e}', 'danger')
        return jsonify({'success': False, 'error': str(e)}), 500

@admin_bp.route('/backups/delete', methods=['POST'])
@admin_required
def delete_backup():
    """删除备份"""
    try:
        backup_name = request.form.get('backup_name')
        if not backup_name:
            return jsonify({'success': False, 'error': '未指定备份文件'}), 400
        manager = BackupManager()
        success = manager.delete_backup(backup_name)
        if success:
            flash(f'备份已删除: {backup_name}', 'success')
            return jsonify({'success': True})
        return jsonify({'success': False, 'error': '备份文件不存在'}), 404
    except Exception as e:
        flash(f'备份删除失败: {e}', 'danger')
        return jsonify({'success': False, 'error': str(e)}), 500

@admin_bp.route('/backups/download/<backup_name>')
@admin_required
def download_backup(backup_name):
    """下载备份"""
    try:
        from utils.backup import BackupConfig
        import os
        backup_path = os.path.join(BackupConfig.BACKUP_DIR, backup_name)
        if not os.path.exists(backup_path):
            flash('备份文件不存在', 'danger')
            return redirect(url_for('admin.backups'))
        return send_file(backup_path, as_attachment=True, download_name=backup_name, mimetype='application/zip')
    except Exception as e:
        flash(f'备份下载失败: {e}', 'danger')
        return redirect(url_for('admin.backups'))


# ========== 导入日志审查 ==========

@admin_bp.route('/import-logs')
@admin_required
def import_logs():
    """导入日志审查页面"""
    conn = get_db()
    cur = conn.cursor()

    # 获取筛选参数
    module_filter = request.args.get('module', '').strip()
    user_filter = request.args.get('user', '').strip()
    start_date = request.args.get('start_date', '').strip()
    end_date = request.args.get('end_date', '').strip()
    page = request.args.get('page', 1, type=int)
    per_page = 50

    # 构建查询条件
    conditions = []
    params = []

    if module_filter:
        conditions.append("module = ?")
        params.append(module_filter)

    if user_filter:
        conditions.append("username LIKE ?")
        params.append(f"%{user_filter}%")

    if start_date:
        conditions.append("DATE(created_at) >= ?")
        params.append(start_date)

    if end_date:
        conditions.append("DATE(created_at) <= ?")
        params.append(end_date)

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    # 获取总记录数
    cur.execute(f"SELECT COUNT(*) FROM import_logs WHERE {where_clause}", params)
    total_count = cur.fetchone()[0]

    # 获取分页数据
    offset = (page - 1) * per_page
    cur.execute(f"""
        SELECT
            il.*,
            d.name as dept_name,
            CASE
                WHEN il.user_role = 'admin' THEN '系统管理员'
                WHEN il.user_role = 'manager' THEN '部门管理员'
                ELSE '普通用户'
            END as role_display,
            CASE il.module
                WHEN 'personnel' THEN '人员管理'
                WHEN 'performance' THEN '绩效管理'
                WHEN 'training' THEN '培训管理'
                WHEN 'safety' THEN '安全管理'
                ELSE il.module
            END as module_display
        FROM import_logs il
        LEFT JOIN departments d ON il.department_id = d.id
        WHERE {where_clause}
        ORDER BY il.created_at DESC
        LIMIT ? OFFSET ?
    """, params + [per_page, offset])

    logs = cur.fetchall()

    # 解析 JSON 详情
    logs_with_details = []
    for log in logs:
        log_dict = dict(log)
        if log_dict.get('import_details'):
            try:
                log_dict['details_parsed'] = json.loads(log_dict['import_details'])
            except:
                log_dict['details_parsed'] = {}
        else:
            log_dict['details_parsed'] = {}
        logs_with_details.append(log_dict)

    # 计算分页信息
    total_pages = (total_count + per_page - 1) // per_page

    # 获取统计信息
    cur.execute("""
        SELECT
            module,
            COUNT(*) as count,
            SUM(total_rows) as total_rows,
            SUM(success_rows) as success_rows,
            SUM(failed_rows) as failed_rows,
            SUM(skipped_rows) as skipped_rows
        FROM import_logs
        GROUP BY module
        ORDER BY module
    """)
    stats_by_module = cur.fetchall()

    # 最近7天的导入趋势
    cur.execute("""
        SELECT
            DATE(created_at) as date,
            COUNT(*) as import_count,
            SUM(success_rows) as total_success
        FROM import_logs
        WHERE DATE(created_at) >= DATE('now', '-7 days')
        GROUP BY DATE(created_at)
        ORDER BY date DESC
    """)
    recent_trend = cur.fetchall()

    return render_template(
        'admin_import_logs.html',
        title='导入日志审查 | ' + APP_TITLE,
        logs=logs_with_details,
        stats_by_module=stats_by_module,
        recent_trend=recent_trend,
        total_count=total_count,
        page=page,
        total_pages=total_pages,
        per_page=per_page,
        # 筛选参数
        module_filter=module_filter,
        user_filter=user_filter,
        start_date=start_date,
        end_date=end_date
    )


@admin_bp.route('/import-logs/<int:log_id>')
@admin_required
def import_log_detail(log_id):
    """查看导入日志详情"""
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            il.*,
            d.name as dept_name,
            CASE
                WHEN il.user_role = 'admin' THEN '系统管理员'
                WHEN il.user_role = 'manager' THEN '部门管理员'
                ELSE '普通用户'
            END as role_display,
            CASE il.module
                WHEN 'personnel' THEN '人员管理'
                WHEN 'performance' THEN '绩效管理'
                WHEN 'training' THEN '培训管理'
                WHEN 'safety' THEN '安全管理'
                ELSE il.module
            END as module_display
        FROM import_logs il
        LEFT JOIN departments d ON il.department_id = d.id
        WHERE il.id = ?
    """, (log_id,))

    log = cur.fetchone()

    if not log:
        flash('日志记录不存在', 'warning')
        return redirect(url_for('admin.import_logs'))

    log_dict = dict(log)
    if log_dict.get('import_details'):
        try:
            log_dict['details_parsed'] = json.loads(log_dict['import_details'])
        except:
            log_dict['details_parsed'] = {}
    else:
        log_dict['details_parsed'] = {}

    return render_template(
        'admin_import_log_detail.html',
        title=f'导入日志详情 #{log_id} | ' + APP_TITLE,
        log=log_dict
    )


@admin_bp.route('/import-logs/export')
@admin_required
def export_import_logs():
    """导出导入日志为Excel"""
    conn = get_db()
    cur = conn.cursor()

    # 获取筛选参数（与列表页相同）
    module_filter = request.args.get('module', '').strip()
    user_filter = request.args.get('user', '').strip()
    start_date = request.args.get('start_date', '').strip()
    end_date = request.args.get('end_date', '').strip()

    # 构建查询条件
    conditions = []
    params = []

    if module_filter:
        conditions.append("module = ?")
        params.append(module_filter)

    if user_filter:
        conditions.append("username LIKE ?")
        params.append(f"%{user_filter}%")

    if start_date:
        conditions.append("DATE(created_at) >= ?")
        params.append(start_date)

    if end_date:
        conditions.append("DATE(created_at) <= ?")
        params.append(end_date)

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    # 查询数据
    cur.execute(f"""
        SELECT
            il.id,
            CASE il.module
                WHEN 'personnel' THEN '人员管理'
                WHEN 'performance' THEN '绩效管理'
                WHEN 'training' THEN '培训管理'
                WHEN 'safety' THEN '安全管理'
                ELSE il.module
            END as module_display,
            il.operation,
            il.username,
            CASE
                WHEN il.user_role = 'admin' THEN '系统管理员'
                WHEN il.user_role = 'manager' THEN '部门管理员'
                ELSE '普通用户'
            END as role_display,
            d.name as dept_name,
            il.file_name,
            il.total_rows,
            il.success_rows,
            il.failed_rows,
            il.skipped_rows,
            il.error_message,
            il.ip_address,
            il.created_at
        FROM import_logs il
        LEFT JOIN departments d ON il.department_id = d.id
        WHERE {where_clause}
        ORDER BY il.created_at DESC
    """, params)

    logs = cur.fetchall()

    # 创建 Excel
    wb = Workbook()
    ws = wb.active
    ws.title = "导入日志"

    # 写入表头
    headers = ['ID', '模块', '操作', '用户', '角色', '部门', '文件名',
               '总行数', '成功', '失败', '跳过', '错误信息', 'IP地址', '操作时间']
    ws.append(headers)

    # 写入数据
    for log in logs:
        ws.append([
            log[0],  # ID
            log[1],  # module_display
            log[2],  # operation
            log[3],  # username
            log[4],  # role_display
            log[5] or '',  # dept_name
            log[6] or '',  # file_name
            log[7],  # total_rows
            log[8],  # success_rows
            log[9],  # failed_rows
            log[10],  # skipped_rows
            log[11] or '',  # error_message
            log[12] or '',  # ip_address
            log[13],  # created_at
        ])

    # 设置列宽
    ws.column_dimensions['A'].width = 8
    ws.column_dimensions['B'].width = 12
    ws.column_dimensions['C'].width = 15
    ws.column_dimensions['D'].width = 15
    ws.column_dimensions['E'].width = 12
    ws.column_dimensions['F'].width = 20
    ws.column_dimensions['G'].width = 30
    ws.column_dimensions['L'].width = 40
    ws.column_dimensions['N'].width = 20

    # 保存文件
    filename = f"导入日志_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    filepath = os.path.join(EXPORT_DIR, filename)
    wb.save(filepath)

    return send_file(
        filepath,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
