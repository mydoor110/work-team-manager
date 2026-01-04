#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
部门管理模块
负责部门层级结构管理和用户分配
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash
import sqlite3
from models.database import get_db
from .decorators import admin_required

# 创建 Blueprint  
departments_bp = Blueprint('departments', __name__, url_prefix='/departments')
APP_TITLE = "绩效汇总 · 简易版"

@departments_bp.route('/', methods=['GET', 'POST'])
@admin_required
def index():
    """部门列表页面"""
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'create':
            name = request.form.get('name', '').strip()
            parent_id = request.form.get('parent_id')
            description = request.form.get('description', '').strip()
            manager_user_id = request.form.get('manager_user_id')

            if not name:
                flash('部门名称不能为空', 'warning')
            else:
                try:
                    conn = get_db()
                    cur = conn.cursor()
                    parent_id = int(parent_id) if parent_id else None
                    manager_user_id = int(manager_user_id) if manager_user_id else None

                    # 计算层级和路径
                    if parent_id:
                        # 获取父部门的层级和路径
                        cur.execute("SELECT level, path FROM departments WHERE id=?", (parent_id,))
                        parent = cur.fetchone()
                        if parent:
                            level = parent['level'] + 1
                            parent_path = parent['path']
                        else:
                            flash('父部门不存在', 'danger')
                            return redirect(url_for('departments.index'))
                    else:
                        # 根部门
                        level = 1
                        parent_path = ""

                    # 插入新部门(不包含path,稍后更新)
                    cur.execute(
                        "INSERT INTO departments(name, parent_id, description, manager_user_id, level) VALUES (?, ?, ?, ?, ?)",
                        (name, parent_id, description, manager_user_id, level)
                    )
                    new_dept_id = cur.lastrowid

                    # 设置新部门的path
                    if parent_path:
                        new_path = f"{parent_path}/{new_dept_id}"
                    else:
                        new_path = f"/{new_dept_id}"

                    cur.execute("UPDATE departments SET path=? WHERE id=?", (new_path, new_dept_id))
                    conn.commit()
                    flash(f'部门创建成功 (层级: {level})', 'success')
                except Exception as e:
                    flash(f'创建失败: {e}', 'danger')
                    
        return redirect(url_for('departments.index'))
        
    # 获取部门列表（包含层级、管理员、子部门统计）
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            d.id,
            d.name,
            d.parent_id,
            d.description,
            d.level,
            d.path,
            d.manager_user_id,
            u.username as manager_name,
            (SELECT COUNT(*) FROM departments WHERE parent_id = d.id) as child_count
        FROM departments d
        LEFT JOIN users u ON d.manager_user_id = u.id
        ORDER BY d.path
    """)
    departments_list = cur.fetchall()

    # 获取所有用户列表（用于部门负责人选择）
    cur.execute("SELECT id, username FROM users ORDER BY username")
    users_list = [dict(row) for row in cur.fetchall()]

    return render_template('departments.html', title='部门管理 | ' + APP_TITLE,
                         departments=departments_list, users=users_list)

@departments_bp.route('/<int:dept_id>', methods=['GET', 'POST'])
@admin_required
def detail(dept_id):
    """部门详情页面"""
    conn = get_db()
    cur = conn.cursor()
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'update':
            name = request.form.get('name', '').strip()
            description = request.form.get('description', '').strip()
            manager_user_id = request.form.get('manager_user_id')
            # 将空字符串转换为 None
            manager_user_id = int(manager_user_id) if manager_user_id else None

            if name:
                # 获取原负责人信息
                cur.execute("SELECT manager_user_id FROM departments WHERE id=?", (dept_id,))
                old_manager = cur.fetchone()
                old_manager_id = old_manager['manager_user_id'] if old_manager else None

                # 更新部门信息
                cur.execute("UPDATE departments SET name=?, description=?, manager_user_id=? WHERE id=?",
                          (name, description, manager_user_id, dept_id))

                # 双向同步用户信息
                # 1. 如果设置了新的负责人,更新该用户的部门和角色
                if manager_user_id:
                    cur.execute(
                        "UPDATE users SET department_id = ?, role = 'manager' WHERE id = ?",
                        (dept_id, manager_user_id)
                    )

                # 2. 如果取消了负责人或更换了负责人,检查旧负责人是否还管理其他部门
                if old_manager_id and old_manager_id != manager_user_id:
                    # 检查该用户是否还是其他部门的负责人
                    cur.execute(
                        "SELECT COUNT(*) as cnt FROM departments WHERE manager_user_id = ?",
                        (old_manager_id,)
                    )
                    other_dept_count = cur.fetchone()[0]
                    # 如果不再是任何部门的负责人,将角色改为普通用户
                    if other_dept_count == 0:
                        cur.execute(
                            "UPDATE users SET role = 'user' WHERE id = ?",
                            (old_manager_id,)
                        )

                conn.commit()
                flash('部门信息更新成功', 'success')
                
        elif action == 'assign_user':
            user_id = request.form.get('user_id', type=int)
            if user_id:
                cur.execute("UPDATE users SET department_id=? WHERE id=?", (dept_id, user_id))
                conn.commit()
                flash('用户分配成功', 'success')
                
        elif action == 'remove_user':
            user_id = request.form.get('user_id', type=int)
            if user_id:
                cur.execute("UPDATE users SET department_id=NULL WHERE id=?", (user_id,))
                conn.commit()
                flash('用户已移出', 'success')
                
        elif action == 'delete':
            # 检查是否可以删除
            cur.execute("SELECT COUNT(*) as cnt FROM departments WHERE parent_id=?", (dept_id,))
            sub_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) as cnt FROM users WHERE department_id=?", (dept_id,))
            user_count = cur.fetchone()[0]
            
            if sub_count > 0:
                flash('无法删除具有子部门的部门', 'warning')
            elif user_count > 0:
                flash('无法删除具有用户的部门', 'warning')
            elif dept_id == 1:
                flash('无法删除根部门', 'warning')
            else:
                cur.execute("DELETE FROM departments WHERE id=?", (dept_id,))
                conn.commit()
                flash('部门删除成功', 'success')
                return redirect(url_for('departments.index'))
                
        return redirect(url_for('departments.detail', dept_id=dept_id))
        
    # 获取部门信息
    cur.execute("SELECT * FROM departments WHERE id=?", (dept_id,))
    dept = cur.fetchone()
    
    if not dept:
        flash('部门不存在', 'danger')
        return redirect(url_for('departments.index'))
        
    # 获取用户列表
    cur.execute("SELECT id, username FROM users ORDER BY username")
    users_list = [dict(row) for row in cur.fetchall()]

    # 获取部门员工列表（包含计算字段）
    cur.execute("""
        SELECT
            emp_no, name, position, class_name,
            certification_date, solo_driving_date
        FROM employees
        WHERE department_id=?
        ORDER BY CAST(emp_no as INTEGER)
    """, (dept_id,))
    employees = []
    for row in cur.fetchall():
        emp_dict = dict(row)
        # 计算取证年限和单独驾驶年限
        if emp_dict.get('certification_date'):
            from .helpers import calculate_years_from_date
            emp_dict['certification_years'] = calculate_years_from_date(emp_dict['certification_date'])
        else:
            emp_dict['certification_years'] = None

        if emp_dict.get('solo_driving_date'):
            from .helpers import calculate_years_from_date
            emp_dict['solo_driving_years'] = calculate_years_from_date(emp_dict['solo_driving_date'])
        else:
            emp_dict['solo_driving_years'] = None

        employees.append(emp_dict)

    return render_template('department_detail.html', title=f'部门管理 - {dept["name"]} | ' + APP_TITLE,
                         department=dept, users=users_list, employees=employees)
