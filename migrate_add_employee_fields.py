#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库迁移脚本: 添加员工部门关联和日期字段
功能:
1. 为employees表添加department_id字段(部门关联)
2. 为employees表添加certification_date字段(取证日期)
3. 为employees表添加solo_driving_date字段(单独驾驶日期)
4. 修改UNIQUE约束: 从UNIQUE(emp_no, user_id)改为UNIQUE(emp_no)
5. 迁移现有数据: 将user的department_id同步到员工记录
"""

import sqlite3
import os
from datetime import datetime

DB_PATH = 'app.db'

def backup_database():
    """备份数据库"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f'app.db.backup_{timestamp}'

    if os.path.exists(DB_PATH):
        import shutil
        shutil.copy2(DB_PATH, backup_path)
        print(f"✓ 数据库已备份到: {backup_path}")
        return backup_path
    else:
        print("✗ 数据库文件不存在")
        return None

def check_migration_status(conn):
    """检查迁移状态"""
    cur = conn.cursor()

    # 检查是否已经存在department_id字段
    cur.execute("PRAGMA table_info(employees)")
    columns = {row[1]: row for row in cur.fetchall()}

    has_department_id = 'department_id' in columns
    has_certification_date = 'certification_date' in columns
    has_solo_driving_date = 'solo_driving_date' in columns

    if has_department_id and has_certification_date and has_solo_driving_date:
        print("✓ 迁移已完成，无需重复执行")
        return True

    print(f"迁移状态检查:")
    print(f"  - department_id: {'已存在' if has_department_id else '需要添加'}")
    print(f"  - certification_date: {'已存在' if has_certification_date else '需要添加'}")
    print(f"  - solo_driving_date: {'已存在' if has_solo_driving_date else '需要添加'}")

    return False

def migrate_database():
    """执行数据库迁移"""

    print("=" * 60)
    print("开始数据库迁移: 添加员工部门关联和日期字段")
    print("=" * 60)

    # 步骤1: 备份数据库
    print("\n[步骤1/11] 备份数据库")
    backup_path = backup_database()
    if not backup_path:
        print("✗ 备份失败，迁移终止")
        return False

    # 步骤2: 连接数据库
    print("\n[步骤2/11] 连接数据库")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        print("✓ 数据库连接成功")
    except Exception as e:
        print(f"✗ 数据库连接失败: {e}")
        return False

    # 步骤3: 检查迁移状态
    print("\n[步骤3/11] 检查迁移状态")
    if check_migration_status(conn):
        conn.close()
        return True

    try:
        # 步骤4: 查询现有员工数据
        print("\n[步骤4/11] 查询现有员工数据")
        cur.execute("SELECT COUNT(*) as count FROM employees")
        employee_count = cur.fetchone()[0]
        print(f"✓ 现有员工记录数: {employee_count}")

        # 步骤5: 添加新字段
        print("\n[步骤5/11] 添加新字段到employees表")

        # 添加department_id字段
        try:
            cur.execute("ALTER TABLE employees ADD COLUMN department_id INTEGER")
            print("✓ 已添加 department_id 字段")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                print("  - department_id 字段已存在，跳过")
            else:
                raise

        # 添加certification_date字段
        try:
            cur.execute("ALTER TABLE employees ADD COLUMN certification_date TEXT")
            print("✓ 已添加 certification_date 字段")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                print("  - certification_date 字段已存在，跳过")
            else:
                raise

        # 添加solo_driving_date字段
        try:
            cur.execute("ALTER TABLE employees ADD COLUMN solo_driving_date TEXT")
            print("✓ 已添加 solo_driving_date 字段")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                print("  - solo_driving_date 字段已存在，跳过")
            else:
                raise

        conn.commit()

        # 步骤6: 迁移department_id数据
        print("\n[步骤6/11] 迁移department_id数据(从users表同步)")
        cur.execute("""
            UPDATE employees
            SET department_id = (
                SELECT department_id
                FROM users
                WHERE users.id = employees.user_id
            )
            WHERE department_id IS NULL
        """)
        updated_rows = cur.rowcount
        print(f"✓ 已更新 {updated_rows} 条员工记录的department_id")
        conn.commit()

        # 步骤7: 检查NULL的department_id
        print("\n[步骤7/11] 检查未分配部门的员工")
        cur.execute("SELECT COUNT(*) FROM employees WHERE department_id IS NULL")
        null_dept_count = cur.fetchone()[0]
        if null_dept_count > 0:
            print(f"⚠️  警告: 有 {null_dept_count} 条员工记录未分配部门")
            cur.execute("""
                SELECT emp_no, name, user_id
                FROM employees
                WHERE department_id IS NULL
                LIMIT 5
            """)
            for row in cur.fetchall():
                print(f"   - 工号: {row[0]}, 姓名: {row[1]}, user_id: {row[2]}")
        else:
            print("✓ 所有员工都已分配部门")

        # 步骤8: 创建新表(修改唯一约束)
        print("\n[步骤8/11] 创建新表结构(修改唯一约束)")
        cur.execute("""
            CREATE TABLE employees_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                emp_no TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                class_name TEXT,
                position TEXT,
                birth_date TEXT,
                marital_status TEXT,
                hometown TEXT,
                political_status TEXT,
                specialty TEXT,
                education TEXT,
                graduation_school TEXT,
                work_start_date TEXT,
                entry_date TEXT,
                department_id INTEGER,
                certification_date TEXT,
                solo_driving_date TEXT,
                created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE SET NULL
            )
        """)
        print("✓ 新表结构创建成功")

        # 步骤9: 复制数据到新表
        print("\n[步骤9/11] 复制数据到新表")
        cur.execute("""
            INSERT INTO employees_new
            (id, emp_no, name, user_id, class_name, position, birth_date,
             marital_status, hometown, political_status, specialty, education,
             graduation_school, work_start_date, entry_date, department_id,
             certification_date, solo_driving_date, created_at)
            SELECT
                id, emp_no, name, user_id, class_name, position, birth_date,
                marital_status, hometown, political_status, specialty, education,
                graduation_school, work_start_date, entry_date, department_id,
                certification_date, solo_driving_date, created_at
            FROM employees
        """)
        copied_rows = cur.rowcount
        print(f"✓ 已复制 {copied_rows} 条员工记录")
        conn.commit()

        # 步骤10: 替换旧表
        print("\n[步骤10/11] 替换旧表")
        cur.execute("DROP TABLE employees")
        cur.execute("ALTER TABLE employees_new RENAME TO employees")
        print("✓ 表替换成功")

        # 重建索引
        print("\n  - 重建索引")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_employees_user_id ON employees(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_employees_department_id ON employees(department_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_employees_emp_no ON employees(emp_no)")
        print("✓ 索引创建成功")
        conn.commit()

        # 步骤11: 验证数据完整性
        print("\n[步骤11/11] 验证数据完整性")
        cur.execute("SELECT COUNT(*) FROM employees")
        final_count = cur.fetchone()[0]

        if final_count == employee_count:
            print(f"✓ 数据完整性验证通过: {final_count}/{employee_count}")
        else:
            print(f"✗ 数据完整性验证失败: {final_count}/{employee_count}")
            raise Exception("数据丢失，回滚迁移")

        # 验证唯一约束
        cur.execute("""
            SELECT emp_no, COUNT(*) as cnt
            FROM employees
            GROUP BY emp_no
            HAVING cnt > 1
        """)
        duplicate_emp_nos = cur.fetchall()
        if duplicate_emp_nos:
            print(f"⚠️  警告: 发现重复工号:")
            for row in duplicate_emp_nos:
                print(f"   - 工号 {row[0]} 重复 {row[1]} 次")
        else:
            print("✓ 工号唯一性验证通过")

        # 提交事务
        conn.commit()
        print("\n" + "=" * 60)
        print("✓ 数据库迁移完成!")
        print("=" * 60)
        print("\n迁移摘要:")
        print(f"  - 员工记录总数: {final_count}")
        print(f"  - 已分配部门: {final_count - null_dept_count}")
        print(f"  - 未分配部门: {null_dept_count}")
        print(f"  - 备份文件: {backup_path}")

        return True

    except Exception as e:
        print(f"\n✗ 迁移失败: {e}")
        conn.rollback()
        print("已回滚所有更改")
        print(f"可从备份恢复: {backup_path}")
        return False
    finally:
        conn.close()

if __name__ == '__main__':
    print("\n警告: 此脚本将修改数据库结构")
    print("确保已经:")
    print("  1. 停止应用程序")
    print("  2. 备份重要数据")
    print("  3. 在测试环境验证过")

    response = input("\n是否继续执行迁移? (yes/no): ")
    if response.lower() in ['yes', 'y']:
        success = migrate_database()
        if success:
            print("\n✓ 迁移成功完成！")
            print("可以启动应用程序了。")
        else:
            print("\n✗ 迁移失败！")
            print("请检查错误信息并从备份恢复。")
    else:
        print("\n已取消迁移")
