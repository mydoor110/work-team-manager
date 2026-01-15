#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库迁移脚本：移除user_id数据隔离，改为department_id权限控制
包含安全管理模块
执行方案2的数据库架构改造
"""

import sqlite3
import shutil
from datetime import datetime

DB_PATH = 'app.db'

def backup_database():
    """备份数据库"""
    backup_path = f"{DB_PATH}.migration_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(DB_PATH, backup_path)
    print(f"✅ 数据库已备份到: {backup_path}")
    return backup_path

def clean_orphan_data():
    """清理孤儿数据"""
    print("\n🔍 检查并清理孤儿数据...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 清理training_records孤儿数据
    cur.execute("DELETE FROM training_records WHERE user_id NOT IN (SELECT id FROM users)")
    training_deleted = cur.rowcount
    print(f"✅ 清理training_records孤儿数据: {training_deleted}条")

    # 清理performance_records孤儿数据
    cur.execute("DELETE FROM performance_records WHERE user_id NOT IN (SELECT id FROM users)")
    performance_deleted = cur.rowcount
    print(f"✅ 清理performance_records孤儿数据: {performance_deleted}条")

    conn.commit()
    conn.close()

def handle_duplicate_data():
    """处理重复数据 - 保留ID最大的记录"""
    print("\n🔍 检查并处理重复数据...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 处理performance_records重复数据
    cur.execute("""
        DELETE FROM performance_records
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM performance_records
            GROUP BY emp_no, year, month
        )
    """)
    duplicates_deleted = cur.rowcount
    print(f"✅ 删除performance_records重复数据: {duplicates_deleted}条（保留最新记录）")

    # 检查training_records是否有重复
    cur.execute("""
        SELECT emp_no, training_date, project_category, COUNT(*) as cnt
        FROM training_records
        GROUP BY emp_no, training_date, project_category
        HAVING cnt > 1
    """)
    training_duplicates = cur.fetchall()
    if training_duplicates:
        cur.execute("""
            DELETE FROM training_records
            WHERE id NOT IN (
                SELECT MAX(id)
                FROM training_records
                GROUP BY emp_no, training_date, project_category
            )
        """)
        print(f"✅ 删除training_records重复数据: {cur.rowcount}条")
    else:
        print("✅ training_records无重复数据")

    conn.commit()
    conn.close()

def migrate_employees_table():
    """迁移employees表"""
    print("\n📦 迁移employees表...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 添加created_by字段
    try:
        cur.execute("ALTER TABLE employees ADD COLUMN created_by INTEGER")
        print("✅ 添加created_by字段")
    except sqlite3.OperationalError:
        print("ℹ️  created_by字段已存在")

    # 迁移数据
    cur.execute("UPDATE employees SET created_by = user_id WHERE created_by IS NULL")
    print("✅ 迁移user_id到created_by")

    # 创建新表（匹配实际字段）
    cur.execute("""
        CREATE TABLE employees_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_no TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            department_id INTEGER,
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
            certification_date TEXT,
            solo_driving_date TEXT,
            created_by INTEGER,
            created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
            FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE RESTRICT,
            FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
        )
    """)
    print("✅ 创建新表结构")

    # 迁移数据
    cur.execute("""
        INSERT INTO employees_new (id, emp_no, name, department_id, class_name, position,
                                    birth_date, marital_status, hometown, political_status,
                                    specialty, education, graduation_school, work_start_date,
                                    entry_date, certification_date, solo_driving_date, created_by)
        SELECT id, emp_no, name, department_id, class_name, position,
               birth_date, marital_status, hometown, political_status,
               specialty, education, graduation_school, work_start_date,
               entry_date, certification_date, solo_driving_date, created_by
        FROM employees
    """)
    print("✅ 迁移数据到新表")

    # 替换表
    cur.execute("DROP TABLE employees")
    cur.execute("ALTER TABLE employees_new RENAME TO employees")
    print("✅ 替换为新表")

    conn.commit()
    conn.close()

def migrate_performance_records_table():
    """迁移performance_records表"""
    print("\n📦 迁移performance_records表...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 添加created_by字段
    try:
        cur.execute("ALTER TABLE performance_records ADD COLUMN created_by INTEGER")
        print("✅ 添加created_by字段")
    except sqlite3.OperationalError:
        print("ℹ️  created_by字段已存在")

    # 迁移数据
    cur.execute("UPDATE performance_records SET created_by = user_id WHERE created_by IS NULL")
    print("✅ 迁移user_id到created_by")

    # 创建新表
    cur.execute("""
        CREATE TABLE performance_records_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_no TEXT NOT NULL,
            name TEXT,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            score REAL,
            grade TEXT,
            src_file TEXT,
            created_by INTEGER,
            created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
            UNIQUE(emp_no, year, month),
            FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
        )
    """)
    print("✅ 创建新表结构")

    # 迁移数据
    cur.execute("""
        INSERT INTO performance_records_new (id, emp_no, name, year, month, score, grade, src_file, created_by)
        SELECT id, emp_no, name, year, month, score, grade, src_file, created_by
        FROM performance_records
    """)
    print("✅ 迁移数据到新表")

    # 替换表
    cur.execute("DROP TABLE performance_records")
    cur.execute("ALTER TABLE performance_records_new RENAME TO performance_records")
    print("✅ 替换为新表")

    conn.commit()
    conn.close()

def migrate_training_records_table():
    """迁移training_records表"""
    print("\n📦 迁移training_records表...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 添加created_by字段
    try:
        cur.execute("ALTER TABLE training_records ADD COLUMN created_by INTEGER")
        print("✅ 添加created_by字段")
    except sqlite3.OperationalError:
        print("ℹ️  created_by字段已存在")

    # 迁移数据
    cur.execute("UPDATE training_records SET created_by = user_id WHERE created_by IS NULL")
    print("✅ 迁移user_id到created_by")

    # 创建新表
    cur.execute("""
        CREATE TABLE training_records_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_no TEXT NOT NULL,
            name TEXT,
            team_name TEXT,
            training_date TEXT NOT NULL,
            project_category TEXT,
            problem_type TEXT,
            specific_problem TEXT,
            corrective_measures TEXT,
            time_spent TEXT,
            score INTEGER,
            assessor TEXT,
            remarks TEXT,
            is_qualified INTEGER DEFAULT 1,
            is_disqualified INTEGER DEFAULT 0,
            is_retake INTEGER DEFAULT 0,
            retake_of_record_id INTEGER,
            source_file TEXT,
            created_by INTEGER,
            created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
            UNIQUE(emp_no, training_date, project_category),
            FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
            FOREIGN KEY (retake_of_record_id) REFERENCES training_records(id) ON DELETE SET NULL
        )
    """)
    print("✅ 创建新表结构")

    # 迁移数据
    cur.execute("""
        INSERT INTO training_records_new (id, emp_no, name, team_name, training_date,
                                          project_category, problem_type, specific_problem,
                                          corrective_measures, time_spent, score, assessor,
                                          remarks, is_qualified, is_disqualified, is_retake,
                                          retake_of_record_id, source_file, created_by)
        SELECT id, emp_no, name, team_name, training_date,
               project_category, problem_type, specific_problem,
               corrective_measures, time_spent, score, assessor,
               remarks, is_qualified, is_disqualified, is_retake,
               retake_of_record_id, source_file, created_by
        FROM training_records
    """)
    print("✅ 迁移数据到新表")

    # 替换表
    cur.execute("DROP TABLE training_records")
    cur.execute("ALTER TABLE training_records_new RENAME TO training_records")
    print("✅ 替换为新表")

    conn.commit()
    conn.close()

def migrate_config_tables():
    """迁移绩效配置表"""
    print("\n📦 迁移绩效配置表...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # grade_map
    print("处理 grade_map 表...")
    cur.execute("""
        CREATE TABLE grade_map_new (
            grade TEXT PRIMARY KEY,
            value REAL NOT NULL
        )
    """)
    # 使用GROUP BY和MAX确保每个grade只有一条记录
    cur.execute("INSERT INTO grade_map_new SELECT grade, MAX(value) FROM grade_map GROUP BY grade")
    cur.execute("DROP TABLE grade_map")
    cur.execute("ALTER TABLE grade_map_new RENAME TO grade_map")
    print("✅ grade_map表迁移完成")

    # quarter_overrides
    print("处理 quarter_overrides 表...")
    cur.execute("""
        CREATE TABLE quarter_overrides_new (
            emp_no TEXT,
            year INTEGER,
            quarter INTEGER,
            grade TEXT,
            PRIMARY KEY (emp_no, year, quarter)
        )
    """)
    cur.execute("INSERT INTO quarter_overrides_new SELECT emp_no, year, quarter, grade FROM quarter_overrides")
    cur.execute("DROP TABLE quarter_overrides")
    cur.execute("ALTER TABLE quarter_overrides_new RENAME TO quarter_overrides")
    print("✅ quarter_overrides表迁移完成")

    # quarter_grade_options
    print("处理 quarter_grade_options 表...")
    cur.execute("""
        CREATE TABLE quarter_grade_options_new (
            grade TEXT PRIMARY KEY,
            display_order INTEGER NOT NULL,
            is_default INTEGER NOT NULL DEFAULT 0,
            color TEXT
        )
    """)
    # 使用GROUP BY确保每个grade只有一条记录
    cur.execute("INSERT INTO quarter_grade_options_new SELECT grade, MAX(display_order), MAX(is_default), MAX(color) FROM quarter_grade_options GROUP BY grade")
    cur.execute("DROP TABLE quarter_grade_options")
    cur.execute("ALTER TABLE quarter_grade_options_new RENAME TO quarter_grade_options")
    print("✅ quarter_grade_options表迁移完成")

    conn.commit()
    conn.close()

def create_indexes():
    """创建索引"""
    print("\n📊 创建性能优化索引...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_employees_dept ON employees(department_id)",
        "CREATE INDEX IF NOT EXISTS idx_employees_created_by ON employees(created_by)",
        "CREATE INDEX IF NOT EXISTS idx_performance_created_by ON performance_records(created_by)",
        "CREATE INDEX IF NOT EXISTS idx_training_created_by ON training_records(created_by)",
    ]

    for index_sql in indexes:
        try:
            cur.execute(index_sql)
        except sqlite3.Error as e:
            print(f"⚠️  索引创建失败: {e}")

    print("✅ 索引创建完成")
    conn.commit()
    conn.close()

def verify_migration():
    """验证迁移结果"""
    print("\n🔍 验证迁移结果...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 检查表结构
    for table in ['employees', 'performance_records', 'training_records']:
        cur.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in cur.fetchall()]

        if 'created_by' in columns:
            print(f"✅ {table}表包含created_by字段")
        else:
            print(f"❌ {table}表缺少created_by字段")

        if 'user_id' not in columns:
            print(f"✅ {table}表已移除user_id字段")
        else:
            print(f"⚠️  {table}表仍包含user_id字段")

    # 检查UNIQUE约束
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='employees'")
    employees_sql = cur.fetchone()[0]
    if 'UNIQUE(emp_no)' in employees_sql and 'user_id' not in employees_sql:
        print("✅ employees表UNIQUE约束正确")
    else:
        print("⚠️  employees表UNIQUE约束可能有问题")

    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='performance_records'")
    perf_sql = cur.fetchone()[0]
    if 'UNIQUE(emp_no, year, month)' in perf_sql and 'user_id' not in perf_sql:
        print("✅ performance_records表UNIQUE约束正确")
    else:
        print("⚠️  performance_records表UNIQUE约束可能有问题")

    # 检查数据完整性
    cur.execute("SELECT COUNT(*) FROM employees WHERE created_by IS NULL")
    null_count = cur.fetchone()[0]
    if null_count == 0:
        print("✅ employees表created_by无空值")
    else:
        print(f"⚠️  employees表有{null_count}条created_by为空")

    conn.close()

def main():
    """主函数"""
    print("=" * 60)
    print("数据库迁移：移除user_id数据隔离")
    print("改为基于department_id的权限控制")
    print("=" * 60)

    # 1. 备份
    backup_path = backup_database()

    # 2. 检查并清理数据
    clean_orphan_data()
    handle_duplicate_data()

    # 3. 确认执行
    print("\n⚠️  即将执行数据库迁移，这将修改数据库结构")
    response = input("是否继续? (yes/no): ")
    if response.lower() != 'yes':
        print("❌ 迁移已取消")
        return

    # 4. 执行迁移
    try:
        migrate_employees_table()
        migrate_performance_records_table()
        migrate_training_records_table()
        migrate_config_tables()
        create_indexes()

        # 5. 验证
        verify_migration()

        print("\n" + "=" * 60)
        print("✅ 数据库迁移完成！")
        print(f"   备份文件: {backup_path}")
        print("   请继续修改代码文件以完成权限改造")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 迁移失败: {e}")
        print(f"   请从备份恢复: {backup_path}")
        raise

if __name__ == '__main__':
    main()
