#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置表迁移脚本：移除user_id，使配置全局共享
grade_map, quarter_overrides, quarter_grade_options
"""

import sqlite3
import shutil
from datetime import datetime

DB_PATH = 'app.db'

def backup_database():
    """备份数据库"""
    backup_path = f"{DB_PATH}.config_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(DB_PATH, backup_path)
    print(f"✅ 数据库已备份到: {backup_path}")
    return backup_path

def migrate_grade_map():
    """迁移grade_map表 - 移除user_id，grade作为主键"""
    print("\n📦 迁移grade_map表...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    try:
        # 创建新表
        cur.execute("""
            CREATE TABLE grade_map_new (
                grade TEXT PRIMARY KEY,
                value REAL NOT NULL
            )
        """)
        print("✅ 创建新表结构")

        # 迁移数据 - 使用GROUP BY去重，取MAX(value)
        cur.execute("""
            INSERT INTO grade_map_new (grade, value)
            SELECT grade, MAX(value) as value
            FROM grade_map
            GROUP BY grade
        """)
        migrated_rows = cur.rowcount
        print(f"✅ 迁移数据到新表: {migrated_rows}条记录")

        # 替换表
        cur.execute("DROP TABLE grade_map")
        cur.execute("ALTER TABLE grade_map_new RENAME TO grade_map")
        print("✅ 替换为新表")

        conn.commit()
    except Exception as e:
        print(f"❌ 迁移失败: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def migrate_quarter_overrides():
    """迁移quarter_overrides表 - 移除user_id"""
    print("\n📦 迁移quarter_overrides表...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    try:
        # 创建新表
        cur.execute("""
            CREATE TABLE quarter_overrides_new (
                emp_no TEXT NOT NULL,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                grade TEXT NOT NULL,
                PRIMARY KEY (emp_no, year, quarter)
            )
        """)
        print("✅ 创建新表结构")

        # 迁移数据 - 如果有重复，保留第一条
        cur.execute("""
            INSERT INTO quarter_overrides_new (emp_no, year, quarter, grade)
            SELECT emp_no, year, quarter, grade
            FROM quarter_overrides
            GROUP BY emp_no, year, quarter
        """)
        migrated_rows = cur.rowcount
        print(f"✅ 迁移数据到新表: {migrated_rows}条记录")

        # 替换表
        cur.execute("DROP TABLE quarter_overrides")
        cur.execute("ALTER TABLE quarter_overrides_new RENAME TO quarter_overrides")
        print("✅ 替换为新表")

        conn.commit()
    except Exception as e:
        print(f"❌ 迁移失败: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def migrate_quarter_grade_options():
    """迁移quarter_grade_options表 - 移除user_id"""
    print("\n📦 迁移quarter_grade_options表...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    try:
        # 创建新表
        cur.execute("""
            CREATE TABLE quarter_grade_options_new (
                grade TEXT PRIMARY KEY,
                display_order INTEGER NOT NULL,
                is_default INTEGER NOT NULL DEFAULT 0,
                color TEXT
            )
        """)
        print("✅ 创建新表结构")

        # 迁移数据 - 使用GROUP BY去重
        cur.execute("""
            INSERT INTO quarter_grade_options_new (grade, display_order, is_default, color)
            SELECT grade, MAX(display_order), MAX(is_default), MAX(color)
            FROM quarter_grade_options
            GROUP BY grade
        """)
        migrated_rows = cur.rowcount
        print(f"✅ 迁移数据到新表: {migrated_rows}条记录")

        # 替换表
        cur.execute("DROP TABLE quarter_grade_options")
        cur.execute("ALTER TABLE quarter_grade_options_new RENAME TO quarter_grade_options")
        print("✅ 替换为新表")

        conn.commit()
    except Exception as e:
        print(f"❌ 迁移失败: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def verify_migration():
    """验证迁移结果"""
    print("\n🔍 验证迁移结果...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 检查grade_map
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='grade_map'")
    grade_map_sql = cur.fetchone()[0]
    if 'user_id' not in grade_map_sql and 'grade TEXT PRIMARY KEY' in grade_map_sql:
        print("✅ grade_map表结构正确")
    else:
        print("⚠️ grade_map表结构可能有问题")

    # 检查quarter_overrides
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='quarter_overrides'")
    overrides_sql = cur.fetchone()[0]
    if 'user_id' not in overrides_sql and 'PRIMARY KEY (emp_no, year, quarter)' in overrides_sql:
        print("✅ quarter_overrides表结构正确")
    else:
        print("⚠️ quarter_overrides表结构可能有问题")

    # 检查quarter_grade_options
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='quarter_grade_options'")
    options_sql = cur.fetchone()[0]
    if 'user_id' not in options_sql and 'grade TEXT PRIMARY KEY' in options_sql:
        print("✅ quarter_grade_options表结构正确")
    else:
        print("⚠️ quarter_grade_options表结构可能有问题")

    # 统计数据量
    cur.execute("SELECT COUNT(*) FROM grade_map")
    print(f"📊 grade_map: {cur.fetchone()[0]}条记录")

    cur.execute("SELECT COUNT(*) FROM quarter_overrides")
    print(f"📊 quarter_overrides: {cur.fetchone()[0]}条记录")

    cur.execute("SELECT COUNT(*) FROM quarter_grade_options")
    print(f"📊 quarter_grade_options: {cur.fetchone()[0]}条记录")

    conn.close()

def main():
    """主函数"""
    print("=" * 60)
    print("配置表迁移：移除user_id，实现全局共享")
    print("=" * 60)

    # 1. 备份
    backup_path = backup_database()

    # 2. 确认执行
    print("\n⚠️  即将执行配置表迁移，这将修改数据库结构")
    response = input("是否继续? (yes/no): ")
    if response.lower() != 'yes':
        print("❌ 迁移已取消")
        return

    # 3. 执行迁移
    try:
        migrate_grade_map()
        migrate_quarter_overrides()
        migrate_quarter_grade_options()

        # 4. 验证
        verify_migration()

        print("\n" + "=" * 60)
        print("✅ 配置表迁移完成！")
        print(f"   备份文件: {backup_path}")
        print("   下一步: 修改代码文件以完成权限改造")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 迁移失败: {e}")
        print(f"   请从备份恢复: {backup_path}")
        raise

if __name__ == '__main__':
    main()
