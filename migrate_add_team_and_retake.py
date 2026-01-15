#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库迁移脚本：添加 team_name, is_retake, retake_of_record_id 字段
执行方法: python3 migrate_add_team_and_retake.py
"""
import sqlite3
import os
from config.settings import DB_PATH

def migrate():
    """添加新字段并迁移数据"""
    if not os.path.exists(DB_PATH):
        print(f"数据库文件不存在: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    try:
        # 检查字段是否已存在
        cur.execute("PRAGMA table_info(training_records)")
        columns = [row[1] for row in cur.fetchall()]

        # 添加 team_name 字段
        if 'team_name' not in columns:
            print("正在添加 team_name 字段...")
            cur.execute("ALTER TABLE training_records ADD COLUMN team_name TEXT")

            # 从 employees 表迁移 class_name 数据
            print("正在从 employees 表迁移班级数据到 team_name...")
            cur.execute("""
                UPDATE training_records
                SET team_name = (
                    SELECT class_name
                    FROM employees
                    WHERE employees.emp_no = training_records.emp_no
                    AND employees.user_id = training_records.user_id
                    LIMIT 1
                )
            """)
            print(f"✓ team_name 字段添加成功")
        else:
            print("✓ team_name 字段已存在")

        # 添加 is_retake 字段
        if 'is_retake' not in columns:
            print("正在添加 is_retake 字段...")
            cur.execute("ALTER TABLE training_records ADD COLUMN is_retake INTEGER DEFAULT 0")
            print(f"✓ is_retake 字段添加成功")
        else:
            print("✓ is_retake 字段已存在")

        # 添加 retake_of_record_id 字段
        if 'retake_of_record_id' not in columns:
            print("正在添加 retake_of_record_id 字段...")
            cur.execute("ALTER TABLE training_records ADD COLUMN retake_of_record_id INTEGER")
            print(f"✓ retake_of_record_id 字段添加成功")
        else:
            print("✓ retake_of_record_id 字段已存在")

        conn.commit()
        print("\n✓ 所有迁移成功完成！")

        # 显示统计信息
        cur.execute("SELECT COUNT(*) FROM training_records")
        total_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM training_records WHERE team_name IS NOT NULL")
        team_count = cur.fetchone()[0]

        print(f"\n统计信息:")
        print(f"  总记录数: {total_count}")
        print(f"  已设置班组的记录: {team_count}")

    except sqlite3.Error as e:
        conn.rollback()
        print(f"✗ 迁移失败: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
