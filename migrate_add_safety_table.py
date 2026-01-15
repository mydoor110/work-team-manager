#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库迁移脚本：添加安全检查记录表
"""

import sqlite3

DB_PATH = 'app.db'

def migrate():
    """添加safety_inspection_records表"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    try:
        # 检查表是否已存在
        cur.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='safety_inspection_records'
        """)

        if cur.fetchone():
            print("✓ safety_inspection_records 表已存在，无需创建")
            return

        # 创建安全检查记录表
        print("正在创建 safety_inspection_records 表...")
        cur.execute("""
            CREATE TABLE safety_inspection_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                inspection_date TEXT NOT NULL,
                location TEXT,
                hazard_description TEXT,
                corrective_measures TEXT,
                deadline_date TEXT,
                inspected_person TEXT,
                responsible_team TEXT,
                assessment TEXT,
                rectification_status TEXT,
                rectifier TEXT,
                work_type TEXT,
                responsibility_location TEXT,
                inspection_item TEXT,
                created_by INTEGER,
                source_file TEXT,
                created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
                FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
            )
        """)
        print("✓ 表创建成功")

        # 创建索引
        print("正在创建索引...")
        indexes = [
            "CREATE INDEX idx_safety_inspection_created_by ON safety_inspection_records(created_by)",
            "CREATE INDEX idx_safety_inspection_date ON safety_inspection_records(inspection_date)",
            "CREATE INDEX idx_safety_inspection_category ON safety_inspection_records(category)",
            "CREATE INDEX idx_safety_inspection_team ON safety_inspection_records(responsible_team)"
        ]

        for index_sql in indexes:
            cur.execute(index_sql)
        print("✓ 索引创建成功")

        conn.commit()
        print("\n✅ 迁移完成！安全检查记录表已成功添加。")

    except sqlite3.Error as e:
        print(f"❌ 迁移失败: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()

if __name__ == '__main__':
    print("=" * 60)
    print("数据库迁移：添加安全检查记录表")
    print("=" * 60)
    migrate()
