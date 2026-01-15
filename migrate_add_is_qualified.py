#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库迁移脚本：添加 is_qualified 字段到 training_records 表
执行方法: python3 migrate_add_is_qualified.py
"""
import sqlite3
import os
from config.settings import DB_PATH

def migrate():
    """添加 is_qualified 字段"""
    if not os.path.exists(DB_PATH):
        print(f"数据库文件不存在: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    try:
        # 检查字段是否已存在
        cur.execute("PRAGMA table_info(training_records)")
        columns = [row[1] for row in cur.fetchall()]

        if 'is_qualified' in columns:
            print("✓ is_qualified 字段已存在，无需迁移")
            return

        # 添加 is_qualified 字段，默认值为 1（合格）
        print("正在添加 is_qualified 字段...")
        cur.execute("ALTER TABLE training_records ADD COLUMN is_qualified INTEGER DEFAULT 1")

        # 根据 is_disqualified 字段更新 is_qualified 的值
        print("正在更新现有记录的 is_qualified 值...")
        cur.execute("""
            UPDATE training_records
            SET is_qualified = CASE
                WHEN is_disqualified = 1 THEN 0
                ELSE 1
            END
        """)

        conn.commit()
        print(f"✓ 迁移成功！已更新 {cur.rowcount} 条记录")

        # 显示统计信息
        cur.execute("SELECT COUNT(*) FROM training_records WHERE is_qualified = 1")
        qualified_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM training_records WHERE is_qualified = 0")
        disqualified_count = cur.fetchone()[0]

        print(f"\n统计信息:")
        print(f"  合格记录: {qualified_count}")
        print(f"  不合格记录: {disqualified_count}")

    except sqlite3.Error as e:
        conn.rollback()
        print(f"✗ 迁移失败: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
