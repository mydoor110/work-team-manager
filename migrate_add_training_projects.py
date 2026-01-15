#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
添加培训项目和项目分类表
"""
import sqlite3
import os
from datetime import datetime

DB_PATH = 'app.db'

def backup_database():
    """创建数据库备份"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f'{DB_PATH}.backup_{timestamp}'

    import shutil
    shutil.copy2(DB_PATH, backup_path)
    print(f"✅ 数据库已备份到: {backup_path}")
    return backup_path

def migrate():
    """执行迁移"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    try:
        # 1. 创建培训项目分类表
        print("\n📋 创建培训项目分类表...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS training_project_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                description TEXT,
                display_order INTEGER DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (DATETIME('now'))
            )
        """)

        # 2. 创建培训项目表
        print("📋 创建培训项目表...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS training_projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                category_id INTEGER NOT NULL,
                description TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
                FOREIGN KEY (category_id) REFERENCES training_project_categories(id)
            )
        """)

        # 3. 为 training_records 添加 project_id 字段
        print("📋 为 training_records 表添加 project_id 字段...")
        cur.execute("PRAGMA table_info(training_records)")
        columns = [row[1] for row in cur.fetchall()]

        if 'project_id' not in columns:
            cur.execute("""
                ALTER TABLE training_records
                ADD COLUMN project_id INTEGER
            """)
            print("✅ project_id 字段添加成功")
        else:
            print("ℹ️  project_id 字段已存在")

        # 4. 创建索引
        print("\n📋 创建索引...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_training_projects_category_id ON training_projects(category_id)",
            "CREATE INDEX IF NOT EXISTS idx_training_records_project_id ON training_records(project_id)",
        ]

        for index_sql in indexes:
            cur.execute(index_sql)

        # 5. 从现有数据中提取项目分类和项目
        print("\n📋 分析现有数据...")
        cur.execute("""
            SELECT DISTINCT project_category
            FROM training_records
            WHERE project_category IS NOT NULL
            AND project_category != ''
        """)

        existing_projects = [row[0] for row in cur.fetchall()]
        print(f"ℹ️  发现 {len(existing_projects)} 个不同的项目")

        # 6. 创建默认分类
        print("\n📋 创建默认项目分类...")
        default_categories = [
            ('车门系统', '车门相关故障和维护项目', 1),
            ('制动系统', '制动相关故障和维护项目', 2),
            ('网络通信', '网络和通信相关故障', 3),
            ('牵引系统', '牵引和动力相关故障', 4),
            ('信号系统', '信号和ATO相关故障', 5),
            ('其他系统', '其他未分类项目', 99),
        ]

        for name, desc, order in default_categories:
            cur.execute("""
                INSERT OR IGNORE INTO training_project_categories
                (name, description, display_order)
                VALUES (?, ?, ?)
            """, (name, desc, order))

        conn.commit()

        # 获取"其他系统"分类ID
        cur.execute("SELECT id FROM training_project_categories WHERE name = '其他系统'")
        other_category_id = cur.fetchone()[0]

        # 7. 将现有项目导入到项目表（默认归类到"其他系统"）
        print("\n📋 导入现有项目...")
        for project_name in existing_projects:
            cur.execute("""
                INSERT OR IGNORE INTO training_projects
                (name, category_id, description)
                VALUES (?, ?, ?)
            """, (project_name, other_category_id, '从历史数据导入'))

        conn.commit()

        # 8. 更新 training_records 的 project_id
        print("\n📋 关联历史记录到项目...")
        cur.execute("""
            UPDATE training_records
            SET project_id = (
                SELECT id FROM training_projects
                WHERE training_projects.name = training_records.project_category
            )
            WHERE project_category IS NOT NULL
            AND project_category != ''
            AND project_id IS NULL
        """)

        updated_count = cur.rowcount
        conn.commit()

        print(f"✅ 已更新 {updated_count} 条历史记录")

        # 统计信息
        print("\n" + "=" * 60)
        print("📊 迁移统计")
        print("=" * 60)

        cur.execute("SELECT COUNT(*) FROM training_project_categories")
        cat_count = cur.fetchone()[0]
        print(f"项目分类数量: {cat_count}")

        cur.execute("SELECT COUNT(*) FROM training_projects")
        proj_count = cur.fetchone()[0]
        print(f"项目数量: {proj_count}")

        cur.execute("SELECT COUNT(*) FROM training_records WHERE project_id IS NOT NULL")
        linked_count = cur.fetchone()[0]
        print(f"已关联的培训记录: {linked_count}")

        print("\n✅ 迁移成功完成！")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ 迁移失败: {e}")
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    print("=" * 60)
    print("培训项目和项目分类表迁移")
    print("=" * 60)

    if not os.path.exists(DB_PATH):
        print(f"❌ 数据库文件不存在: {DB_PATH}")
        exit(1)

    # 备份数据库
    backup_path = backup_database()

    # 执行迁移
    try:
        migrate()
    except Exception as e:
        print(f"\n❌ 迁移过程中出错")
        print(f"可以从备份恢复: {backup_path}")
        exit(1)
