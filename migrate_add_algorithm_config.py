#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
算法配置表迁移脚本
创建算法配置相关的数据库表，并初始化预设方案数据
"""
import sqlite3
import json
from datetime import datetime

DATABASE = 'app.db'

# ==================== 预设方案配置 ====================

# 标准档配置（当前默认）
STANDARD_CONFIG = {
    "performance": {
        "grade_coefficients": {
            "D": 0.0,
            "C": 0.6,
            "B": 0.9,
            "B+": 1.0,
            "A": 1.1
        },
        "grade_ranges": {
            "D": {"min": 0, "max": 79.9, "radar_override": 50},
            "C": {"min": 80, "max": 89.9},
            "B": {"min": 90, "max": 94.9},
            "B+": {"min": 95, "max": 99.9},
            "A": {"min": 100, "max": 110}
        },
        "contamination_rules": {
            "d_count_threshold": 1,
            "c_count_threshold": 2,
            "d_cap_score": 90,
            "c_cap_score": 94.9
        }
    },
    "safety": {
        "behavior_track": {
            "freq_thresholds": [2, 5, 6],
            "freq_multipliers": [2, 5, 10]
        },
        "severity_track": {
            "score_ranges": [
                {"max": 3, "multiplier": 1.0},
                {"min": 3, "max": 5, "multiplier": 2.5},
                {"min": 5, "multiplier": 5.0}
            ],
            "critical_threshold": 12
        },
        "thresholds": {
            "fail_score": 60,
            "warning_score": 90
        }
    },
    "training": {
        "penalty_rules": {
            "absolute_threshold": {
                "fail_count": 3,
                "coefficient": 0.5
            },
            "small_sample": {
                "sample_size": 10,
                "coefficient": 0.7
            },
            "afr_thresholds": [
                {"min": 2.5, "coefficient": 0.5, "label": "高频失格"},
                {"min": 1.5, "max": 2.5, "coefficient": 0.7, "label": "频率偏高"},
                {"min": 0.5, "max": 1.5, "coefficient": 0.9, "label": "偶发失格"}
            ]
        },
        "duration_thresholds": {
            "short_term_days": 60,
            "mid_term_days": 180,
            "default_scores": {
                "short": 65,
                "mid": 50,
                "long": 0
            }
        }
    },
    "comprehensive": {
        "score_weights": {
            "performance": 0.35,
            "safety": 0.30,
            "training": 0.20,
            "stability": 0.10,
            "learning": 0.05
        }
    },
    "key_personnel": {
        "comprehensive_threshold": 70,
        "monthly_violation_threshold": 3
    }
}

# 严格档配置
STRICT_CONFIG = {
    **STANDARD_CONFIG,
    "performance": {
        **STANDARD_CONFIG["performance"],
        "contamination_rules": {
            "d_count_threshold": 1,
            "c_count_threshold": 2,
            "d_cap_score": 85,  # 更严格：标准90→严格85
            "c_cap_score": 92   # 更严格：标准94.9→严格92
        }
    },
    "safety": {
        **STANDARD_CONFIG["safety"],
        "severity_track": {
            **STANDARD_CONFIG["safety"]["severity_track"],
            "critical_threshold": 10  # 更严格：标准12→严格10
        }
    },
    "training": {
        **STANDARD_CONFIG["training"],
        "penalty_rules": {
            "absolute_threshold": {
                "fail_count": 3,
                "coefficient": 0.4  # 更严格：标准0.5→严格0.4
            },
            "small_sample": {
                "sample_size": 10,
                "coefficient": 0.6  # 更严格：标准0.7→严格0.6
            },
            "afr_thresholds": [
                {"min": 2.5, "coefficient": 0.4, "label": "高频失格"},
                {"min": 1.5, "max": 2.5, "coefficient": 0.6, "label": "频率偏高"},
                {"min": 0.5, "max": 1.5, "coefficient": 0.85, "label": "偶发失格"}
            ]
        }
    },
    "key_personnel": {
        "comprehensive_threshold": 75,  # 更严格：标准70→严格75
        "monthly_violation_threshold": 2  # 更严格：标准3→严格2
    }
}

# 宽松档配置
LENIENT_CONFIG = {
    **STANDARD_CONFIG,
    "performance": {
        **STANDARD_CONFIG["performance"],
        "contamination_rules": {
            "d_count_threshold": 1,
            "c_count_threshold": 3,  # 更宽松：标准2→宽松3
            "d_cap_score": 95,  # 更宽松：标准90→宽松95
            "c_cap_score": 97   # 更宽松：标准94.9→宽松97
        }
    },
    "safety": {
        **STANDARD_CONFIG["safety"],
        "severity_track": {
            **STANDARD_CONFIG["safety"]["severity_track"],
            "critical_threshold": 15  # 更宽松：标准12→宽松15
        }
    },
    "training": {
        **STANDARD_CONFIG["training"],
        "penalty_rules": {
            "absolute_threshold": {
                "fail_count": 4,  # 更宽松：标准3→宽松4
                "coefficient": 0.6  # 更宽松：标准0.5→宽松0.6
            },
            "small_sample": {
                "sample_size": 10,
                "coefficient": 0.8  # 更宽松：标准0.7→宽松0.8
            },
            "afr_thresholds": [
                {"min": 3.0, "coefficient": 0.6, "label": "高频失格"},
                {"min": 2.0, "max": 3.0, "coefficient": 0.8, "label": "频率偏高"},
                {"min": 0.8, "max": 2.0, "coefficient": 0.95, "label": "偶发失格"}
            ]
        }
    },
    "key_personnel": {
        "comprehensive_threshold": 65,  # 更宽松：标准70→宽松65
        "monthly_violation_threshold": 4  # 更宽松：标准3→宽松4
    }
}

def migrate():
    """执行数据库迁移"""
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    try:
        print("开始数据库迁移...")

        # 1. 创建预设方案表
        print("创建表: algorithm_presets")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS algorithm_presets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                preset_name TEXT NOT NULL UNIQUE,
                preset_key TEXT NOT NULL UNIQUE,
                description TEXT,
                config_data TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (DATETIME('now'))
            )
        """)

        # 2. 创建当前配置表
        print("创建表: algorithm_active_config")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS algorithm_active_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                based_on_preset TEXT,
                is_customized INTEGER DEFAULT 0,
                config_data TEXT NOT NULL,
                updated_by INTEGER,
                updated_at TEXT,
                FOREIGN KEY (updated_by) REFERENCES users(id) ON DELETE SET NULL
            )
        """)

        # 3. 创建配置变更日志表
        print("创建表: algorithm_config_logs")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS algorithm_config_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                preset_name TEXT,
                old_config TEXT,
                new_config TEXT,
                change_reason TEXT,
                changed_by INTEGER NOT NULL,
                changed_by_name TEXT,
                changed_at TEXT NOT NULL DEFAULT (DATETIME('now')),
                ip_address TEXT,
                FOREIGN KEY (changed_by) REFERENCES users(id) ON DELETE SET NULL
            )
        """)

        # 4. 创建索引
        print("创建索引...")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_config_logs_changed_at
            ON algorithm_config_logs(changed_at)
        """)

        # 5. 插入预设方案数据
        print("插入预设方案数据...")
        presets = [
            ('严格', 'strict', '更严格的惩罚力度，适用于高要求场景', json.dumps(STRICT_CONFIG, ensure_ascii=False)),
            ('标准', 'standard', '标准惩罚力度，平衡公平与激励', json.dumps(STANDARD_CONFIG, ensure_ascii=False)),
            ('宽松', 'lenient', '较宽松的惩罚力度，适用于培养阶段', json.dumps(LENIENT_CONFIG, ensure_ascii=False))
        ]

        for preset_name, preset_key, description, config_data in presets:
            cur.execute("""
                INSERT OR IGNORE INTO algorithm_presets
                (preset_name, preset_key, description, config_data)
                VALUES (?, ?, ?, ?)
            """, (preset_name, preset_key, description, config_data))
            print(f"  - 插入预设方案: {preset_name}")

        # 6. 初始化当前配置为"标准"档
        print("初始化当前配置（标准档）...")
        cur.execute("""
            INSERT OR IGNORE INTO algorithm_active_config
            (id, based_on_preset, is_customized, config_data, updated_at)
            VALUES (1, 'standard', 0, ?, ?)
        """, (json.dumps(STANDARD_CONFIG, ensure_ascii=False), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        # 7. 记录初始化日志
        print("记录初始化日志...")
        cur.execute("""
            INSERT INTO algorithm_config_logs
            (action, preset_name, new_config, change_reason, changed_by, changed_by_name)
            VALUES ('INIT', 'standard', ?, '系统初始化', 1, 'system')
        """, (json.dumps(STANDARD_CONFIG, ensure_ascii=False),))

        conn.commit()
        print("✅ 数据库迁移成功完成！")

        # 8. 验证数据
        print("\n验证迁移结果...")
        cur.execute("SELECT COUNT(*) FROM algorithm_presets")
        preset_count = cur.fetchone()[0]
        print(f"  预设方案数量: {preset_count}")

        cur.execute("SELECT COUNT(*) FROM algorithm_active_config")
        config_count = cur.fetchone()[0]
        print(f"  当前配置数量: {config_count}")

        cur.execute("SELECT COUNT(*) FROM algorithm_config_logs")
        log_count = cur.fetchone()[0]
        print(f"  配置日志数量: {log_count}")

        if preset_count == 3 and config_count == 1 and log_count == 1:
            print("✅ 数据验证通过！")
        else:
            print("⚠️  警告: 数据数量不符合预期")

    except Exception as e:
        conn.rollback()
        print(f"❌ 迁移失败: {str(e)}")
        raise
    finally:
        conn.close()

def rollback():
    """回滚迁移（删除表）"""
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    try:
        print("开始回滚迁移...")
        cur.execute("DROP TABLE IF EXISTS algorithm_config_logs")
        cur.execute("DROP TABLE IF EXISTS algorithm_active_config")
        cur.execute("DROP TABLE IF EXISTS algorithm_presets")
        conn.commit()
        print("✅ 回滚完成！")
    except Exception as e:
        conn.rollback()
        print(f"❌ 回滚失败: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'rollback':
        rollback()
    else:
        migrate()
