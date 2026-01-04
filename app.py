#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
班组管理系统 - Blueprint 架构版本
主应用程序入口
"""
import os
import sqlite3
import json

from flask import Flask, redirect, url_for, session
from werkzeug.security import generate_password_hash

try:
    from flask_wtf.csrf import CSRFProtect, generate_csrf
    CSRF_AVAILABLE = True
except ImportError:
    CSRF_AVAILABLE = False
    def generate_csrf():
        return ""

# 导入配置
from config.settings import (
    APP_TITLE, SECRET_KEY, DB_PATH,
    UPLOAD_DIR, EXPORT_DIR
)

# 导入数据库工具
from models.database import get_db, close_db

# ==================== Flask 应用初始化 ====================

app = Flask(__name__)
app.config["SECRET_KEY"] = SECRET_KEY

# Security configurations
app.config["WTF_CSRF_TIME_LIMIT"] = None
app.config["WTF_CSRF_SSL_STRICT"] = False
app.config["SESSION_COOKIE_SECURE"] = False
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["PERMANENT_SESSION_LIFETIME"] = 3600 * 8

# Initialize CSRF protection
if CSRF_AVAILABLE:
    csrf = CSRFProtect(app)
else:
    csrf = None


# ==================== 数据库连接管理 ====================

@app.teardown_appcontext
def teardown_db(exception=None):
    """在请求结束时关闭数据库连接"""
    close_db()


# ==================== 数据库初始化 ====================

def init_db():
    """初始化数据库表和索引 - 完整标准版本"""
    conn = get_db()
    cur = conn.cursor()

    # ==================== 核心表 ====================

    # 1. 用户表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            department_id INTEGER,
            role TEXT DEFAULT 'user',
            created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
            FOREIGN KEY (department_id) REFERENCES departments(id)
        )
    """)

    # 2. 部门表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            parent_id INTEGER,
            description TEXT,
            manager_user_id INTEGER,
            level INTEGER DEFAULT 1,
            path TEXT,
            created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
            FOREIGN KEY (parent_id) REFERENCES departments(id),
            FOREIGN KEY (manager_user_id) REFERENCES users(id)
        )
    """)

    # 3. 员工表 (以部门为基准)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS employees (
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

    # ==================== 绩效模块 ====================

    # 4. 绩效记录表 (全局共享)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS performance_records (
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

    # 5. 绩效等级映射表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS grade_map (
            grade TEXT PRIMARY KEY,
            value REAL NOT NULL
        )
    """)

    # 6. 季度绩效覆盖表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS quarter_overrides (
            emp_no TEXT,
            year INTEGER,
            quarter INTEGER,
            grade TEXT,
            PRIMARY KEY (emp_no, year, quarter)
        )
    """)

    # 7. 季度等级选项表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS quarter_grade_options (
            grade TEXT PRIMARY KEY,
            display_order INTEGER NOT NULL,
            is_default INTEGER NOT NULL DEFAULT 0,
            color TEXT
        )
    """)

    # ==================== 培训模块 ====================

    # 8. 培训项目分类表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS training_project_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT,
            display_order INTEGER DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (DATETIME('now'))
        )
    """)

    # 9. 培训项目表
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

    # 10. 培训记录表 (全局共享)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS training_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_no TEXT NOT NULL,
            name TEXT,
            team_name TEXT,
            training_date TEXT NOT NULL,
            project_id INTEGER,
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
            FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
            FOREIGN KEY (retake_of_record_id) REFERENCES training_records(id) ON DELETE SET NULL,
            FOREIGN KEY (project_id) REFERENCES training_projects(id) ON DELETE SET NULL
        )
    """)

    # ==================== 安全模块 ====================

    # 11. 安全检查记录表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS safety_inspection_records (
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

    # ==================== 审计日志模块 ====================

    # 12. 导入日志审计表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS import_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            module TEXT NOT NULL,
            operation TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            user_role TEXT NOT NULL,
            department_id INTEGER,
            department_name TEXT,
            file_name TEXT,
            total_rows INTEGER DEFAULT 0,
            success_rows INTEGER DEFAULT 0,
            failed_rows INTEGER DEFAULT 0,
            skipped_rows INTEGER DEFAULT 0,
            error_message TEXT,
            import_details TEXT,
            ip_address TEXT,
            created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
            FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE SET NULL
        )
    """)

    # ==================== 算法配置模块 ====================

    # 13. 算法预设表
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

    # 14. 算法激活配置表
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

    # 15. 算法配置变更日志表
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

    # ==================== 创建所有索引 ====================

    # Users 表索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_department_id ON users(department_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")

    # Departments 表索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_departments_path ON departments(path)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_departments_parent_id ON departments(parent_id)")

    # Employees 表索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_employees_dept ON employees(department_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_employees_created_by ON employees(created_by)")

    # Performance 表索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_performance_created_by ON performance_records(created_by)")

    # Training 表索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_training_projects_category_id ON training_projects(category_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_training_created_by ON training_records(created_by)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_training_records_project_id ON training_records(project_id)")

    # Safety 表索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_safety_inspection_created_by ON safety_inspection_records(created_by)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_safety_inspection_date ON safety_inspection_records(inspection_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_safety_inspection_category ON safety_inspection_records(category)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_safety_inspection_team ON safety_inspection_records(responsible_team)")

    # Import Logs 表索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_import_logs_module ON import_logs(module)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_import_logs_user_id ON import_logs(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_import_logs_created_at ON import_logs(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_import_logs_department_id ON import_logs(department_id)")

    # Algorithm 表索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_config_logs_changed_at ON algorithm_config_logs(changed_at)")

    conn.commit()

    # ==================== 创建视图 ====================

    # 导入日志视图：方便查询最近的导入记录
    cur.execute("""
        CREATE VIEW IF NOT EXISTS v_recent_imports AS
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
        ORDER BY il.created_at DESC
        LIMIT 100
    """)

    conn.commit()

    # Bootstrap default department and admin account
    cur.execute("SELECT COUNT(1) FROM departments")
    if cur.fetchone()[0] == 0:
        # 创建根部门时明确设置level和path
        cur.execute(
            "INSERT INTO departments(name, description, level, path) VALUES(?, ?, ?, ?)",
            ("总公司", "顶级部门", 1, "/1")
        )
        conn.commit()
        print("✅ 初始化根部门: 总公司 (level=1, path=/1)")

    # Bootstrap admin account if missing
    cur.execute("SELECT COUNT(1) FROM users")
    if cur.fetchone()[0] == 0:
        bootstrap_user = os.environ.get("APP_USER", "wang").strip()
        bootstrap_pass = os.environ.get("APP_PASS", "wang@-1989").strip()
        cur.execute(
            "INSERT INTO users(username, password_hash, department_id, role) VALUES(?, ?, ?, ?)",
            (bootstrap_user, generate_password_hash(bootstrap_pass), 1, "admin"),
        )
        conn.commit()

    # Initialize algorithm configuration presets and active config
    cur.execute("SELECT COUNT(1) FROM algorithm_presets")
    if cur.fetchone()[0] == 0:
        # 标准档配置
        standard_config = {
            "performance": {
                "grade_coefficients": {"D": 0.0, "C": 0.6, "B": 0.9, "B+": 1.0, "A": 1.1},
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
                "thresholds": {"fail_score": 60, "warning_score": 90}
            },
            "training": {
                "penalty_rules": {
                    "absolute_threshold": {"fail_count": 3, "coefficient": 0.5},
                    "small_sample": {"sample_size": 10, "coefficient": 0.7},
                    "afr_thresholds": [
                        {"min": 2.5, "coefficient": 0.5, "label": "高频失格"},
                        {"min": 1.5, "max": 2.5, "coefficient": 0.7, "label": "频率偏高"},
                        {"min": 0.5, "max": 1.5, "coefficient": 0.9, "label": "偶发失格"}
                    ]
                },
                "duration_thresholds": {
                    "short_term_days": 60,
                    "mid_term_days": 180,
                    "default_scores": {"short": 65, "mid": 50, "long": 0}
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
            },
            "learning": {
                "potential_threshold": 0.5,
                "decline_threshold": -0.2,
                "decline_penalty": 0.8,
                "slope_amplifier": 10
            }
        }

        # 严格档配置
        strict_config = json.loads(json.dumps(standard_config))
        strict_config["performance"]["contamination_rules"] = {
            "d_count_threshold": 1, "c_count_threshold": 2,
            "d_cap_score": 85, "c_cap_score": 92
        }
        strict_config["safety"]["severity_track"]["critical_threshold"] = 10
        strict_config["training"]["penalty_rules"]["absolute_threshold"]["coefficient"] = 0.4
        strict_config["training"]["penalty_rules"]["small_sample"]["coefficient"] = 0.6
        strict_config["training"]["penalty_rules"]["afr_thresholds"] = [
            {"min": 2.5, "coefficient": 0.4, "label": "高频失格"},
            {"min": 1.5, "max": 2.5, "coefficient": 0.6, "label": "频率偏高"},
            {"min": 0.5, "max": 1.5, "coefficient": 0.85, "label": "偶发失格"}
        ]
        strict_config["key_personnel"] = {
            "comprehensive_threshold": 75,
            "monthly_violation_threshold": 2
        }
        strict_config["learning"] = {
            "potential_threshold": 0.6,
            "decline_threshold": -0.2,
            "decline_penalty": 0.7,
            "slope_amplifier": 10
        }

        # 宽松档配置
        lenient_config = json.loads(json.dumps(standard_config))
        lenient_config["performance"]["contamination_rules"] = {
            "d_count_threshold": 1, "c_count_threshold": 3,
            "d_cap_score": 95, "c_cap_score": 97
        }
        lenient_config["safety"]["severity_track"]["critical_threshold"] = 15
        lenient_config["training"]["penalty_rules"]["absolute_threshold"]["fail_count"] = 4
        lenient_config["training"]["penalty_rules"]["absolute_threshold"]["coefficient"] = 0.6
        lenient_config["training"]["penalty_rules"]["small_sample"]["coefficient"] = 0.8
        lenient_config["training"]["penalty_rules"]["afr_thresholds"] = [
            {"min": 3.0, "coefficient": 0.6, "label": "高频失格"},
            {"min": 2.0, "max": 3.0, "coefficient": 0.8, "label": "频率偏高"},
            {"min": 0.8, "max": 2.0, "coefficient": 0.95, "label": "偶发失格"}
        ]
        lenient_config["key_personnel"] = {
            "comprehensive_threshold": 65,
            "monthly_violation_threshold": 4
        }
        lenient_config["learning"] = {
            "potential_threshold": 0.4,
            "decline_threshold": -0.2,
            "decline_penalty": 0.9,
            "slope_amplifier": 10
        }

        # 插入预设方案
        presets = [
            ('严格', 'strict', '更严格的惩罚力度，适用于高要求场景', json.dumps(strict_config, ensure_ascii=False)),
            ('标准', 'standard', '标准惩罚力度，平衡公平与激励', json.dumps(standard_config, ensure_ascii=False)),
            ('宽松', 'lenient', '较宽松的惩罚力度，适用于培养阶段', json.dumps(lenient_config, ensure_ascii=False))
        ]
        for preset_name, preset_key, description, config_data in presets:
            cur.execute(
                "INSERT INTO algorithm_presets (preset_name, preset_key, description, config_data) VALUES (?, ?, ?, ?)",
                (preset_name, preset_key, description, config_data)
            )

        # 初始化当前配置为"标准"档
        from datetime import datetime
        cur.execute(
            "INSERT INTO algorithm_active_config (id, based_on_preset, is_customized, config_data, updated_at) VALUES (1, 'standard', 0, ?, ?)",
            (json.dumps(standard_config, ensure_ascii=False), datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        )

        # 记录初始化日志
        cur.execute(
            "INSERT INTO algorithm_config_logs (action, preset_name, new_config, change_reason, changed_by, changed_by_name) VALUES ('INIT', 'standard', ?, '系统初始化', 1, 'system')",
            (json.dumps(standard_config, ensure_ascii=False),)
        )

        conn.commit()
        print("✅ 算法配置初始化完成: 已创建3个预设方案(严格/标准/宽松)，当前配置为'标准'档")

    conn.close()


# ==================== 上下文处理器和安全头 ====================

@app.context_processor
def inject_csrf_token():
    """Inject CSRF token into all templates"""
    if CSRF_AVAILABLE:
        return {'csrf_token': generate_csrf}
    return {'csrf_token': lambda: ""}


@app.after_request
def set_security_headers(response):
    """Add security headers to all responses"""
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Content-Security-Policy'] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "font-src 'self' https://cdn.jsdelivr.net; "
        "img-src 'self' data:; "
        "connect-src 'self' https://cdn.jsdelivr.net; "
        "object-src 'none'"
    )
    return response


# ==================== Blueprint 注册 ====================

from blueprints.auth import auth_bp
from blueprints.admin import admin_bp
from blueprints.departments import departments_bp
from blueprints.personnel import personnel_bp
from blueprints.training import training_bp
from blueprints.performance import performance_bp
from blueprints.safety import safety_bp
from blueprints.system_config import system_config_bp

app.register_blueprint(auth_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(departments_bp)
app.register_blueprint(personnel_bp)
app.register_blueprint(training_bp)
app.register_blueprint(performance_bp)
app.register_blueprint(safety_bp)
app.register_blueprint(system_config_bp)


# ==================== 根路由 ====================

@app.route('/')
def index():
    """首页，重定向到绩效管理"""
    if not session.get('logged_in'):
        return redirect(url_for('auth.login'))
    return redirect(url_for('performance.index'))


# ==================== 应用入口 ====================

if __name__ == "__main__":
    # 确保必要的目录存在
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    os.makedirs(EXPORT_DIR, exist_ok=True)

    # 初始化数据库（使用 IF NOT EXISTS，安全地创建缺失的表）
    init_db()

    # 启动应用
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5001)), debug=True)
