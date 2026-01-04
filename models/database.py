#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database connection and management module
Separated from main app for better maintainability
"""
import sqlite3
import os
from threading import local
from config.settings import DB_PATH, DatabaseConfig

# Thread-local storage for database connections
_local = local()

def get_db():
    """Get database connection with optimized settings"""
    if not hasattr(_local, 'connection'):
        _local.connection = sqlite3.connect(
            DB_PATH,
            timeout=DatabaseConfig.TIMEOUT,
            check_same_thread=DatabaseConfig.CHECK_SAME_THREAD
        )
        _local.connection.row_factory = sqlite3.Row

        # Performance optimizations
        if DatabaseConfig.FOREIGN_KEYS:
            _local.connection.execute("PRAGMA foreign_keys = ON")

        _local.connection.execute(f"PRAGMA journal_mode = {DatabaseConfig.JOURNAL_MODE}")
        _local.connection.execute(f"PRAGMA synchronous = {DatabaseConfig.SYNCHRONOUS}")
        _local.connection.execute(f"PRAGMA cache_size = {DatabaseConfig.CACHE_SIZE}")

    return _local.connection

def close_db():
    """Close database connection"""
    if hasattr(_local, 'connection'):
        _local.connection.close()
        delattr(_local, 'connection')

def init_database():
    """Initialize database with all tables and indexes"""
    conn = get_db()
    cur = conn.cursor()

    # Create tables with foreign key constraints
    tables = [
        # Users table
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            department_id INTEGER,
            role TEXT DEFAULT 'user',
            created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
            FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE SET NULL
        )
        """,

        # Departments table
        """
        CREATE TABLE IF NOT EXISTS departments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            parent_id INTEGER,
            description TEXT,
            manager_user_id INTEGER,
            level INTEGER DEFAULT 1,
            path TEXT,
            created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
            FOREIGN KEY (parent_id) REFERENCES departments(id) ON DELETE SET NULL,
            FOREIGN KEY (manager_user_id) REFERENCES users(id) ON DELETE SET NULL
        )
        """,

        # Employees table
        """
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_no TEXT NOT NULL,
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
            created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
            UNIQUE(emp_no, user_id),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """,

        # Performance records table
        """
        CREATE TABLE IF NOT EXISTS performance_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_no TEXT NOT NULL,
            name TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            score REAL,
            grade TEXT,
            src_file TEXT,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
            UNIQUE(emp_no, year, month, user_id),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """,

        # Training records table
        """
        CREATE TABLE IF NOT EXISTS training_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_no TEXT NOT NULL,
            name TEXT NOT NULL,
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
            user_id INTEGER NOT NULL,
            source_file TEXT,
            created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (retake_of_record_id) REFERENCES training_records(id) ON DELETE SET NULL,
            FOREIGN KEY (project_id) REFERENCES training_projects(id) ON DELETE SET NULL
        )
        """,

        # Safety inspection records table
        """
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
        """,

        # Grade mapping table
        """
        CREATE TABLE IF NOT EXISTS grade_map (
            user_id INTEGER NOT NULL,
            grade TEXT NOT NULL,
            value REAL NOT NULL,
            PRIMARY KEY (user_id, grade),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """,

        # Quarter overrides table
        """
        CREATE TABLE IF NOT EXISTS quarter_overrides (
            user_id INTEGER NOT NULL,
            emp_no TEXT NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            grade TEXT NOT NULL,
            PRIMARY KEY (user_id, emp_no, year, quarter),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """,

        # Quarter grade options table
        """
        CREATE TABLE IF NOT EXISTS quarter_grade_options (
            user_id INTEGER NOT NULL,
            grade TEXT NOT NULL,
            display_order INTEGER NOT NULL,
            is_default INTEGER NOT NULL DEFAULT 0,
            color TEXT,
            PRIMARY KEY (user_id, grade),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    ]

    # Execute table creation
    for table_sql in tables:
        try:
            cur.execute(table_sql)
        except sqlite3.Error as e:
            print(f"Error creating table: {e}")

    # Create performance indexes
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_departments_path ON departments(path)",
        "CREATE INDEX IF NOT EXISTS idx_departments_parent_id ON departments(parent_id)",
        "CREATE INDEX IF NOT EXISTS idx_users_department_id ON users(department_id)",
        "CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)",
        "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)",
        "CREATE INDEX IF NOT EXISTS idx_employees_user_id ON employees(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_employees_emp_no ON employees(emp_no)",
        "CREATE INDEX IF NOT EXISTS idx_performances_user_id_year_month ON performances(user_id, year, month)",
        "CREATE INDEX IF NOT EXISTS idx_performances_emp_no ON performances(emp_no)",
        "CREATE INDEX IF NOT EXISTS idx_training_records_user_id ON training_records(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_training_records_emp_no ON training_records(emp_no)",
        "CREATE INDEX IF NOT EXISTS idx_training_records_date ON training_records(training_date)",
        "CREATE INDEX IF NOT EXISTS idx_training_records_disqualified ON training_records(is_disqualified)",
        "CREATE INDEX IF NOT EXISTS idx_safety_inspection_created_by ON safety_inspection_records(created_by)",
        "CREATE INDEX IF NOT EXISTS idx_safety_inspection_date ON safety_inspection_records(inspection_date)",
        "CREATE INDEX IF NOT EXISTS idx_safety_inspection_category ON safety_inspection_records(category)",
        "CREATE INDEX IF NOT EXISTS idx_safety_inspection_team ON safety_inspection_records(responsible_team)"
    ]

    for index_sql in indexes:
        try:
            cur.execute(index_sql)
        except sqlite3.Error as e:
            print(f"Warning: Could not create index: {e}")

    conn.commit()
    return conn

def bootstrap_data():
    """Bootstrap initial data if database is empty"""
    conn = get_db()
    cur = conn.cursor()

    # Bootstrap default department
    cur.execute("SELECT COUNT(1) FROM departments")
    if cur.fetchone()[0] == 0:
        cur.execute(
            "INSERT INTO departments(name, description, level, path) VALUES(?, ?, ?, ?)",
            ("总公司", "顶级部门", 1, "/1")
        )
        conn.commit()

    # Bootstrap admin account
    cur.execute("SELECT COUNT(1) FROM users")
    if cur.fetchone()[0] == 0:
        from werkzeug.security import generate_password_hash

        bootstrap_user = os.environ.get("APP_USER", "admin").strip()
        bootstrap_pass = os.environ.get("APP_PASS", "admin123").strip()

        cur.execute(
            "INSERT INTO users(username, password_hash, department_id, role) VALUES(?, ?, ?, ?)",
            (bootstrap_user, generate_password_hash(bootstrap_pass), 1, "admin"),
        )
        conn.commit()

class DatabaseManager:
    """Database management helper class"""

    @staticmethod
    def execute_query(query, params=None, fetch=False):
        """Execute a query with optional parameters"""
        conn = get_db()
        cur = conn.cursor()

        try:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)

            if fetch:
                return cur.fetchall()
            else:
                conn.commit()
                return cur.rowcount

        except sqlite3.Error as e:
            conn.rollback()
            raise e

    @staticmethod
    def execute_many(query, params_list):
        """Execute a query with multiple parameter sets"""
        conn = get_db()
        cur = conn.cursor()

        try:
            cur.executemany(query, params_list)
            conn.commit()
            return cur.rowcount

        except sqlite3.Error as e:
            conn.rollback()
            raise e

    @staticmethod
    def transaction(func):
        """Decorator for database transactions"""
        def wrapper(*args, **kwargs):
            conn = get_db()
            try:
                result = func(*args, **kwargs)
                conn.commit()
                return result
            except Exception as e:
                conn.rollback()
                raise e
        return wrapper