#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Blueprint 注册中心
负责导入和注册所有Blueprint模块
"""
from flask import Flask


def register_blueprints(app: Flask):
    """
    注册所有 Blueprint 到 Flask 应用

    Args:
        app: Flask 应用实例

    注册顺序:
        1. auth - 认证模块 (基础模块)
        2. personnel - 人员管理
        3. departments - 部门管理
        4. performance - 绩效管理
        5. training - 培训管理
        6. safety - 安全管理
        7. admin - 系统管理 (依赖其他模块)
    """

    # 导入所有 Blueprint
    from .auth import auth_bp
    from .personnel import personnel_bp
    from .departments import departments_bp
    from .performance import performance_bp
    from .training import training_bp
    from .safety import safety_bp
    from .admin import admin_bp

    # 注册 Blueprint
    app.register_blueprint(auth_bp)
    app.register_blueprint(personnel_bp)
    app.register_blueprint(departments_bp)
    app.register_blueprint(performance_bp)
    app.register_blueprint(training_bp)
    app.register_blueprint(safety_bp)
    app.register_blueprint(admin_bp)

    app.logger.info('All blueprints registered successfully')
