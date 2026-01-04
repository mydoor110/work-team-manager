#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Error handling module
Custom exceptions and error handlers for the application
"""
from flask import render_template, jsonify, request
from werkzeug.exceptions import HTTPException
import traceback


# ========== Custom Exceptions ==========

class AppError(Exception):
    """Base application error"""
    status_code = 500
    message = "应用程序错误"

    def __init__(self, message=None, status_code=None, payload=None):
        super().__init__()
        if message is not None:
            self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['error'] = self.message
        rv['status'] = self.status_code
        return rv


class ValidationError(AppError):
    """Data validation error"""
    status_code = 400
    message = "数据验证失败"


class AuthenticationError(AppError):
    """Authentication error"""
    status_code = 401
    message = "认证失败"


class AuthorizationError(AppError):
    """Authorization error"""
    status_code = 403
    message = "权限不足"


class ResourceNotFoundError(AppError):
    """Resource not found error"""
    status_code = 404
    message = "资源不存在"


class DatabaseError(AppError):
    """Database operation error"""
    status_code = 500
    message = "数据库操作失败"


class FileOperationError(AppError):
    """File operation error"""
    status_code = 500
    message = "文件操作失败"


# ========== Error Handlers ==========

def register_error_handlers(app):
    """Register error handlers for the application"""

    @app.errorhandler(AppError)
    def handle_app_error(error):
        """Handle custom application errors"""
        app.logger.error(f"Application Error: {error.message}", exc_info=True)

        if request.is_json or request.path.startswith('/api/'):
            return jsonify(error.to_dict()), error.status_code

        return render_template(
            'error.html',
            error_code=error.status_code,
            error_message=error.message,
            show_details=app.config.get('DEBUG', False)
        ), error.status_code

    @app.errorhandler(400)
    def bad_request(error):
        """Handle 400 Bad Request"""
        app.logger.warning(f"Bad Request: {request.url}")

        if request.is_json or request.path.startswith('/api/'):
            return jsonify({'error': '请求格式错误', 'status': 400}), 400

        return render_template(
            'error.html',
            error_code=400,
            error_message="请求格式错误",
            error_description="服务器无法理解您的请求"
        ), 400

    @app.errorhandler(401)
    def unauthorized(error):
        """Handle 401 Unauthorized"""
        app.logger.warning(f"Unauthorized access attempt: {request.url}")

        if request.is_json or request.path.startswith('/api/'):
            return jsonify({'error': '未授权访问', 'status': 401}), 401

        from flask import redirect, url_for
        return redirect(url_for('login', next=request.url))

    @app.errorhandler(403)
    def forbidden(error):
        """Handle 403 Forbidden"""
        app.logger.warning(f"Forbidden access: {request.url} by user {request.remote_addr}")

        if request.is_json or request.path.startswith('/api/'):
            return jsonify({'error': '禁止访问', 'status': 403}), 403

        return render_template(
            'error.html',
            error_code=403,
            error_message="禁止访问",
            error_description="您没有权限访问此资源"
        ), 403

    @app.errorhandler(404)
    def not_found(error):
        """Handle 404 Not Found"""
        app.logger.info(f"Page not found: {request.url}")

        if request.is_json or request.path.startswith('/api/'):
            return jsonify({'error': '资源不存在', 'status': 404}), 404

        return render_template(
            'error.html',
            error_code=404,
            error_message="页面不存在",
            error_description="您访问的页面不存在"
        ), 404

    @app.errorhandler(405)
    def method_not_allowed(error):
        """Handle 405 Method Not Allowed"""
        app.logger.warning(f"Method not allowed: {request.method} {request.url}")

        if request.is_json or request.path.startswith('/api/'):
            return jsonify({'error': '方法不允许', 'status': 405}), 405

        return render_template(
            'error.html',
            error_code=405,
            error_message="方法不允许",
            error_description="该请求方法不被允许"
        ), 405

    @app.errorhandler(500)
    def internal_server_error(error):
        """Handle 500 Internal Server Error"""
        app.logger.error(f"Internal Server Error: {error}", exc_info=True)

        # Rollback database transaction if exists
        try:
            from models.database import get_db
            db = get_db()
            db.rollback()
        except:
            pass

        if request.is_json or request.path.startswith('/api/'):
            return jsonify({
                'error': '服务器内部错误',
                'status': 500,
                'details': str(error) if app.config.get('DEBUG') else None
            }), 500

        return render_template(
            'error.html',
            error_code=500,
            error_message="服务器内部错误",
            error_description="服务器遇到了一个错误，无法完成您的请求",
            error_details=str(error) if app.config.get('DEBUG') else None
        ), 500

    @app.errorhandler(Exception)
    def handle_unexpected_error(error):
        """Handle unexpected errors"""
        app.logger.critical(f"Unexpected error: {error}", exc_info=True)

        # Log full traceback
        app.logger.critical(traceback.format_exc())

        # Rollback database transaction if exists
        try:
            from models.database import get_db
            db = get_db()
            db.rollback()
        except:
            pass

        if request.is_json or request.path.startswith('/api/'):
            return jsonify({
                'error': '发生了意外错误',
                'status': 500,
                'type': type(error).__name__,
                'details': str(error) if app.config.get('DEBUG') else None
            }), 500

        return render_template(
            'error.html',
            error_code=500,
            error_message="发生了意外错误",
            error_description="系统遇到了一个未预期的错误",
            error_type=type(error).__name__ if app.config.get('DEBUG') else None,
            error_details=str(error) if app.config.get('DEBUG') else None
        ), 500


# ========== Error Response Helpers ==========

def error_response(message, status_code=400, **kwargs):
    """Generate standardized error response"""
    response = {
        'success': False,
        'error': message,
        'status': status_code
    }
    response.update(kwargs)

    if request.is_json or request.path.startswith('/api/'):
        return jsonify(response), status_code

    from flask import flash, redirect, url_for
    flash(message, 'danger')
    return redirect(kwargs.get('redirect_url', url_for('index')))


def success_response(message=None, data=None, status_code=200):
    """Generate standardized success response"""
    response = {
        'success': True,
        'status': status_code
    }

    if message:
        response['message'] = message
    if data is not None:
        response['data'] = data

    return jsonify(response), status_code