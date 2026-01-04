#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Logging and audit module
Comprehensive logging system with audit trail support
"""
import logging
import os
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from datetime import datetime
from functools import wraps
from flask import request, session
import json


# ========== Logging Configuration ==========

def setup_logging(app):
    """Setup application logging configuration"""

    # Create logs directory if it doesn't exist
    log_dir = os.path.join(app.root_path, 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # Log level based on environment
    log_level = logging.DEBUG if app.config.get('DEBUG') else logging.INFO

    # Formatter for log messages
    detailed_formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s in %(module)s (%(funcName)s:%(lineno)d): %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    simple_formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # ===== Application Log =====
    # Rotating file handler for application logs (10MB per file, keep 10 backups)
    app_handler = RotatingFileHandler(
        os.path.join(log_dir, 'app.log'),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=10,
        encoding='utf-8'
    )
    app_handler.setLevel(log_level)
    app_handler.setFormatter(detailed_formatter)

    # ===== Error Log =====
    # Separate file for errors and above (5MB per file, keep 5 backups)
    error_handler = RotatingFileHandler(
        os.path.join(log_dir, 'error.log'),
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=5,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)

    # ===== Access Log =====
    # Daily rotating access log
    access_handler = TimedRotatingFileHandler(
        os.path.join(log_dir, 'access.log'),
        when='midnight',
        interval=1,
        backupCount=30,  # Keep 30 days
        encoding='utf-8'
    )
    access_handler.setLevel(logging.INFO)
    access_handler.setFormatter(simple_formatter)

    # ===== Audit Log =====
    # Daily rotating audit trail log
    audit_handler = TimedRotatingFileHandler(
        os.path.join(log_dir, 'audit.log'),
        when='midnight',
        interval=1,
        backupCount=90,  # Keep 90 days for compliance
        encoding='utf-8'
    )
    audit_handler.setLevel(logging.INFO)
    audit_handler.setFormatter(simple_formatter)

    # ===== Console Handler (Development only) =====
    if app.config.get('DEBUG'):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(simple_formatter)
        app.logger.addHandler(console_handler)

    # Add handlers to app logger
    app.logger.addHandler(app_handler)
    app.logger.addHandler(error_handler)
    app.logger.setLevel(log_level)

    # Create separate loggers for access and audit
    access_logger = logging.getLogger('access')
    access_logger.addHandler(access_handler)
    access_logger.setLevel(logging.INFO)
    access_logger.propagate = False

    audit_logger = logging.getLogger('audit')
    audit_logger.addHandler(audit_handler)
    audit_logger.setLevel(logging.INFO)
    audit_logger.propagate = False

    app.logger.info('='*80)
    app.logger.info(f'Application started - {app.name}')
    app.logger.info(f'Debug mode: {app.config.get("DEBUG")}')
    app.logger.info(f'Log directory: {log_dir}')
    app.logger.info('='*80)


# ========== Access Logging ==========

def log_request(app):
    """Log HTTP requests"""

    @app.before_request
    def before_request_logging():
        """Log request details before processing"""
        access_logger = logging.getLogger('access')

        # Skip static files
        if request.path.startswith('/static/'):
            return

        user_id = session.get('user_id', 'anonymous')
        username = session.get('username', 'anonymous')

        access_logger.info(
            f"{request.method} {request.path} | "
            f"User: {username} ({user_id}) | "
            f"IP: {request.remote_addr} | "
            f"UA: {request.headers.get('User-Agent', 'Unknown')[:100]}"
        )

    @app.after_request
    def after_request_logging(response):
        """Log response details after processing"""
        access_logger = logging.getLogger('access')

        # Skip static files
        if request.path.startswith('/static/'):
            return response

        user_id = session.get('user_id', 'anonymous')

        access_logger.info(
            f"Response: {response.status_code} | "
            f"User: {user_id} | "
            f"Path: {request.path} | "
            f"Size: {response.content_length or 0} bytes"
        )

        return response


# ========== Audit Trail ==========

class AuditLogger:
    """Audit logger for tracking user actions"""

    @staticmethod
    def log(action, resource, details=None, status='success', user_id=None):
        """Log an audit event"""
        audit_logger = logging.getLogger('audit')

        if user_id is None:
            user_id = session.get('user_id', 'system')

        username = session.get('username', 'system')
        ip_address = request.remote_addr if request else 'system'

        audit_data = {
            'timestamp': datetime.now().isoformat(),
            'user_id': user_id,
            'username': username,
            'ip_address': ip_address,
            'action': action,
            'resource': resource,
            'status': status,
            'details': details or {}
        }

        audit_logger.info(json.dumps(audit_data, ensure_ascii=False))

    @staticmethod
    def login(username, success=True, reason=None):
        """Log login attempt"""
        status = 'success' if success else 'failed'
        details = {'reason': reason} if reason else {}

        AuditLogger.log(
            action='login',
            resource='authentication',
            details=details,
            status=status
        )

    @staticmethod
    def logout(username):
        """Log logout"""
        AuditLogger.log(
            action='logout',
            resource='authentication',
            status='success'
        )

    @staticmethod
    def create(resource_type, resource_id, details=None):
        """Log resource creation"""
        AuditLogger.log(
            action='create',
            resource=f"{resource_type}/{resource_id}",
            details=details or {},
            status='success'
        )

    @staticmethod
    def update(resource_type, resource_id, changes=None):
        """Log resource update"""
        AuditLogger.log(
            action='update',
            resource=f"{resource_type}/{resource_id}",
            details={'changes': changes} if changes else {},
            status='success'
        )

    @staticmethod
    def delete(resource_type, resource_id):
        """Log resource deletion"""
        AuditLogger.log(
            action='delete',
            resource=f"{resource_type}/{resource_id}",
            status='success'
        )

    @staticmethod
    def access(resource_type, resource_id, action='view'):
        """Log resource access"""
        AuditLogger.log(
            action=action,
            resource=f"{resource_type}/{resource_id}",
            status='success'
        )

    @staticmethod
    def permission_denied(resource, reason=None):
        """Log permission denied"""
        AuditLogger.log(
            action='access_denied',
            resource=resource,
            details={'reason': reason} if reason else {},
            status='denied'
        )


# ========== Audit Decorators ==========

def audit_action(action, resource_type):
    """Decorator to automatically log audited actions"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)

                # Try to extract resource ID from kwargs or args
                resource_id = kwargs.get('id') or kwargs.get('dept_id') or kwargs.get('user_id')
                if resource_id is None and args:
                    resource_id = args[0] if args else 'unknown'

                AuditLogger.log(
                    action=action,
                    resource=f"{resource_type}/{resource_id}",
                    status='success'
                )

                return result

            except Exception as e:
                # Log failed action
                resource_id = kwargs.get('id') or 'unknown'
                AuditLogger.log(
                    action=action,
                    resource=f"{resource_type}/{resource_id}",
                    details={'error': str(e)},
                    status='failed'
                )
                raise

        return wrapper
    return decorator


# ========== Performance Logging ==========

def log_slow_queries(threshold_ms=1000):
    """Decorator to log slow database queries"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            import time
            start_time = time.time()

            result = func(*args, **kwargs)

            duration_ms = (time.time() - start_time) * 1000

            if duration_ms > threshold_ms:
                logging.getLogger('app').warning(
                    f"Slow query detected: {func.__name__} took {duration_ms:.2f}ms "
                    f"(threshold: {threshold_ms}ms)"
                )

            return result

        return wrapper
    return decorator


# ========== Security Logging ==========

class SecurityLogger:
    """Security event logger"""

    @staticmethod
    def suspicious_activity(event_type, details):
        """Log suspicious activity"""
        logger = logging.getLogger('app')
        logger.warning(
            f"SECURITY: {event_type} | "
            f"User: {session.get('username', 'anonymous')} | "
            f"IP: {request.remote_addr if request else 'unknown'} | "
            f"Details: {json.dumps(details, ensure_ascii=False)}"
        )

        # Also log to audit trail
        AuditLogger.log(
            action='security_event',
            resource=event_type,
            details=details,
            status='suspicious'
        )

    @staticmethod
    def failed_login(username, reason):
        """Log failed login attempt"""
        SecurityLogger.suspicious_activity(
            'failed_login',
            {'username': username, 'reason': reason}
        )

    @staticmethod
    def brute_force_attempt(username, attempt_count):
        """Log potential brute force attack"""
        SecurityLogger.suspicious_activity(
            'brute_force_attempt',
            {'username': username, 'attempts': attempt_count}
        )

    @staticmethod
    def unauthorized_access(resource):
        """Log unauthorized access attempt"""
        SecurityLogger.suspicious_activity(
            'unauthorized_access',
            {'resource': resource}
        )

    @staticmethod
    def data_breach_attempt(details):
        """Log potential data breach attempt"""
        SecurityLogger.suspicious_activity(
            'data_breach_attempt',
            details
        )