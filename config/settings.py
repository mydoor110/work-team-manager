#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Application configuration settings
Separated from main app for better maintainability
"""
import os
from datetime import timedelta

# Base configuration
BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
DB_PATH = os.path.join(BASE_DIR, "app.db")
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
EXPORT_DIR = os.path.join(BASE_DIR, "exports")

# Application settings
APP_TITLE = "班组管理系统"
SECRET_KEY = os.environ.get("APP_SECRET_KEY", "dev-secret-change-in-production")
ALLOWED_EXTENSIONS = {"pdf", "xlsx", "xls"}

# Security configuration
class SecurityConfig:
    # CSRF Protection
    WTF_CSRF_TIME_LIMIT = None  # No time limit for CSRF tokens
    WTF_CSRF_SSL_STRICT = False  # Set to True in production with HTTPS

    # Session Security
    SESSION_COOKIE_SECURE = os.environ.get("SESSION_COOKIE_SECURE", "False").lower() == "true"
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"
    PERMANENT_SESSION_LIFETIME = int(os.environ.get("SESSION_TIMEOUT", 3600 * 8))  # 8 hours default

    # Security Headers
    X_FRAME_OPTIONS = "DENY"
    X_CONTENT_TYPE_OPTIONS = "nosniff"
    X_XSS_PROTECTION = "1; mode=block"
    REFERRER_POLICY = "strict-origin-when-cross-origin"

    # Content Security Policy
    CSP = {
        "default-src": "'self'",
        "script-src": "'self' 'unsafe-inline' https://cdn.jsdelivr.net",
        "style-src": "'self' 'unsafe-inline' https://cdn.jsdelivr.net",
        "font-src": "'self' https://cdn.jsdelivr.net",
        "img-src": "'self' data:",
        "object-src": "'none'"
    }

# Database configuration
class DatabaseConfig:
    # Enable foreign key constraints
    FOREIGN_KEYS = True

    # Performance settings
    JOURNAL_MODE = "WAL"  # Write-Ahead Logging for better performance
    SYNCHRONOUS = "NORMAL"  # Balance between performance and safety
    CACHE_SIZE = 2000  # Cache size in pages

    # Connection settings
    TIMEOUT = 20.0  # Database lock timeout in seconds
    CHECK_SAME_THREAD = False  # Allow multiple threads (use with caution)

# Application environment
class Config:
    """Base configuration class"""

    def __init__(self):
        self.SECRET_KEY = SECRET_KEY
        self.SQLALCHEMY_TRACK_MODIFICATIONS = False

        # Security settings
        for key, value in vars(SecurityConfig).items():
            if not key.startswith('_'):
                setattr(self, key, value)

        # Database settings
        for key, value in vars(DatabaseConfig).items():
            if not key.startswith('_'):
                setattr(self, key, value)

class DevelopmentConfig(Config):
    """Development environment configuration"""
    DEBUG = True
    TESTING = False
    SESSION_COOKIE_SECURE = False
    WTF_CSRF_SSL_STRICT = False

class ProductionConfig(Config):
    """Production environment configuration"""
    DEBUG = False
    TESTING = False
    SESSION_COOKIE_SECURE = True
    WTF_CSRF_SSL_STRICT = True

    # Override with production settings
    # Note: SECRET_KEY will be validated when this config is actually used
    SECRET_KEY = os.environ.get("SECRET_KEY") or None

    def __init__(self):
        if not self.SECRET_KEY:
            raise ValueError("SECRET_KEY environment variable must be set in production")

class TestingConfig(Config):
    """Testing environment configuration"""
    DEBUG = True
    TESTING = True
    WTF_CSRF_ENABLED = False
    SESSION_COOKIE_SECURE = False

# Configuration selection
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}

def get_config(config_name=None):
    """Get configuration class based on environment"""
    if config_name is None:
        config_name = os.environ.get('FLASK_ENV', 'default')

    return config.get(config_name, config['default'])()