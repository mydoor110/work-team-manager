#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Input validation and data sanitization module
Comprehensive validation utilities for user input
"""
import re
from datetime import datetime
from functools import wraps
from flask import request, flash, redirect, url_for
from utils.errors import ValidationError


# ========== String Validation ==========

class StringValidator:
    """String validation utilities"""

    @staticmethod
    def is_empty(value):
        """Check if string is empty or whitespace"""
        return not value or not value.strip()

    @staticmethod
    def length_between(value, min_length=0, max_length=None):
        """Check if string length is within range"""
        if value is None:
            return False

        length = len(value)

        if length < min_length:
            return False

        if max_length and length > max_length:
            return False

        return True

    @staticmethod
    def contains_only(value, allowed_chars):
        """Check if string contains only allowed characters"""
        if not value:
            return True

        pattern = f"^[{re.escape(allowed_chars)}]+$"
        return bool(re.match(pattern, value))

    @staticmethod
    def is_alphanumeric(value, allow_spaces=False):
        """Check if string is alphanumeric"""
        if not value:
            return False

        if allow_spaces:
            return bool(re.match(r'^[a-zA-Z0-9\s]+$', value))

        return value.isalnum()

    @staticmethod
    def is_username(value):
        """Validate username format"""
        if not value:
            return False

        # 3-20 characters, alphanumeric and underscore only
        pattern = r'^[a-zA-Z0-9_]{3,20}$'
        return bool(re.match(pattern, value))

    @staticmethod
    def is_chinese_name(value):
        """Validate Chinese name format"""
        if not value:
            return False

        # 2-10 Chinese characters
        pattern = r'^[\u4e00-\u9fa5]{2,10}$'
        return bool(re.match(pattern, value))


# ========== Number Validation ==========

class NumberValidator:
    """Number validation utilities"""

    @staticmethod
    def is_integer(value):
        """Check if value is integer"""
        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def is_float(value):
        """Check if value is float"""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def is_positive(value):
        """Check if number is positive"""
        try:
            return float(value) > 0
        except (ValueError, TypeError):
            return False

    @staticmethod
    def in_range(value, min_value=None, max_value=None):
        """Check if number is within range"""
        try:
            num = float(value)

            if min_value is not None and num < min_value:
                return False

            if max_value is not None and num > max_value:
                return False

            return True

        except (ValueError, TypeError):
            return False


# ========== Date Validation ==========

class DateValidator:
    """Date validation utilities"""

    @staticmethod
    def is_valid_date(date_string, format='%Y-%m-%d'):
        """Check if string is valid date"""
        try:
            datetime.strptime(date_string, format)
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def is_past_date(date_string, format='%Y-%m-%d'):
        """Check if date is in the past"""
        try:
            date = datetime.strptime(date_string, format)
            return date < datetime.now()
        except (ValueError, TypeError):
            return False

    @staticmethod
    def is_future_date(date_string, format='%Y-%m-%d'):
        """Check if date is in the future"""
        try:
            date = datetime.strptime(date_string, format)
            return date > datetime.now()
        except (ValueError, TypeError):
            return False

    @staticmethod
    def date_in_range(date_string, start_date=None, end_date=None, format='%Y-%m-%d'):
        """Check if date is within range"""
        try:
            date = datetime.strptime(date_string, format)

            if start_date:
                start = datetime.strptime(start_date, format)
                if date < start:
                    return False

            if end_date:
                end = datetime.strptime(end_date, format)
                if date > end:
                    return False

            return True

        except (ValueError, TypeError):
            return False


# ========== Data Sanitization ==========

class Sanitizer:
    """Data sanitization utilities"""

    @staticmethod
    def clean_string(value, strip=True, lower=False, upper=False):
        """Clean string value"""
        if value is None:
            return None

        result = str(value)

        if strip:
            result = result.strip()

        if lower:
            result = result.lower()

        if upper:
            result = result.upper()

        return result

    @staticmethod
    def remove_html(value):
        """Remove HTML tags from string"""
        if not value:
            return value

        # Simple HTML tag removal (for basic sanitization)
        clean_text = re.sub(r'<[^>]+>', '', str(value))
        return clean_text.strip()

    @staticmethod
    def sanitize_sql(value):
        """Sanitize SQL input (basic protection)"""
        if not value:
            return value

        # Remove common SQL injection patterns
        dangerous_patterns = [
            r';\s*DROP',
            r';\s*DELETE',
            r';\s*UPDATE',
            r'--',
            r'/\*',
            r'\*/',
            r'xp_',
            r'sp_'
        ]

        result = str(value)

        for pattern in dangerous_patterns:
            result = re.sub(pattern, '', result, flags=re.IGNORECASE)

        return result

    @staticmethod
    def sanitize_filename(filename):
        """Sanitize filename for safe storage"""
        if not filename:
            return filename

        # Remove path separators and dangerous characters
        safe_name = re.sub(r'[<>:"/\\|?*]', '_', filename)

        # Remove leading/trailing dots and spaces
        safe_name = safe_name.strip('. ')

        # Limit length
        if len(safe_name) > 255:
            name, ext = safe_name.rsplit('.', 1) if '.' in safe_name else (safe_name, '')
            safe_name = name[:250] + (f'.{ext}' if ext else '')

        return safe_name


# ========== Form Validation ==========

class FormValidator:
    """Form validation helper"""

    def __init__(self, data):
        self.data = data
        self.errors = {}

    def require(self, field, message=None):
        """Require field to be present and non-empty"""
        value = self.data.get(field)

        if StringValidator.is_empty(value):
            self.errors[field] = message or f"{field}不能为空"
            return False

        return True

    def validate_length(self, field, min_length=0, max_length=None, message=None):
        """Validate field length"""
        value = self.data.get(field, '')

        if not StringValidator.length_between(value, min_length, max_length):
            if message:
                self.errors[field] = message
            else:
                if max_length:
                    self.errors[field] = f"{field}长度应在{min_length}-{max_length}之间"
                else:
                    self.errors[field] = f"{field}长度至少为{min_length}"

            return False

        return True

    def validate_integer(self, field, min_value=None, max_value=None, message=None):
        """Validate integer field"""
        value = self.data.get(field)

        if not NumberValidator.is_integer(value):
            self.errors[field] = message or f"{field}必须是整数"
            return False

        if not NumberValidator.in_range(value, min_value, max_value):
            self.errors[field] = message or f"{field}超出有效范围"
            return False

        return True

    def validate_date(self, field, format='%Y-%m-%d', message=None):
        """Validate date field"""
        value = self.data.get(field)

        if not DateValidator.is_valid_date(value, format):
            self.errors[field] = message or f"{field}日期格式无效"
            return False

        return True

    def is_valid(self):
        """Check if form is valid"""
        return len(self.errors) == 0

    def get_errors(self):
        """Get validation errors"""
        return self.errors


# ========== Request Validation Decorators ==========

def validate_request(*required_fields):
    """Decorator to validate required request fields"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            missing_fields = []

            for field in required_fields:
                value = request.form.get(field) or request.args.get(field)
                if StringValidator.is_empty(value):
                    missing_fields.append(field)

            if missing_fields:
                flash(f"缺少必填字段: {', '.join(missing_fields)}", 'danger')
                return redirect(request.referrer or url_for('index'))

            return func(*args, **kwargs)

        return wrapper
    return decorator


def validate_json(*required_fields):
    """Decorator to validate required JSON fields"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not request.is_json:
                raise ValidationError("请求必须是JSON格式")

            data = request.get_json()
            missing_fields = []

            for field in required_fields:
                if field not in data or StringValidator.is_empty(str(data.get(field))):
                    missing_fields.append(field)

            if missing_fields:
                raise ValidationError(f"缺少必填字段: {', '.join(missing_fields)}")

            return func(*args, **kwargs)

        return wrapper
    return decorator