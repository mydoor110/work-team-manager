#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
算法配置服务
提供算法配置的读取、更新、校验等功能
"""
import json
import time
from typing import Dict, Tuple, Optional, List
from datetime import datetime
from models.database import get_db


class AlgorithmConfigService:
    """算法配置服务 - 立即生效模式"""

    # 配置缓存
    _cache: Optional[dict] = None
    _cache_time: float = 0
    _cache_ttl: int = 300  # 5分钟缓存

    @classmethod
    def get_active_config(cls) -> dict:
        """
        获取当前生效配置（带缓存）

        Returns:
            dict: 当前生效的算法配置

        Raises:
            ValueError: 配置不存在或无效
        """
        # 检查缓存
        current_time = time.time()
        if cls._cache is not None and (current_time - cls._cache_time) < cls._cache_ttl:
            return cls._cache

        # 从数据库读取
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT config_data FROM algorithm_active_config WHERE id = 1
        """)
        row = cur.fetchone()

        if not row:
            raise ValueError("系统配置未初始化，请联系管理员")

        config_data = json.loads(row['config_data'])

        # 更新缓存
        cls._cache = config_data
        cls._cache_time = current_time

        return config_data

    @classmethod
    def apply_preset(cls, preset_key: str, user_id: int, reason: str, username: str = None, ip_address: str = None) -> Tuple[bool, str]:
        """
        应用预设方案

        Args:
            preset_key: 预设方案标识（strict/standard/lenient）
            user_id: 操作者用户ID
            reason: 变更原因
            username: 操作者姓名（可选）
            ip_address: 操作者IP地址（可选）

        Returns:
            Tuple[bool, str]: (成功标志, 消息)
        """
        conn = get_db()
        cur = conn.cursor()

        try:
            # 1. 查询预设方案
            cur.execute("""
                SELECT preset_name, config_data FROM algorithm_presets
                WHERE preset_key = ?
            """, (preset_key,))
            preset_row = cur.fetchone()

            if not preset_row:
                return False, f"预设方案不存在: {preset_key}"

            preset_name = preset_row['preset_name']
            new_config_data = preset_row['config_data']

            # 2. 获取当前配置（用于日志）
            cur.execute("""
                SELECT config_data FROM algorithm_active_config WHERE id = 1
            """)
            current_row = cur.fetchone()
            old_config_data = current_row['config_data'] if current_row else None

            # 3. 更新当前配置
            cur.execute("""
                INSERT OR REPLACE INTO algorithm_active_config
                (id, based_on_preset, is_customized, config_data, updated_by, updated_at)
                VALUES (1, ?, 0, ?, ?, ?)
            """, (preset_key, new_config_data, user_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

            # 4. 记录变更日志
            if not username:
                cur.execute("SELECT username FROM users WHERE id = ?", (user_id,))
                user_row = cur.fetchone()
                username = user_row['username'] if user_row else f"用户{user_id}"

            cur.execute("""
                INSERT INTO algorithm_config_logs
                (action, preset_name, old_config, new_config, change_reason, changed_by, changed_by_name, ip_address)
                VALUES ('APPLY_PRESET', ?, ?, ?, ?, ?, ?, ?)
            """, (preset_name, old_config_data, new_config_data, reason, user_id, username, ip_address))

            conn.commit()

            # 5. 清除缓存
            cls.clear_cache()

            return True, f"成功应用预设方案: {preset_name}"

        except Exception as e:
            conn.rollback()
            return False, f"应用预设方案失败: {str(e)}"

    @classmethod
    def update_custom_config(cls, config_data: dict, user_id: int, reason: str, username: str = None, ip_address: str = None) -> Tuple[bool, str]:
        """
        更新自定义配置

        Args:
            config_data: 新的配置数据
            user_id: 操作者用户ID
            reason: 变更原因
            username: 操作者姓名（可选）
            ip_address: 操作者IP地址（可选）

        Returns:
            Tuple[bool, str]: (成功标志, 消息)
        """
        # 1. 校验配置
        is_valid, error_msg = cls.validate_config(config_data)
        if not is_valid:
            return False, f"配置校验失败: {error_msg}"

        conn = get_db()
        cur = conn.cursor()

        try:
            # 2. 获取当前配置（用于日志）
            cur.execute("""
                SELECT config_data FROM algorithm_active_config WHERE id = 1
            """)
            current_row = cur.fetchone()
            old_config_data = current_row['config_data'] if current_row else None

            # 3. 更新当前配置
            new_config_json = json.dumps(config_data, ensure_ascii=False)
            cur.execute("""
                INSERT OR REPLACE INTO algorithm_active_config
                (id, based_on_preset, is_customized, config_data, updated_by, updated_at)
                VALUES (1, NULL, 1, ?, ?, ?)
            """, (new_config_json, user_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

            # 4. 记录变更日志
            if not username:
                cur.execute("SELECT username FROM users WHERE id = ?", (user_id,))
                user_row = cur.fetchone()
                username = user_row['username'] if user_row else f"用户{user_id}"

            cur.execute("""
                INSERT INTO algorithm_config_logs
                (action, old_config, new_config, change_reason, changed_by, changed_by_name, ip_address)
                VALUES ('CUSTOM_UPDATE', ?, ?, ?, ?, ?, ?)
            """, (old_config_data, new_config_json, reason, user_id, username, ip_address))

            conn.commit()

            # 5. 清除缓存
            cls.clear_cache()

            return True, "成功更新自定义配置"

        except Exception as e:
            conn.rollback()
            return False, f"更新配置失败: {str(e)}"

    @classmethod
    def simulate_calculation(cls, config_data: dict, sample_data: dict) -> dict:
        """
        模拟计算（不保存配置）

        Args:
            config_data: 要模拟的配置数据
            sample_data: 样例数据，格式：
                {
                    "performance": {"grades": ["D", "C", "B+"]},
                    "safety": {"violations": [3, 5, 12]},
                    "training": {"scores": [85, 0, 90], "is_qualified": [1, 0, 1]}
                }

        Returns:
            dict: 模拟计算结果
        """
        # 导入算法函数（避免循环导入）
        from blueprints.personnel import (
            calculate_performance_score_monthly,
            calculate_safety_score_dual_track,
            calculate_training_score_with_penalty
        )

        results = {
            "performance": [],
            "safety": [],
            "training": [],
            "comprehensive": [],
            "errors": []
        }

        try:
            # 模拟绩效计算
            if "performance" in sample_data and "grades" in sample_data["performance"]:
                for grade in sample_data["performance"]["grades"]:
                    try:
                        result = calculate_performance_score_monthly(grade, 95.0, config=config_data)
                        results["performance"].append({
                            "grade": grade,
                            "score": result["score"],
                            "label": result["label"]
                        })
                    except Exception as e:
                        results["errors"].append(f"绩效计算错误 ({grade}): {str(e)}")

            # 模拟安全计算
            if "safety" in sample_data and "violations" in sample_data["safety"]:
                for violation_score in sample_data["safety"]["violations"]:
                    try:
                        # 构造虚拟数据
                        violations_list = [violation_score]
                        result = calculate_safety_score_dual_track(violations_list, 1, config=config_data)
                        results["safety"].append({
                            "violation_score": violation_score,
                            "score": result["score"],
                            "label": result["label"]
                        })
                    except Exception as e:
                        results["errors"].append(f"安全计算错误 ({violation_score}分): {str(e)}")

            # 模拟培训计算
            if "training" in sample_data:
                scores = sample_data["training"].get("scores", [])
                is_qualified = sample_data["training"].get("is_qualified", [])

                if len(scores) == len(is_qualified):
                    for i, (score, qualified) in enumerate(zip(scores, is_qualified)):
                        try:
                            # 构造虚拟记录
                            training_records = [(score, qualified, 0 if qualified else 1)]
                            result = calculate_training_score_with_penalty(training_records, 90, config=config_data)
                            results["training"].append({
                                "index": i + 1,
                                "input_score": score,
                                "is_qualified": qualified,
                                "final_score": result["score"],
                                "label": result["label"]
                            })
                        except Exception as e:
                            results["errors"].append(f"培训计算错误 (样本{i+1}): {str(e)}")

            # 计算综合分（使用权重）
            weights = config_data.get("comprehensive", {}).get("score_weights", {})
            if results["performance"] and results["safety"] and results["training"]:
                perf_score = results["performance"][0]["score"]
                safety_score = results["safety"][0]["score"]
                training_score = results["training"][0]["score"]

                comprehensive_score = (
                    perf_score * weights.get("performance", 0.35) +
                    safety_score * weights.get("safety", 0.30) +
                    training_score * weights.get("training", 0.20)
                )
                results["comprehensive"].append({
                    "score": round(comprehensive_score, 1),
                    "weights": weights
                })

        except Exception as e:
            results["errors"].append(f"模拟计算异常: {str(e)}")

        return results

    @classmethod
    def validate_config(cls, config_data: dict) -> Tuple[bool, str]:
        """
        配置数据校验

        Args:
            config_data: 配置数据

        Returns:
            Tuple[bool, str]: (是否有效, 错误消息)
        """
        try:
            # 1. 结构完整性校验
            required_sections = ["performance", "safety", "training", "comprehensive", "key_personnel"]
            for section in required_sections:
                if section not in config_data:
                    return False, f"缺少必填配置节: {section}"

            # 2. 绩效配置校验
            perf = config_data["performance"]
            if "grade_coefficients" not in perf:
                return False, "缺少绩效等级系数配置"

            required_grades = ["D", "C", "B", "B+", "A"]
            for grade in required_grades:
                if grade not in perf["grade_coefficients"]:
                    return False, f"缺少等级系数: {grade}"

                coeff = perf["grade_coefficients"][grade]
                if not isinstance(coeff, (int, float)) or coeff < 0 or coeff > 2:
                    return False, f"等级系数 {grade} 超出范围 [0, 2]: {coeff}"

            # 3. 安全配置校验
            safety = config_data["safety"]
            if "severity_track" in safety and "critical_threshold" in safety["severity_track"]:
                threshold = safety["severity_track"]["critical_threshold"]
                if not isinstance(threshold, (int, float)) or threshold < 1 or threshold > 50:
                    return False, f"重大违规红线超出范围 [1, 50]: {threshold}"

            # 4. 培训配置校验
            training = config_data["training"]
            if "penalty_rules" in training and "absolute_threshold" in training["penalty_rules"]:
                fail_count = training["penalty_rules"]["absolute_threshold"].get("fail_count", 3)
                if not isinstance(fail_count, int) or fail_count < 1 or fail_count > 10:
                    return False, f"绝对失格次数超出范围 [1, 10]: {fail_count}"

            # 5. 综合评分权重校验
            comprehensive = config_data["comprehensive"]
            if "score_weights" not in comprehensive:
                return False, "缺少综合评分权重配置"

            weights = comprehensive["score_weights"]
            total_weight = sum(weights.values())
            if abs(total_weight - 1.0) > 0.01:  # 容忍0.01的浮点误差
                return False, f"综合评分权重总和必须为1.0，当前为: {total_weight}"

            # 6. 关键人员判定标准校验
            key_personnel = config_data["key_personnel"]
            if "comprehensive_threshold" not in key_personnel:
                return False, "缺少关键人员综合分阈值"

            threshold = key_personnel["comprehensive_threshold"]
            if not isinstance(threshold, (int, float)) or threshold < 0 or threshold > 100:
                return False, f"关键人员综合分阈值超出范围 [0, 100]: {threshold}"

            return True, "校验通过"

        except Exception as e:
            return False, f"校验异常: {str(e)}"

    @classmethod
    def get_logs(cls, limit: int = 50, offset: int = 0) -> List[dict]:
        """
        获取配置变更日志

        Args:
            limit: 返回记录数
            offset: 分页偏移

        Returns:
            List[dict]: 日志列表
        """
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                id, action, preset_name, change_reason,
                changed_by, changed_by_name, changed_at, ip_address
            FROM algorithm_config_logs
            ORDER BY changed_at DESC
            LIMIT ? OFFSET ?
        """, (limit, offset))

        logs = []
        for row in cur.fetchall():
            logs.append({
                "id": row['id'],
                "action": row['action'],
                "preset_name": row['preset_name'],
                "change_reason": row['change_reason'],
                "changed_by": row['changed_by'],
                "changed_by_name": row['changed_by_name'],
                "changed_at": row['changed_at'],
                "ip_address": row['ip_address']
            })

        return logs

    @classmethod
    def get_current_info(cls) -> dict:
        """
        获取当前配置信息

        Returns:
            dict: 当前配置信息（包含基于的预设方案、是否自定义等）
        """
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT based_on_preset, is_customized, updated_at
            FROM algorithm_active_config
            WHERE id = 1
        """)
        row = cur.fetchone()

        if row:
            return {
                "based_on_preset": row['based_on_preset'],
                "is_customized": bool(row['is_customized']),
                "updated_at": row['updated_at']
            }
        else:
            return {
                "based_on_preset": None,
                "is_customized": False,
                "updated_at": None
            }

    @classmethod
    def get_presets(cls) -> List[dict]:
        """
        获取所有预设方案

        Returns:
            List[dict]: 预设方案列表
        """
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT preset_key, preset_name, description, config_data
            FROM algorithm_presets
            ORDER BY id
        """)

        presets = []
        for row in cur.fetchall():
            presets.append({
                "preset_key": row['preset_key'],
                "preset_name": row['preset_name'],
                "description": row['description'],
                "config_data": json.loads(row['config_data']) if row['config_data'] else {}
            })

        return presets

    @classmethod
    def clear_cache(cls):
        """清除配置缓存"""
        cls._cache = None
        cls._cache_time = 0
