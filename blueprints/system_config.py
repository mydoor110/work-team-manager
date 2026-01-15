#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统配置管理Blueprint
提供算法参数配置的管理界面和API接口
"""
from flask import Blueprint, render_template, request, jsonify, session
from blueprints.decorators import admin_required
from services.algorithm_config_service import AlgorithmConfigService

system_config_bp = Blueprint('system_config', __name__, url_prefix='/system/config')


@system_config_bp.route('/algorithm')
@admin_required
def algorithm_config_page():
    """算法配置管理页面"""
    return render_template('system_algorithm_config.html', title='算法参数配置')


@system_config_bp.route('/api/current-config', methods=['GET'])
@admin_required
def api_get_current_config():
    """API: 获取当前生效的配置"""
    try:
        # 获取配置数据
        config_data = AlgorithmConfigService.get_active_config()
        config_info = AlgorithmConfigService.get_current_info()

        return jsonify({
            'success': True,
            'config': config_data,
            'info': config_info
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'获取配置失败: {str(e)}'
        }), 500


@system_config_bp.route('/api/presets', methods=['GET'])
@admin_required
def api_get_presets():
    """API: 获取所有预设方案"""
    try:
        presets = AlgorithmConfigService.get_presets()
        return jsonify({
            'success': True,
            'presets': presets
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'获取预设方案失败: {str(e)}'
        }), 500


@system_config_bp.route('/api/apply-preset', methods=['POST'])
@admin_required
def api_apply_preset():
    """API: 应用预设方案"""
    try:
        data = request.get_json()
        preset_key = data.get('preset_key')
        reason = data.get('reason', '应用预设方案')

        if not preset_key:
            return jsonify({
                'success': False,
                'error': '预设方案标识不能为空'
            }), 400

        user_id = session.get('user_id')
        username = session.get('username')
        ip_address = request.remote_addr

        success, message = AlgorithmConfigService.apply_preset(
            preset_key, user_id, reason, username, ip_address
        )

        if success:
            return jsonify({
                'success': True,
                'message': message
            })
        else:
            return jsonify({
                'success': False,
                'error': message
            }), 400

    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'应用预设方案失败: {str(e)}'
        }), 500


@system_config_bp.route('/api/update-config', methods=['POST'])
@admin_required
def api_update_config():
    """API: 更新自定义配置"""
    try:
        data = request.get_json()
        config_data = data.get('config_data')
        reason = data.get('reason', '自定义配置修改')

        if not config_data:
            return jsonify({
                'success': False,
                'error': '配置数据不能为空'
            }), 400

        user_id = session.get('user_id')
        username = session.get('username')
        ip_address = request.remote_addr

        success, message = AlgorithmConfigService.update_custom_config(
            config_data, user_id, reason, username, ip_address
        )

        if success:
            return jsonify({
                'success': True,
                'message': message
            })
        else:
            return jsonify({
                'success': False,
                'error': message
            }), 400

    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'更新配置失败: {str(e)}'
        }), 500


@system_config_bp.route('/api/simulate', methods=['POST'])
@admin_required
def api_simulate():
    """API: 模拟计算"""
    try:
        data = request.get_json()
        config_data = data.get('config_data')
        sample_data = data.get('sample_data')

        if not config_data or not sample_data:
            return jsonify({
                'success': False,
                'error': '配置数据和样例数据不能为空'
            }), 400

        # 执行模拟计算
        results = AlgorithmConfigService.simulate_calculation(config_data, sample_data)

        return jsonify({
            'success': True,
            'results': results
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'模拟计算失败: {str(e)}'
        }), 500


@system_config_bp.route('/api/change-logs', methods=['GET'])
@admin_required
def api_get_logs():
    """API: 获取配置变更日志"""
    try:
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)

        logs = AlgorithmConfigService.get_logs(limit, offset)

        return jsonify({
            'success': True,
            'logs': logs,
            'total': len(logs)
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'获取日志失败: {str(e)}'
        }), 500


@system_config_bp.route('/api/validate-config', methods=['POST'])
@admin_required
def api_validate_config():
    """API: 验证配置数据"""
    try:
        data = request.get_json()
        config_data = data.get('config_data')

        if not config_data:
            return jsonify({
                'success': False,
                'error': '配置数据不能为空'
            }), 400

        is_valid, error_msg = AlgorithmConfigService.validate_config(config_data)

        return jsonify({
            'success': True,
            'is_valid': is_valid,
            'message': error_msg if not is_valid else '配置验证通过'
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'验证失败: {str(e)}'
        }), 500


@system_config_bp.route('/api/preview-effect', methods=['POST'])
@admin_required
def api_preview_effect():
    """API: 预览配置效果 - 使用示例数据对比当前配置和新配置的效果"""
    try:
        from blueprints.personnel import (
            calculate_performance_score_monthly,
            calculate_safety_score_dual_track,
            calculate_training_score_with_penalty,
            calculate_learning_ability_longterm
        )

        data = request.get_json()
        new_config = data.get('config_data')

        if not new_config:
            return jsonify({
                'success': False,
                'error': '配置数据不能为空'
            }), 400

        # 1. 获取当前配置
        current_config = AlgorithmConfigService.get_active_config()

        # 2. 使用示例数据（典型的中等表现员工）
        emp_no = "SAMPLE-001"
        employee_name = "示例员工（张三）"
        department_id = 1

        # 3. 准备示例数据
        # 绩效数据：B级，基准分95
        perf_grade = 'B'
        perf_score = 95.0

        # 安全违规记录：最近6个月有3次违规
        # - 轻微违规1次（2分）
        # - 中等违规1次（4分）
        # - 严重违规1次（8分）
        safety_violations = [2.0, 4.0, 8.0]
        safety_months = 6

        # 培训记录：10次培训，8次合格
        # 格式：(score, is_qualified, is_disqualified, training_date)
        from datetime import datetime, timedelta
        base_date = datetime.now()
        training_records = [
            (85, 1, 0, (base_date - timedelta(days=270)).strftime('%Y-%m-%d')),
            (78, 1, 0, (base_date - timedelta(days=240)).strftime('%Y-%m-%d')),
            (92, 1, 0, (base_date - timedelta(days=210)).strftime('%Y-%m-%d')),
            (88, 1, 0, (base_date - timedelta(days=180)).strftime('%Y-%m-%d')),
            (65, 0, 1, (base_date - timedelta(days=150)).strftime('%Y-%m-%d')),  # 失格
            (90, 1, 0, (base_date - timedelta(days=120)).strftime('%Y-%m-%d')),
            (82, 1, 0, (base_date - timedelta(days=90)).strftime('%Y-%m-%d')),
            (75, 0, 1, (base_date - timedelta(days=60)).strftime('%Y-%m-%d')),   # 失格
            (88, 1, 0, (base_date - timedelta(days=30)).strftime('%Y-%m-%d')),
            (91, 1, 0, (base_date - timedelta(days=10)).strftime('%Y-%m-%d'))
        ]
        training_duration_days = 365  # 统计周期：一年
        cert_years = 3.0  # 取证3年

        # 4. 使用两种配置分别计算各维度分数
        result = {
            'employee_id': emp_no,
            'employee_name': employee_name,
            'department_id': department_id,
            'current': {},
            'new': {}
        }

        # 计算绩效维度
        perf_current = calculate_performance_score_monthly(perf_grade, perf_score, current_config)
        perf_new = calculate_performance_score_monthly(perf_grade, perf_score, new_config)

        # 从配置中获取系数
        grade_coef_current = current_config['performance']['grade_coefficients'].get(perf_grade, 0)
        grade_coef_new = new_config['performance']['grade_coefficients'].get(perf_grade, 0)

        result['current']['performance'] = {
            'grade': perf_grade,
            'raw_score': perf_score,
            'final_score': perf_current.get('radar_value', 0),
            'coefficient': grade_coef_current
        }
        result['new']['performance'] = {
            'grade': perf_grade,
            'raw_score': perf_score,
            'final_score': perf_new.get('radar_value', 0),
            'coefficient': grade_coef_new
        }

        # 计算安全维度
        safety_current = calculate_safety_score_dual_track(safety_violations, safety_months, current_config)
        safety_new = calculate_safety_score_dual_track(safety_violations, safety_months, new_config)

        result['current']['safety'] = {
            'violations_count': len(safety_violations),
            'violations_detail': f"轻微{safety_violations[0]}分, 中等{safety_violations[1]}分, 严重{safety_violations[2]}分",
            'final_score': safety_current.get('final_score', 0),
            'dimension_a': safety_current.get('score_a', 0),
            'dimension_b': safety_current.get('score_b', 0)
        }
        result['new']['safety'] = {
            'violations_count': len(safety_violations),
            'violations_detail': f"轻微{safety_violations[0]}分, 中等{safety_violations[1]}分, 严重{safety_violations[2]}分",
            'final_score': safety_new.get('final_score', 0),
            'dimension_a': safety_new.get('score_a', 0),
            'dimension_b': safety_new.get('score_b', 0)
        }

        # 计算培训维度
        training_current = calculate_training_score_with_penalty(
            training_records,
            duration_days=training_duration_days,
            cert_years=cert_years,
            config=current_config
        )
        training_new = calculate_training_score_with_penalty(
            training_records,
            duration_days=training_duration_days,
            cert_years=cert_years,
            config=new_config
        )

        # 计算合格次数
        qualified_count = sum(1 for rec in training_records if rec[1] == 1)

        result['current']['training'] = {
            'records_count': len(training_records),
            'qualified_count': qualified_count,
            'avg_score': training_current.get('original_score', 0),
            'final_score': training_current.get('radar_score', 0),
            'penalty_coefficient': training_current.get('penalty_coefficient', 1.0)
        }
        result['new']['training'] = {
            'records_count': len(training_records),
            'qualified_count': qualified_count,
            'avg_score': training_new.get('original_score', 0),
            'final_score': training_new.get('radar_score', 0),
            'penalty_coefficient': training_new.get('penalty_coefficient', 1.0)
        }

        # ==================================================
        # 4. 学习能力维度对比（基于历史综合分数趋势）
        # ==================================================

        # 示例历史综合分数（6个月，显示稳步上升趋势）
        # 模拟该员工过去6个月的综合评分，展示从82.5到90.0的成长轨迹
        historical_scores = [82.5, 84.0, 85.5, 87.0, 88.5, 90.0]

        # 使用当前配置计算学习能力
        learning_current = calculate_learning_ability_longterm(
            score_list=historical_scores,
            config=current_config
        )

        # 使用新配置计算学习能力
        learning_new = calculate_learning_ability_longterm(
            score_list=historical_scores,
            config=new_config
        )

        # 计算斜率（用于展示）
        import numpy as np
        x = np.arange(len(historical_scores))
        k, b = np.polyfit(x, historical_scores, 1)

        result['current']['learning'] = {
            'historical_count': len(historical_scores),
            'avg_score': learning_current.get('average_score', 0),
            'trend_slope': round(k, 2),
            'tier': learning_current.get('tier', '未知'),
            'final_score': learning_current.get('learning_score', 0),
            'trend_description': f"过去{len(historical_scores)}个月平均分{learning_current.get('average_score', 0):.1f}，斜率{k:.2f}"
        }
        result['new']['learning'] = {
            'historical_count': len(historical_scores),
            'avg_score': learning_new.get('average_score', 0),
            'trend_slope': round(k, 2),
            'tier': learning_new.get('tier', '未知'),
            'final_score': learning_new.get('learning_score', 0),
            'trend_description': f"过去{len(historical_scores)}个月平均分{learning_new.get('average_score', 0):.1f}，斜率{k:.2f}"
        }

        return jsonify({
            'success': True,
            'result': result
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': f'预览失败: {str(e)}'
        }), 500
