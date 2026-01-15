#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
更新活动配置以包含学习能力参数的迁移脚本
"""
import sqlite3
import json

def update_active_config():
    """为活动配置添加learning模块"""
    conn = sqlite3.connect('app.db')
    cur = conn.cursor()

    # 获取当前活动配置
    cur.execute("SELECT id, config_data, based_on_preset FROM algorithm_active_config LIMIT 1")
    result = cur.fetchone()

    if not result:
        print("❌ 未找到活动配置")
        conn.close()
        return

    config_id, config_json, based_on_preset = result
    config = json.loads(config_json)

    # 如果没有learning配置，则添加
    if 'learning' not in config:
        # 根据基础预设获取对应的learning配置
        cur.execute(
            "SELECT config_data FROM algorithm_presets WHERE preset_key = ?",
            (based_on_preset,)
        )
        preset_result = cur.fetchone()

        if preset_result:
            preset_config = json.loads(preset_result[0])
            config['learning'] = preset_config.get('learning', {
                'potential_threshold': 0.5,
                'decline_threshold': -0.2,
                'decline_penalty': 0.8,
                'slope_amplifier': 10
            })
        else:
            # 使用默认值
            config['learning'] = {
                'potential_threshold': 0.5,
                'decline_threshold': -0.2,
                'decline_penalty': 0.8,
                'slope_amplifier': 10
            }

        # 更新数据库
        new_config_json = json.dumps(config, ensure_ascii=False)
        cur.execute("""
            UPDATE algorithm_active_config
            SET config_data = ?
            WHERE id = ?
        """, (new_config_json, config_id))

        conn.commit()
        print(f"✅ 已为活动配置添加 learning 模块（基于 {based_on_preset} 预设）")
        print(f"   - potential_threshold: {config['learning']['potential_threshold']}")
        print(f"   - decline_threshold: {config['learning']['decline_threshold']}")
        print(f"   - decline_penalty: {config['learning']['decline_penalty']}")
        print(f"   - slope_amplifier: {config['learning']['slope_amplifier']}")
    else:
        print("ℹ️  活动配置已包含 learning 模块，无需更新")

    conn.close()
    print("\n迁移完成！")

if __name__ == '__main__':
    update_active_config()
