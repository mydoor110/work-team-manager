# 班组管理系统

一个基于 Flask 的班组人员综合管理系统，包含绩效管理、培训管理、安全管理和智能人员画像功能。

## ✨ 核心功能

- **绩效管理**: Excel数据导入、月度/季度绩效查询、区间统计
- **人员画像**: 综合能力评估、关键人员识别、多维度分析
- **培训管理**: 培训记录管理、不合格人员跟踪、培训项目分类
- **安全管理**: 安全检查记录、违规统计分析、安全看板
- **算法配置**: 3个预设方案（严格/标准/宽松）、可视化公式展示
- **部门管理**: 层级部门结构、权限控制

## 🚀 快速开始

```bash
# 1. 创建虚拟环境
python3 -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

# 2. 安装依赖
pip install -r requirements.txt

# 3. 配置环境变量
cp .env.example .env
# 编辑 .env 文件，设置管理员账户和密钥

# 4. 运行应用（自动初始化数据库）
python app.py
```

访问 http://localhost:5001

## 📖 文档

详细部署和配置说明请查看 [部署指南.md](部署指南.md)

## 🔧 技术栈

- **后端**: Flask 2.3.3, SQLite3
- **前端**: Bootstrap 5, Chart.js, ECharts
- **安全**: Flask-WTF (CSRF), Werkzeug (密码哈希)
- **文件处理**: openpyxl, xlrd, pdfplumber

## 📁 项目结构

```
源码/
├── app.py                  # 主应用入口
├── requirements.txt        # Python依赖
├── .env.example           # 环境变量示例
├── 部署指南.md            # 详细部署文档
├── config/                # 配置模块
├── models/                # 数据库模型
├── blueprints/            # 路由蓝图
│   ├── auth.py           # 认证模块
│   ├── personnel.py      # 人员画像
│   ├── training.py       # 培训管理
│   ├── safety.py         # 安全管理
│   ├── performance.py    # 绩效管理
│   └── system_config.py  # 系统配置
├── services/              # 业务服务层
│   └── algorithm_config_service.py  # 算法配置服务
├── utils/                 # 工具函数
├── templates/             # HTML模板
└── static/                # 静态资源
```

## 🔐 默认账户

- 用户名: `admin`（可通过环境变量 APP_USER 修改）
- 密码: 见 .env 文件中的 APP_PASS 配置

**⚠️ 生产环境务必修改默认密码！**

## 📊 算法配置

系统首次启动会自动初始化3个预设方案：

- **严格档**: 更严格的考核标准，适用于高要求场景
- **标准档**: 平衡公平与激励，默认方案
- **宽松档**: 较宽松的标准，适用于培养阶段

当前配置:
- 关键人员综合分阈值: < 70
- 月均违规次数阈值: ≥ 3次

可通过系统配置页面查看详细公式和调整参数。

## 📝 环境变量

必需配置：

```bash
APP_USER=admin              # 管理员用户名
APP_PASS=your_password      # 管理员密码
SECRET_KEY=your_secret_key  # Flask密钥
PORT=5001                   # 监听端口
```

详见 `.env.example`

## 🛡️ 安全特性

- CSRF 保护
- 密码哈希存储
- 安全HTTP头
- 会话管理
- SQL注入防护

## 📦 生产部署

支持多种部署方式：

1. **Gunicorn** (Linux/macOS) - 推荐
2. **Waitress** (Windows) - 推荐
3. **systemd** (Linux服务)

详见 [部署指南.md](部署指南.md)

## 🔄 数据库

- SQLite3 (开发/小规模生产)
- 自动创建表结构和索引
- 自动初始化管理员账户和算法配置

## 📞 支持

问题反馈请查看日志文件：
- 应用日志: `logs/app.log`
- Gunicorn日志: `logs/gunicorn.log`

## 📄 License

内部使用项目

---

**版本**: v1.0
**更新日期**: 2025-12-25
