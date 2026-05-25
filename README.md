# 📊 Fund Live Nav — 基金实时估值看板

个人自用轻量级基金估值工具，数据来源东方财富公开接口，无需 API Key。

## 功能

- **实时估值** — 44 只基金实时净值估算（东方财富 fundgz 接口）
- **AI 新闻分类** — Tushare 获取财经新闻 → DeepSeek 智能归类到对应基金主题
- **风格分析** — 基金持仓风格雷达图（大盘/小盘、价值/成长等维度）
- **单页看板** — 纯前端 SPA，涨红跌绿（中国习惯）

## 技术栈

- **后端**：Python FastAPI + SQLite
- **前端**：原生 HTML/CSS/JS（单页面）
- **数据源**：东方财富 fundgz（免费，无需注册）
- **新闻**：Tushare Pro + DeepSeek AI 分类

## 快速开始

```bash
cd backend
pip install -r requirements.txt
cp .env.example .env  # 填入你的 DEEPSEEK_API_KEY
python main.py
```

打开 `http://localhost:8766`

## 项目结构

```
backend/
├── main.py              # FastAPI 入口
├── estimator.py         # 基金估值引擎
├── fetcher.py           # 东方财富数据抓取
├── database.py          # SQLite 数据库层
├── news_classifier.py   # DeepSeek 新闻分类
├── tushare_client.py    # Tushare 新闻获取
├── style_engine.py      # 持仓风格分析
├── import_holdings.py   # 持仓导入工具
├── batch_update_themes.py # 批量更新基金主题
├── static/
│   └── index.html       # 前端看板
└── requirements.txt
```

## 数据说明

- 估值数据延迟约 2-3 分钟（东方财富 fundgz 接口刷新频率）
- 新闻每 30 分钟自动刷新
- 所有持仓数据存储在本地 SQLite，不上传任何服务器

## 许可

MIT
