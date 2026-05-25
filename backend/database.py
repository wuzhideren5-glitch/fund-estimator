"""SQLite 数据库初始化和管理"""

import aiosqlite
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "fund_estimator.db")


async def get_db():
    """获取数据库连接"""
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    return db


async def init_db():
    """初始化数据库表"""
    db = await get_db()
    try:
        await db.executescript("""
            -- 用户基金池
            CREATE TABLE IF NOT EXISTS funds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,           -- 基金代码
                name TEXT NOT NULL,                  -- 基金名称
                fund_type TEXT DEFAULT '',           -- 基金类型
                nav REAL DEFAULT 0,                  -- 最新公布净值
                nav_date TEXT DEFAULT '',            -- 净值日期
                acc_nav REAL DEFAULT 0,              -- 累计净值
                sector TEXT DEFAULT '',              -- 实际板块（根据持仓判断）
                theme_name TEXT DEFAULT '',          -- 主题名称
                holdings_json TEXT DEFAULT '[]',     -- 持仓JSON
                holdings_date TEXT DEFAULT '',       -- 持仓报告日期
                asset_value REAL DEFAULT 0,          -- 持有金额
                hold_profit REAL DEFAULT 0,          -- 持仓收益
                hold_profit_rate TEXT DEFAULT '',    -- 持仓收益率
                daily_profit REAL DEFAULT 0,         -- 昨日收益
                alert_threshold REAL DEFAULT 1.5,    -- 加仓提醒阈值（跌幅%）
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- 净值历史
            CREATE TABLE IF NOT EXISTS nav_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_code TEXT NOT NULL,
                date TEXT NOT NULL,
                nav REAL NOT NULL,
                acc_nav REAL DEFAULT 0,
                UNIQUE(fund_code, date)
            );

            -- 实时估值快照
            CREATE TABLE IF NOT EXISTS estimate_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_code TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                estimated_nav REAL NOT NULL,         -- 估算净值
                change_pct REAL NOT NULL,            -- 估算涨跌幅 %
                holdings_weight_coverage REAL DEFAULT 0,  -- 持仓覆盖率（%）
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_nav_fund ON nav_history(fund_code);
            CREATE INDEX IF NOT EXISTS idx_est_fund ON estimate_snapshots(fund_code);
            CREATE INDEX IF NOT EXISTS idx_nav_date ON nav_history(date);

            -- AI 分类新闻
            CREATE TABLE IF NOT EXISTS classified_news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                content TEXT DEFAULT '',
                source TEXT DEFAULT '',
                publish_time TEXT NOT NULL,
                themes TEXT NOT NULL DEFAULT '[]',   -- JSON array
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_news_themes ON classified_news(themes);
        """)
        await db.commit()
        print("✓ 数据库初始化完成")
    finally:
        await db.close()
