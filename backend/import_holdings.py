"""
批量导入持仓 + 更新估值引擎
用法: python3 import_holdings.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import subprocess
import aiosqlite

sys.path.insert(0, os.path.dirname(__file__))

DB_PATH = os.path.join(os.path.dirname(__file__), "fund_estimator.db")

# 从 ttskill 拉取的实际持仓数据
HOLDINGS_DATA = [
    # 2026-06-27 通过 ttskill invoke ACCOUNT_HOLDING 拉取
    {"fundCode": "021528", "fundName": "财通成长优选混合C", "assetValue": "40980.36", "holdProfit": "12433.43", "holdProfitRate": "43.71%", "dailyProfit": "-1069.46", "ptype": "fund"},
    {"fundCode": "024203", "fundName": "永赢制造升级智选混合发起C", "assetValue": "35176.49", "holdProfit": "7246.82", "holdProfitRate": "25.95%", "dailyProfit": "-1264.89", "ptype": "fund"},
    {"fundCode": "002112", "fundName": "德邦鑫星价值灵活配置混合C", "assetValue": "28711.52", "holdProfit": "21140.48", "holdProfitRate": "321.72%", "dailyProfit": "-1856.64", "ptype": "fund"},
    {"fundCode": "016531", "fundName": "鹏华碳中和主题混合C", "assetValue": "27376.05", "holdProfit": "-6699.97", "holdProfitRate": "-19.66%", "dailyProfit": "-931.50", "ptype": "fund"},
    {"fundCode": "025857", "fundName": "华夏中证电网设备主题ETF发起式联接C", "assetValue": "24091.71", "holdProfit": "2171.18", "holdProfitRate": "9.90%", "dailyProfit": "-1463.99", "ptype": "fund"},
    {"fundCode": "002656", "fundName": "南方创业板ETF联接A", "assetValue": "23120.10", "holdProfit": "7033.82", "holdProfitRate": "43.73%", "dailyProfit": "-926.05", "ptype": "fund"},
    {"fundCode": "hqb", "fundName": "活期宝", "assetValue": "22956.18", "holdProfit": "--", "holdProfitRate": "--", "dailyProfit": "--", "ptype": "hqb"},
    {"fundCode": "017574", "fundName": "华夏中证机床ETF发起式联接C", "assetValue": "17171.08", "holdProfit": "1114.44", "holdProfitRate": "6.94%", "dailyProfit": "-693.54", "ptype": "fund"},
    {"fundCode": "290008", "fundName": "泰信发展主题混合", "assetValue": "16154.85", "holdProfit": "2913.24", "holdProfitRate": "22.00%", "dailyProfit": "-1179.26", "ptype": "fund"},
    {"fundCode": "022365", "fundName": "永赢科技智选混合发起C", "assetValue": "15927.07", "holdProfit": "1681.29", "holdProfitRate": "11.80%", "dailyProfit": "-608.29", "ptype": "fund"},
    {"fundCode": "024424", "fundName": "东方阿尔法科技优选混合发起C", "assetValue": "15801.24", "holdProfit": "3791.24", "holdProfitRate": "31.57%", "dailyProfit": "365.00", "ptype": "fund"},
    {"fundCode": "020628", "fundName": "汇添富科创芯片ETF联接A", "assetValue": "15662.46", "holdProfit": "13783.08", "holdProfitRate": "733.38%", "dailyProfit": "-188.59", "ptype": "fund"},
    {"fundCode": "519771", "fundName": "交银优择回报灵活配置混合C", "assetValue": "14385.10", "holdProfit": "8019.37", "holdProfitRate": "125.98%", "dailyProfit": "-439.44", "ptype": "fund"},
    {"fundCode": "011452", "fundName": "华泰柏瑞质量成长C", "assetValue": "13178.35", "holdProfit": "4217.22", "holdProfitRate": "52.97%", "dailyProfit": "-655.97", "ptype": "fund"},
    {"fundCode": "011613", "fundName": "华夏科创50ETF联接C", "assetValue": "12986.63", "holdProfit": "2348.21", "holdProfitRate": "22.07%", "dailyProfit": "-209.14", "ptype": "fund"},
    {"fundCode": "019173", "fundName": "摩根纳斯达克100指数(QDII)人民币C", "assetValue": "12734.94", "holdProfit": "2235.30", "holdProfitRate": "21.33%", "dailyProfit": "86.16", "ptype": "fund"},
    {"fundCode": "013841", "fundName": "银华集成电路混合C", "assetValue": "12033.43", "holdProfit": "3023.43", "holdProfitRate": "33.56%", "dailyProfit": "373.73", "ptype": "fund"},
    {"fundCode": "025209", "fundName": "永赢先锋半导体智选混合发起C", "assetValue": "11758.66", "holdProfit": "3758.66", "holdProfitRate": "46.98%", "dailyProfit": "7.36", "ptype": "fund"},
    {"fundCode": "007491", "fundName": "南方信息创新混合C", "assetValue": "11518.03", "holdProfit": "2508.03", "holdProfitRate": "27.84%", "dailyProfit": "313.55", "ptype": "fund"},
    {"fundCode": "013943", "fundName": "华宝中证稀有金属指数增强发起C", "assetValue": "11467.29", "holdProfit": "-532.71", "holdProfitRate": "-4.44%", "dailyProfit": "-552.84", "ptype": "fund"},
    {"fundCode": "010786", "fundName": "博时创业板指数C", "assetValue": "10519.75", "holdProfit": "8234.96", "holdProfitRate": "640.96%", "dailyProfit": "-377.99", "ptype": "fund"},
    {"fundCode": "024481", "fundName": "财通品质甄选混合C", "assetValue": "9728.24", "holdProfit": "2128.24", "holdProfitRate": "28.38%", "dailyProfit": "-226.93", "ptype": "fund"},
    {"fundCode": "017811", "fundName": "东方人工智能主题混合C", "assetValue": "9383.66", "holdProfit": "3602.59", "holdProfitRate": "62.32%", "dailyProfit": "350.94", "ptype": "fund"},
    {"fundCode": "019924", "fundName": "华泰柏瑞中证2000指数增强C", "assetValue": "8303.01", "holdProfit": "-577.15", "holdProfitRate": "-6.50%", "dailyProfit": "-233.53", "ptype": "fund"},
    {"fundCode": "021511", "fundName": "宏利半导体产业混合发起C", "assetValue": "6147.97", "holdProfit": "1147.97", "holdProfitRate": "22.96%", "dailyProfit": "-80.67", "ptype": "fund"},
    {"fundCode": "008888", "fundName": "华夏国证半导体芯片ETF联接C", "assetValue": "5463.86", "holdProfit": "18.31", "holdProfitRate": "0.41%", "dailyProfit": "-47.70", "ptype": "fund"},
    {"fundCode": "020357", "fundName": "华夏半导体材料设备ETF联接C", "assetValue": "4592.67", "holdProfit": "1082.67", "holdProfitRate": "30.85%", "dailyProfit": "151.68", "ptype": "fund"},
    {"fundCode": "023765", "fundName": "华夏中证5G通信主题ETF联接D", "assetValue": "4092.73", "holdProfit": "92.73", "holdProfitRate": "3.09%", "dailyProfit": "-175.02", "ptype": "fund"},
    {"fundCode": "012920", "fundName": "易方达全球成长精选混合(QDII)人民币A", "assetValue": "1572.55", "holdProfit": "722.55", "holdProfitRate": "85.01%", "dailyProfit": "81.16", "ptype": "fund"},
    {"fundCode": "020465", "fundName": "招商中证半导体产业ETF发起式联接C", "assetValue": "1249.58", "holdProfit": "386.60", "holdProfitRate": "44.80%", "dailyProfit": "18.56", "ptype": "fund"},
    {"fundCode": "025793", "fundName": "东方阿尔法科技甄选混合发起C", "assetValue": "1084.97", "holdProfit": "74.97", "holdProfitRate": "7.42%", "dailyProfit": "-3.44", "ptype": "fund"},
    {"fundCode": "008989", "fundName": "大成科技创新混合C", "assetValue": "1054.46", "holdProfit": "44.46", "holdProfitRate": "4.40%", "dailyProfit": "-34.49", "ptype": "fund"},
    {"fundCode": "001665", "fundName": "平安鑫安混合C", "assetValue": "1021.15", "holdProfit": "21.15", "holdProfitRate": "2.12%", "dailyProfit": "-32.40", "ptype": "fund"},
    {"fundCode": "011120", "fundName": "富国创新科技混合C", "assetValue": "993.37", "holdProfit": "-6.63", "holdProfitRate": "-0.66%", "dailyProfit": "-62.53", "ptype": "fund"},
    {"fundCode": "161226", "fundName": "国投瑞银白银期货(LOF)A", "assetValue": "161.55", "holdProfit": "-138.45", "holdProfitRate": "-46.15%", "dailyProfit": "-0.54", "ptype": "fund"}
]


async def import_all():
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row

    try:
        # 添加持仓相关列（如果不存在）
        try:
            await db.execute("ALTER TABLE funds ADD COLUMN asset_value REAL DEFAULT 0")
        except:
            pass
        try:
            await db.execute("ALTER TABLE funds ADD COLUMN hold_profit REAL DEFAULT 0")
        except:
            pass
        try:
            await db.execute("ALTER TABLE funds ADD COLUMN hold_profit_rate TEXT DEFAULT ''")
        except:
            pass
        try:
            await db.execute("ALTER TABLE funds ADD COLUMN daily_profit REAL DEFAULT 0")
        except:
            pass
        try:
            await db.execute("ALTER TABLE funds ADD COLUMN theme_name TEXT DEFAULT ''")
        except:
            pass
        try:
            await db.execute("ALTER TABLE funds ADD COLUMN theme_code TEXT DEFAULT ''")
        except:
            pass

        total = len(HOLDINGS_DATA)
        new_count = 0
        update_count = 0

        for i, h in enumerate(HOLDINGS_DATA):
            code = h["fundCode"]
            name = h["fundName"]
            av_str = h["assetValue"]; av = float(av_str) if av_str not in ("--", "") else 0
            dp_str = h["dailyProfit"]; dp = float(dp_str) if dp_str not in ("--", "") else 0
            hp_str = h.get("holdProfit", "0").replace(",", "") or "0"
            hp = float(hp_str) if hp_str != "--" else 0
            hpr = h.get("holdProfitRate", "")

            cursor = await db.execute("SELECT id FROM funds WHERE code = ?", (code,))
            existing = await cursor.fetchone()

            if existing:
                await db.execute(
                    """UPDATE funds SET name=?, asset_value=?, hold_profit=?, 
                    hold_profit_rate=?, daily_profit=?, updated_at=CURRENT_TIMESTAMP
                    WHERE code=?""",
                    (name, av, hp, hpr, dp, code),
                )
                update_count += 1
            else:
                await db.execute(
                    """INSERT INTO funds (code, name, asset_value, hold_profit,
                    hold_profit_rate, daily_profit, alert_threshold)
                    VALUES (?, ?, ?, ?, ?, ?, 1.5)""",
                    (code, name, av, hp, hpr, dp),
                )
                new_count += 1

            if (i + 1) % 10 == 0:
                print(f"  进度: {i+1}/{total}")

        await db.commit()
        print(f"\n✅ 导入完成: 新增 {new_count} 只, 更新 {update_count} 只, 合计 {total} 只")

    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(import_all())
