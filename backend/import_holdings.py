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
    # 2026-06-26 通过 ttskill invoke ACCOUNT_HOLDING 拉取
    {"fundCode":"021528","fundName":"财通成长优选混合C","assetValue":"41949.83","holdProfit":"13502.90","holdProfitRate":"47.47%","dailyProfit":"2096.43","ptype":"fund"},
    {"fundCode":"024203","fundName":"永赢制造升级智选混合发起C","assetValue":"36441.37","holdProfit":"8511.70","holdProfitRate":"30.48%","dailyProfit":"273.86","ptype":"fund"},
    {"fundCode":"002112","fundName":"德邦鑫星价值灵活配置混合C","assetValue":"29568.16","holdProfit":"22997.12","holdProfitRate":"349.98%","dailyProfit":"866.29","ptype":"fund"},
    {"fundCode":"016531","fundName":"鹏华碳中和主题混合C","assetValue":"28307.55","holdProfit":"-5768.47","holdProfitRate":"-16.93%","dailyProfit":"-691.01","ptype":"fund"},
    {"fundCode":"hqb","fundName":"活期宝","assetValue":"28154.52","holdProfit":"--","holdProfitRate":"--","dailyProfit":"--","ptype":"hqb"},
    {"fundCode":"025857","fundName":"华夏中证电网设备主题ETF发起式联接C","assetValue":"25555.70","holdProfit":"3635.17","holdProfitRate":"16.58%","dailyProfit":"327.20","ptype":"fund"},
    {"fundCode":"002656","fundName":"南方创业板ETF联接A","assetValue":"24046.15","holdProfit":"7959.87","holdProfitRate":"49.48%","dailyProfit":"629.93","ptype":"fund"},
    {"fundCode":"017574","fundName":"华夏中证机床ETF发起式联接C","assetValue":"17864.62","holdProfit":"1807.98","holdProfitRate":"11.26%","dailyProfit":"264.05","ptype":"fund"},
    {"fundCode":"290008","fundName":"泰信发展主题混合","assetValue":"17334.11","holdProfit":"4092.50","holdProfitRate":"32.12%","dailyProfit":"-789.20","ptype":"fund"},
    {"fundCode":"022365","fundName":"永赢科技智选混合发起C","assetValue":"16535.36","holdProfit":"2289.58","holdProfitRate":"16.07%","dailyProfit":"447.18","ptype":"fund"},
    {"fundCode":"020628","fundName":"汇添富科创芯片ETF联接A","assetValue":"15851.04","holdProfit":"13971.66","holdProfitRate":"743.42%","dailyProfit":"582.44","ptype":"fund"},
    {"fundCode":"024424","fundName":"东方阿尔法科技优选混合发起C","assetValue":"15436.24","holdProfit":"3426.24","holdProfitRate":"28.53%","dailyProfit":"313.57","ptype":"fund"},
    {"fundCode":"519771","fundName":"交银优择回报灵活配置混合C","assetValue":"14824.55","holdProfit":"8458.82","holdProfitRate":"132.88%","dailyProfit":"433.05","ptype":"fund"},
    {"fundCode":"011613","fundName":"华夏科创50ETF联接C","assetValue":"13195.78","holdProfit":"2557.36","holdProfitRate":"24.04%","dailyProfit":"461.93","ptype":"fund"},
    {"fundCode":"011452","fundName":"华泰柏瑞质量成长C","assetValue":"12834.33","holdProfit":"4873.20","holdProfitRate":"61.21%","dailyProfit":"559.40","ptype":"fund"},
    {"fundCode":"019173","fundName":"摩根纳斯达克100指数(QDII)人民币C","assetValue":"12648.77","holdProfit":"2149.13","holdProfitRate":"20.53%","dailyProfit":"-45.24","ptype":"fund"},
    {"fundCode":"013943","fundName":"华宝中证稀有金属指数增强发起C","assetValue":"12020.13","holdProfit":"20.13","holdProfitRate":"0.17%","dailyProfit":"-103.31","ptype":"fund"},
    {"fundCode":"025209","fundName":"永赢先锋半导体智选混合发起C","assetValue":"11751.31","holdProfit":"3751.31","holdProfitRate":"46.89%","dailyProfit":"680.85","ptype":"fund"},
    {"fundCode":"013841","fundName":"银华集成电路混合C","assetValue":"11659.71","holdProfit":"2649.71","holdProfitRate":"29.41%","dailyProfit":"360.81","ptype":"fund"},
    {"fundCode":"007491","fundName":"南方信息创新混合C","assetValue":"11204.47","holdProfit":"2194.47","holdProfitRate":"24.36%","dailyProfit":"230.69","ptype":"fund"},
    {"fundCode":"010786","fundName":"博时创业板指数C","assetValue":"9897.74","holdProfit":"8612.95","holdProfitRate":"670.38%","dailyProfit":"258.73","ptype":"fund"},
    {"fundCode":"024481","fundName":"财通品质甄选混合C","assetValue":"9855.17","holdProfit":"2355.17","holdProfitRate":"31.40%","dailyProfit":"505.53","ptype":"fund"},
    {"fundCode":"017811","fundName":"东方人工智能主题混合C","assetValue":"9032.72","holdProfit":"3251.65","holdProfitRate":"56.25%","dailyProfit":"299.99","ptype":"fund"},
    {"fundCode":"019924","fundName":"华泰柏瑞中证2000指数增强C","assetValue":"8536.54","holdProfit":"-343.62","holdProfitRate":"-3.87%","dailyProfit":"-80.84","ptype":"fund"},
    {"fundCode":"021511","fundName":"宏利半导体产业混合发起C","assetValue":"6228.64","holdProfit":"1228.64","holdProfitRate":"24.57%","dailyProfit":"255.06","ptype":"fund"},
    {"fundCode":"008888","fundName":"华夏国证半导体芯片ETF联接C","assetValue":"4511.56","holdProfit":"66.01","holdProfitRate":"1.48%","dailyProfit":"221.12","ptype":"fund"},
    {"fundCode":"020357","fundName":"华夏半导体材料设备ETF联接C","assetValue":"4440.99","holdProfit":"930.99","holdProfitRate":"26.52%","dailyProfit":"120.10","ptype":"fund"},
    {"fundCode":"023765","fundName":"华夏中证5G通信主题ETF联接D","assetValue":"3267.75","holdProfit":"267.75","holdProfitRate":"8.92%","dailyProfit":"111.13","ptype":"fund"},
    {"fundCode":"012920","fundName":"易方达全球成长精选混合(QDII)人民币A","assetValue":"1491.38","holdProfit":"641.38","holdProfitRate":"75.46%","dailyProfit":"12.38","ptype":"fund"},
    {"fundCode":"020465","fundName":"招商中证半导体产业ETF发起式联接C","assetValue":"1231.02","holdProfit":"368.04","holdProfitRate":"42.65%","dailyProfit":"35.33","ptype":"fund"},
    {"fundCode":"008989","fundName":"大成科技创新混合C","assetValue":"1088.95","holdProfit":"78.95","holdProfitRate":"7.82%","dailyProfit":"50.04","ptype":"fund"},
    {"fundCode":"025793","fundName":"东方阿尔法科技甄选混合发起C","assetValue":"1088.40","holdProfit":"78.40","holdProfitRate":"7.76%","dailyProfit":"71.28","ptype":"fund"},
    {"fundCode":"011120","fundName":"富国创新科技混合C","assetValue":"1055.91","holdProfit":"55.91","holdProfitRate":"5.59%","dailyProfit":"36.26","ptype":"fund"},
    {"fundCode":"001665","fundName":"平安鑫安混合C","assetValue":"1053.54","holdProfit":"53.54","holdProfitRate":"5.35%","dailyProfit":"27.44","ptype":"fund"},
    {"fundCode":"161226","fundName":"国投瑞银白银期货(LOF)A","assetValue":"162.09","holdProfit":"-137.91","holdProfitRate":"-45.97%","dailyProfit":"-9.71","ptype":"fund"},
    {"fundCode":"007590","fundName":"华宝绿色领先股票A","assetValue":"0.00","holdProfit":"0.00","holdProfitRate":"--","dailyProfit":"-0.04","ptype":"fund"},
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
