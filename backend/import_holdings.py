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
    # 2026-06-24 通过 ttskill invoke ACCOUNT_HOLDING 拉取
    {"fundCode":"hqb","fundName":"活期宝","assetValue":"28569.10","holdProfit":"--","holdProfitRate":"--","dailyProfit":"--","ptype":"hqb"},
    {"fundCode":"021528","fundName":"财通成长优选混合C","assetValue":"39853.40","holdProfit":"11406.47","holdProfitRate":"40.10%","dailyProfit":"984.48","ptype":"fund"},
    {"fundCode":"024203","fundName":"永赢制造升级智选混合发起C","assetValue":"36167.51","holdProfit":"8237.84","holdProfitRate":"29.49%","dailyProfit":"411.78","ptype":"fund"},
    {"fundCode":"016531","fundName":"鹏华碳中和主题混合C","assetValue":"28998.56","holdProfit":"-5077.46","holdProfitRate":"-14.90%","dailyProfit":"130.41","ptype":"fund"},
    {"fundCode":"002112","fundName":"德邦鑫星价值灵活配置混合C","assetValue":"28701.88","holdProfit":"22130.84","holdProfitRate":"336.79%","dailyProfit":"619.43","ptype":"fund"},
    {"fundCode":"025857","fundName":"华夏中证电网设备主题ETF发起式联接C","assetValue":"25228.49","holdProfit":"3307.96","holdProfitRate":"15.09%","dailyProfit":"-20.24","ptype":"fund"},
    {"fundCode":"002656","fundName":"南方创业板ETF联接A","assetValue":"23416.22","holdProfit":"7329.94","holdProfitRate":"45.57%","dailyProfit":"313.24","ptype":"fund"},
    {"fundCode":"290008","fundName":"泰信发展主题混合","assetValue":"22335.64","holdProfit":"4881.71","holdProfitRate":"27.97%","dailyProfit":"1226.20","ptype":"fund"},
    {"fundCode":"017574","fundName":"华夏中证机床ETF发起式联接C","assetValue":"17600.57","holdProfit":"1543.93","holdProfitRate":"9.62%","dailyProfit":"258.09","ptype":"fund"},
    {"fundCode":"022365","fundName":"永赢科技智选混合发起C","assetValue":"16088.18","holdProfit":"1842.40","holdProfitRate":"12.93%","dailyProfit":"207.22","ptype":"fund"},
    {"fundCode":"020628","fundName":"汇添富科创芯片ETF联接A","assetValue":"15268.60","holdProfit":"13389.22","holdProfitRate":"712.43%","dailyProfit":"637.57","ptype":"fund"},
    {"fundCode":"024424","fundName":"东方阿尔法科技优选混合发起C","assetValue":"15122.67","holdProfit":"3112.67","holdProfitRate":"25.92%","dailyProfit":"1064.27","ptype":"fund"},
    {"fundCode":"519771","fundName":"交银优择回报灵活配置混合C","assetValue":"14391.50","holdProfit":"8025.77","holdProfitRate":"126.08%","dailyProfit":"400.04","ptype":"fund"},
    {"fundCode":"011613","fundName":"华夏科创50ETF联接C","assetValue":"12733.85","holdProfit":"2095.43","holdProfitRate":"19.70%","dailyProfit":"444.63","ptype":"fund"},
    {"fundCode":"019173","fundName":"摩根纳斯达克100指数(QDII)人民币C","assetValue":"12674.02","holdProfit":"2194.38","holdProfitRate":"20.98%","dailyProfit":"-384.96","ptype":"fund"},
    {"fundCode":"011452","fundName":"华泰柏瑞质量成长C","assetValue":"12274.93","holdProfit":"4313.80","holdProfitRate":"54.19%","dailyProfit":"350.17","ptype":"fund"},
    {"fundCode":"013943","fundName":"华宝中证稀有金属指数增强发起C","assetValue":"12123.44","holdProfit":"123.44","holdProfitRate":"1.03%","dailyProfit":"269.91","ptype":"fund"},
    {"fundCode":"013841","fundName":"银华集成电路混合C","assetValue":"11298.90","holdProfit":"2288.90","holdProfitRate":"25.40%","dailyProfit":"850.84","ptype":"fund"},
    {"fundCode":"025209","fundName":"永赢先锋半导体智选混合发起C","assetValue":"11070.46","holdProfit":"3070.46","holdProfitRate":"38.38%","dailyProfit":"434.40","ptype":"fund"},
    {"fundCode":"007491","fundName":"南方信息创新混合C","assetValue":"10973.78","holdProfit":"1963.78","holdProfitRate":"21.80%","dailyProfit":"837.48","ptype":"fund"},
    {"fundCode":"010786","fundName":"博时创业板指数C","assetValue":"9639.01","holdProfit":"8354.22","holdProfitRate":"650.24%","dailyProfit":"128.69","ptype":"fund"},
    {"fundCode":"024481","fundName":"财通品质甄选混合C","assetValue":"9349.65","holdProfit":"1849.65","holdProfitRate":"24.66%","dailyProfit":"233.58","ptype":"fund"},
    {"fundCode":"017811","fundName":"东方人工智能主题混合C","assetValue":"8732.73","holdProfit":"2951.66","holdProfitRate":"51.06%","dailyProfit":"548.78","ptype":"fund"},
    {"fundCode":"019924","fundName":"华泰柏瑞中证2000指数增强C","assetValue":"8617.38","holdProfit":"-262.78","holdProfitRate":"-2.96%","dailyProfit":"14.70","ptype":"fund"},
    {"fundCode":"021964","fundName":"天弘储能电池指数C","assetValue":"7734.92","holdProfit":"234.92","holdProfitRate":"3.13%","dailyProfit":"30.84","ptype":"fund"},
    {"fundCode":"021511","fundName":"宏利半导体产业混合发起C","assetValue":"5973.57","holdProfit":"973.57","holdProfitRate":"19.47%","dailyProfit":"238.16","ptype":"fund"},
    {"fundCode":"020357","fundName":"华夏半导体材料设备ETF联接C","assetValue":"4320.90","holdProfit":"810.90","holdProfitRate":"23.10%","dailyProfit":"210.94","ptype":"fund"},
    {"fundCode":"008888","fundName":"华夏国证半导体芯片ETF联接C","assetValue":"4290.44","holdProfit":"-155.11","holdProfitRate":"-3.49%","dailyProfit":"207.84","ptype":"fund"},
    {"fundCode":"023765","fundName":"华夏中证5G通信主题ETF联接D","assetValue":"3156.62","holdProfit":"156.62","holdProfitRate":"5.22%","dailyProfit":"90.94","ptype":"fund"},
    {"fundCode":"012920","fundName":"易方达全球成长精选混合(QDII)人民币A","assetValue":"1479.00","holdProfit":"629.00","holdProfitRate":"74.00%","dailyProfit":"-125.23","ptype":"fund"},
    {"fundCode":"020465","fundName":"招商中证半导体产业ETF发起式联接C","assetValue":"1195.69","holdProfit":"332.71","holdProfitRate":"38.55%","dailyProfit":"54.06","ptype":"fund"},
    {"fundCode":"008989","fundName":"大成科技创新混合C","assetValue":"1038.90","holdProfit":"28.90","holdProfitRate":"2.86%","dailyProfit":"28.86","ptype":"fund"},
    {"fundCode":"001665","fundName":"平安鑫安混合C","assetValue":"1026.10","holdProfit":"26.10","holdProfitRate":"2.61%","dailyProfit":"6.46","ptype":"fund"},
    {"fundCode":"011120","fundName":"富国创新科技混合C","assetValue":"1019.64","holdProfit":"19.64","holdProfitRate":"1.96%","dailyProfit":"19.64","ptype":"fund"},
    {"fundCode":"025793","fundName":"东方阿尔法科技甄选混合发起C","assetValue":"1017.12","holdProfit":"7.12","holdProfitRate":"0.70%","dailyProfit":"6.87","ptype":"fund"},
    {"fundCode":"161226","fundName":"国投瑞银白银期货(LOF)A","assetValue":"171.80","holdProfit":"-128.20","holdProfitRate":"-42.73%","dailyProfit":"-7.63","ptype":"fund"},
    {"fundCode":"007590","fundName":"华宝绿色领先股票A","assetValue":"8.03","holdProfit":"-1.97","holdProfitRate":"-19.70%","dailyProfit":"0.28","ptype":"fund"},
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
