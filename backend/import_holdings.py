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
    {"fundCode":"hqb","fundName":"活期宝","assetValue":"76811.43","holdProfit":"--","holdProfitRate":"--","dailyProfit":"--","ptype":"hqb"},
    {"fundCode":"290008","fundName":"泰信发展主题混合","assetValue":"41778.40","holdProfit":"7362.71","holdProfitRate":"21.39%","dailyProfit":"246.72","ptype":"fund"},
    {"fundCode":"024203","fundName":"永赢制造升级智选混合发起C","assetValue":"37645.42","holdProfit":"-661.88","holdProfitRate":"-1.82%","dailyProfit":"-1095.04","ptype":"fund"},
    {"fundCode":"002112","fundName":"德邦鑫星价值灵活配置混合C","assetValue":"33539.17","holdProfit":"20187.08","holdProfitRate":"151.19%","dailyProfit":"102.99","ptype":"fund"},
    {"fundCode":"016531","fundName":"鹏华碳中和主题混合C","assetValue":"32661.75","holdProfit":"585.73","holdProfitRate":"1.83%","dailyProfit":"-492.94","ptype":"fund"},
    {"fundCode":"008888","fundName":"华夏国证半导体芯片ETF联接C","assetValue":"23807.07","holdProfit":"1807.07","holdProfitRate":"15.06%","dailyProfit":"-86.00","ptype":"fund"},
    {"fundCode":"025857","fundName":"华夏中证电网设备主题ETF发起式联接C","assetValue":"23622.83","holdProfit":"1702.30","holdProfitRate":"7.77%","dailyProfit":"-703.31","ptype":"fund"},
    {"fundCode":"013403","fundName":"华夏恒生科技ETF发起式联接(QDII)C","assetValue":"21680.90","holdProfit":"-7951.85","holdProfitRate":"-26.83%","dailyProfit":"313.09","ptype":"fund"},
    {"fundCode":"021528","fundName":"财通成长优选混合C","assetValue":"21432.75","holdProfit":"1985.82","holdProfitRate":"10.21%","dailyProfit":"236.83","ptype":"fund"},
    {"fundCode":"002656","fundName":"南方创业板ETF联接A","assetValue":"21334.45","holdProfit":"6248.17","holdProfitRate":"41.42%","dailyProfit":"112.29","ptype":"fund"},
    {"fundCode":"011613","fundName":"华夏科创50ETF联接C","assetValue":"18888.66","holdProfit":"2384.44","holdProfitRate":"14.45%","dailyProfit":"-276.74","ptype":"fund"},
    {"fundCode":"016874","fundName":"广发远见智选混合C","assetValue":"17421.28","holdProfit":"2266.74","holdProfitRate":"14.96%","dailyProfit":"-966.72","ptype":"fund"},
    {"fundCode":"010786","fundName":"博时创业板指数C","assetValue":"16308.52","holdProfit":"8043.73","holdProfitRate":"97.33%","dailyProfit":"82.64","ptype":"fund"},
    {"fundCode":"017574","fundName":"华夏中证机床ETF发起式联接C","assetValue":"15914.37","holdProfit":"-142.27","holdProfitRate":"-0.89%","dailyProfit":"-142.27","ptype":"fund"},
    {"fundCode":"020628","fundName":"汇添富科创芯片ETF联接A","assetValue":"13412.48","holdProfit":"11533.10","holdProfitRate":"613.67%","dailyProfit":"-272.72","ptype":"fund"},
    {"fundCode":"022365","fundName":"永赢科技智选混合发起C","assetValue":"13274.03","holdProfit":"1028.25","holdProfitRate":"8.40%","dailyProfit":"270.25","ptype":"fund"},
    {"fundCode":"519771","fundName":"交银优择回报灵活配置混合C","assetValue":"12213.05","holdProfit":"5847.32","holdProfitRate":"91.86%","dailyProfit":"12.38","ptype":"fund"},
    {"fundCode":"019173","fundName":"摩根纳斯达克100指数(QDII)人民币C","assetValue":"11681.00","holdProfit":"2291.36","holdProfitRate":"24.93%","dailyProfit":"-9.86","ptype":"fund"},
    {"fundCode":"013943","fundName":"华宝中证稀有金属指数增强发起C","assetValue":"11418.90","holdProfit":"-581.10","holdProfitRate":"-6.46%","dailyProfit":"97.44","ptype":"fund"},
    {"fundCode":"024424","fundName":"东方阿尔法科技优选混合发起C","assetValue":"10255.39","holdProfit":"245.39","holdProfitRate":"2.45%","dailyProfit":"-333.21","ptype":"fund"},
    {"fundCode":"023971","fundName":"泰康上证科创板综合指数增强C","assetValue":"9752.96","holdProfit":"5701.64","holdProfitRate":"140.74%","dailyProfit":"-130.14","ptype":"fund"},
    {"fundCode":"017811","fundName":"东方人工智能主题混合C","assetValue":"9277.41","holdProfit":"1884.90","holdProfitRate":"78.78%","dailyProfit":"-81.88","ptype":"fund"},
    {"fundCode":"025209","fundName":"永赢先锋半导体智选混合发起C","assetValue":"8799.13","holdProfit":"799.13","holdProfitRate":"9.99%","dailyProfit":"-148.60","ptype":"fund"},
    {"fundCode":"019924","fundName":"华泰柏瑞中证2000指数增强C","assetValue":"8747.21","holdProfit":"-132.95","holdProfitRate":"-1.50%","dailyProfit":"-137.59","ptype":"fund"},
    {"fundCode":"013841","fundName":"银华集成电路混合C","assetValue":"8358.11","holdProfit":"348.11","holdProfitRate":"6.95%","dailyProfit":"-68.08","ptype":"fund"},
    {"fundCode":"011452","fundName":"华泰柏瑞质量成长C","assetValue":"8046.11","holdProfit":"2538.62","holdProfitRate":"46.09%","dailyProfit":"-73.44","ptype":"fund"},
    {"fundCode":"017174","fundName":"天弘国证绿色电力指数发起A","assetValue":"7799.22","holdProfit":"799.22","holdProfitRate":"11.42%","dailyProfit":"-0.65","ptype":"fund"},
    {"fundCode":"021938","fundName":"华安半导体产业股票发起式C","assetValue":"7716.00","holdProfit":"12737.59","holdProfitRate":"--","dailyProfit":"-181.04","ptype":"fund"},
    {"fundCode":"007950","fundName":"招商量化精选股票C","assetValue":"7317.26","holdProfit":"1027.30","holdProfitRate":"16.33%","dailyProfit":"-27.22","ptype":"fund"},
    {"fundCode":"021964","fundName":"天弘储能电池指数C","assetValue":"5920.90","holdProfit":"420.90","holdProfitRate":"7.65%","dailyProfit":"-36.23","ptype":"fund"},
    {"fundCode":"021511","fundName":"宏利半导体产业混合发起C","assetValue":"5263.13","holdProfit":"263.13","holdProfitRate":"5.26%","dailyProfit":"-44.56","ptype":"fund"},
    {"fundCode":"007491","fundName":"南方信息创新混合C","assetValue":"5010.73","holdProfit":"0.73","holdProfitRate":"7.30%","dailyProfit":"-0.14","ptype":"fund"},
    {"fundCode":"005359","fundName":"东方阿尔法精选混合C","assetValue":"4811.32","holdProfit":"-188.68","holdProfitRate":"-3.77%","dailyProfit":"-194.26","ptype":"fund"},
    {"fundCode":"024481","fundName":"财通品质甄选混合C","assetValue":"2099.82","holdProfit":"99.82","holdProfitRate":"4.99%","dailyProfit":"32.19","ptype":"fund"},
    {"fundCode":"020357","fundName":"华夏半导体材料设备ETF联接C","assetValue":"2076.59","holdProfit":"66.59","holdProfitRate":"3.31%","dailyProfit":"-60.44","ptype":"fund"},
    {"fundCode":"015839","fundName":"广发招利混合C","assetValue":"1896.05","holdProfit":"-103.95","holdProfitRate":"-5.20%","dailyProfit":"-95.48","ptype":"fund"},
    {"fundCode":"022482","fundName":"国泰中证新能源汽车ETF联接E","assetValue":"1863.90","holdProfit":"-136.10","holdProfitRate":"-6.80%","dailyProfit":"1.78","ptype":"fund"},
    {"fundCode":"012920","fundName":"易方达全球成长精选混合(QDII)人民币A","assetValue":"1273.95","holdProfit":"423.95","holdProfitRate":"49.88%","dailyProfit":"28.94","ptype":"fund"},
    {"fundCode":"020465","fundName":"招商中证半导体产业ETF发起式联接C","assetValue":"1014.37","holdProfit":"151.39","holdProfitRate":"--","dailyProfit":"-0.30","ptype":"fund"},
    {"fundCode":"161226","fundName":"国投瑞银白银期货(LOF)A","assetValue":"217.46","holdProfit":"-82.54","holdProfitRate":"-27.51%","dailyProfit":"0.42","ptype":"fund"},
    {"fundCode":"017223","fundName":"富国中证电池主题ETF发起式联接C","assetValue":"104.38","holdProfit":"4.38","holdProfitRate":"4.38%","dailyProfit":"0.20","ptype":"fund"},
    {"fundCode":"011172","fundName":"广发利鑫灵活配置混合C","assetValue":"10.41","holdProfit":"0.41","holdProfitRate":"4.10%","dailyProfit":"-0.03","ptype":"fund"},
    {"fundCode":"007590","fundName":"华宝绿色领先股票A","assetValue":"8.88","holdProfit":"-1.12","holdProfitRate":"-11.20%","dailyProfit":"-0.01","ptype":"fund"},
    {"fundCode":"014064","fundName":"银华农业产业股票发起式C","assetValue":"0.00","holdProfit":"0.00","holdProfitRate":"--","dailyProfit":"-4.55","ptype":"fund"},
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
