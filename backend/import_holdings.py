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
    {"fundCode":"013403","fundName":"华夏恒生科技ETF发起式联接(QDII)C","assetValue":"42767.73","holdProfit":"-8248.87","holdProfitRate":"-16.17%","dailyProfit":"834.91","ptype":"fund"},
    {"fundCode":"290008","fundName":"泰信发展主题混合","assetValue":"42091.21","holdProfit":"7675.52","holdProfitRate":"23.68%","dailyProfit":"454.34","ptype":"fund"},
    {"fundCode":"024203","fundName":"永赢制造升级智选混合发起C","assetValue":"35890.95","holdProfit":"-416.35","holdProfitRate":"-1.15%","dailyProfit":"1144.15","ptype":"fund"},
    {"fundCode":"002112","fundName":"德邦鑫星价值灵活配置混合C","assetValue":"31690.89","holdProfit":"18338.80","holdProfitRate":"137.35%","dailyProfit":"1908.97","ptype":"fund"},
    {"fundCode":"020628","fundName":"汇添富科创芯片ETF联接A","assetValue":"25665.87","holdProfit":"10953.55","holdProfitRate":"74.45%","dailyProfit":"468.56","ptype":"fund"},
    {"fundCode":"012708","fundName":"东方红中证红利低波动指数A","assetValue":"24274.70","holdProfit":"260.19","holdProfitRate":"1.08%","dailyProfit":"-48.93","ptype":"fund"},
    {"fundCode":"002656","fundName":"南方创业板ETF联接A","assetValue":"20806.99","holdProfit":"5720.71","holdProfitRate":"37.92%","dailyProfit":"548.07","ptype":"fund"},
    {"fundCode":"021528","fundName":"财通成长优选混合C","assetValue":"20459.69","holdProfit":"1012.76","holdProfitRate":"10.72%","dailyProfit":"908.04","ptype":"fund"},
    {"fundCode":"025857","fundName":"华夏中证电网设备主题ETF发起式联接C","assetValue":"19139.89","holdProfit":"2219.36","holdProfitRate":"18.62%","dailyProfit":"279.16","ptype":"fund"},
    {"fundCode":"016874","fundName":"广发远见智选混合C","assetValue":"18427.55","holdProfit":"3273.01","holdProfitRate":"21.60%","dailyProfit":"490.26","ptype":"fund"},
    {"fundCode":"016531","fundName":"鹏华碳中和主题混合C","assetValue":"18176.32","holdProfit":"1100.30","holdProfitRate":"6.44%","dailyProfit":"306.25","ptype":"fund"},
    {"fundCode":"011613","fundName":"华夏科创50ETF联接C","assetValue":"18151.98","holdProfit":"1647.76","holdProfitRate":"9.98%","dailyProfit":"259.85","ptype":"fund"},
    {"fundCode":"022070","fundName":"天弘中证工程机械主题指数发起C","assetValue":"16424.05","holdProfit":"-43.64","holdProfitRate":"-0.27%","dailyProfit":"344.88","ptype":"fund"},
    {"fundCode":"010786","fundName":"博时创业板指数C","assetValue":"15912.07","holdProfit":"7647.28","holdProfitRate":"92.53%","dailyProfit":"416.82","ptype":"fund"},
    {"fundCode":"021938","fundName":"华安半导体产业股票发起式C","assetValue":"15016.01","holdProfit":"12529.60","holdProfitRate":"503.92%","dailyProfit":"365.25","ptype":"fund"},
    {"fundCode":"008888","fundName":"华夏国证半导体芯片ETF联接C","assetValue":"12981.04","holdProfit":"981.04","holdProfitRate":"8.18%","dailyProfit":"312.49","ptype":"fund"},
    {"fundCode":"519771","fundName":"交银优择回报灵活配置混合C","assetValue":"11567.91","holdProfit":"5202.18","holdProfitRate":"81.72%","dailyProfit":"589.44","ptype":"fund"},
    {"fundCode":"019173","fundName":"摩根纳斯达克100指数(QDII)人民币C","assetValue":"11442.67","holdProfit":"2253.03","holdProfitRate":"25.06%","dailyProfit":"14.85","ptype":"fund"},
    {"fundCode":"024424","fundName":"东方阿尔法科技优选混合发起C","assetValue":"10149.01","holdProfit":"139.01","holdProfitRate":"1.39%","dailyProfit":"139.40","ptype":"fund"},
    {"fundCode":"023971","fundName":"泰康上证科创板综合指数增强C","assetValue":"9598.00","holdProfit":"5546.68","holdProfitRate":"136.91%","dailyProfit":"199.07","ptype":"fund"},
    {"fundCode":"025209","fundName":"永赢先锋半导体智选混合发起C","assetValue":"8642.81","holdProfit":"642.81","holdProfitRate":"8.04%","dailyProfit":"249.03","ptype":"fund"},
    {"fundCode":"013943","fundName":"华宝中证稀有金属指数增强发起C","assetValue":"8257.66","holdProfit":"-742.34","holdProfitRate":"-9.28%","dailyProfit":"214.10","ptype":"fund"},
    {"fundCode":"017811","fundName":"东方人工智能主题混合C","assetValue":"8242.42","holdProfit":"1693.14","holdProfitRate":"25.85%","dailyProfit":"52.08","ptype":"fund"},
    {"fundCode":"011452","fundName":"华泰柏瑞质量成长C","assetValue":"7825.30","holdProfit":"2317.81","holdProfitRate":"42.08%","dailyProfit":"423.45","ptype":"fund"},
    {"fundCode":"022365","fundName":"永赢科技智选混合发起C","assetValue":"7663.67","holdProfit":"417.89","holdProfitRate":"5.77%","dailyProfit":"458.39","ptype":"fund"},
    {"fundCode":"017174","fundName":"天弘国证绿色电力指数发起A","assetValue":"7634.29","holdProfit":"634.29","holdProfitRate":"9.06%","dailyProfit":"44.63","ptype":"fund"},
    {"fundCode":"014064","fundName":"银华农业产业股票发起式C","assetValue":"7621.55","holdProfit":"-878.45","holdProfitRate":"-10.33%","dailyProfit":"-121.27","ptype":"fund"},
    {"fundCode":"007950","fundName":"招商量化精选股票C","assetValue":"7340.35","holdProfit":"1050.39","holdProfitRate":"16.70%","dailyProfit":"129.48","ptype":"fund"},
    {"fundCode":"021964","fundName":"天弘储能电池指数C","assetValue":"5976.66","holdProfit":"476.66","holdProfitRate":"8.67%","dailyProfit":"101.93","ptype":"fund"},
    {"fundCode":"005359","fundName":"东方阿尔法精选混合C","assetValue":"5084.58","holdProfit":"84.58","holdProfitRate":"1.69%","dailyProfit":"217.49","ptype":"fund"},
    {"fundCode":"013841","fundName":"银华集成电路混合C","assetValue":"5083.11","holdProfit":"73.11","holdProfitRate":"1.46%","dailyProfit":"70.18","ptype":"fund"},
    {"fundCode":"021511","fundName":"宏利半导体产业混合发起C","assetValue":"5071.07","holdProfit":"71.07","holdProfitRate":"1.42%","dailyProfit":"71.07","ptype":"fund"},
    {"fundCode":"019924","fundName":"华泰柏瑞中证2000指数增强C","assetValue":"3877.30","holdProfit":"-2.86","holdProfitRate":"-0.07%","dailyProfit":"91.22","ptype":"fund"},
    {"fundCode":"020357","fundName":"华夏半导体材料设备ETF联接C","assetValue":"2029.89","holdProfit":"19.89","holdProfitRate":"0.99%","dailyProfit":"19.89","ptype":"fund"},
    {"fundCode":"015839","fundName":"广发招利混合C","assetValue":"2011.65","holdProfit":"11.65","holdProfitRate":"0.58%","dailyProfit":"53.56","ptype":"fund"},
    {"fundCode":"024481","fundName":"财通品质甄选混合C","assetValue":"2000.00","holdProfit":"0.00","holdProfitRate":"--","dailyProfit":"0.00","ptype":"fund"},
    {"fundCode":"022482","fundName":"国泰中证新能源汽车ETF联接E","assetValue":"1894.60","holdProfit":"-105.40","holdProfitRate":"-5.27%","dailyProfit":"28.39","ptype":"fund"},
    {"fundCode":"012920","fundName":"易方达全球成长精选混合(QDII)人民币A","assetValue":"1207.73","holdProfit":"357.73","holdProfitRate":"42.09%","dailyProfit":"6.74","ptype":"fund"},
    {"fundCode":"020465","fundName":"招商中证半导体产业ETF发起式联接C","assetValue":"700.09","holdProfit":"150.81","holdProfitRate":"27.46%","dailyProfit":"5.26","ptype":"fund"},
    {"fundCode":"161226","fundName":"国投瑞银白银期货(LOF)A","assetValue":"214.48","holdProfit":"-85.52","holdProfitRate":"-28.51%","dailyProfit":"0.61","ptype":"fund"},
    {"fundCode":"017223","fundName":"富国中证电池主题ETF发起式联接C","assetValue":"106.73","holdProfit":"6.73","holdProfitRate":"6.73%","dailyProfit":"1.54","ptype":"fund"},
    {"fundCode":"011172","fundName":"广发利鑫灵活配置混合C","assetValue":"10.27","holdProfit":"0.27","holdProfitRate":"2.70%","dailyProfit":"0.51","ptype":"fund"},
    {"fundCode":"007491","fundName":"南方信息创新混合C","assetValue":"10.25","holdProfit":"0.25","holdProfitRate":"2.50%","dailyProfit":"0.18","ptype":"fund"},
    {"fundCode":"007590","fundName":"华宝绿色领先股票A","assetValue":"9.15","holdProfit":"-0.85","holdProfitRate":"-8.50%","dailyProfit":"0.09","ptype":"fund"},
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
            av = float(h["assetValue"])
            dp = float(h["dailyProfit"])
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
