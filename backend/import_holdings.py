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
    {"fundCode":"hqb","fundName":"活期宝","assetValue":"102462.88","holdProfit":"--","holdProfitRate":"--","dailyProfit":"--","ptype":"hqb"},
    {"fundCode":"290008","fundName":"泰信发展主题混合","assetValue":"42531.68","holdProfit":"7115.99","holdProfitRate":"20.68%","dailyProfit":"-559.53","ptype":"fund"},
    {"fundCode":"024203","fundName":"永赢制造升级智选混合发起C","assetValue":"36740.47","holdProfit":"433.17","holdProfitRate":"1.19%","dailyProfit":"849.52","ptype":"fund"},
    {"fundCode":"002112","fundName":"德邦鑫星价值灵活配置混合C","assetValue":"33436.18","holdProfit":"20084.09","holdProfitRate":"150.42%","dailyProfit":"1745.30","ptype":"fund"},
    {"fundCode":"016531","fundName":"鹏华碳中和主题混合C","assetValue":"33154.69","holdProfit":"1078.67","holdProfitRate":"6.32%","dailyProfit":"-21.63","ptype":"fund"},
    {"fundCode":"025857","fundName":"华夏中证电网设备主题ETF发起式联接C","assetValue":"24326.14","holdProfit":"2405.61","holdProfitRate":"14.22%","dailyProfit":"186.25","ptype":"fund"},
    {"fundCode":"013403","fundName":"华夏恒生科技ETF发起式联接(QDII)C","assetValue":"21367.80","holdProfit":"-8264.95","holdProfitRate":"-27.89%","dailyProfit":"-16.07","ptype":"fund"},
    {"fundCode":"002656","fundName":"南方创业板ETF联接A","assetValue":"21222.16","holdProfit":"6135.88","holdProfitRate":"40.67%","dailyProfit":"415.17","ptype":"fund"},
    {"fundCode":"021528","fundName":"财通成长优选混合C","assetValue":"21195.92","holdProfit":"1748.99","holdProfitRate":"8.99%","dailyProfit":"736.23","ptype":"fund"},
    {"fundCode":"011613","fundName":"华夏科创50ETF联接C","assetValue":"19165.40","holdProfit":"2661.18","holdProfitRate":"16.12%","dailyProfit":"1013.42","ptype":"fund"},
    {"fundCode":"016874","fundName":"广发远见智选混合C","assetValue":"18388.00","holdProfit":"3233.46","holdProfitRate":"21.34%","dailyProfit":"-39.55","ptype":"fund"},
    {"fundCode":"022070","fundName":"天弘中证工程机械主题指数发起C","assetValue":"16544.01","holdProfit":"76.32","holdProfitRate":"0.46%","dailyProfit":"119.96","ptype":"fund"},
    {"fundCode":"010786","fundName":"博时创业板指数C","assetValue":"16225.88","holdProfit":"7961.09","holdProfitRate":"96.33%","dailyProfit":"313.81","ptype":"fund"},
    {"fundCode":"017574","fundName":"华夏中证机床ETF发起式联接C","assetValue":"16056.64","holdProfit":"0.00","holdProfitRate":"--","dailyProfit":"0.00","ptype":"fund"},
    {"fundCode":"008888","fundName":"华夏国证半导体芯片ETF联接C","assetValue":"13893.07","holdProfit":"1893.07","holdProfitRate":"15.78%","dailyProfit":"912.03","ptype":"fund"},
    {"fundCode":"020628","fundName":"汇添富科创芯片ETF联接A","assetValue":"13685.20","holdProfit":"11805.82","holdProfitRate":"628.18%","dailyProfit":"852.27","ptype":"fund"},
    {"fundCode":"022365","fundName":"永赢科技智选混合发起C","assetValue":"13003.78","holdProfit":"758.00","holdProfitRate":"10.46%","dailyProfit":"340.11","ptype":"fund"},
    {"fundCode":"519771","fundName":"交银优择回报灵活配置混合C","assetValue":"12200.67","holdProfit":"5834.94","holdProfitRate":"91.66%","dailyProfit":"632.76","ptype":"fund"},
    {"fundCode":"019173","fundName":"摩根纳斯达克100指数(QDII)人民币C","assetValue":"11590.87","holdProfit":"2301.23","holdProfitRate":"25.32%","dailyProfit":"48.20","ptype":"fund"},
    {"fundCode":"024424","fundName":"东方阿尔法科技优选混合发起C","assetValue":"10588.59","holdProfit":"578.59","holdProfitRate":"5.78%","dailyProfit":"439.59","ptype":"fund"},
    {"fundCode":"023971","fundName":"泰康上证科创板综合指数增强C","assetValue":"9883.10","holdProfit":"5831.78","holdProfitRate":"143.95%","dailyProfit":"285.10","ptype":"fund"},
    {"fundCode":"025209","fundName":"永赢先锋半导体智选混合发起C","assetValue":"8947.74","holdProfit":"947.74","holdProfitRate":"11.85%","dailyProfit":"304.93","ptype":"fund"},
    {"fundCode":"019924","fundName":"华泰柏瑞中证2000指数增强C","assetValue":"8884.80","holdProfit":"4.64","holdProfitRate":"0.12%","dailyProfit":"7.50","ptype":"fund"},
    {"fundCode":"013943","fundName":"华宝中证稀有金属指数增强发起C","assetValue":"8321.46","holdProfit":"-678.54","holdProfitRate":"-7.54%","dailyProfit":"63.81","ptype":"fund"},
    {"fundCode":"011452","fundName":"华泰柏瑞质量成长C","assetValue":"8119.55","holdProfit":"2612.06","holdProfitRate":"47.43%","dailyProfit":"294.24","ptype":"fund"},
    {"fundCode":"021938","fundName":"华安半导体产业股票发起式C","assetValue":"7897.04","holdProfit":"12918.63","holdProfitRate":"--","dailyProfit":"389.03","ptype":"fund"},
    {"fundCode":"017174","fundName":"天弘国证绿色电力指数发起A","assetValue":"7799.87","holdProfit":"799.87","holdProfitRate":"11.43%","dailyProfit":"165.58","ptype":"fund"},
    {"fundCode":"014064","fundName":"银华农业产业股票发起式C","assetValue":"7605.16","holdProfit":"-894.84","holdProfitRate":"-10.53%","dailyProfit":"-16.39","ptype":"fund"},
    {"fundCode":"007950","fundName":"招商量化精选股票C","assetValue":"7344.47","holdProfit":"1054.51","holdProfitRate":"16.76%","dailyProfit":"4.12","ptype":"fund"},
    {"fundCode":"021964","fundName":"天弘储能电池指数C","assetValue":"5957.13","holdProfit":"457.13","holdProfitRate":"8.31%","dailyProfit":"-19.54","ptype":"fund"},
    {"fundCode":"013841","fundName":"银华集成电路混合C","assetValue":"5426.19","holdProfit":"416.19","holdProfitRate":"8.31%","dailyProfit":"343.08","ptype":"fund"},
    {"fundCode":"021511","fundName":"宏利半导体产业混合发起C","assetValue":"5307.69","holdProfit":"307.69","holdProfitRate":"6.15%","dailyProfit":"236.62","ptype":"fund"},
    {"fundCode":"005359","fundName":"东方阿尔法精选混合C","assetValue":"5005.57","holdProfit":"5.57","holdProfitRate":"0.11%","dailyProfit":"-79.00","ptype":"fund"},
    {"fundCode":"017811","fundName":"东方人工智能主题混合C","assetValue":"4359.29","holdProfit":"1966.78","holdProfitRate":"82.21%","dailyProfit":"273.64","ptype":"fund"},
    {"fundCode":"020357","fundName":"华夏半导体材料设备ETF联接C","assetValue":"2137.03","holdProfit":"127.03","holdProfitRate":"6.32%","dailyProfit":"107.13","ptype":"fund"},
    {"fundCode":"024481","fundName":"财通品质甄选混合C","assetValue":"2067.63","holdProfit":"67.63","holdProfitRate":"3.38%","dailyProfit":"67.63","ptype":"fund"},
    {"fundCode":"015839","fundName":"广发招利混合C","assetValue":"1991.53","holdProfit":"-8.47","holdProfitRate":"-0.42%","dailyProfit":"-20.12","ptype":"fund"},
    {"fundCode":"022482","fundName":"国泰中证新能源汽车ETF联接E","assetValue":"1862.12","holdProfit":"-137.88","holdProfitRate":"-6.89%","dailyProfit":"-32.48","ptype":"fund"},
    {"fundCode":"012920","fundName":"易方达全球成长精选混合(QDII)人民币A","assetValue":"1245.00","holdProfit":"395.00","holdProfitRate":"46.47%","dailyProfit":"37.27","ptype":"fund"},
    {"fundCode":"161226","fundName":"国投瑞银白银期货(LOF)A","assetValue":"217.04","holdProfit":"-82.96","holdProfitRate":"-27.65%","dailyProfit":"2.56","ptype":"fund"},
    {"fundCode":"017223","fundName":"富国中证电池主题ETF发起式联接C","assetValue":"104.18","holdProfit":"4.18","holdProfitRate":"4.18%","dailyProfit":"-2.55","ptype":"fund"},
    {"fundCode":"020465","fundName":"招商中证半导体产业ETF发起式联接C","assetValue":"14.67","holdProfit":"151.69","holdProfitRate":"--","dailyProfit":"0.87","ptype":"fund"},
    {"fundCode":"007491","fundName":"南方信息创新混合C","assetValue":"10.87","holdProfit":"0.87","holdProfitRate":"8.70%","dailyProfit":"0.63","ptype":"fund"},
    {"fundCode":"011172","fundName":"广发利鑫灵活配置混合C","assetValue":"10.44","holdProfit":"0.44","holdProfitRate":"4.40%","dailyProfit":"0.17","ptype":"fund"},
    {"fundCode":"007590","fundName":"华宝绿色领先股票A","assetValue":"8.89","holdProfit":"-1.11","holdProfitRate":"-11.10%","dailyProfit":"-0.26","ptype":"fund"},
    {"fundCode":"012708","fundName":"东方红中证红利低波动指数A","assetValue":"0.00","holdProfit":"0.00","holdProfitRate":"--","dailyProfit":"-60.69","ptype":"fund"},
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
