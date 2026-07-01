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
    # 2026-07-01 通过 ttskill invoke ACCOUNT_HOLDING 拉取
    {"fundCode": "hqb", "fundName": "活期宝", "assetValue": "47826.40", "holdProfit": "--", "holdProfitRate": "--", "constantProfit": "--", "constantProfitRate": "--", "dailyProfit": "--", "ptype": "hqb"},
    {"fundCode": "021528", "fundName": "财通成长优选混合C", "assetValue": "40556.88", "holdProfit": "11909.95", "holdProfitRate": "41.57%", "constantProfit": "11909.96", "constantProfitRate": "41.58%", "dailyProfit": "747.37", "ptype": "fund"},
    {"fundCode": "024203", "fundName": "永赢制造升级智选混合发起C", "assetValue": "35233.62", "holdProfit": "7303.95", "holdProfitRate": "26.15%", "constantProfit": "6592.15", "constantProfitRate": "23.02%", "dailyProfit": "908.28", "ptype": "fund"},
    {"fundCode": "002112", "fundName": "德邦鑫星价值灵活配置混合C", "assetValue": "30437.16", "holdProfit": "21866.12", "holdProfitRate": "255.12%", "constantProfit": "6764.99", "constantProfitRate": "28.58%", "dailyProfit": "1583.38", "ptype": "fund"},
    {"fundCode": "016531", "fundName": "鹏华碳中和主题混合C", "assetValue": "28835.97", "holdProfit": "-5240.05", "holdProfitRate": "-15.38%", "constantProfit": "-5240.05", "constantProfitRate": "-15.38%", "dailyProfit": "1537.83", "ptype": "fund"},
    {"fundCode": "025857", "fundName": "华夏中证电网设备主题ETF发起式联接C", "assetValue": "24150.74", "holdProfit": "2230.21", "holdProfitRate": "10.17%", "constantProfit": "1625.25", "constantProfitRate": "7.22%", "dailyProfit": "313.71", "ptype": "fund"},
    {"fundCode": "002656", "fundName": "南方创业板ETF联接A", "assetValue": "23899.71", "holdProfit": "7813.43", "holdProfitRate": "48.57%", "constantProfit": "6899.71", "constantProfitRate": "40.59%", "dailyProfit": "661.16", "ptype": "fund"},
    {"fundCode": "017574", "fundName": "华夏中证机床ETF发起式联接C", "assetValue": "17793.81", "holdProfit": "1737.17", "holdProfitRate": "10.82%", "constantProfit": "1737.17", "constantProfitRate": "10.82%", "dailyProfit": "765.67", "ptype": "fund"},
    {"fundCode": "024424", "fundName": "东方阿尔法科技优选混合发起C", "assetValue": "17484.07", "holdProfit": "5474.07", "holdProfitRate": "45.58%", "constantProfit": "5474.07", "constantProfitRate": "45.58%", "dailyProfit": "623.56", "ptype": "fund"},
    {"fundCode": "020628", "fundName": "汇添富科创芯片ETF联接A", "assetValue": "17035.51", "holdProfit": "15156.13", "holdProfitRate": "806.44%", "constantProfit": "8485.51", "constantProfitRate": "99.25%", "dailyProfit": "663.32", "ptype": "fund"},
    {"fundCode": "290008", "fundName": "泰信发展主题混合", "assetValue": "16606.36", "holdProfit": "3364.75", "holdProfitRate": "25.41%", "constantProfit": "-1758.25", "constantProfitRate": "-9.57%", "dailyProfit": "474.47", "ptype": "fund"},
    {"fundCode": "022365", "fundName": "永赢科技智选混合发起C", "assetValue": "16313.21", "holdProfit": "2067.43", "holdProfitRate": "14.51%", "constantProfit": "2067.43", "constantProfitRate": "14.51%", "dailyProfit": "617.72", "ptype": "fund"},
    {"fundCode": "519771", "fundName": "交银优择回报灵活配置混合C", "assetValue": "14810.52", "holdProfit": "8444.79", "holdProfitRate": "132.66%", "constantProfit": "6810.54", "constantProfitRate": "85.13%", "dailyProfit": "513.72", "ptype": "fund"},
    {"fundCode": "011452", "fundName": "华泰柏瑞质量成长C", "assetValue": "14326.53", "holdProfit": "4365.40", "holdProfitRate": "43.82%", "constantProfit": "2707.00", "constantProfitRate": "23.30%", "dailyProfit": "512.62", "ptype": "fund"},
    {"fundCode": "011613", "fundName": "华夏科创50ETF联接C", "assetValue": "14082.58", "holdProfit": "3444.16", "holdProfitRate": "32.37%", "constantProfit": "3339.27", "constantProfitRate": "31.08%", "dailyProfit": "515.45", "ptype": "fund"},
    {"fundCode": "013841", "fundName": "银华集成电路混合C", "assetValue": "13565.75", "holdProfit": "4555.75", "holdProfitRate": "50.56%", "constantProfit": "4555.75", "constantProfitRate": "50.56%", "dailyProfit": "572.66", "ptype": "fund"},
    {"fundCode": "019173", "fundName": "摩根纳斯达克100指数(QDII)人民币C", "assetValue": "12880.10", "holdProfit": "2360.46", "holdProfitRate": "22.48%", "constantProfit": "2265.13", "constantProfitRate": "21.38%", "dailyProfit": "259.64", "ptype": "fund"},
    {"fundCode": "007491", "fundName": "南方信息创新混合C", "assetValue": "12823.93", "holdProfit": "3813.93", "holdProfitRate": "42.33%", "constantProfit": "3813.93", "constantProfitRate": "42.33%", "dailyProfit": "384.35", "ptype": "fund"},
    {"fundCode": "025209", "fundName": "永赢先锋半导体智选混合发起C", "assetValue": "12203.00", "holdProfit": "4203.00", "holdProfitRate": "52.54%", "constantProfit": "4203.00", "constantProfitRate": "52.54%", "dailyProfit": "151.54", "ptype": "fund"},
    {"fundCode": "013943", "fundName": "华宝中证稀有金属指数增强发起C", "assetValue": "11733.47", "holdProfit": "-266.53", "holdProfitRate": "-2.22%", "constantProfit": "-266.53", "constantProfitRate": "-2.22%", "dailyProfit": "149.84", "ptype": "fund"},
    {"fundCode": "010786", "fundName": "博时创业板指数C", "assetValue": "10867.45", "holdProfit": "8582.66", "holdProfitRate": "375.64%", "constantProfit": "2429.03", "constantProfitRate": "28.79%", "dailyProfit": "297.08", "ptype": "fund"},
    {"fundCode": "017811", "fundName": "东方人工智能主题混合C", "assetValue": "10574.24", "holdProfit": "4793.17", "holdProfitRate": "82.91%", "constantProfit": "3701.75", "constantProfitRate": "53.86%", "dailyProfit": "435.03", "ptype": "fund"},
    {"fundCode": "024481", "fundName": "财通品质甄选混合C", "assetValue": "9709.02", "holdProfit": "2009.02", "holdProfitRate": "26.09%", "constantProfit": "2009.03", "constantProfitRate": "26.09%", "dailyProfit": "186.89", "ptype": "fund"},
    {"fundCode": "019924", "fundName": "华泰柏瑞中证2000指数增强C", "assetValue": "8492.04", "holdProfit": "-388.12", "holdProfitRate": "-4.37%", "constantProfit": "-388.12", "constantProfitRate": "-4.37%", "dailyProfit": "244.96", "ptype": "fund"},
    {"fundCode": "021511", "fundName": "宏利半导体产业混合发起C", "assetValue": "6689.20", "holdProfit": "1689.20", "holdProfitRate": "33.78%", "constantProfit": "1689.20", "constantProfitRate": "33.78%", "dailyProfit": "253.14", "ptype": "fund"},
    {"fundCode": "008888", "fundName": "华夏国证半导体芯片ETF联接C", "assetValue": "5877.16", "holdProfit": "431.61", "holdProfitRate": "7.93%", "constantProfit": "1383.11", "constantProfitRate": "30.78%", "dailyProfit": "147.63", "ptype": "fund"},
    {"fundCode": "023765", "fundName": "华夏中证5G通信主题ETF联接D", "assetValue": "5253.31", "holdProfit": "253.31", "holdProfitRate": "5.07%", "constantProfit": "253.31", "constantProfitRate": "5.07%", "dailyProfit": "209.99", "ptype": "fund"},
    {"fundCode": "020357", "fundName": "华夏半导体材料设备ETF联接C", "assetValue": "5084.95", "holdProfit": "1574.95", "holdProfitRate": "44.87%", "constantProfit": "1574.95", "constantProfitRate": "44.87%", "dailyProfit": "134.33", "ptype": "fund"},
    {"fundCode": "011120", "fundName": "富国创新科技混合C", "assetValue": "2057.73", "holdProfit": "57.73", "holdProfitRate": "2.89%", "constantProfit": "57.73", "constantProfitRate": "2.89%", "dailyProfit": "87.74", "ptype": "fund"},
    {"fundCode": "012920", "fundName": "易方达全球成长精选混合(QDII)人民币A", "assetValue": "1520.99", "holdProfit": "660.99", "holdProfitRate": "77.76%", "constantProfit": "660.99", "constantProfitRate": "77.76%", "dailyProfit": "14.56", "ptype": "fund"},
    {"fundCode": "020465", "fundName": "招商中证半导体产业ETF发起式联接C", "assetValue": "1373.36", "holdProfit": "510.38", "holdProfitRate": "59.14%", "constantProfit": "362.54", "constantProfitRate": "35.87%", "dailyProfit": "45.80", "ptype": "fund"},
    {"fundCode": "025793", "fundName": "东方阿尔法科技甄选混合发起C", "assetValue": "1107.58", "holdProfit": "97.58", "holdProfitRate": "9.66%", "constantProfit": "97.58", "constantProfitRate": "9.66%", "dailyProfit": "1.69", "ptype": "fund"},
    {"fundCode": "008989", "fundName": "大成科技创新混合C", "assetValue": "1058.03", "holdProfit": "48.03", "holdProfitRate": "4.76%", "constantProfit": "48.03", "constantProfitRate": "4.76%", "dailyProfit": "25.88", "ptype": "fund"},
    {"fundCode": "161226", "fundName": "国投瑞银白银期货(LOF)A", "assetValue": "162.59", "holdProfit": "-137.41", "holdProfitRate": "-45.80%", "constantProfit": "-137.41", "constantProfitRate": "-45.80%", "dailyProfit": "-2.09", "ptype": "fund"},
    {"fundCode": "020477", "fundName": "泰康半导体量化选股股票发起式C", "assetValue": "10.00", "holdProfit": "0.00", "holdProfitRate": "--", "constantProfit": "0.00", "constantProfitRate": "--", "dailyProfit": "0.00", "ptype": "fund"},
    {"fundCode": "001665", "fundName": "平安鑫安混合C", "assetValue": "0.00", "holdProfit": "0.00", "holdProfitRate": "--", "constantProfit": "0.00", "constantProfitRate": "--", "dailyProfit": "-1.03", "ptype": "fund"},
]
