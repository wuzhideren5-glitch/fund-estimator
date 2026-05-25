#!/usr/bin/env python3
"""
持仓数据同步脚本
用法: python3 sync_from_json.py <holdings.json>

将 ttskill 导出的 JSON 转换为 import_holdings.py 中的 HOLDINGS_DATA。
支持两种输入格式：
  A. ttskill signed-invoke 原始输出（自动提取 data.holdings）
  B. 直接数组格式 [{fundCode, fundName, assetValue, ...}]
"""
from __future__ import annotations

import json
import re
import sys
import os
from pathlib import Path


def normalize_holding(item: dict) -> dict:
    """将不同格式的持仓记录统一为标准格式"""
    return {
        "fundCode": str(item.get("fundCode", item.get("fund_code", item.get("code", "")))),
        "fundName": str(item.get("fundName", item.get("fund_name", item.get("name", "")))),
        "assetValue": str(item.get("assetValue", item.get("asset_value", item.get("assetValue", 0)))),
        "holdProfit": str(item.get("holdProfit", item.get("hold_profit", item.get("holdProfit", 0)))).replace(",", ""),
        "holdProfitRate": str(item.get("holdProfitRate", item.get("hold_profit_rate", item.get("holdProfitRate", "--")))),
        "dailyProfit": str(item.get("dailyProfit", item.get("daily_profit", item.get("dailyProfit", 0)))),
        "ptype": str(item.get("ptype", "fund")),
    }


def extract_holdings(data: dict | list) -> list[dict]:
    """从 ttskill 原始输出中提取持仓数组"""
    if isinstance(data, list):
        return data

    # 尝试多种可能的嵌套路径
    for path in [
        ["data", "holdings"],
        ["data", "items"],
        ["data", "funds"],
        ["data", "result"],
        ["holdings"],
        ["result"],
    ]:
        d = data
        for key in path:
            if isinstance(d, dict) and key in d:
                d = d[key]
            else:
                break
        else:
            if isinstance(d, list) and len(d) > 0:
                return d

    # 都没找到，直接返回 dict 的 values 里第一个 list
    for v in data.values():
        if isinstance(v, list) and len(v) > 0:
            return v

    raise ValueError("无法从输入中提取持仓数组。请检查 JSON 格式。")


def update_import_holdings(holdings: list[dict], filepath: str) -> str:
    """更新 import_holdings.py 中的 HOLDINGS_DATA"""
    with open(filepath, "r") as f:
        content = f.read()

    # 生成新的 HOLDINGS_DATA 代码块
    lines = ["HOLDINGS_DATA = ["]
    for h in holdings:
        nh = normalize_holding(h)
        lines.append(
            '    {{"fundCode":"{}","fundName":"{}","assetValue":"{}","holdProfit":"{}","holdProfitRate":"{}","dailyProfit":"{}","ptype":"{}"}},'.format(
                nh["fundCode"], nh["fundName"], nh["assetValue"],
                nh["holdProfit"], nh["holdProfitRate"], nh["dailyProfit"], nh["ptype"],
            )
        )
    lines.append("]")

    new_block = "\n".join(lines)

    # 替换 HOLDINGS_DATA 块
    pattern = r"HOLDINGS_DATA = \[.*?\]"
    new_content = re.sub(pattern, new_block, content, count=1, flags=re.DOTALL)

    if new_content == content:
        raise ValueError("未找到 HOLDINGS_DATA 定义，import_holdings.py 格式可能已变更。")

    with open(filepath, "w") as f:
        f.write(new_content)

    return f"✅ 已更新 {len(holdings)} 条持仓记录"


def main():
    if len(sys.argv) < 2:
        print("用法: python3 sync_from_json.py <holdings.json>")
        sys.exit(1)

    json_path = sys.argv[1]
    if not os.path.exists(json_path):
        print(f"❌ 文件不存在: {json_path}")
        sys.exit(1)

    with open(json_path, "r") as f:
        data = json.load(f)

    holdings = extract_holdings(data)
    normalized = [normalize_holding(h) for h in holdings]

    # 计算汇总
    total_asset = sum(float(h["assetValue"]) for h in normalized)
    total_profit = sum(float(h["holdProfit"]) for h in normalized)
    total_daily = sum(float(h["dailyProfit"]) for h in normalized)
    rate = total_profit / (total_asset - total_profit) * 100 if total_asset != total_profit else 0

    print(f"📦 持仓: {len(normalized)} 只")
    print(f"💰 总资产: ¥{total_asset:,.2f}")
    print(f"📈 累计收益: ¥{total_profit:+,.2f} ({rate:+.2f}%)")
    print(f"📊 今日收益: ¥{total_daily:+,.2f}")

    # 更新 import_holdings.py
    target = Path(__file__).parent.parent / "backend" / "import_holdings.py"
    msg = update_import_holdings(normalized, str(target))
    print(f"\n{msg} → {target}")


if __name__ == "__main__":
    main()
