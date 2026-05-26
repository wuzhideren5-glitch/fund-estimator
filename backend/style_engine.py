"""风格识别引擎 — 根据持仓推断未披露仓位的指数分布"""

from __future__ import annotations

import json
import hashlib
import os

CACHE_DIR = os.path.join(os.path.dirname(__file__), "data")
CACHE_FILE = os.path.join(CACHE_DIR, "style_cache.json")


def _load_cache() -> dict:
    """加载风格缓存"""
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_cache(cache: dict):
    """保存风格缓存"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def _make_cache_key(fund_code: str, holdings: list[dict]) -> str:
    """生成缓存键：基金代码 + 持仓签名"""
    codes = sorted([h.get("stock_code", "") for h in holdings])
    weights = sorted([f"{h.get('weight', 0):.2f}" for h in holdings])
    signature = hashlib.md5(("|".join(codes) + "|" + "|".join(weights)).encode()).hexdigest()[:12]
    # 季度粗略用年月确定（简化：每季度第一天）
    return f"{fund_code}:{signature}"


# 行业 → 指数倾向映射（2026-05-26 拟合校准）
INDUSTRY_STYLE_MAP = {
    # 消费类 → 消费指数
    "白酒": {"中证消费": 70, "沪深300": 30},
    "白酒Ⅱ": {"中证消费": 70, "沪深300": 30},
    "食品饮料": {"中证消费": 60, "沪深300": 40},
    "食品": {"中证消费": 60, "沪深300": 40},
    "饮料制造": {"中证消费": 70, "沪深300": 30},
    "家电": {"中证消费": 50, "沪深300": 50},
    "汽车整车": {"中证消费": 40, "沪深300": 60},
    # 金融类 → 沪深300
    "银行": {"沪深300": 90, "中证500": 10},
    "保险": {"沪深300": 90, "中证500": 10},
    "证券": {"沪深300": 80, "中证500": 20},
    "房地产": {"沪深300": 70, "中证500": 30},
    # 医药类 → 均衡
    "医药": {"沪深300": 40, "创业板": 30, "中证500": 30},
    "医疗器械": {"沪深300": 30, "创业板": 40, "中证500": 30},
    "生物制品": {"沪深300": 30, "创业板": 40, "中证500": 30},
    "化学制药": {"沪深300": 40, "中证500": 40, "创业板": 20},
    "中药": {"沪深300": 50, "中证500": 30, "中证消费": 20},
    # 科技/TMT → 中证500为主（拟合发现中证500更贴合科技未披露仓位）
    "半导体": {"中证500": 50, "创业板": 30, "沪深300": 20},
    "通信设备": {"中证500": 50, "创业板": 30, "沪深300": 20},
    "计算机": {"中证500": 50, "创业板": 30, "沪深300": 20},
    "电子": {"中证500": 45, "创业板": 30, "沪深300": 25},
    "软件": {"中证500": 40, "创业板": 40, "沪深300": 20},
    "互联网": {"中证500": 30, "创业板": 40, "沪深300": 30},
    # 光刻胶/材料 → 中证500为主
    "光刻胶": {"中证500": 60, "创业板": 25, "沪深300": 15},
    "光学光电子": {"中证500": 50, "创业板": 35, "沪深300": 15},
    "元件": {"中证500": 45, "创业板": 35, "沪深300": 20},
    # 新能源 → 创业板+沪深300（保持，拟合支持）
    "新能源": {"创业板": 40, "沪深300": 40, "中证500": 20},
    "光伏": {"创业板": 40, "沪深300": 40, "中证500": 20},
    "电力": {"沪深300": 50, "中证500": 40, "创业板": 10},
    "电力设备": {"中证500": 50, "沪深300": 30, "创业板": 20},
    "电池": {"创业板": 45, "中证500": 35, "沪深300": 20},
    "电网设备": {"中证500": 50, "沪深300": 40, "创业板": 10},
    # 周期/制造业 → 中证500+沪深300
    "化工": {"中证500": 50, "沪深300": 50},
    "钢铁": {"沪深300": 50, "中证500": 50},
    "有色": {"沪深300": 50, "中证500": 50},
    "有色金属": {"中证500": 55, "沪深300": 45},
    "煤炭": {"沪深300": 60, "中证500": 40},
    "石油": {"沪深300": 70, "中证500": 30},
    "建筑": {"沪深300": 50, "中证500": 50},
    "建材": {"中证500": 50, "沪深300": 50},
    "机械": {"中证500": 55, "沪深300": 45},
    "机械设备": {"中证500": 55, "沪深300": 45},
    "军工": {"中证500": 45, "创业板": 30, "沪深300": 25},
    "交通运输": {"沪深300": 60, "中证500": 40},
    "公用事业": {"沪深300": 60, "中证500": 40},
    "农林牧渔": {"中证500": 50, "沪深300": 50},
    # 养殖/农业 → 中证500+中证消费
    "养殖业": {"中证500": 50, "中证消费": 30, "沪深300": 20},
    "农业": {"中证500": 45, "中证消费": 30, "沪深300": 25},
    # 传媒 → 创业板+中证500
    "传媒": {"创业板": 40, "中证500": 40, "沪深300": 20},
    # 机器人/自动化 → 中证500为主
    "自动化设备": {"中证500": 50, "创业板": 35, "沪深300": 15},
    "通用设备": {"中证500": 55, "沪深300": 45},
    # 绿色电力/新能源运营 → 沪深300为主
    "绿色电力": {"沪深300": 60, "中证500": 40},
    # AI/人工智能 → 创业板+中证500
    "人工智能": {"创业板": 40, "中证500": 40, "沪深300": 20},
}

DEFAULT_STYLE = {"中证500": 40, "沪深300": 35, "创业板": 20, "中证消费": 5}


def analyze_style(fund_code: str, holdings: list[dict]) -> dict[str, float]:
    """
    根据前十大持仓推断基金风格分布。
    返回：{沪深300: 0.35, 中证500: 0.25, 创业板: 0.30, 中证消费: 0.10}
    """

    # 查缓存
    cache = _load_cache()
    cache_key = _make_cache_key(fund_code, holdings)
    if cache_key in cache:
        entry = cache[cache_key]
        return entry.get("weights", {})

    # 按行业加权计算风格
    total_weight = sum(h.get("weight", 0) for h in holdings)
    if total_weight == 0:
        return dict(DEFAULT_STYLE)

    style_scores: dict[str, float] = {}

    for h in holdings:
        industry = h.get("industry", "")
        weight = h.get("weight", 0)
        stock_code = h.get("stock_code", "")

        # 1. 先按行业查
        style_map = None
        for key, sm in INDUSTRY_STYLE_MAP.items():
            if key in industry:
                style_map = sm
                break

        # 2. 行业未匹配 → 按代码前缀
        if not style_map:
            if stock_code.startswith("688"):
                style_map = {"创业板": 40, "中证500": 40, "沪深300": 20}
            elif stock_code.startswith("300"):
                style_map = {"创业板": 60, "中证500": 30, "沪深300": 10}
            elif len(stock_code) == 5:  # 港股
                style_map = {"沪深300": 50, "中证500": 30, "创业板": 20}
            else:
                style_map = dict(DEFAULT_STYLE)

        # 加权累加
        for idx, score in style_map.items():
            style_scores[idx] = style_scores.get(idx, 0) + weight * score

    # 归一化为 100%
    total_score = sum(style_scores.values())
    if total_score > 0:
        weights = {k: round(v / total_score, 4) for k, v in style_scores.items()}
    else:
        weights = dict(DEFAULT_STYLE)

    # 写入缓存
    cache[cache_key] = {"weights": weights, "holdings_count": len(holdings)}
    _save_cache(cache)

    return weights


# 指数代码 → 东方财富 secid
INDEX_MAP = {
    "沪深300": "1.000300",
    "中证500": "1.000905",
    "创业板": "0.399006",
    "中证消费": "1.000932",
}
