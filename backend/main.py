"""基金实时估值看板 — FastAPI 后端"""

from __future__ import annotations

import os
import json
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import httpx

from database import init_db, get_db
from fetcher import fetch_fund_info, fetch_fund_holdings, identify_sector_from_industries, fetch_fund_theme
from estimator import calculate_estimate
from news_classifier import refresh_news, get_news_by_theme, get_all_themes

# ── 简单内存缓存 ──
_cache: dict = {}
ESTIMATE_TTL = 60       # 估值缓存 60 秒（天天基金本身 ~30s 刷新一次）
FUNDS_TTL = 300         # 基金列表缓存 5 分钟
NEWS_TTL = 600          # 新闻缓存 10 分钟

def _get_cache(key: str, ttl: int):
    """返回缓存值或 None"""
    if key in _cache:
        data, ts = _cache[key]
        if time.time() - ts < ttl:
            return data
        del _cache[key]
    return None

def _set_cache(key: str, data):
    _cache[key] = (data, time.time())


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(title="基金估值看板", lifespan=lifespan)

static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
async def index():
    return FileResponse(os.path.join(static_dir, "index.html"))


# ============================================================
# API: 基金池管理
# ============================================================


class AddFundRequest(BaseModel):
    code: str
    alert_threshold: float = 1.5
    asset_value: float = 0.0
    hold_profit: float = 0.0
    hold_profit_rate: str = ""
    daily_profit: float = 0.0


@app.post("/api/funds")
async def add_fund(req: AddFundRequest):
    """添加基金到监控池"""
    code = req.code.strip()

    info = await fetch_fund_info(code)
    if not info or not info.get("name"):
        raise HTTPException(status_code=404, detail=f"未找到基金 {code}")

    holdings = await fetch_fund_holdings(code)
    sector = identify_sector_from_industries(holdings) if holdings else "未知"
    holdings_json = json.dumps(holdings, ensure_ascii=False) if holdings else "[]"

    # 主题识别
    theme_name = await fetch_fund_theme(info["name"], code)
    if theme_name:
        sector = theme_name

    db = await get_db()
    try:
        cursor = await db.execute("SELECT id FROM funds WHERE code = ?", (code,))
        existing = await cursor.fetchone()

        if existing:
            await db.execute(
                """UPDATE funds SET name=?, fund_type=?, nav=?, nav_date=?,
                   sector=?, theme_name=?, holdings_json=?,
                   asset_value=?, hold_profit=?, hold_profit_rate=?, daily_profit=?,
                   alert_threshold=?, updated_at=CURRENT_TIMESTAMP
                   WHERE code=?""",
                (
                    info["name"], info.get("fund_type", ""),
                    info["nav"], info["nav_date"],
                    sector, theme_name, holdings_json,
                    req.asset_value, req.hold_profit, req.hold_profit_rate,
                    req.daily_profit, req.alert_threshold, code,
                ),
            )
        else:
            await db.execute(
                """INSERT INTO funds (code, name, fund_type, nav, nav_date,
                   sector, theme_name, holdings_json,
                   asset_value, hold_profit, hold_profit_rate, daily_profit,
                   alert_threshold)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    code, info["name"], info.get("fund_type", ""),
                    info["nav"], info["nav_date"],
                    sector, theme_name, holdings_json,
                    req.asset_value, req.hold_profit, req.hold_profit_rate,
                    req.daily_profit, req.alert_threshold,
                ),
            )
        await db.commit()

        _cache.pop("funds_list", None)  # 清除缓存
        _cache.pop("estimate_all", None)

        return {
            "success": True,
            "fund": {
                "code": code,
                "name": info["name"],
                "nav": info["nav"],
                "nav_date": info["nav_date"],
                "sector": sector,
                "holdings_count": len(holdings) if holdings else 0,
            },
        }
    finally:
        await db.close()


@app.get("/api/funds")
async def list_funds():
    """获取所有已添加的基金"""
    cached = _get_cache("funds_list", FUNDS_TTL)
    if cached is not None:
        return cached

    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM funds ORDER BY asset_value DESC")
        rows = await cursor.fetchall()
        result = [
            {
                "id": row["id"],
                "code": row["code"],
                "name": row["name"],
                "fund_type": row["fund_type"],
                "nav": row["nav"],
                "nav_date": row["nav_date"],
                "sector": row["sector"],
                "theme_name": row["theme_name"] if "theme_name" in row.keys() else "",
                "asset_value": row["asset_value"] if "asset_value" in row.keys() else 0,
                "hold_profit": row["hold_profit"] if "hold_profit" in row.keys() else 0,
                "hold_profit_rate": row["hold_profit_rate"] if "hold_profit_rate" in row.keys() else "",
                "daily_profit": row["daily_profit"] if "daily_profit" in row.keys() else 0,
                "holdings_count": len(json.loads(row["holdings_json"])) if row["holdings_json"] else 0,
                "alert_threshold": row["alert_threshold"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]
    finally:
        await db.close()

    _set_cache("funds_list", result)
    return result


@app.delete("/api/funds/{code}")
async def remove_fund(code: str):
    db = await get_db()
    try:
        await db.execute("DELETE FROM funds WHERE code = ?", (code,))
        await db.commit()
        _cache.pop("funds_list", None)  # 清除缓存
        return {"success": True}
    finally:
        await db.close()


@app.put("/api/funds/{code}")
async def update_fund_threshold(code: str, threshold: float = Query(1.5)):
    db = await get_db()
    try:
        await db.execute(
            "UPDATE funds SET alert_threshold = ?, updated_at = CURRENT_TIMESTAMP WHERE code = ?",
            (threshold, code),
        )
        await db.commit()
        _cache.pop("funds_list", None)  # 清除缓存
        return {"success": True, "code": code, "threshold": threshold}
    finally:
        await db.close()


# ============================================================
# API: 实时估值
# ============================================================


def _model_row_to_dict(row) -> dict:
    """安全地将数据库行转为字典"""
    d = dict(row)
    for k in ("asset_value", "hold_profit", "daily_profit", "alert_threshold"):
        if k not in d:
            d[k] = 0.0
    for k in ("hold_profit_rate", "theme_name"):
        if k not in d:
            d[k] = ""
    return d


@app.get("/api/estimate/all")
async def get_all_estimates():
    """获取所有基金的实时估值（并发）"""
    cached = _get_cache("estimate_all", ESTIMATE_TTL)
    if cached is not None:
        return cached

    import asyncio as _asyncio

    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM funds ORDER BY asset_value DESC")
        rows = await cursor.fetchall()
    finally:
        await db.close()

    async def _estimate_one(row_dict):
        try:
            result = await calculate_estimate(
                fund_code=row_dict["code"],
                fund_name=row_dict["name"],
                alert_threshold=row_dict["alert_threshold"],
                nav=row_dict.get("nav", 0),
                asset_value=row_dict["asset_value"],
                hold_profit=row_dict["hold_profit"],
                hold_profit_rate=row_dict["hold_profit_rate"],
                daily_profit=row_dict["daily_profit"],
            )
            return {
                "code": result.fund_code,
                "name": result.fund_name,
                "nav": result.nav,
                "nav_date": result.nav_date,
                "prev_nav": result.prev_nav,
                "prev_nav_date": result.prev_nav_date,
                "estimated_nav": result.estimated_nav,
                "estimate_time": result.estimate_time,
                "change_pct": result.change_pct,
                "real_change_pct": result.real_change_pct,
                "disclosed_contribution": result.disclosed_contribution,
                "undisclosed_contribution": result.undisclosed_contribution,
                "coverage": result.coverage,
                "holdings_count": result.holdings_count,
                "sector": result.sector,
                "theme_name": result.theme_name,
                "style_weights": result.style_weights,
                "index_quotes": result.index_quotes,
                "asset_value": result.asset_value,
                "hold_profit": result.hold_profit,
                "hold_profit_rate": result.hold_profit_rate,
                "daily_profit": result.daily_profit,
                "alert": result.alert,
                "alert_reason": result.alert_reason,
                "details": result.details,
            }
        except Exception as e:
            return {"code": row_dict["code"], "name": row_dict["name"], "error": str(e)}

    rows_dict = [_model_row_to_dict(row) for row in rows]
    _semaphore = _asyncio.Semaphore(10)

    async def _estimate_with_limit(d):
        async with _semaphore:
            return await _estimate_one(d)

    results = await _asyncio.gather(*[_estimate_with_limit(d) for d in rows_dict])

    # 将实时净值写回数据库
    try:
        import asyncio
        async def _sync_nav():
            sync_db = await get_db()
            try:
                for r in results:
                    if "error" in r:
                        continue
                    nav_val = r.get("nav", 0)
                    nav_date_val = r.get("nav_date", "")
                    if nav_val and nav_date_val:
                        await sync_db.execute(
                            "UPDATE funds SET nav=?, nav_date=?, updated_at=CURRENT_TIMESTAMP WHERE code=?",
                            (nav_val, nav_date_val, r["code"]),
                        )
                await sync_db.commit()
            finally:
                await sync_db.close()
        asyncio.create_task(_sync_nav())
    except Exception:
        pass

    result = {
        "funds": results,
        "count": len(results),
        "timestamp": datetime.now().isoformat(),
    }
    _set_cache("estimate_all", result)
    return result


@app.get("/api/estimate/{code}")
async def get_estimate(code: str):
    """获取单只基金的实时估值"""
    cache_key = f"estimate_{code}"
    cached = _get_cache(cache_key, ESTIMATE_TTL)
    if cached is not None:
        return cached

    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM funds WHERE code = ?", (code,))
        row = await cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="基金未在监控池中")

        d = _model_row_to_dict(row)
        result = await calculate_estimate(
            fund_code=d["code"],
            fund_name=d["name"],
            alert_threshold=d["alert_threshold"],
            nav=d.get("nav", 0),
            asset_value=d["asset_value"],
            hold_profit=d["hold_profit"],
            hold_profit_rate=d["hold_profit_rate"],
            daily_profit=d["daily_profit"],
        )

        resp = {
            "code": result.fund_code,
            "name": result.fund_name,
            "nav": result.nav,
            "nav_date": result.nav_date,
            "prev_nav": result.prev_nav,
            "prev_nav_date": result.prev_nav_date,
            "estimated_nav": result.estimated_nav,
            "estimate_time": result.estimate_time,
            "change_pct": result.change_pct,
            "real_change_pct": result.real_change_pct,
            "disclosed_contribution": result.disclosed_contribution,
            "undisclosed_contribution": result.undisclosed_contribution,
            "coverage": result.coverage,
            "holdings_count": result.holdings_count,
            "sector": result.sector,
            "theme_name": result.theme_name,
            "style_weights": result.style_weights,
            "index_quotes": result.index_quotes,
            "asset_value": result.asset_value,
            "hold_profit": result.hold_profit,
            "hold_profit_rate": result.hold_profit_rate,
            "daily_profit": result.daily_profit,
            "alert": result.alert,
            "alert_reason": result.alert_reason,
            "details": result.details,
            "timestamp": datetime.now().isoformat(),
        }
        _set_cache(cache_key, resp)
        return resp
    finally:
        await db.close()


# ============================================================
# API: 搜索
# ============================================================


@app.get("/api/search")
async def search_fund(q: str = Query(..., min_length=1)):
    info = await fetch_fund_info(q.strip())
    if not info or not info.get("name"):
        raise HTTPException(status_code=404, detail=f"未找到基金 {q}")
    return info


@app.get("/api/holdings/{code}")
async def get_holdings(code: str):
    holdings = await fetch_fund_holdings(code)
    if holdings is None:
        raise HTTPException(status_code=404, detail="无法获取持仓数据")
    sector = identify_sector_from_industries(holdings)
    return {"code": code, "holdings": holdings, "sector": sector}


# ============================================================
# API: 板块新闻搜索
# ============================================================

import xml.etree.ElementTree as ET
from urllib.parse import quote
from email.utils import parsedate_to_datetime
from datetime import datetime, timedelta, timezone

GOOGLE_NEWS_PROXY = "http://127.0.0.1:9674"
NEWS_MAX_HOURS = 48  # 只保留最近 48 小时新闻
NEWS_MIN_RESULTS = 3  # 不足时放宽到 7 天

# 常用主题的英文字段，用于全球新闻搜索
_THEME_EN_MAP = {
    "锂矿": "lithium",
    "半导体": "semiconductor chip",
    "5G": "5G",
    "人工智能": "AI artificial intelligence",
    "新能源": "clean energy renewable",
    "电池": "battery EV",
    "光伏": "solar photovoltaic",
    "人形机器人": "humanoid robot",
    "光刻胶": "photoresist",
    "养殖业": "aquaculture livestock",
    "通信设备": "telecom equipment",
    "机械设备": "machinery equipment",
    "有色金属": "nonferrous metals",
    "绿色电力": "green power renewable",
    "科创50": "STAR 50 tech",
    "创业板": "ChiNext",
    "纳斯达克": "Nasdaq",
    "量化": "quantitative",
    "消费": "consumer",
    "医药": "pharmaceutical healthcare",
    "军工": "defense military",
    "汽车": "automotive EV",
    "白酒": "baijiu liquor",
}


@app.get("/api/news")
async def get_theme_news(keyword: str = Query(..., min_length=1)):
    """搜索板块最新投资资讯 — 只保留 48 小时内新闻（按时间倒序，取前 10 条）"""
    url = (
        f"https://news.google.com/rss/search?"
        f"q={quote(keyword)}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
    )

    try:
        async with httpx.AsyncClient(proxy=GOOGLE_NEWS_PROXY, timeout=10) as client:
            resp = await client.get(url)
            xml_text = resp.text
    except Exception as e:
        return {"news": [], "keyword": keyword, "error": str(e)}

    # 解析所有条目
    all_items = []
    try:
        root = ET.fromstring(xml_text)
        for item in root.findall(".//item"):
            title = item.findtext("title", "")
            link = item.findtext("link", "")
            pub_date = item.findtext("pubDate", "")
            source = item.findtext("source", "")

            dt = None
            try:
                dt = parsedate_to_datetime(pub_date)
            except Exception:
                pass

            all_items.append({
                "title": title,
                "url": link,
                "time": pub_date,
                "source": source,
                "intro": "",
                "_dt": dt,
            })
    except ET.ParseError as e:
        return {"news": [], "keyword": keyword, "error": f"RSS 解析失败: {e}"}

    # 按时间降序排列
    all_items.sort(
        key=lambda x: x["_dt"].timestamp() if x["_dt"] else 0,
        reverse=True,
    )

    # 过滤：48 小时内
    now = datetime.now(timezone.utc)
    cutoff_48h = now - timedelta(hours=NEWS_MAX_HOURS)
    recent = []
    for i in all_items:
        dt = i["_dt"]
        if dt is None:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if dt >= cutoff_48h:
            recent.append(i)

    # 48h 无结果 → 延长到 72h
    if not recent:
        cutoff_72h = now - timedelta(hours=72)
        for i in all_items:
            dt = i["_dt"]
            if dt is None:
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt >= cutoff_72h:
                recent.append(i)

    # 清理内部字段，只返回前 10 条
    result = recent[:10]
    for it in result:
        del it["_dt"]

    return {"news": result, "keyword": keyword, "count": len(result)}


# ============================================================
# API: AI 分类新闻（Tushare + DeepSeek）
# ============================================================

@app.post("/api/news/refresh")
async def refresh_classified_news():
    """触发新闻刷新：Tushare 获取 → DeepSeek 分类 → 入库"""
    db = await get_db()
    try:
        result = await refresh_news(db)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await db.close()


@app.get("/api/news/classified")
async def get_classified_news(theme: str = Query(..., min_length=1), limit: int = Query(20, ge=1, le=100)):
    """按主题获取 AI 分类后的新闻"""
    db = await get_db()
    try:
        news = await get_news_by_theme(db, theme, limit)
        return {"theme": theme, "count": len(news), "news": news}
    finally:
        await db.close()


@app.get("/api/news/themes")
async def get_news_themes():
    """获取所有主题及新闻数量"""
    db = await get_db()
    try:
        themes = await get_all_themes(db)
        return {"themes": themes, "count": len(themes)}
    finally:
        await db.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8766)
