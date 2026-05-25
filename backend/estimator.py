"""实时估值计算引擎 — 天天基金 + 主题识别 + 双涨跌幅"""

from __future__ import annotations

from dataclasses import dataclass, field
from fetcher import (
    fetch_fund_estimate,
    fetch_fund_holdings,
    fetch_stock_quotes_with_industry,
    fetch_index_quotes,
    fetch_fund_theme,
    fetch_fund_nav_history,
    identify_sector_from_industries,
)
from style_engine import analyze_style, INDEX_MAP


@dataclass
class EstimateResult:
    fund_code: str
    fund_name: str
    # 净值
    nav_date: str = ""             # 最新净值日期
    nav: float = 0.0               # 最新公布净值(昨日)
    prev_nav_date: str = ""        # 前日净值日期
    prev_nav: float = 0.0          # 前日净值
    # 估值
    estimated_nav: float = 0.0     # 估算净值
    estimate_time: str = ""        # 估值时间
    # 双涨跌幅
    change_pct: float = 0.0        # 估算涨跌幅 (估算vs前日)
    real_change_pct: float = 0.0    # 真实涨跌幅 (昨日净值vs前日净值)
    # 估值拆解
    disclosed_contribution: float = 0
    undisclosed_contribution: float = 0
    # 持仓明细
    sector: str = ""               # 天天基金主题
    theme_name: str = ""           # 主题名称
    holdings_count: int = 0
    coverage: float = 0
    style_weights: dict = field(default_factory=dict)
    index_quotes: dict = field(default_factory=dict)
    details: list[dict] = field(default_factory=list)
    # 持仓收益
    asset_value: float = 0.0       # 持有金额
    hold_profit: float = 0.0       # 持仓收益
    hold_profit_rate: str = ""     # 持仓收益率
    daily_profit: float = 0.0      # 昨日收益
    # 加仓提醒
    alert: bool = False
    alert_reason: str = ""


async def calculate_estimate(
    fund_code: str,
    fund_name: str = "",
    alert_threshold: float = 1.5,
    nav: float = 0.0,
    asset_value: float = 0.0,
    hold_profit: float = 0.0,
    hold_profit_rate: str = "",
    daily_profit: float = 0.0,
) -> EstimateResult:
    """计算基金实时估值"""

    # 1. 天天基金官方估值（主数据源）
    gz = await fetch_fund_estimate(fund_code)

    nav_date = ""
    estimated_nav = 0.0
    change_pct = 0.0
    estimate_time = ""

    if gz:
        nav_date = gz["nav_date"]
        nav = gz["nav"]
        change_pct = gz["change_pct"]
        estimate_time = gz["estimate_time"]
        estimated_nav = gz["estimated_nav"]
        fund_name = gz["name"] or fund_name

    # 2. 真实涨跌幅（从净值历史计算）
    # 关键：真实涨跌 = lsjz[0]（最新实际净值）vs lsjz[1]（前一个实际净值）
    # 不用 fundgz.nav_date 对齐，因为 fundgz.nav_date 是估算基准日而非最新日
    # 例：周六看，fundgz.nav_date=周四，est=周五15:00；lsjz[0]=周五实际，lsjz[1]=周四实际
    real_change_pct = 0.0
    prev_nav = 0.0
    prev_nav_date = ""

    if nav > 0:
        nav_history = await fetch_fund_nav_history(fund_code, days=5)
        if nav_history and len(nav_history) >= 2:
            # 直接用 lsjz 最新两条：latest 和 previous
            latest_actual = float(nav_history[0].get("nav", 0) or 0)
            latest_date = nav_history[0].get("date", nav_history[0].get("nav_date", ""))
            prev_actual = float(nav_history[1].get("nav", 0) or 0)
            prev_date = nav_history[1].get("date", nav_history[1].get("nav_date", ""))

            if latest_actual > 0 and prev_actual > 0:
                # 用 lsjz 的最新实际净值作为 nav（替代 fundgz 的可能滞后的 nav）
                nav = latest_actual
                nav_date = latest_date
                prev_nav = prev_actual
                prev_nav_date = prev_date
                real_change_pct = round((latest_actual - prev_actual) / prev_actual * 100, 2)

        # 降级：从数据库读取上次存储的净值
        if prev_nav <= 0:
            try:
                from database import get_db
                db = await get_db()
                cursor = await db.execute(
                    "SELECT nav FROM nav_history WHERE fund_code=? ORDER BY date DESC LIMIT 2",
                    (fund_code,)
                )
                rows = await cursor.fetchall()
                await db.close()
                if len(rows) >= 2:
                    prev_nav = float(rows[1][0])
                    if prev_nav > 0:
                        real_change_pct = round((nav - prev_nav) / prev_nav * 100, 2)
            except Exception:
                pass

        # 存储最新净值到历史
        if nav > 0 and nav_date:
            try:
                from database import get_db
                db = await get_db()
                await db.execute(
                    "INSERT OR IGNORE INTO nav_history (fund_code, date, nav) VALUES (?, ?, ?)",
                    (fund_code, nav_date, nav)
                )
                await db.commit()
                await db.close()
            except Exception:
                pass

    # 3. 主题识别（天天基金API）
    theme_name = await fetch_fund_theme(fund_name, fund_code)
    sector = theme_name or "未知"

    # 4. 持仓 + 行业 + 风格
    holdings = await fetch_fund_holdings(fund_code)
    details = []
    coverage = 0.0
    style_weights = {}
    index_quotes = {}
    disclosed_contribution = 0.0
    undisclosed_contribution = 0.0

    if holdings:
        stock_codes = [h["stock_code"] for h in holdings]
        quotes = await fetch_stock_quotes_with_industry(stock_codes)

        total_weight = 0.0
        weighted_change_sum = 0.0

        for h in holdings:
            code = h["stock_code"]
            weight = h["weight"]
            quote = quotes.get(code, {})

            change = 0.0
            try:
                change = float(quote.get("change_pct", 0) or 0)
            except (ValueError, TypeError):
                pass

            industry = quote.get("industry", "")
            h["industry"] = industry
            total_weight += weight
            weighted_change_sum += weight * change

            details.append({
                "code": code,
                "name": h.get("stock_name", ""),
                "weight": weight,
                "change_pct": round(change, 2) if change else None,
                "industry": industry,
            })

        coverage = round(total_weight, 1)

        # 如果主题API没返回，降级使用行业统计
        if not theme_name:
            sector = identify_sector_from_industries(holdings)

        # 风格推断补全
        style_weights = analyze_style(fund_code, holdings)
        index_quotes = await fetch_index_quotes()

        disclosed_contribution = round(weighted_change_sum / 100, 4)

        uncovered_weight = max(0, 100 - coverage)
        if uncovered_weight > 0 and index_quotes:
            style_change = 0.0
            for idx_name, weight in style_weights.items():
                idx_data = index_quotes.get(idx_name, {})
                idx_change = 0.0
                try:
                    idx_change = float(idx_data.get("change_pct", 0) or 0)
                except (ValueError, TypeError):
                    pass
                style_change += weight * idx_change
            undisclosed_contribution = round(style_change * uncovered_weight / 100, 4)

        # 如果没有天天基金估值，自己算
        if not gz and nav > 0:
            total_change = disclosed_contribution + undisclosed_contribution
            estimated_nav = round(nav * (1 + total_change), 4)
            change_pct = round(total_change * 100, 4)

    # 5. 加仓提醒（基于估算涨跌幅）
    alert = False
    alert_reason = ""
    if change_pct < 0 and abs(change_pct) >= alert_threshold:
        alert = True
        alert_reason = f"跌幅 {abs(change_pct):.2f}% 超过阈值 {alert_threshold}%，关注加仓机会"

    return EstimateResult(
        fund_code=fund_code,
        fund_name=fund_name,
        nav_date=nav_date,
        nav=nav,
        prev_nav_date=prev_nav_date,
        prev_nav=prev_nav,
        estimated_nav=estimated_nav,
        estimate_time=estimate_time,
        change_pct=change_pct,
        real_change_pct=real_change_pct,
        disclosed_contribution=disclosed_contribution,
        undisclosed_contribution=undisclosed_contribution,
        sector=sector,
        theme_name=theme_name,
        holdings_count=len(holdings) if holdings else 0,
        coverage=coverage,
        style_weights=style_weights,
        index_quotes=index_quotes,
        details=details,
        asset_value=asset_value,
        hold_profit=hold_profit,
        hold_profit_rate=hold_profit_rate,
        daily_profit=daily_profit,
        alert=alert,
        alert_reason=alert_reason,
    )
