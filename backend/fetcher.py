"""数据抓取服务 — 天天基金估值 + 东方财富行情 + 行业识别"""

from __future__ import annotations

import re
import json
import aiohttp
from datetime import datetime
from collections import Counter


import time as _time

# ── 共享连接池 ──
_shared_session = None

async def _get_session():
    global _shared_session
    if _shared_session is None or _shared_session.closed:
        import aiohttp
        _shared_session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=20, force_close=False),
            timeout=aiohttp.ClientTimeout(total=10),
        )
    return _shared_session

# ── 指数行情缓存 ──
_idx_cache = None
_IDX_CACHE_TTL = 30

# ── 股票行情缓存（去重，多基金共享）──
_stock_cache: dict[str, tuple[float, dict]] = {}
_STOCK_CACHE_TTL = 30

# ============================================================
# 天天基金实时估值（主估值源）
# ============================================================


async def fetch_fund_estimate(code: str) -> dict | None:
    """从天天基金获取实时估值（所有类型基金通用）
    
    数据源：fundgz.1234567.com.cn（公开免费，无需 API Key）
    期货/LOF 型降级到东方财富 lsjz 接口
    """
    url = f"http://fundgz.1234567.com.cn/js/{code}.js?rt={datetime.now().timestamp()}"
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "http://fund.eastmoney.com/"}

    session = await _get_session()
    try:
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                return None
            data = await resp.text()

        m = re.search(r"jsonpgz\((.*)\)", data)
        if not m:
            # fundgz 不支持（如期货型基金）→ 降级到 lsjz
            return await _fetch_fund_estimate_lsjz(code)

        gz = json.loads(m.group(1))
        nav = float(gz.get("dwjz", 0))
        # 如果 nav 为 0，也降级
        if nav == 0:
            fallback = await _fetch_fund_estimate_lsjz(code)
            if fallback:
                return fallback

        return {
            "code": gz.get("fundcode", code),
            "name": gz.get("name", ""),
            "nav_date": gz.get("jzrq", ""),  # 净值日期
            "nav": nav,  # 昨日净值
            "estimated_nav": float(gz.get("gsz", 0)),  # 估算净值
            "change_pct": float(gz.get("gszzl", 0)),  # 估算涨跌幅 %
            "estimate_time": gz.get("gztime", ""),  # 估值时间
        }
    except Exception as e:
        print(f"[fetcher] 天天基金估值失败 {code}: {e}")
        return await _fetch_fund_estimate_lsjz(code)


async def _fetch_fund_estimate_lsjz(code: str) -> dict | None:
    """备用估值源：东方财富历史净值接口（覆盖期货/LOF 等 fundgz 不支持的基金）"""
    url = f"https://api.fund.eastmoney.com/f10/lsjz?fundCode={code}&pageIndex=1&pageSize=1"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://fund.eastmoney.com/",
    }
    session = await _get_session()
    try:
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()

        items = data.get("Data", {}).get("LSJZList", [])
        if not items:
            return None

        item = items[0]
        nav = float(item.get("DWJZ", 0))
        return {
            "code": code,
            "name": "",
            "nav_date": item.get("FSRQ", ""),
            "nav": nav,
            "estimated_nav": nav,  # 无实时估值，用最新净值代替
            "change_pct": float(item.get("JZZZL", 0)),
            "estimate_time": item.get("FSRQ", ""),
        }
    except Exception as e:
        print(f"[fetcher] lsjz 降级失败 {code}: {e}")
        return None


# ============================================================
# 东方财富基金基本信息
# ============================================================


async def fetch_fund_info(code: str) -> dict | None:
    """从东方财富获取基金基本信息"""
    url = f"http://fund.eastmoney.com/pingzhongdata/{code}.js"
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "http://fund.eastmoney.com/"}

    session = await _get_session()
    try:
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                return None
            data = await resp.text()

        name_match = re.search(r'fS_name\s*=\s*"(.+?)"', data)
        name = name_match.group(1) if name_match else ""

        type_match = re.search(r'fS_typename\s*=\s*"(.+?)"', data)
        fund_type = type_match.group(1) if type_match else ""

        net_match = re.search(r"Data_netWorthTrend\s*=\s*(\[.+?\])", data)
        latest_nav = 0.0
        nav_date = ""
        if net_match:
            try:
                net_data = json.loads(net_match.group(1))
                if net_data:
                    latest = net_data[-1]
                    latest_nav = float(latest.get("y", 0))
                    ts = latest.get("x", 0)
                    if ts:
                        nav_date = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
            except json.JSONDecodeError:
                pass

        return {
            "code": code,
            "name": name,
            "fund_type": fund_type,
            "nav": latest_nav,
            "nav_date": nav_date,
        }
    except Exception as e:
        print(f"[fetcher] 获取基金信息失败 {code}: {e}")
        return None


# ============================================================
# 天天基金持仓数据
# ============================================================


async def fetch_fund_holdings(code: str) -> list[dict] | None:
    """从天天基金获取前十大持仓"""
    url = (
        f"http://fundf10.eastmoney.com/FundArchivesDatas.aspx"
        f"?type=jjcc&code={code}&topline=10&year=&month=&rt={datetime.now().timestamp()}"
    )
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "http://fundf10.eastmoney.com/",
    }

    session = await _get_session()
    try:
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                return None
            data = await resp.text()

        content_match = re.search(r'var apidata=\{ content:"(.+?)",arryear:', data)
        if not content_match:
            print(f"[fetcher] 未找到持仓数据 content: {code}")
            return None

        html = content_match.group(1)
        html = html.replace('\\"', '"').replace("\\n", "\n").replace("\\t", " ")

        date_match = re.search(r"截止至：.*?(\d{4}-\d{2}-\d{2})", html)
        report_date = date_match.group(1) if date_match else ""

        # 解析表格 — 每行 9 个 td：序号|代码|名称|最新价|涨跌幅|相关资讯|占净值比|持股数|持仓市值
        tbody_match = re.search(r"<tbody>(.+?)</tbody>", html, re.DOTALL)
        if not tbody_match:
            print(f"[fetcher] 未找到 tbody: {code}")
            return None

        tr_rows = re.findall(r"<tr>(.+?)</tr>", tbody_match.group(1), re.DOTALL)
        holdings = []
        for tr in tr_rows:
            tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.DOTALL)
            if len(tds) < 7:
                continue
            stock_code = re.sub(r"<[^>]+>", "", tds[1]).strip()
            stock_name = re.sub(r"<[^>]+>", "", tds[2]).strip()
            pct_str = re.sub(r"<[^>]+>", "", tds[6]).strip()

            try:
                weight = float(pct_str.replace("%", ""))
            except ValueError:
                continue

            holdings.append({
                "stock_code": stock_code,
                "stock_name": stock_name,
                "weight": weight,
                "weight_str": pct_str,
            })

        if holdings:
            print(f"[fetcher] 解析到 {len(holdings)} 只持仓 (code={code}, date={report_date})")

        return holdings if holdings else None

    except Exception as e:
        print(f"[fetcher] 获取持仓失败 {code}: {e}")
        return None


# ============================================================
# 实时行情 + 行业数据
# ============================================================


async def fetch_stock_quotes_with_industry(stock_codes: list[str]) -> dict[str, dict]:
    """批量获取股票实时行情（腾讯 qt.gtimg.cn，GFW 直连；东方财富 push2 被墙）

    qt 响应格式：
    v_sz300014="51~亿纬锂能~300014~64.38~63.95~64.60~..."
    字段：~市场~名称~代码~现价~昨收~开盘~...
    """
    if not stock_codes:
        return {}

    # 先查缓存
    now = _time.time()
    cached_results: dict[str, dict] = {}
    uncached_codes: list[str] = []
    for code in stock_codes:
        entry = _stock_cache.get(code)
        if entry and (now - entry[0] < _STOCK_CACHE_TTL):
            cached_results[code] = entry[1]
        else:
            uncached_codes.append(code)

    # 构造腾讯 qt 代码：sz + 代码 或 sh + 代码
    qt_codes = []
    code_map: dict[str, str] = {}  # qt_code -> raw_code
    for code in uncached_codes:
        if code.startswith("6"):
            qt = f"sh{code}"
        elif code.startswith(("0", "3")):
            qt = f"sz{code}"
        elif len(code) == 5:  # 港股
            qt = f"hk{code}"
        else:
            continue
        qt_codes.append(qt)
        code_map[qt] = code

    if not qt_codes:
        return {}

    result = {}
    BATCH_SIZE = 50

    connector = aiohttp.TCPConnector(limit=10, force_close=True, family=0)
    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(0, len(qt_codes), BATCH_SIZE):
            batch = qt_codes[i:i + BATCH_SIZE]
            url = f"http://qt.gtimg.cn/q={','.join(batch)}"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    raw = await resp.text(encoding="gbk", errors="replace")
            except Exception:
                continue

            for line in raw.strip().split("\n"):
                line = line.strip()
                if not line or '="' not in line:
                    continue
                try:
                    # 提取 v_xxx="内容"
                    eq_pos = line.index('="')
                    qt_code = line[2:eq_pos]  # 跳过 v_
                    content = line[eq_pos + 2:].rstrip('";')
                    parts = content.split("~")
                    if len(parts) < 5:
                        continue

                    name = parts[1]
                    price = float(parts[3]) if parts[3] and parts[3] != "0.00" else 0.0
                    prev_close = float(parts[4]) if parts[4] and parts[4] != "0.00" else 0.0
                    change_pct = round((price - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0

                    raw_code = code_map.get(qt_code)
                    if raw_code:
                        result[raw_code] = {
                            "price": price,
                            "change_pct": change_pct,
                            "name": name,
                            "industry": "",  # qt 无行业，后续可用 EastMoney f10 补充
                        }
                except (ValueError, IndexError):
                    continue
    # 写入缓存 + 合并已缓存结果
    for code, data in result.items():
        _stock_cache[code] = (now, data)
    result.update(cached_results)
    return result


# ============================================================
# 天天基金主题识别（主数据源，替代行业统计）
# ============================================================

TTFUND_API_KEY = "ttf_sk_live_01KPHHVVCANX91H48J8TG1DMK2.t341D77Nb5pxpELaYBpAoO6q8dNvTnLKvicevmP-kFk"
TTFUND_GATEWAY = "https://skills.tiantianfunds.com/ai-smart-skill-service/openapi/skill/invoke"

# 离线关键词 → 主题映射（优先使用，不限流）
THEME_KEYWORD_MAP: dict[str, str] = {
    # 半导体
    "半导体设备": "半导体设备", "半导体芯片": "半导体芯片", "半导体材料": "半导体材料",
    "半导体": "半导体", "科创芯片": "科创芯片", "集成电路": "集成电路",
    # AI/科技
    "人工智能": "人工智能", "恒生科技": "恒生科技", "科创50": "科创50",
    "科创板": "科创50", "纳斯达克": "纳斯达克", "中概": "中概互联",
    # 新能源/资源
    "新能源": "新能源", "稀有金属": "稀有金属", "锂矿": "锂矿",
    "锂电": "锂电池", "绿色电力": "绿色电力", "储能电池": "储能",
    "碳中和": "碳中和", "工程机械": "工程机械",
    # 通信
    "通信设备": "通信设备", "信息创新": "信创",
    # 消费
    "消费": "消费", "白酒": "白酒", "农业产业": "农业",
    # 制造
    "制造升级": "高端制造", "高端装备": "高端装备",
    # 金融
    "中证红利": "红利", "红利低波": "红利低波",
    # 宽基
    "创业板": "创业板", "中证2000": "中证2000",
    # 量化/成长
    "量化精选": "量化", "量化": "量化",
    "成长优选": "成长", "优质精选": "价值精选",
    "科技智选": "科技", "科技优选": "科技",
    # 其他
    "白银期货": "白银",
}

THEME_CACHE: dict[str, str] = {}  # fund_code -> theme_name


async def _scrape_fund_theme_from_page(fund_code: str) -> str:
    """从天天基金网页抓取「相关主题基金」中的主题标签（免费，不限流）
    
    注意区分：
    - 「相关主题基金」→ 该基金专属主题（准确）
    - 「热门主题基金」→ 全站通用热门（不准确，跳过）
    """
    url = f"https://fund.eastmoney.com/{fund_code}.html"
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://fund.eastmoney.com/"}

    session = await _get_session()
    try:
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                return ""
            html = await resp.text()

        # 按区块分割，逐个查找包含「相关主题基金」（非「热门」）的区块
        sections = html.split('<div class="fund_item relatedThemeFund">')
        for section in sections[1:]:
            # 取到该区块结束（下一个 fund_item 或同公司旗下基金）
            end_marker = section.find('同公司旗下基金')
            if end_marker < 0:
                end_marker = section.find('<!-- 相关主题基金 end -->')
            if end_marker < 0:
                end_marker = len(section) // 2  # 安全截断
            chunk = section[:end_marker]
                
            # 必须包含「相关主题基金」且不包含「热门主题基金」
            if '相关主题基金' not in chunk:
                continue
            if '热门主题基金' in chunk[:500]:  # 开头就是热门则跳过
                continue
                
            # 提取: <li class="tabBtn titleItemActive" data-id=BK000645><span>锂矿</span></li>
            match = re.search(r'titleItemActive[^>]*data-id=BK\d+[^>]*><span>([^<]+)</span>', chunk)
            if match:
                theme = match.group(1)
                print(f"[fetcher] 网页抓取主题 {fund_code}: {theme}")
                return theme
    except Exception as e:
        print(f"[fetcher] 网页抓取失败 {fund_code}: {e}")
    return ""


async def _call_theme_api(keywords: list[str]) -> str:
    """调用天天基金主题API（受每日调用次数限制）"""
    headers = {
        "X-API-Key": TTFUND_API_KEY,
        "Content-Type": "application/json",
    }
    session = await _get_session()
    for kw in keywords[:2]:
        try:
            body = {
                "skill_id": "FUND_THEME_INFO",
                "_skill_version": "1.0.0",
                "query": kw,
            }
            async with session.post(
                TTFUND_GATEWAY, json=body, headers=headers, timeout=10
            ) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json()

            if data.get("code") != 0:
                continue

            raw = data["data"]["raw_result"]["body"]
            if not raw.get("success"):
                continue

            theme_data = raw.get("data")
            if isinstance(theme_data, dict) and theme_data.get("theme_name"):
                return theme_data["theme_name"]
            if isinstance(theme_data, list) and theme_data:
                return theme_data[0].get("theme_name", "")

        except Exception as e:
            print(f"[fetcher] 主题API调用失败 {kw}: {e}")
            continue
    return ""


async def fetch_fund_theme(fund_name: str, fund_code: str = "") -> str:
    """识别基金主题: 网页抓取 > 离线映射 > API（降级）→ 持仓兜底"""
    cache_key = fund_code or fund_name
    if cache_key in THEME_CACHE:
        return THEME_CACHE[cache_key]

    # 方案1: 从天天基金网页抓取主题标签（免费、不限流、最准确）
    if fund_code:
        theme = await _scrape_fund_theme_from_page(fund_code)
        if theme:
            THEME_CACHE[cache_key] = theme
            return theme

    # 方案2: 离线关键词映射
    keywords = _extract_theme_keywords(fund_name)
    for kw in keywords:
        if kw in THEME_KEYWORD_MAP:
            theme = THEME_KEYWORD_MAP[kw]
            THEME_CACHE[cache_key] = theme
            print(f"[fetcher] 离线映射: {fund_name} → {theme}")
            return theme

    # 方案3: 调用API（受每日调用次数限制，谨慎使用）
    if keywords:
        theme = await _call_theme_api(keywords[:2])
        if theme:
            THEME_CACHE[cache_key] = theme
            print(f"[fetcher] API识别: {fund_name} → {theme}")
            return theme

    # 空结果 → 由 estimator 用持仓行业兜底
    return ""


def _extract_theme_keywords(fund_name: str) -> list[str]:
    """从基金名称提取主题关键词"""
    # 常见主题词（越长越优先）
    theme_words = [
        # 长关键词优先（匹配越精准越好）
        "半导体设备", "半导体芯片", "半导体材料", "半导体",
        "科创芯片", "集成电路", "信息创新",
        "人工智能", "恒生科技", "科创50", "科创板", "纳斯达克", "中概",
        "电网设备", "储能电池", "绿色电力", "碳中和",
        "新能源", "新能源汽车", "稀有金属", "锂电", "锂矿",
        "工程机械", "高端装备", "制造升级",
        "通信设备", "5G", "光模块",
        "消费", "白酒", "农业产业", "白银期货",
        "中证红利", "红利低波", "低波动", "红利",
        "创业板", "中证2000",
        "量化精选", "量化",
        "成长优选", "优质精选", "价值精选",
        "科技智选", "科技优选", "科技",
        "发展主题", "中小盘", "大盘",
    ]
    found = []
    for w in theme_words:
        if w in fund_name:
            found.append(w)
    # 按长度排序，长的更精准
    found.sort(key=lambda x: -len(x))
    return found if found else [fund_name]


# ============================================================
# 天天基金净值历史（用于计算真实涨跌幅）
# ============================================================


async def fetch_fund_nav_history(fund_code: str, days: int = 5) -> list[dict]:
    """获取最近N个交易日的净值（东方财富公开接口，无需 API Key）"""
    url = f"https://api.fund.eastmoney.com/f10/lsjz?fundCode={fund_code}&pageIndex=1&pageSize={days}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://fund.eastmoney.com/",
    }
    session = await _get_session()
    try:
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()

        items = data.get("Data", {}).get("LSJZList", [])
        result = []
        for item in items:
            result.append({
                "nav": float(item.get("DWJZ", 0)),
                "date": item.get("FSRQ", ""),
                "nav_date": item.get("FSRQ", ""),
                "change_pct": float(item.get("JZZZL", 0)),
            })
        return result
    except Exception as e:
        print(f"[fetcher] NAV历史获取失败 {fund_code}: {e}")
        return []


# ============================================================
# 板块识别 — 基于真实行业数据（降级方案）
# ============================================================


def identify_sector_from_industries(holdings: list[dict]) -> str:
    """根据持仓股票的真实行业，加权计算基金板块"""
    if not holdings:
        return "未知"

    # 统计每个行业的权重
    industry_weights: dict[str, float] = {}

    for h in holdings:
        industry = h.get("industry", "")
        weight = h.get("weight", 0)

        if industry:
            industry_weights[industry] = industry_weights.get(industry, 0) + weight

    if not industry_weights:
        return "综合"

    # 按权重排序
    sorted_items = sorted(industry_weights.items(), key=lambda x: -x[1])
    top = sorted_items[0]

    # 单一行业占主导（>40%）→ 直接标该行业
    if top[1] >= 40:
        return top[0]

    # 否则标前两个
    if len(sorted_items) >= 2 and sorted_items[1][1] >= 15:
        return f"{sorted_items[0][0]}+{sorted_items[1][0]}"

    return top[0]


# ============================================================
# 指数实时行情
# ============================================================

async def fetch_index_quotes() -> dict[str, dict]:
    """获取四大指数实时行情（腾讯 qt.gtimg.cn）—— 带缓存，所有基金共享"""
    global _idx_cache
    now = _time.time()
    if _idx_cache and (now - _idx_cache[0] < _IDX_CACHE_TTL):
        return _idx_cache[1]

    idx_codes = {
        "sh000300": "沪深300",
        "sh000905": "中证500",
        "sz399006": "创业板",    # style_engine 用「创业板」
        "sh000932": "中证消费",
    }
    url = f"http://qt.gtimg.cn/q={','.join(idx_codes.keys())}"

    connector = aiohttp.TCPConnector(limit=5, force_close=True, family=0)
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                raw = await resp.text(encoding="gbk", errors="replace")
        except Exception:
            return {}

    result = {}
    for line in raw.strip().split("\n"):
        line = line.strip()
        if not line or '="' not in line:
            continue
        try:
            eq_pos = line.index('="')
            qt_code = line[2:eq_pos]
            content = line[eq_pos + 2:].rstrip('";')
            parts = content.split("~")
            if len(parts) < 5:
                continue

            idx_name = idx_codes.get(qt_code, parts[1])
            price = float(parts[3]) if parts[3] else 0.0
            prev_close = float(parts[4]) if parts[4] else 0.0
            change_pct = round((price - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0

            result[idx_name] = {
                "price": price,
                "change_pct": change_pct,
                "name": parts[1],
            }
        except (ValueError, IndexError):
            continue
    _idx_cache = (_time.time(), result)
    return result