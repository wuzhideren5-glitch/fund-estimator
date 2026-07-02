"""
并发拉取基金净值 + 更新 nav_latest.json + SQLite
用法: python3 sync_nav.py
"""
import asyncio
import json
import os
import re
import sys
import time
import aiosqlite
import aiohttp

# Config
FUND_CODES = [
    "021528", "024203", "016531", "002112", "002656", "024424", "290008",
    "020628", "519771", "011452", "011613", "013841", "019173", "007491",
    "017811", "025209", "013943", "025857", "010786", "024481", "017574",
    "019924", "021511", "008888", "020357", "023765", "011120", "012920",
    "020465", "025793", "008989", "161226", "020477", "017836", "022365",
]

FUND_NAMES = {
    "021528": "财通成长优选混合C", "024203": "永赢制造升级智选混合发起C",
    "016531": "鹏华碳中和主题混合C", "002112": "德邦鑫星价值灵活配置混合C",
    "002656": "南方创业板ETF联接A", "024424": "东方阿尔法科技优选混合发起C",
    "290008": "泰信发展主题混合", "020628": "汇添富科创芯片ETF联接A",
    "519771": "交银优择回报灵活配置混合C", "011452": "华泰柏瑞质量成长C",
    "011613": "华夏科创50ETF联接C", "013841": "银华集成电路混合C",
    "019173": "摩根纳斯达克100指数(QDII)人民币C", "007491": "南方信息创新混合C",
    "017811": "东方人工智能主题混合C", "025209": "永赢先锋半导体智选混合发起C",
    "013943": "华宝中证稀有金属指数增强发起C", "025857": "华夏中证电网设备主题ETF发起式联接C",
    "010786": "博时创业板指数C", "024481": "财通品质甄选混合C",
    "017574": "华夏中证机床ETF发起式联接C", "019924": "华泰柏瑞中证2000指数增强C",
    "021511": "宏利半导体产业混合发起C", "008888": "华夏国证半导体芯片ETF联接C",
    "020357": "华夏半导体材料设备ETF联接C", "023765": "华夏中证5G通信主题ETF联接D",
    "011120": "富国创新科技混合C", "012920": "易方达全球成长精选混合(QDII)人民币A",
    "020465": "招商中证半导体产业ETF发起式联接C", "025793": "东方阿尔法科技甄选混合发起C",
    "008989": "大成科技创新混合C", "161226": "国投瑞银白银期货(LOF)A",
    "020477": "泰康半导体量化选股股票发起式C", "017836": "信澳匠心回报混合C",
    "022365": "永赢科技智选混合发起C",
}

DB_PATH = os.path.join(os.path.dirname(__file__), "fund_estimator.db")
NAV_JSON_PATH = os.path.join(os.path.dirname(__file__), "data", "nav_latest.json")

sem = asyncio.Semaphore(10)

async def fetch_fundgz(session, code):
    """从 fundgz API 拉取净值"""
    url = f"http://fundgz.1234567.com.cn/js/{code}.js"
    try:
        async with sem:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                text = await resp.text()
        # Parse jsonpgz({...})
        match = re.search(r'jsonpgz\((.+)\)', text)
        if match:
            return json.loads(match.group(1))
    except Exception as e:
        print(f"  ⚠️ fundgz failed for {code}: {e}")
    return None

async def fetch_eastmoney(session, code):
    """降级方案：东方财富历史净值"""
    url = f"https://api.fund.eastmoney.com/f10/lsjz"
    params = {"fundCode": code, "pageIndex": 1, "pageSize": 1}
    headers = {"Referer": "https://fundf10.eastmoney.com/"}
    try:
        async with sem:
            async with session.get(url, params=params, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
        items = data.get("Data", {}).get("LSJZList", [])
        if items:
            item = items[0]
            return {
                "DWJZ": item.get("DWJZ"),
                "FSRQ": item.get("FSRQ"),
                "LJJZ": item.get("LJJZ"),
                "JZZZL": item.get("JZZZL"),
            }
    except Exception as e:
        print(f"  ⚠️ eastmoney failed for {code}: {e}")
    return None

async def fetch_nav(session, code):
    """拉取单个基金净值，优先 fundgz，降级 eastmoney"""
    name = FUND_NAMES.get(code, code)

    result = await fetch_fundgz(session, code)
    if result and result.get("dwjz") and float(result.get("dwjz", 0)) > 0:
        nav = float(result["dwjz"])
        nav_date = result.get("jzrq", "")
        acc_nav = float(result.get("ljjz", 0))
        gszzl = float(result.get("gszzl", 0))
        estimated_nav = nav * (1 + gszzl / 100) if gszzl else nav
        change_pct = gszzl
        print(f"  ✅ {code} {name}: NAV={nav}, date={nav_date}, est_change={change_pct}%")
        return {
            "fundCode": code, "fundName": name,
            "nav_date": nav_date, "nav": nav, "acc_nav": acc_nav,
            "estimated_nav": round(estimated_nav, 4), "change_pct": change_pct,
        }

    # Fallback to eastmoney
    print(f"  🔄 {code} 降级到东方财富...")
    result = await fetch_eastmoney(session, code)
    if result and result.get("DWJZ"):
        nav = float(result["DWJZ"])
        nav_date = result.get("FSRQ", "")
        acc_nav = float(result.get("LJJZ", 0))
        change_pct = float(result.get("JZZZL", 0))
        print(f"  ✅ {code} {name} (eastmoney): NAV={nav}, date={nav_date}")
        return {
            "fundCode": code, "fundName": name,
            "nav_date": nav_date, "nav": nav, "acc_nav": acc_nav,
            "estimated_nav": nav, "change_pct": change_pct,
        }

    print(f"  ❌ {code} {name}: 净值拉取失败")
    return {
        "fundCode": code, "fundName": name,
        "nav_date": "", "nav": 0, "acc_nav": 0,
        "estimated_nav": 0, "change_pct": 0,
    }

async def update_db(db, nav_data):
    """更新 SQLite 数据库"""
    for item in nav_data:
        code = item["fundCode"]
        name = item["fundName"]
        nav = item["nav"]
        nav_date = item["nav_date"]
        acc_nav = item["acc_nav"]

        # Ensure table exists
        await db.execute("""
            CREATE TABLE IF NOT EXISTS funds (
                code TEXT PRIMARY KEY,
                name TEXT,
                nav REAL DEFAULT 0,
                nav_date TEXT,
                acc_nav REAL DEFAULT 0,
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS nav_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_code TEXT NOT NULL,
                date TEXT NOT NULL,
                nav REAL,
                acc_nav REAL,
                UNIQUE(fund_code, date)
            )
        """)

        # Update or insert fund
        await db.execute("""
            INSERT INTO funds (code, name, nav, nav_date, acc_nav, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(code) DO UPDATE SET
                name=excluded.name,
                nav=excluded.nav,
                nav_date=excluded.nav_date,
                acc_nav=excluded.acc_nav,
                updated_at=datetime('now')
        """, [code, name, nav, nav_date, acc_nav])

        # Insert nav history (only if we have valid data)
        if nav > 0 and nav_date:
            await db.execute("""
                INSERT OR IGNORE INTO nav_history (fund_code, date, nav, acc_nav)
                VALUES (?, ?, ?, ?)
            """, [code, nav_date, nav, acc_nav])

    # Clean up funds not in current holdings
    codes_str = ",".join(f"'{c}'" for c in FUND_CODES)
    await db.execute(f"DELETE FROM funds WHERE code NOT IN ({codes_str})")

    await db.commit()

async def main():
    start = time.time()

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_nav(session, code) for code in FUND_CODES]
        results = await asyncio.gather(*tasks)

    nav_data = list(results)

    # Save nav_latest.json
    os.makedirs(os.path.dirname(NAV_JSON_PATH), exist_ok=True)
    with open(NAV_JSON_PATH, "w") as f:
        json.dump(nav_data, f, indent=2, ensure_ascii=False)
    print(f"\n✅ nav_latest.json saved ({len(nav_data)} funds)")

    # Update SQLite
    async with aiosqlite.connect(DB_PATH) as db:
        await update_db(db, nav_data)
    print("✅ SQLite database updated")

    # Summary
    success = [n for n in nav_data if n["nav"] > 0]
    failed = [n for n in nav_data if n["nav"] == 0]
    elapsed = time.time() - start
    print(f"\n📊 Summary: {len(success)}/{len(nav_data)} NAVs fetched in {elapsed:.1f}s")
    if failed:
        print(f"❌ Failed: {[f['fundCode'] for f in failed]}")

    print("__RESULT__")
    print(json.dumps({
        "total": len(nav_data),
        "success": len(success),
        "failed_codes": [f["fundCode"] for f in failed],
        "elapsed": round(elapsed, 1),
    }))

if __name__ == "__main__":
    asyncio.run(main())
