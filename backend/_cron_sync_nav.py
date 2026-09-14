"""Cron NAV sync: derive fund codes from holdings_latest.json, pull NAV, save + update SQLite."""
import asyncio
import json
import os
import re
import time
import aiosqlite
import aiohttp

BASE = os.path.dirname(os.path.abspath(__file__))
HOLDINGS_PATH = os.path.join(BASE, "data", "holdings_latest.json")
NAV_JSON_PATH = os.path.join(BASE, "data", "nav_latest.json")
DB_PATH = os.path.join(BASE, "fund_estimator.db")

with open(HOLDINGS_PATH, encoding="utf-8") as f:
    holdings = json.load(f)

# Extract fund codes (ptype == fund, exclude hqb)
FUNDS = []
for h in holdings:
    ptype = h.get("ptype", "").lower()
    code = h.get("fundCode", "")
    name = h.get("fundName", "")
    if ptype == "fund" and code and code != "hqb":
        FUNDS.append({"code": code, "name": name})

FUND_CODES = [f["code"] for f in FUNDS]
FUND_NAMES = {f["code"]: f["name"] for f in FUNDS}

async def fetch_fundgz(session, code):
    url = f"http://fundgz.1234567.com.cn/js/{code}.js"
    try:
        async with sem:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                text = await resp.text()
        m = re.search(r'jsonpgz\((.+)\)', text)
        if m:
            return json.loads(m.group(1))
    except Exception as e:
        print(f"  ⚠️ fundgz failed for {code}: {e}")
    return None

async def fetch_eastmoney(session, code):
    url = "https://api.fund.eastmoney.com/f10/lsjz"
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
    name = FUND_NAMES.get(code, code)
    result = await fetch_fundgz(session, code)
    if result and result.get("dwjz") and float(result.get("dwjz", 0)) > 0:
        nav = float(result["dwjz"])
        nav_date = result.get("jzrq", "")
        acc_nav = float(result.get("ljjz", 0) or 0)
        gszzl = float(result.get("gszzl", 0) or 0)
        estimated_nav = nav * (1 + gszzl / 100) if gszzl else nav
        return {
            "fundCode": code, "fundName": name,
            "nav_date": nav_date, "nav": nav, "acc_nav": acc_nav,
            "estimated_nav": round(estimated_nav, 4), "change_pct": gszzl,
        }

    result = await fetch_eastmoney(session, code)
    if result and result.get("DWJZ"):
        nav = float(result["DWJZ"])
        nav_date = result.get("FSRQ", "")
        acc_nav = float(result.get("LJJZ", 0) or 0)
        change_pct = float(result.get("JZZZL", 0) or 0)
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
    await db.execute("""CREATE TABLE IF NOT EXISTS funds (
        code TEXT PRIMARY KEY, name TEXT, nav REAL DEFAULT 0, nav_date TEXT,
        acc_nav REAL DEFAULT 0, updated_at TEXT DEFAULT (datetime('now')))""")
    await db.execute("""CREATE TABLE IF NOT EXISTS nav_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, fund_code TEXT NOT NULL,
        date TEXT NOT NULL, nav REAL, acc_nav REAL, UNIQUE(fund_code, date))""")

    for item in nav_data:
        code, name = item["fundCode"], item["fundName"]
        nav, nav_date, acc_nav = item["nav"], item["nav_date"], item["acc_nav"]
        await db.execute("""
            INSERT INTO funds (code, name, nav, nav_date, acc_nav, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(code) DO UPDATE SET
                name=excluded.name, nav=excluded.nav, nav_date=excluded.nav_date,
                acc_nav=excluded.acc_nav, updated_at=datetime('now')
        """, [code, name, nav, nav_date, acc_nav])
        if nav > 0 and nav_date:
            await db.execute("""
                INSERT OR IGNORE INTO nav_history (fund_code, date, nav, acc_nav)
                VALUES (?, ?, ?, ?)
            """, [code, nav_date, nav, acc_nav])

    codes_str = ",".join(f"'{c}'" for c in FUND_CODES)
    cur = await db.execute(f"DELETE FROM funds WHERE code NOT IN ({codes_str})")
    await db.commit()
    return cur

async def main():
    global sem
    sem = asyncio.Semaphore(10)  # 必须在运行中的事件循环内创建，避免跨 loop 报错
    start = time.time()
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_nav(session, code) for code in FUND_CODES]
        results = await asyncio.gather(*tasks)
    nav_data = list(results)

    os.makedirs(os.path.dirname(NAV_JSON_PATH), exist_ok=True)
    with open(NAV_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(nav_data, f, indent=2, ensure_ascii=False)

    async with aiosqlite.connect(DB_PATH) as db:
        deleted_cur = await update_db(db, nav_data)
        deleted = deleted_cur.rowcount if deleted_cur else 0

    success = [n for n in nav_data if n["nav"] > 0]
    failed = [n["fundCode"] for n in nav_data if n["nav"] == 0]
    elapsed = time.time() - start
    print("__RESULT__")
    print(json.dumps({
        "total": len(nav_data), "success": len(success), "failed": failed,
        "deleted": deleted, "elapsed": round(elapsed, 1),
    }))

asyncio.run(main())
