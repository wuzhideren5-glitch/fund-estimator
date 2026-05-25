"""批量更新所有基金的主题信息"""
from __future__ import annotations

import asyncio
import sqlite3
import aiosqlite
import sys
import os

# 确保能找到同目录的模块
sys.path.insert(0, os.path.dirname(__file__))

from fetcher import fetch_fund_theme


async def main():
    sync_conn = sqlite3.connect(
        os.path.join(os.path.dirname(__file__), "fund_estimator.db")
    )
    cur = sync_conn.cursor()
    cur.execute("SELECT id, code, name FROM funds ORDER BY id")
    funds = [(r[0], r[1], r[2]) for r in cur.fetchall()]
    sync_conn.close()

    print(f"共 {len(funds)} 只基金需要更新主题\n")

    db = await aiosqlite.connect(
        os.path.join(os.path.dirname(__file__), "fund_estimator.db")
    )

    success = 0
    for i, (fid, code, name) in enumerate(funds, 1):
        try:
            theme_name = await fetch_fund_theme(name, code)
            if theme_name:
                await db.execute(
                    "UPDATE funds SET theme_name=? WHERE id=?",
                    (theme_name, fid),
                )
                print(f"[{i:2d}/{len(funds)}] {code} {name[:30]:31s} → {theme_name}")
                success += 1
            else:
                print(f"[{i:2d}/{len(funds)}] {code} {name[:30]:31s} → (无主题)")
        except Exception as e:
            print(f"[{i:2d}/{len(funds)}] {code} {name[:30]:31s} → 错误: {e}")

        await db.commit()

    await db.close()
    print(f"\n✅ 完成！{success}/{len(funds)} 成功识别主题")


if __name__ == "__main__":
    asyncio.run(main())
