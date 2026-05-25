"""
新闻获取 + DeepSeek 智能分类模块
数据流: Tushare 5源取新闻 → 去重 → DeepSeek 按基金主题分类 → 入库
"""
from __future__ import annotations

import asyncio
import datetime
import json
import os
import sys
import time
from typing import List, Dict, Optional, Set

import aiohttp
import tushare as ts

from tushare_client import get_pro

# ===== 配置 =====
# DeepSeek API（从项目 .env 读取）
_ENV_PATH = os.path.join(os.path.dirname(__file__), '.env')
if not os.path.exists(_ENV_PATH):
    # 回退：从求职AI项目读取（Linux 服务器路径）
    _ENV_PATH = '/home/ubuntu/sfi/backend/.env'

DEEPSEEK_API_KEY = ""
DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"

def _load_env():
    global DEEPSEEK_API_KEY
    try:
        with open(_ENV_PATH) as f:
            for line in f:
                line = line.strip()
                if line.startswith('DEEPSEEK_API_KEY='):
                    DEEPSEEK_API_KEY = line.split('=', 1)[1]
                elif line.startswith('DEEPSEEK_BASE_URL='):
                    DEEPSEEK_BASE_URL = line.split('=', 1)[1]
    except Exception:
        pass

_load_env()

# 新闻来源
NEWS_SOURCES = ['sina', '10jqka', 'eastmoney', 'cls', 'wallstreetcn']

# 分类设置
MAX_NEWS_PER_RUN = 200  # 每次最多取 200 条
DEEPSEEK_BATCH_SIZE = 50  # DeepSeek 每批处理新闻数
NEWS_MAX_AGE_HOURS = 48  # 只保留 48 小时内新闻

# ===== DeepSeek 分类 =====

CLASSIFY_PROMPT = """你是一个专业的财经新闻分类分析师。请分析以下新闻标题，判断每条新闻与哪些投资主题相关。

## 投资主题（共 28 个）：
半导体, 通信设备, 电力设备, 创业板, 科创50, 5G, 电池, 锂矿, 高端制造, CPO, 电子, 人形机器人, 机械设备, 光刻胶, 恒生科技, 红利, 纳斯达克, 有色金属, 人工智能, 绿色电力, 养殖业, 量化, 中证2000, 材料, 元件, 汽车, 白银

## 分类规则：
1. 严格匹配主题含义
   - "半导体" ← 芯片、集成电路、晶圆、台积电、英伟达、中芯国际、光刻、EDA
   - "电池" ← 锂电池、储能、宁德时代、固态电池、钠电池
   - "锂矿" ← 碳酸锂、锂资源、盐湖提锂、雅化集团、赣锋锂业
   - "人工智能" ← AI大模型、算力、GPT、DeepSeek、智能驾驶
   - "汽车" ← 新能源车、比亚迪、特斯拉、华为汽车、汽车零部件
   - "养殖业" ← 猪肉、生猪、牧原、温氏、养殖
   - "有色金属" ← 铜铝锌、稀土、黄金、矿业
   - "5G" ← 5G-A、通信基站、华为通信
   - "CPO" ← 光模块、光通信、共封装光学
   - "人形机器人" ← 具身智能、机器人
   - "绿色电力" ← 光伏、风电、电力改革、绿电
   - "红利" ← 高股息、分红、红利策略
   - "纳斯达克" ← 美股、美联储、纳斯达克指数
   - "恒生科技" ← 港股科技、恒生指数
   - "量化" ← 量化交易、量化基金
   - "白银" ← 白银期货、贵金属（非黄金）
   
2. 如果新闻与投资主题无关（如社会新闻、娱乐、体育），返回空数组 []
3. 一条新闻可以匹配多个主题（最多3个）
4. 只返回 JSON，不要任何额外文字

## 新闻列表：
{news_block}

请以 JSON 格式返回：{{"results":[{{"id":新闻编号,"themes":["匹配主题"]}}]}}"""


async def classify_news_batch(
    session: aiohttp.ClientSession,
    news_batch: List[Dict],
    batch_idx: int,
) -> List[Dict]:
    """用 DeepSeek 对一批新闻进行分类"""
    if not DEEPSEEK_API_KEY:
        print("[classifier] DeepSeek API key 未配置，跳过分类")
        return [{"id": i, "themes": []} for i in range(len(news_batch))]

    # 构建新闻块
    news_block = "\n".join([
        f"[{i}] {n['title'][:80]}"
        for i, n in enumerate(news_batch)
    ])
    prompt = CLASSIFY_PROMPT.replace("{news_block}", news_block)

    payload = {
        "model": "deepseek-chat",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "max_tokens": 2048,
    }

    try:
        async with session.post(
            f"{DEEPSEEK_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=aiohttp.ClientTimeout(total=45),
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                print(f"[classifier] DeepSeek API 错误 batch#{batch_idx}: {resp.status} {text[:200]}")
                return [{"id": i, "themes": []} for i in range(len(news_batch))]

            data = await resp.json()
            content = data["choices"][0]["message"]["content"]
            tokens = data.get("usage", {}).get("total_tokens", 0)
            print(f"[classifier] batch#{batch_idx} {len(news_batch)}条新闻, {tokens} tokens")

            result = json.loads(content)
            return result.get("results", [])

    except Exception as e:
        print(f"[classifier] batch#{batch_idx} 异常: {e}")
        return [{"id": i, "themes": []} for i in range(len(news_batch))]


# ===== Tushare 新闻获取 =====

def fetch_news_from_tushare() -> List[Dict]:
    """从 Tushare 5 个源获取最新新闻，去重排序"""
    pro = get_pro()
    today = datetime.date.today()
    end = today.strftime('%Y%m%d')
    start = (today - datetime.timedelta(days=2)).strftime('%Y%m%d')

    all_news: List[Dict] = []
    seen_titles: Set[str] = set()

    for src in NEWS_SOURCES:
        try:
            df = pro.news(src=src, start_date=start, end_date=end)
            for _, row in df.iterrows():
                # 用标题前 60 字符去重
                key = row['title'][:60].strip()
                if key in seen_titles:
                    continue
                seen_titles.add(key)

                all_news.append({
                    'title': row['title'],
                    'content': str(row.get('content', ''))[:500],
                    'time': row['datetime'],
                    'source': src,
                })
        except Exception as e:
            print(f"[fetcher] Tushare {src} 新闻获取失败: {e}")

    # 按时间倒序，取前 MAX_NEWS_PER_RUN
    all_news.sort(key=lambda x: x['time'], reverse=True)
    return all_news[:MAX_NEWS_PER_RUN]


# ===== 主流程 =====

async def refresh_news(db) -> Dict:
    """
    完整新闻刷新流程：
    1. 从 Tushare 获取最新新闻
    2. 用 DeepSeek 分类
    3. 写入数据库
    返回统计信息
    """
    start_time = time.time()

    # 1. 获取新闻
    print("[news] 从 Tushare 获取新闻...")
    raw_news = fetch_news_from_tushare()
    print(f"[news] 去重后 {len(raw_news)} 条")

    if not raw_news:
        return {"status": "empty", "total": 0, "classified": 0}

    # 2. 分批用 DeepSeek 分类
    print(f"[news] 用 DeepSeek 分类 {len(raw_news)} 条新闻...")
    all_results = []

    async with aiohttp.ClientSession() as session:
        for batch_idx in range(0, len(raw_news), DEEPSEEK_BATCH_SIZE):
            batch = raw_news[batch_idx:batch_idx + DEEPSEEK_BATCH_SIZE]
            results = await classify_news_batch(session, batch, batch_idx // DEEPSEEK_BATCH_SIZE)
            all_results.extend(results)
            # 避免 API 限流
            if batch_idx + DEEPSEEK_BATCH_SIZE < len(raw_news):
                await asyncio.sleep(0.5)

    # 3. 合并结果
    classified_count = 0
    classified_news: List[Dict] = []
    for i, news in enumerate(raw_news):
        themes = []
        for r in all_results:
            if r.get("id") == i:
                themes = r.get("themes", [])
                break

        if themes:
            classified_count += 1
            classified_news.append({
                **news,
                "themes": themes,
            })

    print(f"[news] 分类完成: {classified_count}/{len(raw_news)} 条有主题匹配")

    # 4. 写入数据库
    await _save_news_to_db(db, classified_news)

    elapsed = time.time() - start_time
    print(f"[news] 刷新完成，耗时 {elapsed:.1f}s")

    return {
        "status": "ok",
        "total": len(raw_news),
        "classified": classified_count,
        "elapsed_seconds": round(elapsed, 1),
    }


async def _save_news_to_db(db, news_list: List[Dict]):
    """将分类后的新闻写入 SQLite"""
    # 创建表（如果不存在）
    await db.execute("""
        CREATE TABLE IF NOT EXISTS classified_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            content TEXT,
            source TEXT,
            publish_time TEXT NOT NULL,
            themes TEXT NOT NULL,  -- JSON array
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 清空旧数据
    await db.execute("DELETE FROM classified_news")
    await db.execute("DELETE FROM sqlite_sequence WHERE name='classified_news'")

    # 批量插入
    for news in news_list:
        await db.execute(
            """INSERT INTO classified_news (title, content, source, publish_time, themes)
               VALUES (?, ?, ?, ?, ?)""",
            (
                news['title'],
                news.get('content', ''),
                news['source'],
                news['time'],
                json.dumps(news.get('themes', []), ensure_ascii=False),
            )
        )

    await db.commit()
    print(f"[news] 已写入 {len(news_list)} 条分类新闻到数据库")


async def get_news_by_theme(db, theme: str, limit: int = 20) -> List[Dict]:
    """按主题查询新闻"""
    cursor = await db.execute(
        """SELECT id, title, content, source, publish_time, themes
           FROM classified_news
           WHERE themes LIKE ?
           ORDER BY publish_time DESC
           LIMIT ?""",
        (f'%"{theme}"%', limit)
    )
    rows = await cursor.fetchall()
    return [
        {
            "id": row[0],
            "title": row[1],
            "content": row[2],
            "source": row[3],
            "publish_time": row[4],
            "themes": json.loads(row[5]),
        }
        for row in rows
    ]


async def get_all_themes(db) -> List[Dict]:
    """获取所有主题及其新闻数量"""
    cursor = await db.execute(
        "SELECT themes FROM classified_news"
    )
    rows = await cursor.fetchall()
    theme_count: Dict[str, int] = {}
    for (themes_json,) in rows:
        for theme in json.loads(themes_json):
            theme_count[theme] = theme_count.get(theme, 0) + 1
    return [
        {"theme": t, "count": c}
        for t, c in sorted(theme_count.items(), key=lambda x: -x[1])
    ]
