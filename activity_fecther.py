"""
Polymarket 钱包仓位操作记录爬取脚本
====================================
功能：爬取指定钱包在某个特定市场下的所有操作记录，精确到秒。

依赖：pip install requests

配置说明（修改下方 CONFIG）：
  WALLET        - 目标钱包地址（0x 开头，40 位十六进制）
  MARKET_QUERY  - 市场名称关键词（支持部分匹配，不区分大小写）
                  例: "Bitcoin Up or Down - April 7, 9:55AM-10:00AM ET"
  EXACT_MATCH   - True: 标题必须完整包含 MARKET_QUERY（精确子串，推荐）
                  False: 拆词宽松匹配
  START_DATE    - 只抓这个日期之后的记录（UTC），格式 "YYYY-MM-DD"
                  设为目标市场所在日期可大幅减少拉取量，留空 "" 不限
  ACTIVITY_TYPES - 要抓取的操作类型，空列表 [] 表示全部类型
                   支持: TRADE / REDEEM / MERGE / SPLIT / REWARD / CONVERSION

运行：python activity_fecther.py
"""

import csv
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from typing import Optional

import requests

# ──────────────────── CONFIG（按需修改） ────────────────────────────
WALLET = "0x2cad53bb58c266ea91eea0d7ca54303a10bceb66"

# 仓位名称关键词
MARKET_QUERY = "Counter-Strike: MIBR vs EYEBALLERS - Map 1 Winner"

# True = 标题必须完整包含 MARKET_QUERY（推荐精确查找）
EXACT_MATCH = True

# 只抓这个日期之后的记录（UTC），留空 "" 不限
# 强烈建议设置为目标市场所在日期，可规避 API 深度分页限制
START_DATE = "2026-04-07"

# 操作类型，留空 [] = 全部类型
ACTIVITY_TYPES: list[str] = ["TRADE", "REDEEM"]

# 输出文件名前缀
OUTPUT_FILE = "market_activity"
# ─────────────────────────────────────────────────────────────────────

DATA_API  = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
PAGE_SIZE = 500   # activity API 单页最大 500
MAX_OFFSET = 5000  # 实测 offset 超过约 3500 会返回 400，保守取 3000


# ───────────────────── 工具函数 ──────────────────────────────────────

def ts_to_str(unix_ts: int) -> str:
    """Unix 秒级时间戳 → 'YYYY-MM-DD HH:MM:SS UTC'"""
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def parse_start_ts(date_str: str) -> Optional[int]:
    """'YYYY-MM-DD' → Unix 时间戳（UTC 00:00:00）"""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        print(f"⚠️  START_DATE 格式错误（应为 YYYY-MM-DD）: {date_str!r}，已忽略")
        return None


def title_matches(title: str, query: str, exact: bool) -> bool:
    title_lower = (title or "").lower()
    query_lower = query.lower()
    if exact:
        return query_lower in title_lower
    return any(word in title_lower for word in query_lower.split())


# ───────────────────── 核心：拉取 activity ───────────────────────────

def fetch_activity(
    wallet: str,
    activity_types: list[str],
    start_ts: Optional[int] = None,
    condition_id: Optional[str] = None,
) -> list[dict]:
    """
    分页拉取钱包活动记录。
    - 排序：DESC（最新记录优先），确保最近的操作优先被拿到
    - start_ts：Unix 时间戳下界，可大幅减少拉取量
    - condition_id：如已知则直接让 API 过滤，效率最高
    """
    all_records: list[dict] = []
    types_to_fetch: list[Optional[str]] = activity_types if activity_types else [None]

    for act_type in types_to_fetch:
        label = act_type or "ALL"
        print(f"  → 类型 {label} ...", end=" ", flush=True)
        offset = 0
        count = 0

        while True:
            if offset > MAX_OFFSET:
                print(f"\n    ⚠️  已达安全 offset 上限({MAX_OFFSET})，停止分页。")
                break

            params: dict = {
                "user": wallet,
                "limit": PAGE_SIZE,
                "offset": offset,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC",   # ← 最新记录优先，规避深分页问题
            }
            if act_type:
                params["type"] = act_type
            if condition_id:
                params["market"] = condition_id
            if start_ts:
                params["start"] = start_ts

            try:
                resp = requests.get(f"{DATA_API}/activity", params=params, timeout=30)
                if resp.status_code == 400:
                    # API 对超出范围的 offset 返回 400，视为无更多数据
                    break
                resp.raise_for_status()
                page = resp.json()
            except requests.RequestException as e:
                print(f"\n    ❌ 请求失败 (offset={offset}): {e}")
                break

            if not page:
                break

            all_records.extend(page)
            count += len(page)

            if len(page) < PAGE_SIZE:
                break  # 最后一页
            offset += PAGE_SIZE

        print(f"共 {count} 条")

    # 去重
    seen: set[str] = set()
    unique: list[dict] = []
    for r in all_records:
        key = r.get("transactionHash") or json.dumps(r, sort_keys=True)
        if key not in seen:
            seen.add(key)
            unique.append(r)

    # 最终按时间正序排列，方便阅读
    unique.sort(key=lambda x: x.get("timestamp", 0))
    return unique


# ───────────────────── conditionId 搜索（可选优化） ──────────────────

def lookup_condition_id(query: str) -> Optional[str]:
    """
    通过 Gamma /public-search 查找 conditionId。
    失败返回 None，降级为客户端标题过滤。
    """
    try:
        resp = requests.get(
            f"{GAMMA_API}/public-search",
            params={"term": query},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        markets = data.get("markets", []) if isinstance(data, dict) else []
        for m in markets:
            question = m.get("question") or m.get("title") or ""
            if query.lower() in question.lower():
                cid = m.get("conditionId") or m.get("condition_id")
                if cid:
                    print(f"  搜索命中 conditionId: {cid[:20]}...")
                    print(f"  市场标题: {question[:72]}")
                    return cid

        # 若搜索结果存在但不含 conditionId，尝试用 slug 补全
        if markets:
            first = markets[0]
            slug = first.get("slug") or first.get("marketSlug")
            if slug:
                r2 = requests.get(
                    f"{GAMMA_API}/markets",
                    params={"slug": slug, "limit": 1},
                    timeout=15,
                )
                r2.raise_for_status()
                items = r2.json()
                if items:
                    cid = items[0].get("conditionId", "")
                    if cid:
                        print(f"  通过 slug 补全 conditionId: {cid[:20]}...")
                        return cid

    except requests.RequestException as e:
        print(f"  搜索请求失败: {e}")

    return None


# ───────────────────── 输出 ──────────────────────────────────────────

def add_datetime(records: list[dict]) -> list[dict]:
    for r in records:
        ts = r.get("timestamp", 0)
        r["datetime"] = ts_to_str(ts) if ts else "N/A"
    return records


def print_summary(records: list[dict]) -> None:
    if not records:
        return

    titles = sorted({r.get("title", "") for r in records})
    print("\n" + "=" * 72)
    print(f"  钱包  : {WALLET}")
    print(f"  关键词: {MARKET_QUERY}")
    for t in titles:
        print(f"  市场  : {t}")
    print(f"  总记录: {len(records)} 条")
    print("=" * 72)

    type_counts = Counter(r.get("type", "?") for r in records)
    side_counts = Counter(r.get("side", "") for r in records if r.get("side"))
    print(f"\n  操作类型: {dict(type_counts)}")
    if side_counts:
        print(f"  买卖方向: {dict(side_counts)}")

    trades = [r for r in records if r.get("type") == "TRADE"]
    if trades:
        buys     = [r for r in trades if r.get("side") == "BUY"]
        sells    = [r for r in trades if r.get("side") == "SELL"]
        spent    = sum(r.get("usdcSize", 0) for r in buys)
        received = sum(r.get("usdcSize", 0) for r in sells)
        net = received - spent
        print(f"\n  交易统计:")
        print(f"    买入 {len(buys):>4} 次  花费  ${spent:>10,.2f} USDC")
        print(f"    卖出 {len(sells):>4} 次  获得  ${received:>10,.2f} USDC")
        sign = "+" if net >= 0 else ""
        print(f"    买卖差 PnL      {sign}${abs(net):>10,.2f} USDC  {'✅' if net >= 0 else '❌'}")

    redeems = [r for r in records if r.get("type") == "REDEEM"]
    if redeems:
        redeemed = sum(r.get("usdcSize", 0) for r in redeems)
        print(f"\n  结算赎回: {len(redeems)} 次  共 ${redeemed:,.2f} USDC")

    print(f"\n  最早操作: {records[0].get('datetime', 'N/A')}")
    print(f"  最新操作: {records[-1].get('datetime', 'N/A')}")

    print(f"\n  {'时间 (UTC)':<24} {'类型':<10} {'方向':<5} {'结果':<6} {'价格':>7} {'USDC':>9}  市场名称")
    print("  " + "-" * 90)
    for r in records:
        print(
            f"  {r.get('datetime','N/A'):<24}"
            f" {r.get('type',''):<10}"
            f" {r.get('side',''):<5}"
            f" {str(r.get('outcome','')):<6}"
            f" {r.get('price', 0):>7.4f}"
            f" {r.get('usdcSize', 0):>9.2f}"
            f"  {str(r.get('title',''))[:40]}"
        )
    print("=" * 72)


def save_json(records: list[dict], filename: str) -> None:
    path = f"{filename}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON: {path}  ({len(records)} 条)")


def save_csv(records: list[dict], filename: str) -> None:
    if not records:
        return
    priority = [
        "datetime", "timestamp", "type", "side", "outcome",
        "price", "size", "usdcSize", "title",
        "conditionId", "transactionHash", "proxyWallet",
        "asset", "slug", "eventSlug",
    ]
    all_keys   = list(records[0].keys())
    other      = [k for k in all_keys if k not in priority]
    fieldnames = [k for k in priority if k in all_keys] + other

    path = f"{filename}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    print(f"💾 CSV: {path}  ({len(records)} 条)")


# ───────────────────── 主流程 ────────────────────────────────────────

def main() -> None:
    print(f"钱包   : {WALLET}")
    print(f"关键词 : {MARKET_QUERY}")
    print(f"日期筛选: {START_DATE or '不限'}")
    print(f"类型   : {ACTIVITY_TYPES or '全部'}\n")

    start_ts = parse_start_ts(START_DATE)

    # ── 第一步：尝试获取 conditionId（可让 API 直接过滤）──────────
    print("【第一步】尝试搜索市场 conditionId...")
    condition_id = lookup_condition_id(MARKET_QUERY)
    if not condition_id:
        print("  未找到 conditionId，将拉取全量后按标题过滤。\n")

    # ── 第二步：拉取 activity（DESC 排序，最新记录优先）─────────────
    print("【第二步】拉取活动记录（最新优先）...")
    records = fetch_activity(WALLET, ACTIVITY_TYPES, start_ts, condition_id)

    # ── 第三步：无 conditionId 时按标题客户端过滤 ───────────────────
    if not condition_id:
        before = len(records)
        records = [r for r in records if title_matches(r.get("title", ""), MARKET_QUERY, EXACT_MATCH)]
        print(f"\n  标题过滤: {before} → {len(records)} 条匹配 (关键词: {MARKET_QUERY!r})")

        if not records and before > 0:
            # 调试：展示实际拿到的市场标题，帮助用户核对
            print("\n  ⚠️  未匹配到任何记录。以下是拉取到的部分市场标题，供参考：")
            sample_titles = sorted({r.get("title", "") for r in records[:500] if r.get("title")})
            for t in sample_titles[:20]:
                print(f"    {t!r}")
            print("\n  请对照以上标题调整 MARKET_QUERY 后重试。")

    # ── 第四步：添加可读时间 ────────────────────────────────────────
    records = add_datetime(records)

    # ── 第五步：展示 & 保存 ─────────────────────────────────────────
    if not records:
        print("\n❌ 未找到任何匹配记录，退出。")
        sys.exit(0)

    print_summary(records)

    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in MARKET_QUERY)[:40]
    output = f"{OUTPUT_FILE}_{safe}"
    save_json(records, output)
    save_csv(records, output)


if __name__ == "__main__":
    main()
