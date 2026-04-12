"""
Polymarket 已关闭仓位检索 & 盈亏统计
======================================

功能：
    1. 根据钱包地址，拉取该钱包所有已关闭（市场结算）的仓位
    2. 按关键词过滤仓位标题
    3. 计算每个仓位的盈亏（花费 vs 赎回/卖出所得）
    4. 统计并输出：赢了多少单、输了多少单、总盈利
    5. 将结果保存为 JSON 文件

"已关闭仓位"定义：
    - 存在 REDEEM 操作记录（市场已结算，用户已兑现），或
    - 存在 SELL 操作且持仓股数趋近于 0（用户已手动平仓）

运行：
    python closed_position_searcher.py
"""

from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ═══════════════════════════════════════════════════════════════════════════════
# ★ CONFIG — 按需修改这里
# ═══════════════════════════════════════════════════════════════════════════════

WALLET  = "0x2005d16a84ceefa912d4e380cd32e7ff827875ea"   # 目标钱包地址

KEYWORD = "Counter-Strike"          # 关键词（不区分大小写，支持部分匹配）
                             # 例: "tennis" / "Bitcoin" / "Counter-Strike"

# 日期范围筛选（UTC，格式 "YYYY-MM-DD"，留空 "" 表示不限）
# 以仓位第一笔交易时间为准，只保留落在范围内的仓位
START_DATE = ""              # 例: "2026-01-01"  — 只看这天及之后开仓的
END_DATE   = ""              # 例: "2026-04-08"  — 只看这天及之前开仓的

# 输出 JSON 文件路径（留空 "" 则自动生成文件名）
OUTPUT_FILE = ""

# 最大拉取 offset（API 限制约 5000），太大会报 400
MAX_OFFSET  = 5000
PAGE_SIZE   = 500            # 每页条数（API 最大 500）
RATE_SLEEP  = 0.2            # 请求间隔（秒），防止触发限速

# ═══════════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════════

DATA_API  = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP Session (auto retry / backoff)
# ═══════════════════════════════════════════════════════════════════════════════

def _create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "PolymarketClosedPositionSearcher/1.0",
    })
    return session


SESSION = _create_session()


# ═══════════════════════════════════════════════════════════════════════════════
# Utilities
# ═══════════════════════════════════════════════════════════════════════════════

def ts_to_str(ts: int | float) -> str:
    if not ts:
        return "N/A"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def keyword_match(title: str, kw: str) -> bool:
    """不区分大小写的子串匹配"""
    return kw.lower() in (title or "").lower()


def parse_date_to_ts(date_str: str, end_of_day: bool = False) -> Optional[int]:
    """
    将 'YYYY-MM-DD' 转为 UTC Unix 时间戳。
    end_of_day=True 时返回当天 23:59:59，用于终止日期的闭区间比较。
    """
    if not date_str.strip():
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if end_of_day:
            dt = dt.replace(hour=23, minute=59, second=59)
        return int(dt.timestamp())
    except ValueError:
        print(f"  ⚠  日期格式错误（应为 YYYY-MM-DD）: {date_str!r}，已忽略。")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Step 1 — 拉取活动记录
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_activity(
    wallet: str,
    act_type: str,
    start_ts: Optional[int] = None,
) -> list[dict]:
    """
    分页拉取钱包指定类型的活动记录（DESC 排序），
    直至无更多数据或超出 offset 上限。
    start_ts 传给 API 可有效减少拉取量。
    """
    records: list[dict] = []
    offset = 0

    while True:
        if offset > MAX_OFFSET:
            print(f"    ⚠  offset 达上限 {MAX_OFFSET}，停止分页。")
            break

        time.sleep(RATE_SLEEP)
        params: dict = {
            "user": wallet,
            "type": act_type,
            "limit": PAGE_SIZE,
            "offset": offset,
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
        }
        if start_ts is not None:
            params["start"] = start_ts

        try:
            resp = SESSION.get(
                f"{DATA_API}/activity",
                params=params,
                timeout=30,
            )
            if resp.status_code == 400:
                break  # API 超出范围时返回 400
            resp.raise_for_status()
            page: list[dict] = resp.json()
        except requests.RequestException as exc:
            print(f"    ❌ 请求失败 (type={act_type}, offset={offset}): {exc}")
            break

        if not page:
            break

        records.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return records


def fetch_all_activity(wallet: str, start_ts: Optional[int] = None) -> list[dict]:
    """拉取 TRADE 和 REDEEM 两种类型的全量记录并去重"""
    print("【第1步】拉取活动记录...")
    all_records: list[dict] = []

    for act_type in ("TRADE", "REDEEM"):
        print(f"  → {act_type} ...", end="  ", flush=True)
        recs = fetch_activity(wallet, act_type, start_ts=start_ts)
        print(f"{len(recs)} 条")
        all_records.extend(recs)

    # 去重（以 transactionHash + type + outcome 联合键）
    seen: set[str] = set()
    unique: list[dict] = []
    for r in all_records:
        key = (
            f"{r.get('transactionHash','')}|{r.get('type','')}|"
            f"{r.get('timestamp','')}|{r.get('outcome','')}|"
            f"{r.get('conditionId','')}|{r.get('size','')}"
        )
        if key not in seen:
            seen.add(key)
            unique.append(r)

    # 按时间正序排列
    unique.sort(key=lambda x: x.get("timestamp", 0))
    print(f"  去重后共 {len(unique)} 条记录。")
    return unique


# ═══════════════════════════════════════════════════════════════════════════════
# Step 2 — 按 conditionId 分组，识别"已关闭"仓位
# ═══════════════════════════════════════════════════════════════════════════════

def group_by_position(records: list[dict]) -> dict[str, dict]:
    """
    将活动记录按 (conditionId, outcome) 分组，
    每组代表一个独立仓位，并计算基础指标。

    返回结构:
        {
          "<conditionId>|<outcome>": {
              "title": str,
              "conditionId": str,
              "outcome": str,
              "trades": [...],      # 所有原始记录
              "has_redeem": bool,   # 是否有市场结算赎回
              "is_closed": bool,    # 是否已关闭
              "shares_net": float,  # 净持仓股数（≈0 表示已平仓）
              "spent_usdc": float,  # 买入共花费
              "sell_received": float, # 卖出所得
              "redeem_received": float, # 结算赎回所得
              "total_received": float,
              "pnl": float,
              "result": "WIN" | "LOSS" | "BREAKEVEN",
          }
        }
    """
    print("【第2步】按仓位分组并计算盈亏...")

    groups: dict[str, dict] = {}

    for r in records:
        cid     = r.get("conditionId", "unknown")
        outcome = r.get("outcome") or "unknown"
        rtype   = r.get("type", "")
        side    = r.get("side", "")
        usdc    = safe_float(r.get("usdcSize", 0))
        size    = safe_float(r.get("size", 0))

        # REDEEM 不含 outcome 时，归入同一 conditionId 下最多买入的 outcome
        if rtype == "REDEEM" and outcome == "unknown":
            # 先暂存到 conditionId 层面，后续合并
            key = f"{cid}|__redeem__"
        else:
            key = f"{cid}|{outcome}"

        if key not in groups:
            groups[key] = {
                "title": r.get("title", ""),
                "conditionId": cid,
                "outcome": outcome if rtype != "REDEEM" else "unknown",
                "trades": [],
                "has_redeem": False,
                "shares_net": 0.0,
                "spent_usdc": 0.0,
                "sell_received": 0.0,
                "redeem_received": 0.0,
            }

        groups[key]["trades"].append(r)

        if not groups[key]["title"] and r.get("title"):
            groups[key]["title"] = r["title"]

        if rtype == "TRADE":
            if side == "BUY":
                groups[key]["shares_net"]  += size
                groups[key]["spent_usdc"]  += usdc
            elif side == "SELL":
                groups[key]["shares_net"]  -= size
                groups[key]["sell_received"] += usdc
        elif rtype == "REDEEM":
            groups[key]["has_redeem"] = True
            groups[key]["redeem_received"] += usdc

    # ── 将悬空的 __redeem__ 合并进对应 conditionId 的最大仓位 ──────────────
    redeem_keys = [k for k in groups if k.endswith("|__redeem__")]
    for rk in redeem_keys:
        cid = groups[rk]["conditionId"]
        redeem_usdc = groups[rk]["redeem_received"]
        redeem_trades = groups[rk]["trades"]

        # 找同 conditionId 下买入最多的那个 outcome 组
        sibling_keys = [
            k for k in groups
            if k.startswith(f"{cid}|") and not k.endswith("|__redeem__")
        ]
        if sibling_keys:
            best = max(sibling_keys, key=lambda k: groups[k]["spent_usdc"])
            groups[best]["has_redeem"] = True
            groups[best]["redeem_received"] += redeem_usdc
            groups[best]["trades"].extend(redeem_trades)
        else:
            # 没有对应的 TRADE 记录，保留独立 redeem 组
            groups[rk]["has_redeem"] = True
        del groups[rk]

    # ── 计算最终指标 ─────────────────────────────────────────────────────────
    CLOSED_THRESHOLD = 0.01  # 净持仓 < 此值视为已平仓

    for key, g in groups.items():
        g["total_received"] = g["sell_received"] + g["redeem_received"]
        g["pnl"]            = g["total_received"] - g["spent_usdc"]

        # 关闭判断：有赎回 OR 净持仓接近零（且有过买入）
        is_zero_position = (
            g["spent_usdc"] > 0 and abs(g["shares_net"]) < CLOSED_THRESHOLD
        )
        g["is_closed"] = g["has_redeem"] or is_zero_position

        pnl = g["pnl"]
        if pnl > 0.005:
            g["result"] = "WIN"
        elif pnl < -0.005:
            g["result"] = "LOSS"
        else:
            g["result"] = "BREAKEVEN"

    total_groups = len(groups)
    closed = sum(1 for g in groups.values() if g["is_closed"])
    print(f"  共发现 {total_groups} 个仓位组，其中已关闭: {closed} 个。")
    return groups


# ═══════════════════════════════════════════════════════════════════════════════
# Step 3 — 按关键词过滤已关闭仓位
# ═══════════════════════════════════════════════════════════════════════════════

def filter_closed_by_keyword(
    groups: dict[str, dict],
    keyword: str,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
) -> list[dict]:
    """
    返回已关闭且标题含关键词的仓位列表（按第一笔交易时间正序）。
    start_ts / end_ts：以仓位第一笔交易时间为准做日期范围过滤（Unix 时间戳）。
    """
    date_hint = ""
    if start_ts or end_ts:
        s = ts_to_str(start_ts) if start_ts else "不限"
        e = ts_to_str(end_ts)   if end_ts   else "不限"
        date_hint = f"，日期范围 [{s} ~ {e}]"
    print(f"【第3步】筛选已关闭且含关键词 {keyword!r} 的仓位{date_hint}...")

    matched: list[dict] = []
    for key, g in groups.items():
        if not g["is_closed"]:
            continue
        if not keyword_match(g["title"], keyword):
            continue

        # 日期过滤：以仓位最早一笔交易时间为准
        first_ts = g["trades"][0].get("timestamp", 0) if g["trades"] else 0
        if start_ts is not None and first_ts < start_ts:
            continue
        if end_ts is not None and first_ts > end_ts:
            continue

        g["_group_key"] = key
        matched.append(g)

    # 按最早交易时间排序
    matched.sort(key=lambda g: g["trades"][0].get("timestamp", 0) if g["trades"] else 0)
    print(f"  匹配到 {len(matched)} 个已关闭仓位。")
    return matched


# ═══════════════════════════════════════════════════════════════════════════════
# Step 4 — 输出摘要
# ═══════════════════════════════════════════════════════════════════════════════

def print_summary(positions: list[dict], keyword: str, wallet: str) -> None:
    if not positions:
        print("\n❌ 未找到任何符合条件的已关闭仓位。")
        return

    wins      = [p for p in positions if p["result"] == "WIN"]
    losses    = [p for p in positions if p["result"] == "LOSS"]
    evens     = [p for p in positions if p["result"] == "BREAKEVEN"]
    total_pnl = sum(p["pnl"] for p in positions)

    print("\n" + "=" * 72)
    print(f"  钱包地址 : {wallet}")
    print(f"  关键词   : {keyword!r}")
    print(f"  共找到   : {len(positions)} 个已关闭仓位")
    print(f"  赢单数   : {len(wins)}")
    print(f"  输单数   : {len(losses)}")
    print(f"  平局数   : {len(evens)}")
    print(f"  胜率     : {len(wins)/len(positions)*100:.1f}%")
    sign = "+" if total_pnl >= 0 else ""
    icon = "✅" if total_pnl >= 0 else "❌"
    print(f"  总盈亏   : {sign}${total_pnl:,.2f} USDC  {icon}")
    print("=" * 72)

    print(f"\n{'序号':>3}  {'结果':<8}  {'盈亏(USDC)':>12}  {'花费':>10}  {'赎回+卖出':>10}  {'市场标题'}")
    print("  " + "-" * 90)
    for idx, p in enumerate(positions, 1):
        result_icon = {"WIN": "✅", "LOSS": "❌", "BREAKEVEN": "🔵"}.get(p["result"], "?")
        sign = "+" if p["pnl"] >= 0 else ""
        earliest = p["trades"][0].get("timestamp", 0) if p["trades"] else 0
        title_str = (p["title"] or "")[:45]
        print(
            f"  {idx:>3}.  {result_icon} {p['result']:<7}  "
            f"{sign}{p['pnl']:>10.2f}  "
            f"{p['spent_usdc']:>10.2f}  "
            f"{p['total_received']:>10.2f}  "
            f"{title_str}"
        )

    if len(wins) > 0:
        print(f"\n  最大单笔盈利: +${max(p['pnl'] for p in wins):,.2f} USDC")
    if len(losses) > 0:
        print(f"  最大单笔亏损:  ${min(p['pnl'] for p in losses):,.2f} USDC")
    print("=" * 72)


# ═══════════════════════════════════════════════════════════════════════════════
# Step 5 — 保存 JSON
# ═══════════════════════════════════════════════════════════════════════════════

def save_results(positions: list[dict], keyword: str, wallet: str) -> str:
    """将结果保存为 JSON，返回文件路径"""
    wins      = [p for p in positions if p["result"] == "WIN"]
    losses    = [p for p in positions if p["result"] == "LOSS"]
    evens     = [p for p in positions if p["result"] == "BREAKEVEN"]
    total_pnl = sum(p["pnl"] for p in positions)

    output = {
        "meta": {
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
            "wallet_address": wallet,
            "keyword": keyword,
            "date_range_start": START_DATE or "unlimited",
            "date_range_end": END_DATE or "unlimited",
            "total_closed_positions": len(positions),
            "wins": len(wins),
            "losses": len(losses),
            "breakevens": len(evens),
            "win_rate_pct": round(len(wins) / len(positions) * 100, 2) if positions else 0,
            "total_pnl_usdc": round(total_pnl, 4),
            "total_spent_usdc": round(sum(p["spent_usdc"] for p in positions), 4),
            "total_received_usdc": round(sum(p["total_received"] for p in positions), 4),
        },
        "positions": [
            {
                "rank": idx,
                "title": p["title"],
                "conditionId": p["conditionId"],
                "outcome": p["outcome"],
                "result": p["result"],
                "pnl_usdc": round(p["pnl"], 4),
                "spent_usdc": round(p["spent_usdc"], 4),
                "sell_received_usdc": round(p["sell_received"], 4),
                "redeem_received_usdc": round(p["redeem_received"], 4),
                "total_received_usdc": round(p["total_received"], 4),
                "has_redeem": p["has_redeem"],
                "shares_net": round(p["shares_net"], 6),
                "num_trades": len(p["trades"]),
                "first_trade_time": ts_to_str(p["trades"][0].get("timestamp", 0)) if p["trades"] else "N/A",
                "last_trade_time": ts_to_str(p["trades"][-1].get("timestamp", 0)) if p["trades"] else "N/A",
                "raw_trades": p["trades"],
            }
            for idx, p in enumerate(positions, 1)
        ],
    }

    # 生成文件名
    global OUTPUT_FILE
    if not OUTPUT_FILE:
        safe_kw = "".join(c if c.isalnum() else "_" for c in keyword)[:30]
        OUTPUT_FILE = f"closed_positions_{safe_kw}.json"

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n💾 结果已保存: {OUTPUT_FILE}  ({len(positions)} 条)")
    return OUTPUT_FILE


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 72)
    print("  Polymarket 已关闭仓位检索")
    print(f"  钱包   : {WALLET}")
    print(f"  关键词 : {KEYWORD!r}")
    print(f"  日期   : {START_DATE or '不限'} ~ {END_DATE or '不限'}")
    print("=" * 72 + "\n")

    if not WALLET or not WALLET.startswith("0x"):
        print("❌ 请在脚本顶部 CONFIG 区域设置正确的 WALLET 地址（0x 开头）。")
        exit(1)

    if not KEYWORD.strip():
        print("❌ 请在脚本顶部 CONFIG 区域设置 KEYWORD 关键词。")
        exit(1)

    # 解析日期
    start_ts = parse_date_to_ts(START_DATE, end_of_day=False)
    end_ts   = parse_date_to_ts(END_DATE,   end_of_day=True)

    # 拉取全量活动（start_ts 直接传给 API，减少无效数据量）
    all_records = fetch_all_activity(WALLET, start_ts=start_ts)

    if not all_records:
        print("❌ 未拉取到任何活动记录，请检查钱包地址或网络连接。")
        exit(1)

    # 分组 & 判断关闭状态
    groups = group_by_position(all_records)

    # 筛选匹配关键词 + 日期范围的已关闭仓位
    matched_positions = filter_closed_by_keyword(groups, KEYWORD, start_ts, end_ts)

    if not matched_positions:
        print(f"\n⚠  未找到符合条件的已关闭仓位。")
        print("   建议：检查关键词拼写、日期范围，或尝试更短的关键词。")
        exit(0)

    # 打印摘要
    print_summary(matched_positions, KEYWORD, WALLET)

    # 保存结果
    save_results(matched_positions, KEYWORD, WALLET)

    print("\n✅ 完成。")
