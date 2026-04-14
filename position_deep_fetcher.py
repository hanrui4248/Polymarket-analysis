"""
Polymarket 钱包持仓深度分析爬取脚本
=====================================

功能：
    爬取指定钱包在某个具体持仓/市场/outcome 上的完整下注数据，
    并生成结构化 JSON 供后续 AI 深度分析。

    覆盖维度：
      - 市场与持仓基础信息
      - 逐笔交易明细（FIFO 盈亏计算）
      - 仓位汇总指标
      - 交易时市场价格上下文
      - AI 友好的衍生分析特征

依赖：
    pip install requests

用法：
    python position_deep_fetcher.py \\
        --wallet 0x2cad53bb58c266ea91eea0d7ca54303a10bceb66 \\
        --position "Bitcoin Up or Down - April 7, 12:15PM-12:20PM ET" \\
        --start-date 2026-04-07

    python position_deep_fetcher.py \\
        --wallet 0x2cad53bb58c266ea91eea0d7ca54303a10bceb66 \\
        --position "Counter-Strike: MIBR vs EYEBALLERS - Map 1 Winner" \\
        --output result.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ═══════════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════════

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

PAGE_SIZE = 500
MAX_OFFSET = 5000
RATE_LIMIT_SLEEP = 0.25
UNAVAILABLE = "unavailable"
SCRIPT_VERSION = "1.0.0"


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP Session (with automatic retry / back-off)
# ═══════════════════════════════════════════════════════════════════════════════

def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "PolymarketPositionAnalyzer/1.0",
    })
    return session


# ═══════════════════════════════════════════════════════════════════════════════
# Utility helpers
# ═══════════════════════════════════════════════════════════════════════════════

def ts_to_iso(ts: int | float) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()


def ts_to_str(ts: int | float) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def safe_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def safe_int(val: Any, default: int = 0) -> int:
    try:
        return int(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def parse_date_to_ts(date_str: str) -> Optional[int]:
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        print(f"  ⚠ 日期格式错误 (应为 YYYY-MM-DD): {date_str!r}")
        return None


def title_matches(title: str, query: str) -> bool:
    return query.lower() in (title or "").lower()


def _throttle() -> None:
    time.sleep(RATE_LIMIT_SLEEP)


# ═══════════════════════════════════════════════════════════════════════════════
# Module 1 — fetch_market_info
# ═══════════════════════════════════════════════════════════════════════════════

def _build_search_candidates(position_name: str) -> list[str]:
    """
    生成多个搜索候选词，应对 Gamma API 对冒号等特殊字符返回 422 的问题。
    按优先级排列：原始 → 去特殊字符 → 截取前几个词。
    """
    candidates: list[str] = [position_name]
    cleaned = re.sub(r"[^\w\s\-]", " ", position_name)
    cleaned = " ".join(cleaned.split())
    if cleaned != position_name:
        candidates.append(cleaned)
    words = cleaned.split()
    if len(words) > 5:
        candidates.append(" ".join(words[:5]))
    if len(words) > 3:
        candidates.append(" ".join(words[:3]))
    seen: set[str] = set()
    return [c for c in candidates if c.strip() and c not in seen and not seen.add(c)]  # type: ignore[func-returns-value]


def fetch_market_info(session: requests.Session, position_name: str) -> dict:
    """
    通过 Gamma API 搜索市场/事件，返回包含 conditionId、tokens、
    时间信息等的 metadata 字典。
    如果搜索不到，返回仅含默认值的字典（后续步骤将回退到标题过滤）。
    """
    print(f"[1/6] 搜索市场信息: {position_name[:70]}...")

    info: dict[str, Any] = {
        "market_id": UNAVAILABLE,
        "market_slug": UNAVAILABLE,
        "market_question": UNAVAILABLE,
        "event_name": UNAVAILABLE,
        "event_slug": UNAVAILABLE,
        "outcome_name": UNAVAILABLE,
        "token_id": UNAVAILABLE,
        "position_side": UNAVAILABLE,
        "market_category": UNAVAILABLE,
        "market_open_time": UNAVAILABLE,
        "market_close_time": UNAVAILABLE,
        "market_resolve_time": UNAVAILABLE,
        "market_status": UNAVAILABLE,
        "resolution_result": UNAVAILABLE,
        "position_status": UNAVAILABLE,
        # internal — 不输出到 JSON
        "_condition_ids": [],
        "_tokens_map": {},
    }

    # 尝试多种搜索词（原始 → 去特殊字符 → 截短），规避 422
    search_terms = _build_search_candidates(position_name)
    data: dict = {}
    for term in search_terms:
        _throttle()
        try:
            resp = session.get(
                f"{GAMMA_API}/public-search",
                params={"term": term},
                timeout=15,
            )
            resp.raise_for_status()
            raw = resp.json()
            data = raw if isinstance(raw, dict) else {}
            if data.get("events") or data.get("markets"):
                print(f"  搜索命中 (term={term[:50]})")
                break
        except requests.RequestException:
            continue

    if not data:
        print("  ⚠ 所有搜索策略均失败，将回退到标题过滤")
        return info

    events = data.get("events", [])
    markets = data.get("markets", [])

    # ---------- 在 events 中匹配 ----------
    matched_event: Optional[dict] = None
    matched_markets: list[dict] = []

    for ev in events:
        ev_title = ev.get("title") or ev.get("name") or ""
        if title_matches(ev_title, position_name):
            matched_event = ev
            matched_markets = ev.get("markets", [])
            break

    # ---------- 在 markets 中匹配（fallback） ----------
    if not matched_markets:
        for m in markets:
            q = m.get("question") or m.get("title") or ""
            if title_matches(q, position_name):
                matched_markets.append(m)

    if not matched_markets:
        print("  ⚠ 未在搜索结果中找到匹配市场")
        return info

    # ---------- 收集所有 conditionId ----------
    condition_ids: list[str] = []
    tokens_map: dict[str, list[dict]] = {}

    for m in matched_markets:
        cid = m.get("conditionId") or m.get("condition_id") or ""
        # 如果缺少 conditionId，尝试通过 slug 补全
        if not cid:
            slug = m.get("slug") or m.get("marketSlug")
            if slug:
                cid = _lookup_cid_by_slug(session, slug)
        if cid and cid not in condition_ids:
            condition_ids.append(cid)
            tokens_map[cid] = m.get("tokens", [])

    # ---------- 取第一个 market 填充基础字段 ----------
    first = matched_markets[0]
    cid_first = condition_ids[0] if condition_ids else ""
    info.update({
        "market_id": cid_first or first.get("id", UNAVAILABLE),
        "market_slug": first.get("slug", UNAVAILABLE),
        "market_question": first.get("question") or first.get("title", UNAVAILABLE),
        "market_category": first.get("category", UNAVAILABLE),
        "market_status": _resolve_status(first),
        "_condition_ids": condition_ids,
        "_tokens_map": tokens_map,
    })

    outcomes = first.get("outcomes")
    if isinstance(outcomes, list):
        info["outcome_name"] = ", ".join(str(o) for o in outcomes)

    # 事件级信息
    if matched_event:
        info["event_name"] = matched_event.get("title", UNAVAILABLE)
        info["event_slug"] = matched_event.get("slug", UNAVAILABLE)

    for src, dst in [
        ("startDate", "market_open_time"),
        ("endDate", "market_close_time"),
        ("resolvedAt", "market_resolve_time"),
        ("resolutionDate", "market_resolve_time"),
    ]:
        val = first.get(src)
        if val:
            info[dst] = val

    res = first.get("resolution")
    if res is not None:
        info["resolution_result"] = str(res)

    print(f"  市场: {info['market_question'][:60]}")
    print(f"  conditionId 数量: {len(condition_ids)}")
    for cid in condition_ids:
        print(f"    {cid[:40]}...")
    return info


def _lookup_cid_by_slug(session: requests.Session, slug: str) -> str:
    _throttle()
    try:
        r = session.get(
            f"{GAMMA_API}/markets", params={"slug": slug, "limit": 1}, timeout=15
        )
        r.raise_for_status()
        items = r.json()
        if items and isinstance(items, list):
            return items[0].get("conditionId", "")
    except Exception:
        pass
    return ""


def _lookup_market_by_cid(
    session: requests.Session, condition_id: str
) -> Optional[dict]:
    """通过 conditionId 从 Gamma API 获取市场完整信息（含 tokens）。"""
    _throttle()
    try:
        r = session.get(
            f"{GAMMA_API}/markets",
            params={"condition_id": condition_id, "limit": 1},
            timeout=15,
        )
        r.raise_for_status()
        items = r.json()
        if items and isinstance(items, list) and items[0]:
            return items[0]
    except Exception:
        pass
    return None


def _resolve_status(market: dict) -> str:
    if market.get("resolved"):
        return "resolved"
    if market.get("closed"):
        return "closed"
    if market.get("active") is False:
        return "inactive"
    return "active"


# ═══════════════════════════════════════════════════════════════════════════════
# Module 2 — fetch_wallet_trades
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_wallet_trades(
    session: requests.Session,
    wallet: str,
    condition_ids: list[str],
    start_ts: Optional[int] = None,
) -> list[dict]:
    """
    分页拉取钱包的 TRADE / REDEEM / MERGE / SPLIT 记录。
    - 如果有 conditionId 列表，逐个精确拉取。
    - 否则拉取全量。
    """
    print(f"[2/6] 拉取钱包交易记录...")

    all_records: list[dict] = []

    targets: list[Optional[str]] = condition_ids if condition_ids else [None]

    for cid in targets:
        for act_type in ("TRADE", "REDEEM", "MERGE", "SPLIT"):
            label = f"{act_type}"
            if cid:
                label += f" (cid={cid[:16]}…)"
            print(f"  → {label}", end="  ", flush=True)

            offset = 0
            count = 0

            while True:
                if offset > MAX_OFFSET:
                    print(f"  ⚠ offset 达上限 {MAX_OFFSET}")
                    break

                params: dict[str, Any] = {
                    "user": wallet,
                    "limit": PAGE_SIZE,
                    "offset": offset,
                    "type": act_type,
                    "sortBy": "TIMESTAMP",
                    "sortDirection": "DESC",
                }
                if cid:
                    params["market"] = cid
                if start_ts:
                    params["start"] = start_ts

                _throttle()
                try:
                    resp = session.get(
                        f"{DATA_API}/activity", params=params, timeout=30
                    )
                    if resp.status_code == 400:
                        break
                    resp.raise_for_status()
                    page = resp.json()
                except requests.RequestException as exc:
                    print(f"请求失败: {exc}")
                    break

                if not page:
                    break

                all_records.extend(page)
                count += len(page)

                if len(page) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE

            print(f"{count} 条")

    # 去重
    seen: set[str] = set()
    unique: list[dict] = []
    for r in all_records:
        uid = (
            f"{r.get('transactionHash', '')}|{r.get('type', '')}|"
            f"{r.get('timestamp', '')}|{r.get('outcome', '')}|"
            f"{r.get('conditionId', '')}|{r.get('size', '')}"
        )
        if uid not in seen:
            seen.add(uid)
            unique.append(r)

    unique.sort(key=lambda x: x.get("timestamp", 0))
    print(f"  去重后: {len(unique)} 条")
    return unique


# ═══════════════════════════════════════════════════════════════════════════════
# Module 3 — filter_position_trades
# ═══════════════════════════════════════════════════════════════════════════════

def filter_position_trades(
    records: list[dict],
    position_name: str,
    condition_ids: list[str],
    tokens_map: dict[str, list[dict]] | None = None,
) -> dict[str, list[dict]]:
    """
    过滤记录并按 (conditionId, outcome) 分组，每组代表一个独立持仓。

    通过 asset(token_id) → outcome 映射精确归属 REDEEM 记录，
    避免缺失 outcome 的 REDEEM 被错分到另一侧。
    """
    print(f"[3/6] 过滤并分组持仓...")

    if condition_ids:
        cid_set = set(condition_ids)
        filtered = [r for r in records if r.get("conditionId", "") in cid_set]
        print(f"  conditionId 精确匹配: {len(records)} → {len(filtered)}")
    else:
        filtered = [
            r for r in records if title_matches(r.get("title", ""), position_name)
        ]
        print(f"  标题模糊匹配: {len(records)} → {len(filtered)}")

    if not filtered:
        titles = sorted({r.get("title", "") for r in records if r.get("title")})
        if titles:
            print("  ⚠ 未匹配任何记录。已拉取的部分市场标题：")
            for t in titles[:15]:
                print(f"    · {t}")
        return {}

    # ── 构建 asset(token_id) → outcome 映射 ──────────────────────────
    # 优先级：tokens_map (Gamma API 权威数据) → TRADE 记录 (实际交易)
    asset_to_outcome: dict[str, str] = {}
    if tokens_map:
        for _cid, tokens in tokens_map.items():
            for tk in tokens:
                if isinstance(tk, dict):
                    tid = tk.get("token_id", "")
                    oc = tk.get("outcome", "")
                    if tid and oc:
                        asset_to_outcome[tid] = oc
    for r in filtered:
        asset = r.get("asset", "")
        outcome = r.get("outcome", "")
        if asset and outcome:
            asset_to_outcome[asset] = outcome

    if asset_to_outcome:
        print(f"  asset→outcome 映射: {len(asset_to_outcome)} 条")
        for tid, oc in asset_to_outcome.items():
            print(f"    {tid[:30]}… → {oc}")

    def _resolve_outcome(r: dict) -> str:
        """获取记录的有效 outcome：先看原始字段，再查 asset 映射"""
        o = r.get("outcome", "")
        if not o:
            o = asset_to_outcome.get(r.get("asset", ""), "")
        return o

    # ── 分类记录 ───────────────────────────────────────────────────────
    # 1. MERGE/SPLIT: CTF 双边操作，等量消耗/创建每侧代币，USDC 平分
    # 2. REDEEM 无 outcome/asset: CTF redeemPositions 整市场结算，
    #    USDC 按各侧剩余持仓比例分配（赢方得 $1/份，输方得 $0）
    # 3. 普通记录: TRADE 或有明确归属的 REDEEM
    _BILATERAL_TYPES = {"MERGE", "SPLIT"}
    normal_records: list[dict] = []
    bilateral_records: list[dict] = []
    orphan_redeems: list[dict] = []

    for r in filtered:
        rec_type = r.get("type", "")
        if rec_type in _BILATERAL_TYPES:
            bilateral_records.append(r)
        elif rec_type == "REDEEM" and not _resolve_outcome(r):
            orphan_redeems.append(r)
        else:
            normal_records.append(r)

    if bilateral_records:
        print(f"  MERGE/SPLIT 记录: {len(bilateral_records)} 条")
    if orphan_redeems:
        print(f"  待归属 REDEEM 记录: {len(orphan_redeems)} 条")

    # ── 按 conditionId 聚合普通记录 ──────────────────────────────────
    by_cid: dict[str, list[dict]] = {}
    for r in normal_records:
        cid = r.get("conditionId", "unknown")
        by_cid.setdefault(cid, []).append(r)

    groups: dict[str, list[dict]] = {}
    for cid, cid_records in by_cid.items():
        known_outcomes = {
            _resolve_outcome(r) for r in cid_records if _resolve_outcome(r)
        }
        if len(known_outcomes) <= 1:
            outcome = known_outcomes.pop() if known_outcomes else "unknown"
            groups[f"{cid}|{outcome}"] = cid_records
        else:
            primary = max(
                known_outcomes,
                key=lambda o: sum(
                    1 for r in cid_records if _resolve_outcome(r) == o
                ),
            )
            resolved_count = 0
            fallback_count = 0
            for r in cid_records:
                o = _resolve_outcome(r)
                if not o:
                    o = primary
                    fallback_count += 1
                elif not r.get("outcome"):
                    resolved_count += 1
                groups.setdefault(f"{cid}|{o}", []).append(r)
            if resolved_count:
                print(f"  ✓ 通过 asset 映射修正了 {resolved_count} 条缺失 outcome 的记录")
            if fallback_count:
                print(f"  ⚠ {fallback_count} 条记录无法确定 outcome，回退到 primary={primary}")

    # ── 将 MERGE/SPLIT 复制到同 conditionId 的每个 outcome 组 ────────
    # MERGE 消耗等量的每侧代币返还 USDC, SPLIT 反之。
    # 每侧的 USDC 份额 = 总 usdcSize / outcome 数。
    for r in bilateral_records:
        cid = r.get("conditionId", "unknown")
        matching_keys = [k for k in groups if k.startswith(f"{cid}|")]
        num_outcomes = max(1, len(matching_keys))

        if not matching_keys:
            outcome = _resolve_outcome(r) or "unknown"
            matching_keys = [f"{cid}|{outcome}"]

        usdc_per_side = safe_float(r.get("usdcSize")) / num_outcomes
        for gkey in matching_keys:
            entry = dict(r)
            entry["_usdc_per_side"] = usdc_per_side
            groups.setdefault(gkey, []).append(entry)

        rec_type = r.get("type", "")
        print(f"  ✓ {rec_type} {safe_float(r.get('size')):.2f} 份"
              f" (${safe_float(r.get('usdcSize')):.2f}) → 分配到 {num_outcomes} 个 outcome"
              f" (每侧 ${usdc_per_side:.2f})")

    # ── 按剩余持仓比例分配无归属的 REDEEM ────────────────────────────
    # CTF 的 redeemPositions 是整市场结算，赢方 $1/份、输方 $0/份。
    # 通过各侧已知的 买入-卖出-合并 计算剩余持仓，按比例分配 USDC。
    for r in orphan_redeems:
        cid = r.get("conditionId", "unknown")
        matching_keys = [k for k in groups if k.startswith(f"{cid}|")]

        if len(matching_keys) <= 1:
            target = matching_keys[0] if matching_keys else f"{cid}|unknown"
            groups.setdefault(target, []).append(r)
            continue

        remaining_per_group: dict[str, float] = {}
        for gkey in matching_keys:
            remaining = 0.0
            for rec in groups[gkey]:
                rtype = rec.get("type", "TRADE")
                rside = rec.get("side", "")
                rsize = safe_float(rec.get("size"))
                if (rtype == "TRADE" and rside == "BUY") or rtype == "SPLIT":
                    remaining += rsize
                elif (rtype == "TRADE" and rside == "SELL") or rtype == "MERGE":
                    remaining -= rsize
            remaining_per_group[gkey] = max(0.0, remaining)

        total_remaining = sum(remaining_per_group.values())
        total_usdc = safe_float(r.get("usdcSize"))
        total_size = safe_float(r.get("size"))

        if total_remaining < 1e-6:
            largest = max(matching_keys, key=lambda k: len(groups[k]))
            groups[largest].append(r)
            print(f"  ⚠ REDEEM ${total_usdc:.2f} 无法按持仓分配，回退到最大组")
        else:
            attributed = 0
            for gkey in matching_keys:
                ratio = remaining_per_group[gkey] / total_remaining
                if ratio < 1e-6:
                    continue
                entry = dict(r)
                entry["size"] = total_size * ratio
                entry["usdcSize"] = total_usdc * ratio
                entry["_usdc_per_side"] = total_usdc * ratio
                groups[gkey].append(entry)
                attributed += 1
                _, oname = gkey.split("|", 1)
                print(f"    {oname}: 剩余 {remaining_per_group[gkey]:.2f} 份"
                      f" → 分得 REDEEM {total_size * ratio:.2f} 份"
                      f" (${total_usdc * ratio:.2f})")
            print(f"  ✓ REDEEM ${total_usdc:.2f} 按持仓比例分配到 {attributed} 个 outcome")

    # 每组按时间排序
    for trades in groups.values():
        trades.sort(key=lambda x: (x.get("timestamp", 0), x.get("type", "")))

    print(f"  发现 {len(groups)} 个持仓分组：")
    for key, trades in groups.items():
        _, outcome_part = key.split("|", 1)
        nb = sum(1 for t in trades if t.get("side") == "BUY")
        ns = sum(1 for t in trades if t.get("side") == "SELL")
        nr = sum(1 for t in trades if t.get("type") == "REDEEM")
        nm = sum(1 for t in trades if t.get("type") == "MERGE")
        nsp = sum(1 for t in trades if t.get("type") == "SPLIT")
        title = trades[0].get("title", "")[:50]
        counts = f"买{nb}/卖{ns}/赎回{nr}"
        if nm:
            counts += f"/合并{nm}"
        if nsp:
            counts += f"/拆分{nsp}"
        print(f"    [{outcome_part}] {len(trades)} 条 ({counts})  {title}")
    return groups


# ═══════════════════════════════════════════════════════════════════════════════
# Module 4 — build_trade_details (FIFO PnL)
# ═══════════════════════════════════════════════════════════════════════════════

def _empty_market_context() -> dict:
    """空的市场上下文结构，供后续 enrich 填充"""
    return {
        "market_price_before_trade": UNAVAILABLE,
        "market_price_after_trade": UNAVAILABLE,
        "best_bid_before": UNAVAILABLE,
        "best_ask_before": UNAVAILABLE,
        "spread_before": UNAVAILABLE,
        "mid_price_before": UNAVAILABLE,
        "last_price_before": UNAVAILABLE,
        "price_change_5m": UNAVAILABLE,
        "price_change_15m": UNAVAILABLE,
        "price_change_1h": UNAVAILABLE,
        "volume_5m": UNAVAILABLE,
        "volume_1h": UNAVAILABLE,
        "volume_24h": UNAVAILABLE,
        "liquidity_at_trade_time": UNAVAILABLE,
        "orderbook_depth_nearby": UNAVAILABLE,
        "price_percentile_in_last_24h": UNAVAILABLE,
        "intraday_high_before_trade": UNAVAILABLE,
        "intraday_low_before_trade": UNAVAILABLE,
    }


def build_trade_details(
    trades: list[dict], market_info: dict
) -> tuple[list[dict], float, float]:
    """
    按时间顺序处理每笔交易，用 FIFO 队列跟踪成本基础，
    计算持仓量、均价、逐笔已实现盈亏等。

    Returns:
        (trade_details, max_position_shares, max_capital_usdc)
    """
    sorted_trades = sorted(
        trades, key=lambda x: (x.get("timestamp", 0), x.get("type", ""))
    )

    details: list[dict] = []
    position_shares = 0.0
    fifo_lots: deque[dict] = deque()
    cumulative_pnl = 0.0
    max_position = 0.0
    max_capital = 0.0

    for i, raw in enumerate(sorted_trades):
        seq = i + 1
        rec_type = raw.get("type", "TRADE")
        side = raw.get("side", "")
        shares = safe_float(raw.get("size"))
        price = safe_float(raw.get("price"))
        usdc = safe_float(raw.get("usdcSize"))
        outcome = raw.get("outcome", "")
        ts = safe_int(raw.get("timestamp"))

        realized_pnl = 0.0

        # ── BUY ──────────────────────────────────────────────────────
        if rec_type == "TRADE" and side == "BUY":
            eff_price = (usdc / shares) if shares > 1e-12 else price
            fifo_lots.append({"shares": shares, "price": eff_price})
            position_shares += shares

        # ── SELL ─────────────────────────────────────────────────────
        elif rec_type == "TRADE" and side == "SELL":
            realized_pnl = _fifo_match(fifo_lots, shares, usdc)
            position_shares = max(0.0, position_shares - shares)
            cumulative_pnl += realized_pnl

        # ── SPLIT (拆分 USDC → 各侧代币，等同于 BUY) ────────────────
        elif rec_type == "SPLIT":
            split_cost = safe_float(raw.get("_usdc_per_side", usdc))
            eff_price = (split_cost / shares) if shares > 1e-12 else price
            fifo_lots.append({"shares": shares, "price": eff_price})
            position_shares += shares
            usdc = split_cost
            side = "SPLIT"

        # ── MERGE (合并各侧代币 → USDC，等同于 SELL) ────────────────
        elif rec_type == "MERGE":
            merge_proceeds = safe_float(raw.get("_usdc_per_side", usdc))
            realized_pnl = _fifo_match(fifo_lots, shares, merge_proceeds)
            position_shares = max(0.0, position_shares - shares)
            cumulative_pnl += realized_pnl
            usdc = merge_proceeds
            side = "MERGE"

        # ── REDEEM ───────────────────────────────────────────────────
        elif rec_type == "REDEEM":
            redeemed = shares if shares > 1e-12 else position_shares
            redeem_usdc = safe_float(raw.get("_usdc_per_side", usdc))
            realized_pnl = _fifo_match(fifo_lots, redeemed, redeem_usdc)
            position_shares = max(0.0, position_shares - redeemed)
            cumulative_pnl += realized_pnl
            usdc = redeem_usdc
            side = "REDEEM"

        # ── 衍生指标 ─────────────────────────────────────────────────
        max_position = max(max_position, position_shares)
        cur_capital = sum(lot["shares"] * lot["price"] for lot in fifo_lots)
        max_capital = max(max_capital, cur_capital)

        avg_entry = 0.0
        if position_shares > 1e-12:
            avg_entry = cur_capital / position_shares

        is_entry = side in ("BUY", "SPLIT")
        prior_entries = any(
            d["action_type"] in ("buy", "split") for d in details
        )
        is_opening = is_entry and not prior_entries
        is_add = is_entry and prior_entries
        is_exit = side in ("SELL", "MERGE", "REDEEM")
        is_reduce = is_exit and position_shares > 1e-3
        is_close = is_exit and position_shares < 1e-3

        if rec_type in ("REDEEM", "MERGE", "SPLIT"):
            action_type = rec_type.lower()
            trade_side_label = f"{rec_type.capitalize()} {outcome}"
        else:
            action_type = side.lower() if side else "unknown"
            trade_side_label = f"{side.capitalize()} {outcome}" if side else outcome

        details.append({
            "tx_hash": raw.get("transactionHash", UNAVAILABLE),
            "block_number": UNAVAILABLE,
            "timestamp": ts,
            "datetime_utc": ts_to_str(ts),
            "action_type": action_type,
            "trade_side_label": trade_side_label,
            "shares": round(shares, 6),
            "price": round(price, 6),
            "amount_usdc": round(usdc, 6),
            "fee": UNAVAILABLE,
            "net_amount": UNAVAILABLE,
            "remaining_position_after_trade": round(position_shares, 6),
            "avg_entry_price_after_trade": round(avg_entry, 6),
            "realized_pnl_from_this_trade": round(realized_pnl, 6),
            "cumulative_realized_pnl": round(cumulative_pnl, 6),
            "sequence_in_position": seq,
            "is_opening_trade": is_opening,
            "is_add_to_position": is_add,
            "is_reduce_trade": is_reduce,
            "is_close_trade": is_close,
            "market_context": _empty_market_context(),
        })

    return details, round(max_position, 6), round(max_capital, 6)


def _fifo_match(lots: deque[dict], shares_sold: float, proceeds: float) -> float:
    """FIFO 匹配卖出份额，返回 realized PnL = 收入 − 成本基础"""
    remaining = shares_sold
    cost_basis = 0.0
    while remaining > 1e-12 and lots:
        lot = lots[0]
        matched = min(lot["shares"], remaining)
        cost_basis += matched * lot["price"]
        lot["shares"] -= matched
        remaining -= matched
        if lot["shares"] < 1e-12:
            lots.popleft()
    return proceeds - cost_basis


# ═══════════════════════════════════════════════════════════════════════════════
# Module 5 — build_position_summary
# ═══════════════════════════════════════════════════════════════════════════════

def build_position_summary(
    trade_details: list[dict],
    market_info: dict,
    max_position: float,
    max_capital: float,
) -> dict:
    if not trade_details:
        return {}

    buys = [t for t in trade_details if t["action_type"] == "buy"]
    sells = [t for t in trade_details if t["action_type"] == "sell"]
    redeems = [t for t in trade_details if t["action_type"] == "redeem"]
    merges = [t for t in trade_details if t["action_type"] == "merge"]
    splits = [t for t in trade_details if t["action_type"] == "split"]
    entries = buys + splits
    exits = sells + redeems + merges

    total_bought = sum(t["shares"] for t in buys)
    total_sold = sum(t["shares"] for t in sells)
    total_redeemed = sum(t["shares"] for t in redeems)
    total_merged = sum(t["shares"] for t in merges)
    total_split = sum(t["shares"] for t in splits)
    net_shares = total_bought + total_split - total_sold - total_redeemed - total_merged

    gross_buy = sum(t["amount_usdc"] for t in buys)
    gross_sell = sum(t["amount_usdc"] for t in sells)
    gross_redeem = sum(t["amount_usdc"] for t in redeems)
    gross_merge = sum(t["amount_usdc"] for t in merges)
    gross_split = sum(t["amount_usdc"] for t in splits)
    gross_entry = gross_buy + gross_split

    avg_buy_price = (gross_entry / (total_bought + total_split)) if (total_bought + total_split) > 0 else 0
    avg_sell_price = (gross_sell / total_sold) if total_sold > 0 else 0

    final_pnl = trade_details[-1]["cumulative_realized_pnl"]

    # 时间维度
    entry_ts = [t["timestamp"] for t in entries if t["timestamp"]]
    exit_ts = [t["timestamp"] for t in exits if t["timestamp"]]
    all_ts = [t["timestamp"] for t in trade_details if t["timestamp"]]

    first_entry = min(entry_ts) if entry_ts else 0
    last_entry = max(entry_ts) if entry_ts else 0
    first_exit = min(exit_ts) if exit_ts else 0
    last_exit = max(exit_ts) if exit_ts else 0
    holding = (max(all_ts) - min(all_ts)) if len(all_ts) > 1 else 0

    roi = (final_pnl / gross_entry) if gross_entry > 0 else 0
    won = "won" if final_pnl > 0.01 else ("lost" if final_pnl < -0.01 else "breakeven")

    held_to_res = len(redeems) > 0
    closed_before = not held_to_res and abs(net_shares) < 1e-3

    scaled_in = len(entries) > 1
    scaled_out = len(exits) > 1

    # 轮次统计
    round_trips = 0
    pos = 0.0
    for t in trade_details:
        if t["action_type"] in ("buy", "split"):
            pos += t["shares"]
        elif t["action_type"] in ("sell", "redeem", "merge"):
            pos = max(0.0, pos - t["shares"])
            if pos < 1e-3:
                round_trips += 1

    return {
        "first_entry_time": ts_to_str(first_entry),
        "last_entry_time": ts_to_str(last_entry),
        "first_exit_time": ts_to_str(first_exit),
        "last_exit_time": ts_to_str(last_exit),
        "holding_duration_seconds": holding,
        "number_of_buys": len(buys),
        "number_of_sells": len(sells),
        "number_of_redeems": len(redeems),
        "number_of_merges": len(merges),
        "number_of_splits": len(splits),
        "total_shares_bought": round(total_bought, 6),
        "total_shares_sold": round(total_sold, 6),
        "total_shares_redeemed": round(total_redeemed, 6),
        "total_shares_merged": round(total_merged, 6),
        "total_shares_split": round(total_split, 6),
        "net_shares": round(net_shares, 6),
        "gross_buy_amount": round(gross_buy, 6),
        "gross_sell_amount": round(gross_sell, 6),
        "gross_redeem_amount": round(gross_redeem, 6),
        "gross_merge_amount": round(gross_merge, 6),
        "gross_split_amount": round(gross_split, 6),
        "avg_buy_price": round(avg_buy_price, 6),
        "avg_sell_price": round(avg_sell_price, 6),
        "max_position_size_shares": max_position,
        "max_capital_deployed_usdc": max_capital,
        "final_realized_pnl": round(final_pnl, 6),
        "unrealized_pnl_at_snapshot": UNAVAILABLE,
        "roi_realized": round(roi, 6),
        "roi_total": round(roi, 6),
        "won_or_lost": won,
        "closed_before_resolution": closed_before,
        "held_to_resolution": held_to_res,
        "scaled_in": scaled_in,
        "scaled_out": scaled_out,
        "round_trip_count": round_trips,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Module 6 — enrich_market_context (价格历史)
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_price_history(
    session: requests.Session,
    token_id: str,
) -> list[dict]:
    """
    从 CLOB API 获取价格历史。
    返回 [{"t": unix_ts, "p": float_price}, ...] 按时间升序。
    """
    if not token_id or token_id == UNAVAILABLE:
        return []

    _throttle()
    try:
        resp = session.get(
            f"{CLOB_API}/prices-history",
            params={"market": token_id, "interval": "all", "fidelity": 1},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        history = data.get("history", data) if isinstance(data, dict) else data
        if isinstance(history, list):
            history.sort(key=lambda x: x.get("t", 0))
            return history
    except Exception as exc:
        print(f"  ⚠ 价格历史获取失败: {exc}")
    return []


def _bisect_price(history: list[dict], target_ts: int) -> Optional[float]:
    """二分查找 target_ts 之前最近的价格点"""
    if not history:
        return None
    lo, hi = 0, len(history) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if history[mid].get("t", 0) <= target_ts:
            lo = mid
        else:
            hi = mid - 1
    pt = history[lo]
    if pt.get("t", 0) <= target_ts:
        return safe_float(pt.get("p"))
    return None


def _prices_in_range(
    history: list[dict], lo_ts: int, hi_ts: int
) -> list[float]:
    return [
        safe_float(h.get("p"))
        for h in history
        if lo_ts <= h.get("t", 0) <= hi_ts
    ]


def enrich_market_context(
    trade_details: list[dict],
    session: requests.Session,
    token_id: str,
) -> list[dict]:
    """
    尝试用 CLOB 价格历史填充每笔交易的 market_context。
    当 API 无数据时，字段保持 unavailable（不会报错）。
    """
    print(f"[5/6] 获取市场价格上下文...")

    if not token_id or token_id == UNAVAILABLE:
        print("  ⚠ 无 token_id，跳过价格上下文")
        return trade_details

    history = _fetch_price_history(session, token_id)
    if not history:
        print("  ⚠ 无价格历史数据，跳过")
        return trade_details
    print(f"  获取 {len(history)} 个价格数据点")

    for td in trade_details:
        ts = td["timestamp"]
        if not ts:
            continue

        ctx = td["market_context"]

        p_before = _bisect_price(history, ts - 1)
        p_after = _bisect_price(history, ts + 60)
        if p_before is not None:
            ctx["market_price_before_trade"] = round(p_before, 6)
        if p_after is not None:
            ctx["market_price_after_trade"] = round(p_after, 6)

        for label, delta in [
            ("price_change_5m", 300),
            ("price_change_15m", 900),
            ("price_change_1h", 3600),
        ]:
            p_prev = _bisect_price(history, ts - delta)
            p_now = _bisect_price(history, ts)
            if p_prev and p_now and p_prev > 1e-9:
                ctx[label] = round((p_now - p_prev) / p_prev, 6)

        prices_24h = _prices_in_range(history, ts - 86400, ts)
        if prices_24h:
            ctx["intraday_high_before_trade"] = round(max(prices_24h), 6)
            ctx["intraday_low_before_trade"] = round(min(prices_24h), 6)
            if p_before is not None:
                below = sum(1 for p in prices_24h if p <= p_before)
                ctx["price_percentile_in_last_24h"] = round(
                    below / max(1, len(prices_24h)), 4
                )

    return trade_details


# ═══════════════════════════════════════════════════════════════════════════════
# Module 7 — compute_ai_features (AI 友好衍生特征)
# ═══════════════════════════════════════════════════════════════════════════════

def compute_ai_features(
    trade_details: list[dict],
    summary: dict,
) -> dict:
    buys = [t for t in trade_details if t["action_type"] == "buy"]
    sells = [t for t in trade_details if t["action_type"] == "sell"]
    redeems = [t for t in trade_details if t["action_type"] == "redeem"]
    merges = [t for t in trade_details if t["action_type"] == "merge"]
    all_exits = sells + redeems + merges

    # ── entry_style ──────────────────────────────────────────────────
    if not buys:
        entry_style = "no-entry"
    elif len(buys) == 1:
        entry_style = "single-shot"
    else:
        prices = [b["price"] for b in buys]
        up = all(prices[i] <= prices[i + 1] + 1e-6 for i in range(len(prices) - 1))
        down = all(prices[i] >= prices[i + 1] - 1e-6 for i in range(len(prices) - 1))
        if up:
            entry_style = "chase"
        elif down:
            entry_style = "dip-buy"
        else:
            entry_style = "scale-in"

    # ── exit_style ───────────────────────────────────────────────────
    if redeems:
        exit_style = "hold-to-resolution"
    elif merges and not sells:
        exit_style = "merge-exit"
    elif not all_exits:
        exit_style = "no-exit-yet"
    elif len(all_exits) == 1:
        s = all_exits[0]
        if s["is_close_trade"]:
            avg_e = buys[-1]["avg_entry_price_after_trade"] if buys else 0
            exit_style = (
                "full-close-profit" if s["price"] > avg_e else "stop-out"
            )
        else:
            exit_style = "partial-take-profit"
    else:
        any_close = any(s["is_close_trade"] for s in all_exits)
        exit_style = "scale-out" if not any_close else "partial-take-profit"

    # ── risk_style ───────────────────────────────────────────────────
    capital = summary.get("max_capital_deployed_usdc", 0)
    if capital > 0:
        roi = abs(summary.get("roi_realized", 0))
        risk_style = (
            "conservative" if roi < 0.05
            else "moderate" if roi < 0.20
            else "aggressive"
        )
    else:
        risk_style = "unknown"

    # ── averaging ────────────────────────────────────────────────────
    is_avg_down = is_avg_up = False
    if len(buys) >= 2:
        half = len(buys) // 2
        avg_first = sum(b["price"] for b in buys[:half]) / max(1, half)
        avg_second = sum(b["price"] for b in buys[half:]) / max(
            1, len(buys) - half
        )
        if avg_second < avg_first * 0.95:
            is_avg_down = True
        elif avg_second > avg_first * 1.05:
            is_avg_up = True

    # ── timing ───────────────────────────────────────────────────────
    buy_timestamps = [b["timestamp"] for b in buys if b["timestamp"]]
    if len(buy_timestamps) >= 2:
        intervals = [
            buy_timestamps[i + 1] - buy_timestamps[i]
            for i in range(len(buy_timestamps) - 1)
        ]
        avg_interval = sum(intervals) / len(intervals)
        if avg_interval < 60:
            time_dist = "rapid-fire (<1min)"
        elif avg_interval < 300:
            time_dist = "clustered (1-5min)"
        elif avg_interval < 3600:
            time_dist = "spaced (5-60min)"
        else:
            time_dist = "patient (>1h)"
    else:
        avg_interval = 0.0
        time_dist = "single-entry"

    # ── timing_quality ───────────────────────────────────────────────
    roi_val = summary.get("roi_realized", 0)
    if roi_val > 0.5:
        timing_q = "excellent"
    elif roi_val > 0.2:
        timing_q = "good"
    elif roi_val > 0:
        timing_q = "fair"
    elif roi_val > -0.1:
        timing_q = "poor"
    else:
        timing_q = "bad"

    total_trades = len(trade_details)
    total_usdc = sum(t["amount_usdc"] for t in trade_details)

    return {
        "entry_style": entry_style,
        "exit_style": exit_style,
        "risk_style": risk_style,
        "timing_quality": timing_q,
        "is_averaging_down": is_avg_down,
        "is_averaging_up": is_avg_up,
        "buy_time_distribution": time_dist,
        "average_time_between_buys_seconds": round(avg_interval, 2),
        "total_trade_count": total_trades,
        "buy_sell_ratio": round(len(buys) / max(1, len(sells) + len(redeems)), 4),
        "avg_trade_size_usdc": round(total_usdc / max(1, total_trades), 4),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Module 8 — export_json
# ═══════════════════════════════════════════════════════════════════════════════

def export_json(result: dict, output_path: str) -> None:
    print(f"[6/6] 导出 JSON → {output_path}")
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(result, fh, ensure_ascii=False, indent=2)
    size_kb = len(json.dumps(result, ensure_ascii=False).encode()) / 1024
    print(f"  ✓ {size_kb:.1f} KB")


# ═══════════════════════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════════════════════

def _process_position_group(
    key: str,
    trades: list[dict],
    market_info: dict,
    session: requests.Session,
) -> dict:
    """处理一个 (conditionId|outcome) 分组，返回完整持仓分析对象。"""
    cid_part, outcome_part = key.split("|", 1)

    print(f"\n{'─' * 70}")
    print(f"  处理持仓: [{outcome_part}] ({len(trades)} 条)")
    print(f"{'─' * 70}")

    # 复制并补全 market_info
    mi = {k: v for k, v in market_info.items() if not k.startswith("_")}
    mi["outcome_name"] = outcome_part
    mi["position_side"] = outcome_part
    mi["market_id"] = cid_part

    # token_id — 优先从原始记录的 asset 字段提取
    token_ids = list({t.get("asset", "") for t in trades if t.get("asset")})
    token_id = token_ids[0] if token_ids else UNAVAILABLE

    # 回退：从 market_info 的 _tokens_map 或 Gamma API 获取
    if token_id == UNAVAILABLE:
        tokens_map = market_info.get("_tokens_map", {})
        tokens = tokens_map.get(cid_part, [])
        if not tokens:
            detail = _lookup_market_by_cid(session, cid_part)
            if detail:
                tokens = detail.get("tokens", [])
        if tokens:
            for tk in tokens:
                if isinstance(tk, dict):
                    tk_outcome = tk.get("outcome", "")
                    if tk_outcome.lower() == outcome_part.lower() or not outcome_part:
                        token_id = tk.get("token_id", UNAVAILABLE)
                        break
            if token_id == UNAVAILABLE and tokens:
                token_id = tokens[0].get("token_id", UNAVAILABLE)
            if token_id != UNAVAILABLE:
                print(f"  ✓ 通过 Gamma API 补全 token_id: {token_id[:30]}...")
    mi["token_id"] = token_id

    # market title — 从原始记录取
    titles = list({t.get("title", "") for t in trades if t.get("title")})
    if titles:
        mi["market_question"] = titles[0]

    # position_status
    _entry_types = {"BUY", "SPLIT"}
    _exit_types = {"SELL", "MERGE"}
    nb = sum(safe_float(t.get("size")) for t in trades if t.get("side") in _entry_types or t.get("type") == "SPLIT")
    ns = sum(safe_float(t.get("size")) for t in trades if t.get("side") in _exit_types or t.get("type") == "MERGE")
    nr = sum(
        safe_float(t.get("size")) for t in trades if t.get("type") == "REDEEM"
    )
    mi["position_status"] = "open" if (nb - ns - nr) > 0.001 else "closed"

    # Step 4
    print("[4/6] 处理逐笔交易明细 (FIFO)...")
    details, max_pos, max_cap = build_trade_details(trades, mi)
    print(f"  {len(details)} 笔交易已处理")

    # Step 5
    enrich_market_context(details, session, token_id)

    # Step 6 (summary)
    summary = build_position_summary(details, mi, max_pos, max_cap)
    ai = compute_ai_features(details, summary)
    summary["ai_analysis_features"] = ai

    return {"market_info": mi, "trades": details, "position_summary": summary}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Polymarket 钱包持仓深度分析",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python position_deep_fetcher.py \\
      -w 0x2cad53bb58c266ea91eea0d7ca54303a10bceb66 \\
      -p "Bitcoin Up or Down - April 7, 12:15PM-12:20PM ET" \\
      -s 2026-04-07

  python position_deep_fetcher.py \\
      --wallet 0xABC...DEF \\
      --position "Will Trump win?" \\
      --output my_analysis.json
""",
    )
    parser.add_argument("-w", "--wallet", required=True, help="目标钱包地址")
    parser.add_argument("-p", "--position", required=True, help="持仓/市场名称关键词")
    parser.add_argument("-s", "--start-date", default="", help="起始日期 YYYY-MM-DD（可选）")
    parser.add_argument("-o", "--output", default="", help="输出 JSON 路径（可选）")

    args = parser.parse_args()
    wallet = args.wallet
    position_name = args.position
    start_ts = parse_date_to_ts(args.start_date)

    output_path = args.output
    if not output_path:
        safe = "".join(
            c if c.isalnum() or c in "-_" else "_" for c in position_name
        )[:50]
        output_path = f"position_analysis_{safe}.json"

    print("=" * 70)
    print("  Polymarket 持仓深度分析")
    print("=" * 70)
    print(f"  钱包  : {wallet}")
    print(f"  持仓  : {position_name}")
    print(f"  起始  : {args.start_date or '不限'}")
    print(f"  输出  : {output_path}")
    print("=" * 70 + "\n")

    session = create_session()

    # ── Step 1 ────────────────────────────────────────────────────────
    market_info = fetch_market_info(session, position_name)
    condition_ids: list[str] = market_info.get("_condition_ids", [])

    # ── Step 2 ────────────────────────────────────────────────────────
    records = fetch_wallet_trades(session, wallet, condition_ids, start_ts)
    if not records:
        print("\n❌ 未找到任何交易记录，请检查钱包地址及日期。")
        sys.exit(0)

    # ── Step 3 ────────────────────────────────────────────────────────
    tokens_map: dict[str, list[dict]] = market_info.get("_tokens_map", {})
    groups = filter_position_trades(
        records, position_name, condition_ids, tokens_map
    )
    if not groups:
        print("\n❌ 未找到匹配的持仓记录。")
        sys.exit(0)

    # ── TWO-PASS: 从已匹配记录中提取 conditionId，精确回拉完整数据 ───
    #   场景：搜索失败（422 等）→ 全量拉取被 offset 截断 → 只拿到部分记录
    #   此时从已匹配的记录中提取 conditionId，再次精确拉取该市场全部记录
    if not condition_ids:
        discovered: set[str] = set()
        for trades in groups.values():
            for t in trades:
                cid = t.get("conditionId")
                if cid:
                    discovered.add(cid)
        if discovered:
            condition_ids = list(discovered)
            print(f"\n  ⚡ 从匹配记录中发现 {len(condition_ids)} 个 conditionId，精确回拉完整数据...")
            records = fetch_wallet_trades(
                session, wallet, condition_ids, start_ts=None
            )
            tokens_map = market_info.get("_tokens_map", {})
            groups = filter_position_trades(
                records, position_name, condition_ids, tokens_map
            )
            if not groups:
                print("\n❌ 精确回拉后未找到匹配记录。")
                sys.exit(0)

            # 补全 market_info（用 Gamma API 按 conditionId 查询）
            if market_info.get("market_question") == UNAVAILABLE:
                for cid in condition_ids:
                    detail = _lookup_market_by_cid(session, cid)
                    if detail:
                        market_info["market_question"] = detail.get(
                            "question", UNAVAILABLE
                        )
                        market_info["market_slug"] = detail.get(
                            "slug", UNAVAILABLE
                        )
                        market_info["market_category"] = detail.get(
                            "category", UNAVAILABLE
                        )
                        market_info["market_status"] = _resolve_status(detail)
                        market_info["_tokens_map"][cid] = detail.get(
                            "tokens", []
                        )
                        for src, dst in [
                            ("startDate", "market_open_time"),
                            ("endDate", "market_close_time"),
                            ("resolvedAt", "market_resolve_time"),
                        ]:
                            v = detail.get(src)
                            if v:
                                market_info[dst] = v
                        res = detail.get("resolution")
                        if res is not None:
                            market_info["resolution_result"] = str(res)
                        print(f"  ✓ 通过 conditionId 补全市场信息: {market_info['market_question'][:50]}")
                        break

    # ── Step 4-6: 处理每个持仓 ────────────────────────────────────────
    positions = [
        _process_position_group(key, trades, market_info, session)
        for key, trades in groups.items()
    ]

    # ── 组装最终输出 ──────────────────────────────────────────────────
    result: dict[str, Any] = {
        "meta": {
            "generated_at": ts_to_iso(int(time.time())),
            "wallet_address": wallet,
            "position_query": position_name,
            "script_version": SCRIPT_VERSION,
            "total_positions_found": len(positions),
        },
    }
    if len(positions) == 1:
        result.update(positions[0])
    else:
        result["positions"] = positions

    export_json(result, output_path)

    # ── 控制台摘要 ────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print("  ✓ 分析完成")
    print(f"{'=' * 70}")
    for pos in positions:
        mi = pos["market_info"]
        ps = pos["position_summary"]
        ai = ps.get("ai_analysis_features", {})
        pnl = ps.get("final_realized_pnl", 0)
        sign = "+" if pnl >= 0 else ""
        print(f"\n  [{mi.get('outcome_name', '?')}] {mi.get('market_question', '')[:50]}")
        trade_counts = (
            f"{ps['number_of_buys']}买 / {ps['number_of_sells']}卖 / "
            f"{ps['number_of_redeems']}赎回"
        )
        if ps.get('number_of_merges', 0):
            trade_counts += f" / {ps['number_of_merges']}合并"
        if ps.get('number_of_splits', 0):
            trade_counts += f" / {ps['number_of_splits']}拆分"
        print(f"    交易     : {trade_counts}")
        print(f"    买入总额 : ${ps['gross_buy_amount']:,.2f}")
        print(f"    已实现PnL: {sign}${pnl:,.2f} ({ps['won_or_lost']})")
        print(f"    ROI      : {ps['roi_realized']:.2%}")
        print(f"    建仓方式 : {ai.get('entry_style', '?')}")
        print(f"    平仓方式 : {ai.get('exit_style', '?')}")
        print(f"    风险风格 : {ai.get('risk_style', '?')}")
        print(f"    时机质量 : {ai.get('timing_quality', '?')}")
    print(f"\n  输出文件: {output_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
