"""
Polymarket 交易者行为可视化（交互式 HTML）
==========================================

读取 position_deep_fetcher.py 输出的 JSON，生成可悬停查看明细的 HTML：
    · 两条市场价格曲线（每个 outcome 一条）
    · 散点：用户每笔买入（颜色=方向，点大小∝股数）
    · 紫色虚线 + 钻石标记：每次合并（merge）

悬停明细：
    买入点 → 时间 / 成交价 / 本笔股数 / 累计股数 / 累计金额 / 持仓均价
    合并点 → 时间 / Down 市价 / Up 市价 / 合并股数

依赖:
    pip install plotly

用法:
    python trader_visualizer.py xxx.json              # 输出 xxx.html
    python trader_visualizer.py xxx.json -o chart.html
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import plotly.graph_objects as go

# 每个 outcome 的颜色
COLOR = {
    "Down": "#e74c3c",
    "Up": "#3498db",
    "Yes": "#2ecc71",
    "No": "#e67e22",
    "Over": "#2ecc71",
    "Under": "#e74c3c",
}
MERGE_COLOR = "#9b59b6"  # 紫色


def _positions(data: dict) -> list[dict]:
    return data.get("positions") or [data]


def _to_dt(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _build_price_series(positions: list[dict]) -> dict[str, list[tuple]]:
    """
    用所有 outcome 的 buy/sell 重建市场价时间序列。
    - 同侧成交：直接 price
    - 对手侧：1 - price 反推（二元市场两边互补）
    """
    by_oc: dict[str, dict[int, float]] = {
        p["market_info"]["outcome_name"]: {} for p in positions
    }
    for p in positions:
        my_oc = p["market_info"]["outcome_name"]
        for t in p["trades"]:
            if t["action_type"] not in ("buy", "sell"):
                continue
            ts = t["timestamp"]
            price = t["price"]
            if not ts or price <= 0 or price >= 1:
                continue
            by_oc[my_oc].setdefault(ts, price)
            for other in by_oc:
                if other != my_oc:
                    by_oc[other].setdefault(ts, round(1 - price, 6))
    return {
        oc: [(_to_dt(ts), p) for ts, p in sorted(pts.items())]
        for oc, pts in by_oc.items()
    }


def _collect_merges(positions: list[dict]) -> list[dict]:
    """
    按 timestamp 聚合两侧的 merge 记录，返回带每侧市价 + 股数的字典。
    """
    by_ts: dict[int, dict] = {}
    for p in positions:
        oc = p["market_info"]["outcome_name"]
        for t in p["trades"]:
            if t["action_type"] != "merge":
                continue
            ts = t["timestamp"]
            entry = by_ts.setdefault(ts, {"timestamp": ts, "outcomes": {}})
            ctx = t.get("market_context", {})
            mp = ctx.get("market_price_before_trade")
            entry["outcomes"][oc] = {
                "shares": t["shares"],
                "market_price": mp if isinstance(mp, (int, float)) else None,
            }
    return [by_ts[k] for k in sorted(by_ts.keys())]


def visualize(json_path: str, output: str | None = None) -> None:
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    positions = _positions(data)
    series = _build_price_series(positions)
    merges = _collect_merges(positions)

    fig = go.Figure()

    # ── 1. 市场价格曲线 ──────────────────────────────────────────
    for oc, points in series.items():
        if not points:
            continue
        fig.add_trace(go.Scatter(
            x=[t for t, _ in points],
            y=[p for _, p in points],
            mode="lines",
            name=f"{oc} 市场价",
            line=dict(color=COLOR.get(oc, "gray"), width=1.6),
            opacity=0.4,
            hoverinfo="skip",
            legendrank=1,
        ))

    # ── 2. 买入点（含详细 hover） ────────────────────────────────
    for p in positions:
        oc = p["market_info"]["outcome_name"]
        buys = [t for t in p["trades"] if t["action_type"] == "buy"]
        if not buys:
            continue

        cum_shares = 0.0
        cum_usdc = 0.0
        rows = []
        for b in buys:
            cum_shares += b["shares"]
            cum_usdc += b["amount_usdc"]
            rows.append([
                b["shares"],
                b["amount_usdc"],
                cum_shares,
                cum_usdc,
                b["avg_entry_price_after_trade"],
            ])

        # marker 大小：股数开方再 clamp
        sizes = [max(6, min(28, b["shares"] ** 0.5 + 4)) for b in buys]

        fig.add_trace(go.Scatter(
            x=[_to_dt(b["timestamp"]) for b in buys],
            y=[b["price"] for b in buys],
            mode="markers",
            name=f"买入 {oc} ({len(buys)} 笔)",
            marker=dict(
                size=sizes,
                color=COLOR.get(oc, "gray"),
                line=dict(width=0.6, color="white"),
                opacity=0.78,
            ),
            customdata=rows,
            hovertemplate=(
                f"<b>买入 {oc}</b><br>"
                "时间: %{x|%H:%M:%S}<br>"
                "成交价: $%{y:.4f}<br>"
                "本笔股数: %{customdata[0]:.2f}<br>"
                "本笔金额: $%{customdata[1]:.2f}<br>"
                "─────────<br>"
                "累计买入股数: %{customdata[2]:.2f}<br>"
                "累计买入金额: $%{customdata[3]:.2f}<br>"
                "持仓均价: $%{customdata[4]:.4f}"
                "<extra></extra>"
            ),
            legendrank=2,
        ))

    # ── 3. 合并：紫色加粗虚线 + 顶端钻石标记（含 hover）────────
    if merges:
        for m in merges:
            fig.add_vline(
                x=_to_dt(m["timestamp"]),
                line=dict(color=MERGE_COLOR, width=2, dash="dash"),
                opacity=0.55,
            )

        # 钻石标记（顶部，便于悬停）
        merge_x = [_to_dt(m["timestamp"]) for m in merges]
        merge_y = [1.04] * len(merges)
        merge_custom = []
        cum_shares = 0.0
        total_shares_all = sum(
            max(v["shares"] for v in m["outcomes"].values()) for m in merges
        )
        for idx, m in enumerate(merges, start=1):
            ocs = m["outcomes"]
            this_shares = max(v["shares"] for v in ocs.values())
            cum_shares += this_shares
            shares_set = {round(v["shares"], 4) for v in ocs.values()}
            shares_str = (
                f"{this_shares:.2f}" if len(shares_set) == 1
                else " / ".join(f"{v['shares']:.2f}" for v in ocs.values())
            )
            outcomes_str = " · ".join(
                f"{name}=${v['market_price']:.3f}" if v.get("market_price") is not None else f"{name}=?"
                for name, v in ocs.items()
            )
            merge_custom.append([
                shares_str,
                outcomes_str,
                cum_shares,
                idx,
                len(merges),
                total_shares_all,
            ])

        fig.add_trace(go.Scatter(
            x=merge_x,
            y=merge_y,
            mode="markers",
            name=f"合并 ({len(merges)} 次)",
            marker=dict(
                symbol="diamond",
                size=11,
                color=MERGE_COLOR,
                line=dict(width=1, color="white"),
            ),
            customdata=merge_custom,
            hovertemplate=(
                "<b>合并 (Merge) %{customdata[3]}/%{customdata[4]}</b><br>"
                "时间: %{x|%H:%M:%S}<br>"
                "本次合并股数: %{customdata[0]}<br>"
                "合并时市价: %{customdata[1]}<br>"
                "─────────<br>"
                "累计合并股数: %{customdata[2]:.2f}<br>"
                "全部合并总股数: %{customdata[5]:.2f}"
                "<extra></extra>"
            ),
            legendrank=3,
        ))

    # ── 4. 整体布局 ───────────────────────────────────────────────
    title = positions[0]["market_info"].get("market_question", "")
    wallet = (data.get("meta") or {}).get("wallet_address", "")
    if wallet:
        title += f"<br><sub>钱包 {wallet[:10]}…{wallet[-4:]}</sub>"

    fig.update_layout(
        title=dict(text=title, x=0.5, xanchor="center", font=dict(size=14)),
        xaxis=dict(title="时间 (UTC)", showgrid=True, gridcolor="#ecf0f1"),
        yaxis=dict(
            title="价格（= 该侧获胜概率）",
            tickformat=".0%",
            range=[-0.05, 1.08],
            showgrid=True, gridcolor="#ecf0f1",
        ),
        hovermode="closest",
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(
            x=1.01, y=1, xanchor="left", yanchor="top",
            bgcolor="rgba(255,255,255,0.9)", bordercolor="#bdc3c7", borderwidth=1,
        ),
        margin=dict(l=60, r=180, t=80, b=60),
        height=620,
    )

    if output is None:
        out_dir = Path("analysis_output")
        out_dir.mkdir(parents=True, exist_ok=True)
        output = str(out_dir / Path(json_path).with_suffix(".html").name)
    fig.write_html(output, include_plotlyjs="cdn")
    print(f"✓ 已生成: {output}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("json", help="position_deep_fetcher 输出的 JSON 文件")
    ap.add_argument("-o", "--output", default=None, help="输出 HTML 路径（默认同名 .html）")
    args = ap.parse_args()
    visualize(args.json, args.output)


if __name__ == "__main__":
    main()
