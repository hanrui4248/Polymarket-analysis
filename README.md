# Polymarket 分析工具

两个互相协作的 Python 脚本，抓取 Polymarket 公开 API 的交易数据，计算 FIFO 盈亏，并输出对 AI 友好的结构化 JSON，用于做钱包/持仓层面的交易策略画像。

- [position_deep_fetcher.py](position_deep_fetcher.py) — **单持仓**深度分析（某钱包 × 某个市场）
- [wallet_strategy_analyzer.py](wallet_strategy_analyzer.py) — **整钱包**策略画像（最近 N 个市场）

两个脚本都不读写链上、不需要任何 API Key，只读 Polymarket 三个公开端点：

| 端点 | 用途 |
| --- | --- |
| `gamma-api.polymarket.com` | 市场元信息、conditionId、tokens |
| `data-api.polymarket.com`  | 钱包 `/activity`（TRADE / REDEEM / MERGE / SPLIT）、持仓估值 `/value` |
| `clob.polymarket.com`       | `/prices-history` 价格曲线、`/markets/{cid}` 市场细节 |

---

## 安装

```bash
pip install -r requirements.txt
```

依赖仅 `requests` + `urllib3`，Python 3.9+。

---

## 脚本 1：`position_deep_fetcher.py`

### 它做什么

针对**一个钱包 × 一个具体市场**，把所有相关交易（含 CTF 原生的 MERGE/SPLIT/REDEEM）拉下来，按时间轴逐笔做 FIFO 成本匹配，并附上每笔交易发生时的盘口价格上下文。

### 主要流程（脚本内打印的 6 步）

1. `fetch_market_info` — 从 Gamma `/public-search` 搜索市场，拿到 `conditionId` 和 `tokens`（asset ↔ outcome 映射）。搜索词会在原始名、去特殊字符、截短之间降级重试，绕开 422。
2. `fetch_wallet_trades` — 分页拉 `/activity`，按 `conditionId` 精确过滤，覆盖 TRADE / REDEEM / MERGE / SPLIT 四种类型，自动去重。
3. `filter_position_trades` — 按 `(conditionId, outcome)` 分组。MERGE/SPLIT 均分到同市场每个 outcome，缺失 `outcome` 的 REDEEM 按各侧剩余持仓比例回填。
4. `build_trade_details` — FIFO 队列处理买卖，REDEEM 视作按剩余份数结算；每笔记录都写入 `realized_pnl`、`cumulative_realized_pnl`、`remaining_position_after_trade`、`avg_entry_price_after_trade`、`is_opening_trade` 等。
5. `enrich_market_context` — 用 CLOB `/prices-history` 填充每笔交易的 `market_price_before_trade`、`price_change_5m/15m/1h`、`intraday_high/low`、`price_percentile_in_last_24h` 等。
6. `build_position_summary` + `compute_ai_features` — 汇总持仓级指标（胜负、ROI、仓位时长、加仓/减仓行为），并输出语义化标签：`entry_style`（single-shot / scale-in / chase / dip-buy）、`exit_style`（full-close-profit / stop-out / hold-to-resolution / merge-exit…）、`risk_style`、`timing_quality`。

### 用法

```bash
python position_deep_fetcher.py \
    -w 0x2cad53bb58c266ea91eea0d7ca54303a10bceb66 \
    -p "Bitcoin Up or Down - April 7, 12:15PM-12:20PM ET" \
    -s 2026-04-07 \
    -o my_analysis.json
```

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `-w, --wallet` | ✅ | 目标钱包地址（0x…） |
| `-p, --position` | ✅ | 市场名称关键词（模糊匹配） |
| `-s, --start-date` | ❌ | 起始日期 `YYYY-MM-DD`，用于加速分页 |
| `-o, --output` | ❌ | 输出 JSON 路径（默认 `position_analysis_<slug>.json`） |

也可以改 `run.sh` 的默认参数后 `./run.sh`。

### 输出结构（单持仓）

```jsonc
{
  "meta": { "generated_at": "...", "wallet_address": "...", "position_query": "..." },
  "market_info": {
    "market_id": "0x...", "market_question": "...", "outcome_name": "Yes",
    "token_id": "0x...", "market_status": "resolved", "resolution_result": "Yes"
  },
  "trades": [
    {
      "timestamp": 1712489700, "action_type": "buy", "shares": 100, "price": 0.42,
      "amount_usdc": 42.0, "realized_pnl_from_this_trade": 0.0,
      "cumulative_realized_pnl": 0.0,
      "remaining_position_after_trade": 100,
      "is_opening_trade": true, "is_add_to_position": false,
      "market_context": { "market_price_before_trade": 0.41, "price_change_5m": 0.02, ... }
    }
  ],
  "position_summary": {
    "final_realized_pnl": 158.3, "roi_realized": 0.37, "won_or_lost": "won",
    "ai_analysis_features": { "entry_style": "scale-in", "exit_style": "hold-to-resolution", ... }
  }
}
```

如果同一个 position 关键词匹配到多个 `(conditionId, outcome)`，顶层会是 `positions: [...]` 数组。

---

## 脚本 2：`wallet_strategy_analyzer.py`

### 它做什么

**不是**遍历整个钱包（Polymarket 的 `/activity` 对全量请求 offset > 3000 有硬截断），而是用**两阶段抓取**：

1. 先按时间降序扫前几千条 activity，提取该钱包最近出现过的 `conditionId`，取最新 `--top` 个；
2. 再**按 cid 精确拉取**每个市场的完整交易（每个市场单独分页，不会被全局 offset 截断）。

然后复用 `position_deep_fetcher.py` 里的 FIFO / summary / AI features 函数，把每个 `(cid, outcome)` 当作一个持仓做分析。最后 `aggregate_strategy` 汇总成钱包级画像：胜率、盈亏比（profit factor）、仓位规模分位数、持仓时长中位数、入/出场风格分布、做市（同市场双边持仓）识别、止损行为统计（SELL 止损 vs MERGE 对冲 vs 持有到期），再用 `_render_interpretation` 自动生成一段人话策略解读。

还会顺手拉 Polymarket 排行榜 `lb-api.polymarket.com/profit` 和 `data-api/value` 做交叉验证（lifetime profit / current portfolio value / pseudonym）。

### 用法

```bash
python wallet_strategy_analyzer.py \
    -w 0xeebde7a0e019a63e6b476eb425505b7b3e6eba30 \
    -n 100 \
    -o wallet_strategy_0xeebde7a0.json
```

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `-w, --wallet` | ✅ | 钱包地址 |
| `-n, --top` | ❌ | 分析最近多少个市场（默认 100） |
| `-o, --output` | ❌ | JSON 输出路径，会同时生成同名的 `.report.txt` 可读报告 |
| `--max-records` | ❌ | 最多拉 activity 条数（默认 20000） |

### 输出

两个文件：

- `<output>.json` — 完整机读数据：`meta`（含交叉验证）+ `aggregate_strategy` + 每个持仓的 `positions[]`
- `<output>.report.txt` — 人读文本报告，含盈亏总览、仓位规模、入/出场分布、市场级 Top 盈利/亏损、以及「核心策略推断」段落

项目里已经有一份示例产物，可以直接看：

- [wallet_strategy_0xeebde7a0.report.txt](wallet_strategy_0xeebde7a0.report.txt)
- [wallet_strategy_0xeebde7a0.json](wallet_strategy_0xeebde7a0.json)

---

## 把输出交给 AI 用

两个脚本的 JSON 都是**为喂给大模型设计的**：字段扁平、语义化、没有魔法值，空值统一用字符串 `"unavailable"`。下面是几种常见用法。

### 1. 直接当成 prompt 上下文

最简单的方式：读 JSON，包进一个系统提示，让模型回答关于这个钱包/持仓的问题。

```python
import json, anthropic

data = json.load(open("wallet_strategy_0xeebde7a0.json"))

# 大钱包的 JSON 可能很大（示例里 15MB），上下文里通常只塞聚合部分
slim = {
    "meta":               data["meta"],
    "aggregate_strategy": data["aggregate_strategy"],
    # 按需挑 top N positions 给模型，不要全塞
    "top_wins":   sorted(data["positions"], key=lambda p: p["position_summary"]["final_realized_pnl"], reverse=True)[:10],
    "top_losses": sorted(data["positions"], key=lambda p: p["position_summary"]["final_realized_pnl"])[:10],
}

client = anthropic.Anthropic()
resp = client.messages.create(
    model="claude-opus-4-6",
    max_tokens=2000,
    system="你是加密衍生品交易员。下面是一个 Polymarket 钱包的完整策略画像，用中文给出这个交易员的风格总结、优势、风险点。",
    messages=[{"role": "user", "content": json.dumps(slim, ensure_ascii=False)}],
)
print(resp.content[0].text)
```

### 2. 给 Claude Code / Cursor 分析

直接让本地的 AI 编程助手读文件：

```
帮我读 wallet_strategy_0xeebde7a0.json 的 aggregate_strategy 部分，
然后对比 positions[].position_summary.ai_analysis_features，
找出这个钱包在亏损单上的共同特征。
```

由于 `.report.txt` 已经是自然语言，很多场景下直接贴文本比贴 JSON 更省 token。

### 3. 批量钱包对比

跑多个钱包后，把它们的 `aggregate_strategy` 提出来拼成一张表，让模型横向比较：

```python
summaries = []
for wallet in WALLETS:
    os.system(f"python wallet_strategy_analyzer.py -w {wallet} -n 100 -o out/{wallet}.json")
    data = json.load(open(f"out/{wallet}.json"))
    summaries.append({"wallet": wallet, **data["aggregate_strategy"]})

# 交给模型对比
```

### 4. 做 RAG / 向量检索

每个持仓是天然的 chunk：`positions[i].position_summary + market_info.market_question` 就是一个独立单元，可以直接 embed 进向量库，供后续「找相似交易模式的钱包」类查询。

### 体积注意

`wallet_strategy_*.json` 会包含所有 positions 的完整 trade details，大钱包下能到十几 MB。喂给模型前优先走 `aggregate_strategy` + 少量代表性 `positions`，或者先用 jq / Python 过滤：

```bash
jq '{meta, aggregate_strategy, positions: [.positions[] | .position_summary]}' \
    wallet_strategy_0xeebde7a0.json > slim.json
```

---

## 常见问题

- **跑完发现 `market_category: "unavailable"`？** Gamma 搜索失败会降级到 `conditionId`，CLOB 的 `/markets/{cid}` 并不总是返回 category，属于正常现象，不影响盈亏计算。
- **`position_deep_fetcher.py` 对「大市场 + 多 outcome」会分裂成多个 position？** 是，每个 `(conditionId, outcome)` 独立一份。顶层会变成 `positions: []`。
- **`--top` 实际拿到的 positions 比 N 多？** 正常：`--top` 是 cid 数，而每个 cid 可能有多个 outcome。
- **速率限制？** 默认 `RATE_LIMIT_SLEEP = 0.25s`，并带 4 次指数回退重试。大钱包完整跑一遍通常几分钟。
