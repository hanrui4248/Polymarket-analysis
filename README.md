# Polymarket 分析工具集

基于 [Polymarket](https://polymarket.com) 公开 API 的 Python 脚本集合，用于抓取钱包活动、持仓深度分析与已关闭仓位盈亏统计。适合自用研究与数据导出，**不构成投资建议**。

## 环境要求

- Python 3.10+（推荐；脚本中使用了 `list[dict]` 等类型标注）
- 网络可访问 `data-api.polymarket.com`、`gamma-api.polymarket.com`、`clob.polymarket.com`

## 安装

```bash
cd polymarket分析
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

依赖：`requests`、`urllib3`（见 `requirements.txt`）。

## 脚本说明

| 文件 | 作用 |
|------|------|
| `polymarket_analyzer.py` | 按活动类型分页抓取钱包 `activity`，统计买卖笔数与粗略盈亏（需注意 TRADE 不含 REDEEM，脚本内有说明）。在文件顶部修改 `WALLET` 后运行。 |
| `activity_fecther.py` | 针对**单一市场**拉取该钱包下的操作记录（可筛日期、类型），导出 CSV/JSON。在 `CONFIG` 区配置 `WALLET`、`MARKET_QUERY` 等。 |
| `position_deep_fetcher.py` | **持仓深度分析**：匹配指定市场/持仓，汇总逐笔交易、FIFO、市场上下文等，输出结构化 JSON 供后续分析。支持命令行参数。 |
| `closed_position_searcher.py` | 检索**已关闭**仓位，按关键词过滤并统计盈亏，结果写入 JSON。在 `CONFIG` 区配置 `WALLET`、`KEYWORD` 等。 |
| `run.sh` | 调用 `position_deep_fetcher.py` 的便捷脚本；可传入市场名称、钱包地址覆盖默认变量。 |

## 常用命令示例

**钱包活动概览：**

```bash
python polymarket_analyzer.py
```

**单市场活动导出（先改 `activity_fecther.py` 内配置）：**

```bash
python activity_fecther.py
```

**持仓深度分析（推荐）：**

```bash
python position_deep_fetcher.py \
  --wallet 0xYourWallet \
  --position "市场标题关键词或完整标题" \
  --start-date 2026-04-07
```

或使用 `./run.sh "市场名称" 0xYourWallet`（需 `chmod +x run.sh`）。

**已关闭仓位与盈亏：**

```bash
python closed_position_searcher.py
```

## API 与限制说明

- 脚本使用 Polymarket **Data API / Gamma API / CLOB** 等公开端点；`activity` 等接口存在 **offset 上限**（代码中约 5000 以内），深度历史可能无法一次拉全，可通过 `START_DATE` 缩小时间范围。
- 请求频率过高可能触发限速；`position_deep_fetcher.py`、`closed_position_searcher.py` 中已加入间隔与重试，仍建议勿并发猛刷。

## 免责声明

本仓库仅用于技术学习与数据分析。预测市场存在损失风险，作者不对任何使用本工具产生的后果负责。
