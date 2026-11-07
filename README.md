# A 股市场宽度 · 云端自动版

每天收盘后由 **GitHub Actions** 自动计算「A 股站上 5 / 20 / 50 / 200 日均线的比例」（TradingView `INDEX:S5TH` 同类指标），把结果页提交回仓库，通过 **GitHub Pages** 生成公开网址。全程不依赖本地电脑开机，也不需要通达信/同花顺客户端。

## 数据源评估：`wj624/a-stock-api` 能不能当主源？

实测结论：**不能替换主数据源，但值得作为云出口友好的补充源**。

| 本项目需要什么 | a-stock-api 提供什么 | 判定 |
|---|---|---|
| 全市场逐股历史日线 | README 明确 `/api/spot`（全市场逐股快照）**未提供**：东财 push2 强制 100 条/页，5900+ 只需 60 页，超出免费 Worker 50 子请求上限 | ❌ |
| 复权收盘价 | `/api/kline` 无 `adjust` 参数；`/api/futures-kline` 明确「未复权」；全库无前/后复权字段 | ❌ |
| 「站上 N 日均线」的历史序列 | `/api/market-breadth` 只有当日涨跌/涨停/连板快照，`history_supported` 恒为 `false`、忽略 `date` 参数，无法回填历史 | ❌ |
| 当日涨跌家数 / 涨停 / 连板 | 一次请求即可，Cloudflare 出口实测可达 | ✅ |
| 交易日历、指数日 K | `/api/trade-calendar`、`/api/kline` 可用 | ✅ |

S5TH 的本质是「逐股价格序列 + 复权 + 均线」的历史统计，快照类 API 无论多稳定都算不出来。所以本项目把它只用在两处（**均为可选、失败静默跳过，绝不阻塞主流程**）：

1. `/api/market-breadth` 返回的 `date` 做幂等判断——第二个兜底 cron 若发现已有当日结果就立即退出，不再拉 5900 只历史；
2. 结果页加「当日快照校验」卡片，展示涨停 / 跌停 / 最高连板，并与本项目的涨跌家数对照。

**它还暴露了一个真问题**：该 README 明确写「东财 `push2.eastmoney.com` 对 Cloudflare / 云出口 IP 有 WAF 拦截（502）」。而 `stock_zh_a_spot_em()` 正是走 push2 —— 这是 GitHub Actions 境外 runner 最大的风险点。因此本项目把全市场代码清单做成 **`codes.txt` 快照提交进仓库**：在线快照成功就刷新它，失败就回退它，主流程不会因 WAF 挂掉。逐股历史走 `push2his.eastmoney.com`（另一域名），不受这条 WAF 影响。

本机实测补充源（2026-11-06）：`up 678 / down 4633 / flat 68 / total 5379`、涨停 32、跌停 15、最高连板 5；本项目同期算出上涨 743 / 下跌 4618（样本 5457）。趋势一致，绝对值差异来自样本池与「涨跌由分布直方图求和」的口径差，属正常。

## 为什么主数据源必须是 AKShare

本地版读的是 `D:\new_tdx\vipdoc\*.day` + `T0002\hq_cache\gbbq`，这些文件只在本机硬盘上（且客户端数据有使用条款，不适合上传仓库）。

| | 本地版 | 云端版 |
|---|---|---|
| 数据源 | 通达信本地 `.day` + `gbbq` 除权文件 | AKShare → 东财 `stock_zh_a_spot_em` + `stock_zh_a_hist(hfq)`；`codes.txt` 兜底 |
| 复权 | 自己按 gbbq 算前复权 | 直接取后复权序列 |
| 更新方式 | 手动打开客户端做盘后下载 | GitHub Actions 每日定时自动跑 |
| 结果页 | 本地 HTML | GitHub Pages 公开链接 |
| 电脑关机 | 无法更新 | 照常更新 |

后复权序列价格连续（除权日不会人为跳空），因此**不需要 gbbq 也能正确处理分红送转**；`收盘价 > N 日均线` 在整体等比例缩放下不变，后复权与前复权等价，且后复权历史值不会因未来分红被改写，更适合时间序列。

## 口径

- 等权，一只股票一票，不按市值加权
- 股票池：沪深京 A 股正股（600/601/603/605/688/689、000/001/002/003/300/301、43/83/87/92 开头），排除指数、基金、债券、B 股；ST 未单独剔除
- 剔除上市不足 200 个交易日的股票；停牌日不计入当日分母；每档均线使用独立分母
- 输出 4 条占比曲线、涨跌家数、McClellan 振荡器、MA200 占比的 EMA（可复刻 TradingView 的 "Market Breadth Peaks & Troughs"）

> 想更贴近 S5TH 原义（标普 500 **成分股**口径）：准备 `hs300.txt`（每行一个 6 位代码），运行时加 `--codes-file hs300.txt`，请求量从 ~5900 降到 ~300。

## 存储治理：2~3 年无人维护 + 500 MB 自动清理

| 机制 | 实现 |
|---|---|
| 不越跑越大 | 不落盘逐股原始行情，只留聚合结果；`breadth.csv` 按日期合并去重，滚动保留最近 **750 个交易日**（约 3 年），到顶后不再增长 |
| 单文件护栏 | `docs/` 任一文件超过 **50 MB** 直接报错退出，拒绝把大文件固化进仓库 |
| 500 MB 自动清理 | `housekeeping.py` + `.github/workflows/maintenance.yml`：每周一北京时间 11:00 读仓库 `size`（KB），**低于 512000 KB 一个字节都不改**；达到阈值才裁剪 CSV 到 365 天、删除 `docs/snapshots/` 旧快照，正常 commit，**绝不 force push、不重写 main 历史** |
| 静默失败防护 | 有效样本低于 1000 只、或取数成功少于 200 只时直接退出且**不覆盖**已有结果，Action 变红报警（沿用 a-stock-api README「空结果不等于没数据」的教训） |
| 幂等 + 兜底 | 辅助源日期 == CSV 最后日期则跳过全量取数；每天两个 cron（北京 16:30 / 18:00），后者在前者失败时真正重算 |
| 依赖不腐化 | `akshare==1.20.21` 锁死、`pandas<3`、`numpy<3`（pandas 3.x 移除 `resample('M')` 会打断 akshare 内部逻辑） |
| 股票池不腐化 | `codes.txt`（5923 只）随仓库提交，在线清单失败也能跑；新股偏差随下次成功刷新自动消除 |

按当前实测：`docs/` 约 **0.03 MB**。即使每天提交两个小文件，3 年累计也不到 40 MB，远低于 500 MB；清理工作流是兜底保险，不是日常负担。

## 目录结构

```
.
├── .github/workflows/breadth.yml       # 每日计算 + 提交报告（含兜底 cron）
├── .github/workflows/maintenance.yml   # 每周体积体检，>=500 MB 才清理
├── breadth.py                         # 取数、计算、生成 CSV/HTML、运行时护栏
├── housekeeping.py                    # 纯标准库体积裁剪，供维护工作流调用
├── requirements.txt                   # akshare / pandas<3 / numpy<3
├── codes.txt                          # 全市场代码快照（在线快照失败时兜底）
├── .gitignore
└── docs/                              # GitHub Pages 根目录
    ├── index.html                     # 结果页（ECharts 图 + 数据表）
    └── breadth.csv                    # 最多 750 个交易日的完整序列
```

## 部署

仓库已推送到 `wj402041069-del/a-share-market-breadth`。若要自行重建：

1. GitHub 新建**空仓库**（不要勾 Add README）。
2. `git init && git add . && git commit -m "init" && git branch -M main && git remote add origin <url> && git push -u origin main`。
3. **Settings → Pages**：Deploy from a branch → `main` / `/docs`。访问 `https://<用户名>.github.io/<仓库名>/`。
4. **Actions → A-share Market Breadth → Run workflow** 手动跑一次（全市场约 5~10 分钟），`docs/` 与 `codes.txt` 会自动提交。调试时在 `limit` 填 `300` 并把 `force` 设为 `true`。
5. 之后每交易日北京 **16:30** 自动运行，18:00 兜底（cron 按 UTC 写 `30 8 * * 1-5` 与 `0 10 * * 1-5`）。周末不跑；节假日数据日期不变则跳过提交。

> **凭证纪律**：仓库里没有任何 PAT / Token。CI 提交用 GitHub 自动提供的 `GITHUB_TOKEN`；若需外部密钥，一律放仓库 Secrets。GitHub 不保证 cron 准时，高峰期可能延迟，双 cron 就是为此准备的。

## 本地验证（可选）

```bash
pip install -r requirements.txt
python breadth.py --limit 300 --min-valid 100 --out test-out --force   # 快速验证，约 2 分钟
python breadth.py --out docs --force                                    # 全市场，约 6 分钟
python housekeeping.py --out docs --max-mb 500 --max-rows 750          # 体积体检
```

## 实测与交叉验证

本机跑过一次全市场：快照 5927 行 → A 股正股 5923 只 → 成功取到历史 5465 只 → 计入计算 5457 只，耗时约 6 分钟，数据截止 **2026-11-06**。仓库里 `docs/` 就是这次的真实结果（不是占位样例）。

与上一版「本地通达信 `.day` + gbbq 前复权」在重叠交易日对齐，两套完全不同的数据源 / 复权方式给出的宽度几乎一致：

| 交易日 | 口径 | 站上 MA200 | 站上 MA20 | 上涨/下跌 |
|---|---|---:|---:|---:|
| 2026-09-01 | 通达信 / 东财 | 26.51% / 26.50% | 64.77% / 64.78% | 3313/1975 / 3338/1983 |
| 2026-09-21 | 通达信 / 东财 | 27.57% / 27.45% | 55.07% / 55.01% | 4449/882 / 4469/887 |

差异只来自样本数（5409 vs 5457）与两家行情源的微小价差，说明云端后复权与本地前复权口径等价。本地数据停在 2026-09-21，云端继续更新到 2026-11-06（当日 MA200 占比 12.07%），这正是脱离本地电脑的价值。

## 已知风险

- **接口可达性**：GitHub 境外 runner 访问东财可能限流 / WAF。已用 `codes.txt` 兜底代码清单、3 次重试、6 并发、空结果保护来缓解；逐股历史走 `push2his`（未被 WAF 记录）。持续失败时 Action 变红并发邮件。
- **补充源**：`a-stock-api` 的 Worker 若不可达，只影响幂等判断和校验卡片，不影响主计算。
- **接口变更**：`akshare` 锁在 `1.20.21`，升级前先本地验证字段。
- **额度**：公开仓库 Actions 分钟数免费不限量；私有仓库每月 2000 分钟，每天约 10 分钟也够。
- **费用与合规**：只用公开行情接口，不上传任何行情软件客户端的本地数据文件；结果为公开数据的统计计算，不构成投资建议。
