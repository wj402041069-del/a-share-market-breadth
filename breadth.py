#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A 股市场宽度（TradingView INDEX:S5TH 同类）—— 云端版，完全脱离本地行情客户端

主数据源（算均线宽度必须用逐股历史 + 复权，无法被快照类 API 替代）:
  AKShare -> 东方财富
    stock_zh_a_spot_em()                 全市场代码清单（push2，境外偶发 WAF，失败自动回退 codes.txt）
    stock_zh_a_hist(adjust="hfq")       逐只后复权日线（push2his；后复权序列连续，无需 gbbq）

辅助数据源（a-stock-api / Cloudflare Worker，云出口友好，仅做轻量补充，绝不当主源）:
    /api/market-breadth   当日涨跌家数 / 涨停 / 连板快照，用于幂等判断 + 结果页校验卡片
    该 Worker 没有全市场逐股历史、没有复权、market-breadth 的 history_supported 恒为 false，
    因此无法用来计算「站上 N 日均线」的历史序列。

存储治理（目标：2~3 年无人维护）:
  - 不落盘逐股原始行情，只留聚合结果（无状态）
  - breadth.csv 与已有结果按日期合并，滚动保留最近 750 个交易日（约 3 年），之后不再增长
  - 单文件 50 MB 护栏；docs/ 超过 500 MB 自动裁剪旧数据并清理旧快照
  - 有效样本低于下限（默认 1000）时拒绝覆盖已有结果，避免「静默丢数据」
  - 依赖锁版本 + 上界；股票池有 codes.txt 快照兜底；补充源失败不影响主流程

用法:
    python breadth.py                         # 全市场，约 5~10 分钟
    python breadth.py --limit 300 --force     # 快速验证
    python breadth.py --codes-file hs300.txt  # 自定义股票池（更贴近 S5TH 成分股口径）
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import json
import os
import sys
import time
import urllib.request
from collections import Counter
from datetime import datetime, timedelta

import numpy as np

try:
    import akshare as ak
except ImportError:
    sys.exit("缺少依赖，请先执行: pip install -r requirements.txt")

MA_WINDOWS = (5, 20, 50, 200)
OUT_DAYS = 120               # 图表展示的交易日数
MAX_CSV_ROWS = 750           # CSV 最多保留的交易日（约 3 年），超出自动裁剪
LOOKBACK_DAYS = 620          # 自然日，约 400 个交易日，足够 200 日均线 + 120 天输出
CALENDAR_BARS = 330
MAX_WORKERS = 6
RETRY = 3

CODES_SNAPSHOT = "codes.txt"
AUX_API = "https://a-stock-api.wj4020410.workers.dev"
MAX_DIR_MB = 500             # docs/ 体积上限
SINGLE_FILE_MB = 50          # 单文件护栏
MIN_VALID = 1000             # 有效样本下限，低于此值拒绝覆盖

CSV_COLS = ["date", "n_stocks", "n_traded", "n_ma5", "n_ma20", "n_ma50", "n_ma200",
            "pct_above_5", "pct_above_20", "pct_above_50", "pct_above_200",
            "above_5", "above_20", "above_50", "above_200",
            "advancers", "decliners", "ad_diff", "mcclellan",
            "ema5_pct200", "ema_long_pct200"]


# ----------------------------------------------------------------------------
# 小工具
# ----------------------------------------------------------------------------
def atomic_write(path: str, text: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(text)
    os.replace(tmp, path)


def dir_size_mb(path: str) -> float:
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total / 1_000_000


def d2i(d) -> int:
    if hasattr(d, "year"):
        return d.year * 10000 + d.month * 100 + d.day
    return int(str(d).replace("-", "")[:8])


# ----------------------------------------------------------------------------
# 股票池：在线快照优先，仓库内 codes.txt 兜底
# ----------------------------------------------------------------------------
def is_a_stock(code: str) -> bool:
    c = str(code).zfill(6)
    return c.startswith(("600", "601", "603", "605", "688", "689",
                         "000", "001", "002", "003", "300", "301",
                         "43", "83", "87", "92"))


def read_codes_file(path: str) -> list[str]:
    if not os.path.isfile(path):
        return []
    with open(path, encoding="utf-8") as f:
        return sorted({ln.strip().zfill(6) for ln in f
                       if ln.strip() and ln.strip()[:1].isdigit() and is_a_stock(ln.strip())})


def get_codes(limit: int, codes_file: str | None) -> list[str]:
    if codes_file:
        codes = read_codes_file(codes_file)
        if not codes:
            sys.exit(f"自定义股票池为空或不存在: {codes_file}")
        print(f"[1/5] 自定义股票池 {len(codes)} 只（{codes_file}）", flush=True)
        return codes[:limit] if limit else codes

    print("[1/5] 拉取全市场 A 股快照（约 60~90 秒）...", flush=True)
    try:
        df = ak.stock_zh_a_spot_em()
        codes = sorted({str(c).zfill(6) for c in df["代码"] if is_a_stock(c)})
        if len(codes) < 3000:
            raise RuntimeError(f"快照只有 {len(codes)} 只，疑似上游异常")
        # 完整清单写回仓库，作为境外 runner 被 WAF 拦时的兜底
        atomic_write(CODES_SNAPSHOT, "\n".join(codes) + "\n")
        print(f"[1/5] 全市场快照 {len(df)} 行，A 股正股 {len(codes)} 只；"
              f"已更新 {CODES_SNAPSHOT}", flush=True)
    except Exception as e:
        codes = read_codes_file(CODES_SNAPSHOT)
        print(f"[1/5] 在线快照失败（{type(e).__name__}: {str(e)[:80]}），"
              f"回退 {CODES_SNAPSHOT}：{len(codes)} 只", flush=True)
        if not codes:
            sys.exit("在线快照和 codes.txt 兜底都不可用，本次不覆盖已有结果")
    return codes[:limit] if limit else codes


# ----------------------------------------------------------------------------
# 辅助源：仅用于幂等判断 + 当日快照卡片，失败静默跳过
# ----------------------------------------------------------------------------
def fetch_aux(timeout: int = 12) -> dict | None:
    try:
        req = urllib.request.Request(AUX_API + "/api/market-breadth",
                                     headers={"User-Agent": "market-breadth-ci"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            d = json.load(r)
        if not d.get("total"):
            return None
        return {k: d.get(k) for k in ("date", "up", "down", "flat", "total",
                                      "limit_up", "limit_down", "max_streak", "is_today")}
    except Exception as e:
        print(f"  [aux] 补充源不可用（{type(e).__name__}），跳过", flush=True)
        return None


# ----------------------------------------------------------------------------
# 拉取逐股历史
# ----------------------------------------------------------------------------
def fetch_one(code: str, start: str, end: str):
    for attempt in range(RETRY):
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily",
                                    start_date=start, end_date=end, adjust="hfq")
            if df is None or len(df) < 205 or "收盘" not in df.columns:
                return code, None
            return code, df
        except Exception:
            time.sleep(0.5 * (attempt + 1))
    return code, None


def fetch_all(codes: list[str], start: str, end: str) -> dict:
    out: dict = {}
    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(fetch_one, c, start, end) for c in codes]
        for i, fu in enumerate(cf.as_completed(futs), 1):
            code, df = fu.result()
            if df is not None:
                out[code] = df
            if i % 1000 == 0 or i == len(codes):
                print(f"  历史日线 {i}/{len(codes)}，成功 {len(out)}", flush=True)
    return out


# ----------------------------------------------------------------------------
# 计算
# ----------------------------------------------------------------------------
def rolling_mean(x: np.ndarray, n: int) -> np.ndarray:
    c = np.concatenate(([0.0], np.cumsum(x)))
    out = np.full(len(x), np.nan)
    if len(x) >= n:
        out[n - 1:] = (c[n:] - c[:-n]) / n
    return out


def ema(x: np.ndarray, n: int) -> np.ndarray:
    a = 2.0 / (n + 1.0)
    out = np.empty(len(x))
    out[0] = x[0]
    for i in range(1, len(x)):
        out[i] = a * x[i] + (1 - a) * out[i - 1]
    return out


def compute(data: dict) -> dict:
    freq: Counter = Counter()
    for df in data.values():
        freq.update(d2i(d) for d in df["日期"])
    threshold = max(20, int(len(data) * 0.30))
    calendar = sorted(d for d, c in freq.items() if c >= threshold)[-CALENDAR_BARS:]
    T = len(calendar)
    ci = {d: i for i, d in enumerate(calendar)}
    print(f"[3/5] 交易日历 {T} 天：{calendar[0]} ~ {calendar[-1]}", flush=True)

    N = len(data)
    valid = np.zeros((N, T), dtype=bool)
    ready = {n: np.zeros((N, T), dtype=bool) for n in MA_WINDOWS}
    above = {n: np.zeros((N, T), dtype=bool) for n in MA_WINDOWS}
    is_up = np.zeros((N, T), dtype=bool)
    is_down = np.zeros((N, T), dtype=bool)
    kept = 0

    for row, (_, df) in enumerate(data.items()):
        ds = np.array([d2i(d) for d in df["日期"]], dtype=np.int64)
        cl = df["收盘"].astype(float).to_numpy()
        pos = np.array([ci.get(int(d), -1) for d in ds], dtype=int)
        sel = (pos >= 0) & (cl > 0)
        cl, pos = cl[sel], pos[sel]
        if len(cl) < 205:
            continue
        valid[row, pos] = True
        for n in MA_WINDOWS:
            ma = rolling_mean(cl, n)
            ok = np.isfinite(ma)
            ready[n][row, pos] = ok
            above[n][row, pos] = ok & (cl > ma)
        chg = np.diff(cl, prepend=cl[0])
        is_up[row, pos] = chg > 0
        is_down[row, pos] = chg < 0
        kept += 1

    out_rows = []
    for t in range(T):
        d = calendar[t]
        rec = {
            "date": f"{d // 10000:04d}-{d // 100 % 100:02d}-{d % 100:02d}",
            "n_traded": int(valid[:, t].sum()),
            "advancers": int((valid[:, t] & is_up[:, t]).sum()),
            "decliners": int((valid[:, t] & is_down[:, t]).sum()),
        }
        for w in MA_WINDOWS:
            n = int((valid[:, t] & ready[w][:, t]).sum())
            hits = int((valid[:, t] & above[w][:, t]).sum())
            rec[f"n_ma{w}"] = n
            rec[f"above_{w}"] = hits
            rec[f"pct_above_{w}"] = round(100.0 * hits / n, 2) if n else None
        rec["n_stocks"] = rec["n_ma200"]
        out_rows.append(rec)

    rows = [r for r in out_rows if r["pct_above_200"] is not None][-OUT_DAYS:]
    for r in rows:
        r["ad_diff"] = r["advancers"] - r["decliners"]
    p200 = np.array([float(r["pct_above_200"]) for r in rows])
    net = np.array([float(r["ad_diff"]) for r in rows])
    e5 = ema(p200, 5)
    elong = ema(p200, 200 if len(rows) >= 200 else max(5, len(rows) // 3))
    mc = ema(net, 19) - ema(net, 39)
    for i, r in enumerate(rows):
        r["ema5_pct200"] = round(float(e5[i]), 2)
        r["ema_long_pct200"] = round(float(elong[i]), 2)
        r["mcclellan"] = round(float(mc[i]), 1)

    return {"rows": rows, "n_valid": kept, "n_fetched": len(data),
            "calendar_days": T,
            "latest": rows[-1]["date"] if rows else "",
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}


# ----------------------------------------------------------------------------
# 存储治理
# ----------------------------------------------------------------------------
def load_existing_rows(path: str) -> dict:
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8-sig") as f:
            return {r["date"]: r for r in csv.DictReader(f) if r.get("date")}
    except Exception:
        return {}


def merge_rows(existing: dict, new: list, max_rows: int) -> list:
    merged = dict(existing)
    for r in new:
        merged[r["date"]] = r          # 新算结果覆盖同日旧值
    return [merged[k] for k in sorted(merged)][-max_rows:]


def housekeeping(out_dir: str, max_mb: float, max_rows: int) -> list[str]:
    """体积护栏 + 滚动裁剪。只清理数据产物，绝不碰 git 历史、绝不 force。"""
    notes: list[str] = []
    for root, _, files in os.walk(out_dir):
        for name in files:
            fp = os.path.join(root, name)
            mb = os.path.getsize(fp) / 1_000_000
            if mb > SINGLE_FILE_MB:
                raise RuntimeError(f"单文件超过 {SINGLE_FILE_MB} MB 护栏: {fp} ({mb:.1f} MB)")

    csv_path = os.path.join(out_dir, "breadth.csv")
    rows = load_existing_rows(csv_path)
    if len(rows) > max_rows:
        trimmed = [rows[k] for k in sorted(rows)][-max_rows:]
        import io
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=CSV_COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(trimmed)
        atomic_write(csv_path, buf.getvalue())
        notes.append(f"CSV 裁剪到最近 {max_rows} 个交易日（原 {len(rows)} 行）")

    size = dir_size_mb(out_dir)
    if size > max_mb:
        snap = os.path.join(out_dir, "snapshots")
        if os.path.isdir(snap):
            olds = sorted((os.path.join(snap, f) for f in os.listdir(snap)),
                          key=os.path.getmtime)
            for fp in olds:
                if dir_size_mb(out_dir) <= max_mb * 0.8:
                    break
                os.remove(fp)
                notes.append(f"清理旧快照 {os.path.basename(fp)}")
        rows = load_existing_rows(csv_path)
        if dir_size_mb(out_dir) > max_mb and len(rows) > 365:
            trimmed = [rows[k] for k in sorted(rows)][-365:]
            import io
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=CSV_COLS, extrasaction="ignore")
            w.writeheader()
            w.writerows(trimmed)
            atomic_write(csv_path, buf.getvalue())
            notes.append("体积超阈值，CSV 进一步裁剪到 365 天")
    notes.append(f"docs/ 当前 {dir_size_mb(out_dir):.2f} MB（阈值 {max_mb:.0f} MB）")
    return notes


# ----------------------------------------------------------------------------
# 输出
# ----------------------------------------------------------------------------
def write_csv(rows: list, path: str) -> None:
    import io
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=CSV_COLS, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    atomic_write(path, buf.getvalue())


def write_html(res: dict, aux: dict | None, path: str) -> None:
    rows = res["rows"][-OUT_DAYS:]
    dates = [r["date"] for r in rows]
    series = {str(n): [float(r[f"pct_above_{n}"]) for r in rows] for n in MA_WINDOWS}
    adv = [int(r["advancers"]) for r in rows]
    dec = [-int(r["decliners"]) for r in rows]
    last = rows[-1]
    payload = json.dumps({
        "dates": dates, "series": series, "adv": adv, "dec": dec,
        "ema5": [float(r.get("ema5_pct200") or 0) for r in rows],
        "emaLong": [float(r.get("ema_long_pct200") or 0) for r in rows],
        "mc": [float(r.get("mcclellan") or 0) for r in rows],
    }, ensure_ascii=False)

    cards = "".join(
        f'<div class="card"><div class="lab">站上 {n} 日均线</div>'
        f'<div class="val">{float(last[f"pct_above_{n}"]):.1f}<span>%</span></div>'
        f'<div class="sub">{last[f"above_{n}"]} / {last[f"n_ma{n}"]} 只</div></div>'
        for n in MA_WINDOWS)
    aux_html = ""
    if aux:
        aux_html = (f'<div class="aux"><b>当日快照校验</b>（a-stock-api /api/market-breadth，'
                    f'独立数据源，口径略有差异）：{aux.get("date")} 上涨 {aux.get("up")} / '
                    f'下跌 {aux.get("down")} / 平 {aux.get("flat")}（合计 {aux.get("total")}）；'
                    f'涨停 {aux.get("limit_up")}、跌停 {aux.get("limit_down")}、'
                    f'最高连板 {aux.get("max_streak")} 板。</div>')
    trs = "".join(
        "<tr>" + "".join(f"<td>{r[k]}</td>" for k in
                         ["date", "n_stocks", "pct_above_5", "pct_above_20",
                          "pct_above_50", "pct_above_200", "advancers", "decliners", "mcclellan"])
        + "</tr>" for r in reversed(rows[-15:]))

    html = f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>A 股市场宽度 · S5TH 云端版</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
:root{{--bg:#f7f8fa;--card:#fff;--line:#e6e9ef;--txt:#1f2733;--muted:#6b7688}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--txt);font:14px/1.6 -apple-system,"Segoe UI","Microsoft YaHei",sans-serif}}
.wrap{{max-width:1180px;margin:0 auto;padding:32px 24px 60px}}
h1{{font-size:26px;margin:0 0 6px}}h2{{font-size:18px;margin:34px 0 12px}}
.lead{{color:var(--muted);margin:0 0 20px}}
.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:18px 0}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px 18px}}
.lab{{color:var(--muted);font-size:12px}}.val{{font-size:30px;font-weight:650;line-height:1.25}}.val span{{font-size:15px;margin-left:2px}}
.sub{{color:var(--muted);font-size:12px}}
.aux{{background:#eef4ff;border:1px solid #cfe0f7;color:#284b7a;padding:10px 14px;border-radius:8px;font-size:13px;margin-bottom:8px}}
.panel{{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px;margin-bottom:18px}}
#main{{height:400px}}#sub{{height:250px}}
table{{width:100%;border-collapse:collapse;background:var(--card);font-size:13px}}
th,td{{padding:8px 10px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}}
th{{background:#f1f3f7;color:var(--muted);font-weight:600}}th:first-child,td:first-child{{text-align:left}}
.src{{color:var(--muted);font-size:12px;margin-top:22px;border-top:1px solid var(--line);padding-top:14px}}
code{{background:#eef1f6;padding:1px 5px;border-radius:4px;font-size:12px}}
@media(max-width:760px){{.cards{{grid-template-columns:repeat(2,1fr)}}}}
</style></head><body><div class="wrap">
<h1>A 股市场宽度 · S5TH 云端版</h1>
<p class="lead">等权口径：A 股中「后复权收盘价站上 N 日均线」的占比。由 GitHub Actions 每日收盘后自动计算，无需本地行情软件。</p>
<div class="cards">{cards}</div>
{aux_html}
<h2>四条宽度曲线（%）</h2>
<div class="panel"><div id="main"></div></div>
<h2>涨跌家数 与 McClellan 振荡器</h2>
<div class="panel"><div id="sub"></div></div>
<h2>最近 15 个交易日</h2>
<table><thead><tr><th>日期</th><th>样本数</th><th>&gt;MA5</th><th>&gt;MA20</th><th>&gt;MA50</th><th>&gt;MA200</th><th>上涨</th><th>下跌</th><th>McClellan</th></tr></thead><tbody>{trs}</tbody></table>
<div class="src">
主数据源：AKShare → 东方财富 <code>stock_zh_a_spot_em</code>（代码清单，失败回退仓库内 <code>codes.txt</code>）+ <code>stock_zh_a_hist(adjust="hfq")</code>（逐只后复权日线，已消除除权跳空）。辅助校验：<code>a-stock-api /api/market-breadth</code> 当日快照（无逐股历史、无复权，仅作校验，不参与均线计算）。<br>
数据截止 <b>{res['latest']}</b>；成功取数 <b>{res['n_fetched']:,}</b> 只、计入计算 <b>{res['n_valid']:,}</b> 只；等权、含北交所/科创创业、ST 未单独剔除、上市不足 200 交易日者剔除、停牌日不计入当日分母。<br>
CSV 滚动保留最近 {MAX_CSV_ROWS} 个交易日；docs/ 超过 {MAX_DIR_MB} MB 自动裁剪。由 GitHub Actions 生成于 {res['generated_at']}；本页仅为公开数据的计算结果，不构成投资建议。
</div>
</div>
<script>
var DATA = {payload};
var COLORS = {{'5':'#8a63d2','20':'#2f6fd0','50':'#e0972f','200':'#d64545'}};
function lineChart() {{
  var el = document.getElementById('main');
  if (typeof echarts === 'undefined') {{ el.innerHTML = '<p style="color:#6b7688">图表库未加载，请查看下方数据表。</p>'; return; }}
  var chart = echarts.init(el);
  var names = {{'5':'站上 5 日线','20':'站上 20 日线','50':'站上 50 日线','200':'站上 200 日线（S5TH 同类）'}};
  var series = Object.keys(DATA.series).map(function (k) {{
    return {{name: names[k], type:'line', data: DATA.series[k], smooth:true, showSymbol:false,
      lineStyle:{{width: k === '200' ? 2.6 : 1.7, color: COLORS[k]}}, itemStyle:{{color: COLORS[k]}}}};
  }});
  series.push({{name:'MA200 占比长周期 EMA', type:'line', data: DATA.emaLong, smooth:true,
    showSymbol:false, lineStyle:{{width:1.4, type:'dashed', color:'#8a93a3'}}, itemStyle:{{color:'#8a93a3'}}}});
  chart.setOption({{
    tooltip:{{trigger:'axis', valueFormatter:function(v){{return v + '%'}}}},
    legend:{{top:0, textStyle:{{color:'#1f2733'}}}},
    grid:{{left:50, right:24, top:48, bottom:40}},
    xAxis:{{type:'category', data: DATA.dates, axisLabel:{{color:'#6b7688'}}, axisLine:{{lineStyle:{{color:'#e6e9ef'}}}}}},
    yAxis:{{type:'value', min:0, max:100, axisLabel:{{formatter:'{{value}}%', color:'#6b7688'}}, splitLine:{{lineStyle:{{color:'#eef0f4'}}}}}},
    series: series
  }});
  window.addEventListener('resize', function () {{ chart.resize(); }});
}}
function barChart() {{
  var el = document.getElementById('sub');
  if (typeof echarts === 'undefined') return;
  var chart = echarts.init(el);
  chart.setOption({{
    tooltip:{{trigger:'axis'}}, legend:{{top:0, textStyle:{{color:'#1f2733'}}}},
    grid:{{left:60, right:60, top:40, bottom:36}},
    xAxis:{{type:'category', data: DATA.dates, axisLabel:{{color:'#6b7688'}}}},
    yAxis:[{{type:'value', name:'家数', axisLabel:{{color:'#6b7688'}}, splitLine:{{lineStyle:{{color:'#eef0f4'}}}}}},
           {{type:'value', name:'McClellan', axisLabel:{{color:'#6b7688'}}, splitLine:{{show:false}}}}],
    series:[
      {{name:'上涨家数', type:'bar', stack:'ad', data: DATA.adv, itemStyle:{{color:'#d64545'}}}},
      {{name:'下跌家数', type:'bar', stack:'ad', data: DATA.dec, itemStyle:{{color:'#1f9d6b'}}}},
      {{name:'McClellan', type:'line', yAxisIndex:1, data: DATA.mc, showSymbol:false,
        smooth:true, lineStyle:{{width:1.8, color:'#2f6fd0'}}, itemStyle:{{color:'#2f6fd0'}}}}
    ]
  }});
  window.addEventListener('resize', function () {{ chart.resize(); }});
}}
lineChart(); barChart();
</script>
</body></html>"""
    atomic_write(path, html)


def main() -> int:
    global MAX_WORKERS
    ap = argparse.ArgumentParser(description="云端版 A 股市场宽度（带存储治理）")
    ap.add_argument("--out", default="docs")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--codes-file", default="", help="显式指定股票池文件，指定后不再拉在线快照")
    ap.add_argument("--workers", type=int, default=MAX_WORKERS)
    ap.add_argument("--max-rows", type=int, default=MAX_CSV_ROWS)
    ap.add_argument("--max-mb", type=float, default=MAX_DIR_MB)
    ap.add_argument("--min-valid", type=int, default=MIN_VALID)
    ap.add_argument("--no-aux", action="store_true", help="禁用 a-stock-api 补充源")
    ap.add_argument("--force", action="store_true", help="即使已是最新交易日也强制重算")
    a = ap.parse_args()
    MAX_WORKERS = a.workers

    os.makedirs(a.out, exist_ok=True)
    csv_path = os.path.join(a.out, "breadth.csv")
    html_path = os.path.join(a.out, "index.html")
    existing = load_existing_rows(csv_path)

    aux = None if a.no_aux else fetch_aux()
    if aux:
        print(f"[0/5] 辅助源当日快照：{aux['date']} 上涨 {aux['up']} / 下跌 {aux['down']} "
              f"/ 涨停 {aux['limit_up']}（仅校验，不参与均线计算）", flush=True)
        if not a.force and existing:
            last_date = max(existing)
            if last_date == aux["date"]:
                print(f"已有结果覆盖 {last_date}，无需重算（幂等跳过；--force 可强制）", flush=True)
                return 0

    today = datetime.now()
    start = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    codes = get_codes(a.limit, a.codes_file or None)
    print(f"[2/5] 拉取 {len(codes)} 只股票的后复权日线（并发 {MAX_WORKERS}）...", flush=True)
    data = fetch_all(codes, start, end)
    if len(data) < 200:
        sys.exit(f"仅成功取到 {len(data)} 只，疑似接口被限流，本次不覆盖已有结果")

    res = compute(data)
    if res["n_valid"] < a.min_valid:
        sys.exit(f"有效样本仅 {res['n_valid']} 只（下限 {a.min_valid}），"
                 f"疑似上游异常，本次不覆盖已有结果")

    merged = merge_rows(existing, res["rows"], a.max_rows)
    write_csv(merged, csv_path)
    write_html(res, aux, html_path)
    notes = housekeeping(a.out, a.max_mb, a.max_rows)

    last = res["rows"][-1]
    print(f"[4/5] 数据截止 {res['latest']}，有效样本 {res['n_valid']:,} 只，"
          f"CSV 累计 {len(merged)} 个交易日", flush=True)
    for n in MA_WINDOWS:
        print(f"  站上 MA{n:<3d}: {last[f'pct_above_{n}']:6.2f}%  ({last[f'above_{n}']} 只)")
    print(f"  上涨 {last['advancers']} / 下跌 {last['decliners']} / McClellan {last['mcclellan']}")
    print("[5/5] 存储治理：" + "；".join(notes), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
