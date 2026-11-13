#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GitHub Actions 海外 runner 数据源可用性探测（只读，不写仓库任何文件）。

用途：`breadth.py` 的 `HIST_FALLBACK` 开关开启前，必须先确认新浪 / 腾讯的
逐股后复权历史在 CI 的海外 IP 段可达。本机国内 IP 测通不算数。

刻意不测东方财富：那个接口本来就在限流，探测它只会制造噪声、延长限流。
退出码 0 = 至少有一个备用逐股源成功率 >= 80%，可以启用 HIST_FALLBACK。
退出码 1 = 备用源在海外 runner 上不可用，禁止启用开关。
"""
from __future__ import annotations

import sys
import time
import warnings

warnings.filterwarnings("ignore")
import akshare as ak

SAMPLE = [
    "sh600519", "sh601318", "sh600036", "sh688981", "sh601899", "sh600887",
    "sh603259", "sz000001", "sz000002", "sz300750", "sz002594", "sz300059",
    "sz002415", "sz000651", "sz000333", "sz002230", "sz300124", "sz300760",
    "bj920000", "bj920001",
]
INDEX_SYMBOLS = ["sh000300", "sh000001", "sz399006"]
START, END = "20190701", "20500101"
MIN_OK_RATIO = 0.80


def probe(label: str, fn) -> bool:
    t0 = time.time()
    try:
        df = fn()
        n = 0 if df is None else len(df)
        last = ""
        if n:
            last = ""
            for col in ("date", "日期"):
                if hasattr(df, "columns") and col in df.columns:
                    last = str(df[col].iloc[-1])[:10]
                    break
            if not last:
                last = str(df.index[-1])[:10]
        print(f"  {label:40s} OK   rows={n:<6d} last={last:<12s} {time.time() - t0:5.1f}s",
              flush=True)
        return n > 0
    except Exception as e:
        print(f"  {label:40s} FAIL {type(e).__name__}: {str(e)[:70]}  {time.time() - t0:5.1f}s",
              flush=True)
        return False


print("== ① 代码清单层（非东财源）==", flush=True)
probe("stock_info_a_code_name", lambda: ak.stock_info_a_code_name())
probe("stock_zh_a_spot (新浪)", lambda: ak.stock_zh_a_spot())
probe("stock_zh_a_spot_tx (腾讯)", lambda: ak.stock_zh_a_spot_tx())

print("\n== ② 指数日线层（非东财源）==", flush=True)
for sym in INDEX_SYMBOLS:
    probe(f"stock_zh_index_daily {sym}", lambda s=sym: ak.stock_zh_index_daily(symbol=s))
    probe(f"stock_zh_index_daily_tx {sym}",
          lambda s=sym: ak.stock_zh_index_daily_tx(symbol=s, start_date=START, end_date=END))

print("\n== ③ 逐股后复权历史层（HIST_FALLBACK 的唯一依据）==", flush=True)
ok_sina = ok_tx = 0
for code in SAMPLE:
    if probe(f"stock_zh_a_daily hfq {code}",
             lambda c=code: ak.stock_zh_a_daily(symbol=c, start_date=START,
                                               end_date=END, adjust="hfq")):
        ok_sina += 1
    if probe(f"stock_zh_a_hist_tx hfq {code}",
             lambda c=code: ak.stock_zh_a_hist_tx(symbol=c, start_date=START,
                                                  end_date=END, adjust="hfq")):
        ok_tx += 1

n = len(SAMPLE)
rs, rt = ok_sina / n, ok_tx / n
print("\n== 汇总 ==", flush=True)
print(f"  新浪 stock_zh_a_daily     : {ok_sina}/{n} = {rs:.1%}", flush=True)
print(f"  腾讯 stock_zh_a_hist_tx   : {ok_tx}/{n} = {rt:.1%}", flush=True)
good = max(rs, rt) >= MIN_OK_RATIO
print(f"  阈值 {MIN_OK_RATIO:.0%} → " + ("OVERSEAS_RUNNER_OK：可启用 HIST_FALLBACK=1"
                                     if good else "OVERSEAS_RUNNER_FAIL：禁止启用 HIST_FALLBACK"),
      flush=True)
sys.exit(0 if good else 1)
