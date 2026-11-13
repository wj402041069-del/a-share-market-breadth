#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GitHub Actions 海外 runner 数据源可用性探测（只读，不写仓库任何文件）。

用途：`breadth.py` 的 `HIST_FALLBACK` 开关开启前，必须先确认新浪 / 腾讯的
逐股后复权历史在 CI 的海外 IP 段可达。本机国内 IP 测通不算数。

刻意不测东方财富：那个接口本来就在限流，探测它只会制造噪声、延长限流。

**始终以退出码 0 结束**：这是「探测」不是「断言」，FAIL 只代表备用源在海外不可用、
必须保持 HIST_FALLBACK 关闭，不代表仓库故障；不要让它每周把 CI 染红，
以免和「计算失败」的红灯混淆。结论看日志末尾的 OVERSEAS_RUNNER_OK / FAIL。

铁律 8 的境外四件套同样适用：必须设 socket 超时，否则国内源在海外会无限挂起，
一直拖到 workflow 的 15 分钟上限；逐股 40 个请求用并发压到几分钟内。
"""
from __future__ import annotations

import concurrent.futures as cf
import socket
import sys
import time
import warnings

warnings.filterwarnings("ignore")
import akshare as ak

PROBE_TIMEOUT = 8                 # 秒；比主程序 SOCKET_TIMEOUT=12 更严格，探测能过则主程序必能过
MAX_WORKERS = 8
socket.setdefaulttimeout(PROBE_TIMEOUT)

SAMPLE = [
    "sh600519", "sh601318", "sh600036", "sh688981", "sh601899", "sh600887",
    "sh603259", "sz000001", "sz000002", "sz300750", "sz002594", "sz300059",
    "sz002415", "sz000651", "sz000333", "sz002230", "sz300124", "sz300760",
    "bj920000", "bj920001",
]
INDEX_SYMBOLS = ["sh000300", "sh000001", "sz399006"]
START, END = "20190701", "20500101"
MIN_OK_RATIO = 0.80


def probe(label: str, fn) -> tuple[bool, str]:
    t0 = time.time()
    try:
        df = fn()
        n = 0 if df is None else len(df)
        last = ""
        if n:
            for col in ("date", "日期"):
                if hasattr(df, "columns") and col in df.columns:
                    last = str(df[col].iloc[-1])[:10]
                    break
            if not last:
                last = str(df.index[-1])[:10]
        return n > 0, (f"  {label:40s} OK   rows={n:<6d} last={last:<12s} "
                       f"{time.time() - t0:5.1f}s")
    except Exception as e:
        return False, (f"  {label:40s} FAIL {type(e).__name__}: {str(e)[:70]}  "
                       f"{time.time() - t0:5.1f}s")


def show(label: str, fn) -> bool:
    ok, line = probe(label, fn)
    print(line, flush=True)
    return ok


print(f"== 探测配置：socket 超时 {PROBE_TIMEOUT}s / 并发 {MAX_WORKERS} ==", flush=True)

print("\n== ① 代码清单层（非东财源）==", flush=True)
show("stock_info_a_code_name", lambda: ak.stock_info_a_code_name())
show("stock_zh_a_spot (新浪)", lambda: ak.stock_zh_a_spot())
show("stock_zh_a_spot_tx (腾讯)", lambda: ak.stock_zh_a_spot_tx())

print("\n== ② 指数日线层（非东财源）==", flush=True)
for sym in INDEX_SYMBOLS:
    show(f"stock_zh_index_daily {sym}",
         lambda s=sym: ak.stock_zh_index_daily(symbol=s))
    show(f"stock_zh_index_daily_tx {sym}",
         lambda s=sym: ak.stock_zh_index_daily_tx(symbol=s, start_date=START, end_date=END))

print("\n== ③ 逐股后复权历史层（HIST_FALLBACK 的唯一依据）==", flush=True)
tasks = []
for code in SAMPLE:
    tasks.append((f"stock_zh_a_daily hfq {code}",
                  lambda c=code: ak.stock_zh_a_daily(symbol=c, start_date=START,
                                                     end_date=END, adjust="hfq")))
    tasks.append((f"stock_zh_a_hist_tx hfq {code}",
                  lambda c=code: ak.stock_zh_a_hist_tx(symbol=c, start_date=START,
                                                       end_date=END, adjust="hfq")))
with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    pending = [(label, ex.submit(probe, label, fn)) for label, fn in tasks]
    results = [(label, fu.result()) for label, fu in pending]
for label, (_, line) in results:
    print(line, flush=True)

ok_sina = sum(1 for label, (ok, _) in results
              if ok and label.startswith("stock_zh_a_daily"))
ok_tx = sum(1 for label, (ok, _) in results
            if ok and label.startswith("stock_zh_a_hist_tx"))
n = len(SAMPLE)
rs, rt = ok_sina / n, ok_tx / n
print("\n== 汇总 ==", flush=True)
print(f"  新浪 stock_zh_a_daily     : {ok_sina}/{n} = {rs:.1%}", flush=True)
print(f"  腾讯 stock_zh_a_hist_tx   : {ok_tx}/{n} = {rt:.1%}", flush=True)
good = max(rs, rt) >= MIN_OK_RATIO
print(f"  阈值 {MIN_OK_RATIO:.0%} → " +
      ("OVERSEAS_RUNNER_OK：海外 runner 可用，可人工按 12.4-H 启用 HIST_FALLBACK=1"
       if good else
       "OVERSEAS_RUNNER_FAIL：海外不可用，必须保持 HIST_FALLBACK=0（本机国内 IP 测通不算数）"),
      flush=True)
print("  注意：本探测永远 exit 0，FAIL 不是仓库故障，请勿据此改其它 workflow。", flush=True)
sys.exit(0)
