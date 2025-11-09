from datetime import datetime

import vectorbt as vbt

import akshare as ak
from akshare.utils.context import AkshareConfig


""" 创建代理字典 """
proxies = {
    "http": "http://415b8ce2027f2686bd2d__cr.cn,hk:1207de794b991714@proxy.cheapproxy.net:823",
    "https": "http://415b8ce2027f2686bd2d__cr.cn,hk:1207de794b991714@proxy.cheapproxy.net:823",
}
""" 创建代理字典 """
AkshareConfig.set_proxies(proxies)

stock_df = ak.stock_zh_a_hist(
    symbol="000001",
    period="daily",
    start_date="20170301",
    end_date="20251107",
    adjust="qfq",
)
stock_df["日期"] = stock_df["日期"].apply(lambda x: datetime.strptime(str(x), "%Y-%m-%d"))
stock_df.set_index("日期", inplace=True)

close_price = stock_df["收盘"]

import akshare as ak
from akshare.utils.context import AkshareConfig


""" 创建代理字典 """
proxies = {
    "http": "http://415b8ce2027f2686bd2d__cr.cn,hk:1207de794b991714@proxy.cheapproxy.net:823",
    "https": "http://415b8ce2027f2686bd2d__cr.cn,hk:1207de794b991714@proxy.cheapproxy.net:823",
}
""" 创建代理字典 """
AkshareConfig.set_proxies(proxies)

stock_df = ak.stock_zh_a_hist(
    symbol="000001",
    period="daily",
    start_date="20170301",
    end_date="20251107",
    adjust="qfq",
)
stock_df["日期"] = stock_df["日期"].apply(lambda x: datetime.strptime(str(x), "%Y-%m-%d"))
stock_df.set_index("日期", inplace=True)

price = stock_df["收盘"]

fast_ma = vbt.MA.run(price, 50, short_name="fast_ma")
slow_ma = vbt.MA.run(price, 200, short_name="slow_ma")

entries = fast_ma.ma_crossed_above(slow_ma)
exits = fast_ma.ma_crossed_below(slow_ma)
pf = vbt.Portfolio.from_signals(price, entries, exits, fees=0.005)

pf.orders.records_readable


fig = price.vbt.plot(trace_kwargs=dict(name="Close"))
fast_ma.ma.vbt.plot(trace_kwargs=dict(name="Fast MA"), fig=fig)
slow_ma.ma.vbt.plot(trace_kwargs=dict(name="Slow MA"), fig=fig)
pf.positions.plot(close_trace_kwargs=dict(visible=False), fig=fig)
