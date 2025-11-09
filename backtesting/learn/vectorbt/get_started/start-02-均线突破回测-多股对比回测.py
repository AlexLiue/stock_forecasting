import numpy as np
import pandas as pd
from datetime import datetime

import vectorbt as vbt

import akshare as ak
from akshare.utils.context import AkshareConfig


""" 创建代理字典 """
proxies = {
    "http": "http://415b8ce2027f2686bd2d__cr.cn,hk:1207de794b991714@proxy.cheapproxy.net:823",
    "https": "http://415b8ce2027f2686bd2d__cr.cn,hk:1207de794b991714@proxy.cheapproxy.net:823"
}
""" 创建代理字典 """
AkshareConfig.set_proxies(proxies)

## 光伏设备-隆基绿能
stock1 = ak.stock_zh_a_hist(symbol="601012", period="daily", start_date="20200101", end_date='20251107', adjust="qfq")
stock1["日期"]=stock1["日期"].apply(lambda x : datetime.strptime(str(x),"%Y-%m-%d"))
stock1.set_index("日期", inplace=True)

# 光伏设备-金辰股份
stock2 = ak.stock_zh_a_hist(symbol="603396", period="daily", start_date="20200101", end_date='20251107', adjust="qfq")
stock2["日期"]=stock2["日期"].apply(lambda x : datetime.strptime(str(x),"%Y-%m-%d"))
stock2.set_index("日期", inplace=True)

stock1_price = stock1["收盘"]
stock2_price = stock2["收盘"]
comb_price = stock1_price.vbt.concat(stock2_price, keys=pd.Index(['隆基绿能', '金辰股份'], name='symbol'))
comb_price.vbt.drop_levels(-1, inplace=True)


fast_ma = vbt.MA.run(comb_price, [10, 20], short_name='fast')
slow_ma = vbt.MA.run(comb_price, [30, 30], short_name='slow')

entries = fast_ma.ma_crossed_above(slow_ma)
exits = fast_ma.ma_crossed_below(slow_ma)
pf = vbt.Portfolio.from_signals(comb_price, entries, exits)
res=pf.total_return()


mean_return = pf.total_return().groupby('symbol').mean()
mean_return.vbt.barplot(xaxis_title='Symbol', yaxis_title='Mean total return')
fig = mean_return.vbt.barplot(xaxis_title='Symbol', yaxis_title='Mean total return')

fig.show()
