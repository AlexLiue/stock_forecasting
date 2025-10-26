import akshare as ak

import akshare as ak
import pandas as pd

import akshare as ak
import tushare as ts

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)


import akshare as ak

import akshare as ak
#
# stock_sse_deal_daily_df = ak.tool_trade_date_hist_sina()
# print(stock_sse_deal_daily_df)
# # ak.stock_szse_sector_summary(symbol="当月", date=step_date)


pro = ts.pro_api("60d29499510471150805842b1c7fc97e3a7ece2676b4ead1707f94d0")


df = pro.trade_cal(exchange='', start_date='20180101', end_date='20181231')

print(df)