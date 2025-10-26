import akshare as ak

import akshare as ak
import pandas as pd

import akshare as ak
import tushare as ts
from akshare import stock_hk_spot_em

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

import akshare as ak

stock_hk = stock_hk_spot_em().dropna()
stock_hk["交易所"] = "HKSE"
stock_hk["上市日期"] = ""
stock_hk["板块"] = "港交所"
stock_hk = stock_hk[["代码", "名称", "交易所", "板块", "上市日期"]]
stock_hk.columns = ["symbol", "name", "exchange", "market", "list_date"]