import akshare as ak

import akshare as ak
import pandas as pd

import akshare as ak

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x) #


import akshare as ak

stock_szse_sector_summary_df = ak.stock_szse_sector_summary(symbol="当月", date="201901")
print(stock_szse_sector_summary_df)
#
#
# ak.stock_szse_sector_summary(symbol="当月", date=step_date)