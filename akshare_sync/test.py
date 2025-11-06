import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

#
# stock_sse_deal_daily_df = ak.tool_trade_date_hist_sina()
# print(stock_sse_deal_daily_df)
# # ak.stock_szse_sector_summary(symbol="当月", date=step_date)

import akshare as ak

stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="19900101", end_date='20260528', adjust="hfq")
print(stock_zh_a_hist_df)