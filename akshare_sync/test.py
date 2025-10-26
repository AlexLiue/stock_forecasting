import akshare as ak

stock_sse_summary_df = ak.stock_sse_summary()
print(stock_sse_summary_df)

t1=stock_sse_summary_df.T
print(t1)