import pandas as pd
from akshare import stock_zh_a_hist

from akshare_sync.util.tools import get_engine, save_to_database

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.float_format", lambda x: "%.2f" % x)

#
# stock_sse_deal_daily_df = ak.tool_trade_date_hist_sina()
# print(stock_sse_deal_daily_df)
# # ak.stock_szse_sector_summary(symbol="当月", date=step_date)

engine = get_engine()
start_date = "19700101"
df = stock_zh_a_hist(
    symbol="600309",
    period="weekly",
    start_date=start_date,
    end_date="20250801",
    adjust="qfq",
    timeout=20,
)

if not df.empty:
    df["日期"] = df["日期"].apply(lambda x: x.strftime("%Y%m%d"))
    df = df.loc[df["日期"] != start_date]
    # save_to_database(df, "stock_zh_a_hist_weekly_qfq", engine, index=False, if_exists='append', chunksize=20000)
    save_to_database(
        df,
        "stock_zh_a_hist_weekly_qfq",
        engine,
        index=False,
        if_exists="append",
        chunksize=20000,
    )


print("")
