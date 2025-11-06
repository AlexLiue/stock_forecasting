

from akshare_sync.akshare_overwrite.overwrite_function import stock_zh_a_hist


df = stock_zh_a_hist(symbol="600566", period="monthly", start_date=20020131, end_date=20251031, adjust="qfq",
                     timeout=20)

print(df)

