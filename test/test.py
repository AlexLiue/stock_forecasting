
import tushare as ts
pro = ts.pro_api()

df = pro.margin_detail(trade_date='20180802')

