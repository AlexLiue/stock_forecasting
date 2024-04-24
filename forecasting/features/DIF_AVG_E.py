import pandas as pd
from forecasting.utils.formula import SKDJ

from forecasting.utils.utils import load_table, get_query_condition, get_sql_engine


def GET_DIF_AVG_E(symbol, start_date, end_date, N1, N2):
    condition = get_query_condition(symbol, start_date, end_date)
    # 执行 SQL 查询
    engine = get_sql_engine()
    sql = f"SELECT * FROM stock_daily_qfq t WHERE {condition} ORDER BY symbol, trade_date"
    daily = pd.read_sql(sql, engine, index_col="symbol")
    SKDJ(daily, N, M)

