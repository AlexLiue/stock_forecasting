"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 16:09
# @Author  : PcLiu
# @FileName: daily.py
===========================

加载获取 daily 表数据
"""

import pandas as pd

from utils.utils import get_query_condition, get_sql_engine


def load_daily(ts_code='', start_date='', end_date=''):
    """
    从数据库中获取 A股日线行情 
    :return: A股日线行情
        ts_code: 股票代码
        trade_date: 交易日期
        open: 开盘价
        high: 最高价
        low: 最低价
        close: 收盘价
        pre_close: 昨收价(前复权)
        change '涨跌额
        pct_chg: 涨跌幅 （未复权）
        vol: 成交量 （手）
        amount: 成交额 （千元）

        example:
            id |ts_code  |trade_date|open |high  |low  |close |pre_close|change|pct_chg|vol    |amount|
           ---+---------+----------+-----+------+-----+------+---------+------+-------+-------+------|
             1|600602.SH|1990-12-19|365.7| 384.0|365.7| 384.0|      1.0| 383.0|38300.0| 1160.0| 443.0|
             2|600656.SH|1990-12-19|  2.6|   2.6|  2.6|   2.6|      1.0|   1.6|  160.0|   50.0|  13.0|
             3|600601.SH|1990-12-19|185.3| 185.3|185.3| 185.3|      1.0| 184.3|18430.0|   50.0|  37.0|
             4|600602.SH|1990-12-20|403.2| 403.2|403.2| 403.2|    384.0|  19.2|    5.0|  149.0|  60.0|
             5|600656.SH|1990-12-20| 2.73|  2.73| 2.73|  2.73|      2.6|  0.13|    5.0|   25.0|   6.0|
    """
    # 构建查询 Where 条件
    condition = get_query_condition(ts_code, start_date, end_date)

    # 执行 SQL 查询
    engine = get_sql_engine()
    sql = "SELECT `ts_code`,`trade_date`,`open`,`high`,`low`,`close`,`pre_close`,`change`,`pct_chg`,`vol`,`amount` " \
          "FROM `daily` d " \
          "WHERE %s " % condition
    return pd.read_sql(sql, engine)


if __name__ == '__main__':
    df = load_daily(ts_code='600602.SH', start='19901219', end='19901222')
    print(df)
