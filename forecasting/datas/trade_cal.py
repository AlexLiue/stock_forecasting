"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 14:35
# @Author  : PcLiu
# @FileName: trade_cal.py
===========================
"""

import pandas as pd
from forecasting.datas.tools import get_sql_engine


def load_trade_cal(exchange='SSE'):
    """
    从数据库中获取交易所交易日历信息, 并添加交易计数列 trade_day_nbr
    说明： A 股第一只股票发行日期: 1990年12月19日, 记 1990年12月19日 为第一个交易日, trade_day_nbr 取值为 1

    :param exchange:
    :return: 指定交易所 exchange 的交易日信息
        exchange: 交易所 SSE上交所 SZSE 深交所
        cal_date: 日历日期
        is_open: 是否交易 0休市 1交易
        trade_day_nbr: 交易日

        example:
         exchange    cal_date pretrade_date  trade_day_nbr
         SSE  1990-12-19          None              1
         SSE  1990-12-20    1990-12-19              2
         SSE  1990-12-21    1990-12-20              3
    """

    engine = get_sql_engine()
    sql = "SELECT exchange,cal_date,pretrade_date,CONVERT((@rowNum:=@rowNum+1), UNSIGNED INTEGER) AS trade_day_nbr " \
          "FROM trade_cal a,(SELECT @rowNum:=0) b " \
          "WHERE exchange = '%s' AND is_open ='1' " \
          "ORDER BY a.cal_date ASC" % exchange
    return pd.read_sql(sql, engine)


if __name__ == '__main__':
    df = load_trade_cal()
    print(df)
