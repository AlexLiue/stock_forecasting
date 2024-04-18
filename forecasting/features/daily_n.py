"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/26 11:21
# @Author  : PcLiu
# @FileName: daily_n_change.py
===========================
"""
import datetime

import numpy as np
import pandas as pd
from forecasting.utils.utils import load_table

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)
# 设置value的显示长度为100，默认为50
pd.set_option('display.width', 100000)
np.set_printoptions(threshold=np.inf)

steps = [1, 3, 5, 7, 9, 11, 13, 15, 21, 31, 45, 61]


def get_break_up_day_n(trade_date, n):
    """
    获取上升突破 N 日线的股票 N 取值 [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 21, 31, 45, 61]
    :param trade_date: 交易日
    :param n: 上升突破  N 日线
    :return:
    """
    daily_n = load_table('daily_n', ts_code='', start_date=trade_date, end_date=trade_date)
    return daily_n[daily_n['avg_1'] > daily_n['avg_%d' % n]]


def test():
    trade_date = int((datetime.datetime.now() + datetime.timedelta(days=-2)).strftime('%Y%m%d'))
    break_up_day_7 = get_break_up_day_n(trade_date, 7)
    print(break_up_day_7)


if __name__ == '__main__':
    test()

#
# SH_601699 = load_table('daily_n', ts_code='001330.SZ', start_date='', end_date='')
#
# avg_column = ['avg_' + str(x) for x in steps]
# avg_n = SH_601699.loc[:, ['ts_code', 'trade_date'] + avg_column]
# print(avg_n)
