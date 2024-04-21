"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 16:31
# @Author  : PcLiu
# @FileName: daily_n_diff.py
===========================

计算 "N日线"  的差值并存储数据库
N 日线值: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 21, 31, 45, 61, 91, 123, 187, 365, 731, 1095, 99999]

根据 daily_n 表的 avg, low, high 值, 计算 差值



"""

import datetime

import numpy as np
import pandas as pd
from scipy import signal
from matplotlib import pyplot as plt
from matplotlib.ticker import MultipleLocator
from scipy.interpolate import make_interp_spline

from forecasting.utils.utils import load_table

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)
# 设置value的显示长度为100，默认为50
pd.set_option('display.width', 100000)
np.set_printoptions(threshold=np.inf)


def get_daily_n_diff(ts_code, start_date, end_date):
    """
    获取 N 日线的差值, 获得 短线、中线、长线指标

    N日线可选项： [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 21, 31, 45, 61, 91, 123, 187, 365]

    特征值说明:
    diff_day: 短线， 做 T 指标，当峰值下降时卖出, 等待<0, 再次突破 0 时买入（操作前提： 长线上升, 中线的值 > 0）
    diff_weak: 中线,  建仓指标，突破 0 时准备建仓, 跌破 0 时清苍（操作前提， 长线出现极点后 ）
    diff_month: 长线,  做 T / 建仓 参考指标

    :param ts_code: 股票代码
    :param start_date: 开始时间
    :param end_date: 结束时间
    :return:
    """
    daily_n = load_table('daily_n', ts_code=ts_code, start_date=start_date, end_date=end_date)
    daily_n['trade_date'] = daily_n['trade_date'].map(lambda date:str(date))
    dic = {'ts_code': daily_n['ts_code'], 'trade_date': daily_n['trade_date'], 'avg': daily_n['avg_1'],
           'diff_1': daily_n['avg_2'] - daily_n['avg_5'],
           'diff_2': daily_n['avg_7'] - daily_n['avg_15'],
           'diff_3': signal.savgol_filter((daily_n['avg_31'] - daily_n['avg_61']), 31, 5),

           }

    return pd.DataFrame(dic)


def test():
    ts_code = '000001.SZ'
    start_date = '20230101'
    end_date = '20240501'

    df = get_daily_n_diff(ts_code, start_date, end_date)

    ax = df.plot(x='trade_date', secondary_y=['avg'])
    ax.xaxis.set_major_locator(MultipleLocator(20))

    ax.set_ylabel('N日线差')
    ax.right_ax.set_ylabel('股价')
    ax.legend(loc='upper left')
    ax.right_ax.legend(loc='upper right')
    ax.grid(True, linestyle='--', which='major')

    plt.title(ts_code)
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = 'SimHei'
    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['font.size'] = 15
    plt.xticks(rotation=45)
    plt.xlabel('交易日')
    plt.ylabel('股价')
    plt.show()


if __name__ == '__main__':
    test()

#
# SH_601699 = load_table('daily_n', ts_code='001330.SZ', start_date='', end_date='')
#
# avg_column = ['avg_' + str(x) for x in steps]
# avg_n = SH_601699.loc[:, ['ts_code', 'trade_date'] + avg_column]
# print(avg_n)
