"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2023/4/07 16:31
# @Author  : PcLiu
# @FileName: daily_n_average_pyplot.py
===========================

计算 "N日线" 并存储 MySQL 库， N 日线画图

1. 根据 daily 表的 交易总金额 amount 和 交易总手数 vol 计算 N 日均值
2. 初始化过程使用  pandas.DataFrame.rolling() 窗口函数计算
3. 后续追加使用手工计算： N 日总交易金额 / N 日总交易手数 * 100
"""
import datetime

import pandas as pd
import matplotlib.pyplot as plt


from features_calculate.utils.utils import get_cfg, get_sql_engine


def plot(ts_code, columns):
    # 加载数据
    cfg = get_cfg()
    engine = get_sql_engine()
    load_sql = f"select trade_date, {columns} from {cfg['mysql']['database']}.daily_n_average" \
               f" where ts_code like '{ts_code}%%' order by trade_date asc"
    df = pd.read_sql(load_sql, engine)

    df['trade_date'] = df['trade_date'].map(lambda date: datetime.datetime.strptime(str(date), '%Y%m%d'))
    df.plot(x='trade_date',
            y=['avg_1', 'avg_2', 'avg_3', 'avg_4', 'avg_31', 'avg_61'],
            title=ts_code
            )
    plt.title(ts_code)
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = 'SimHei'
    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['font.size'] = 15
    plt.xlabel('交易日')
    plt.ylabel('股价')
    plt.show()


if __name__ == '__main__':
    ts_code_arg = '000001'
    columns_arg = 'avg_1, avg_2, avg_3, avg_4, avg_31, avg_61'
    plot(ts_code_arg, columns_arg)
