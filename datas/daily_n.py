"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 16:08
# @Author  : PcLiu
# @FileName: daily_n_change.py
===========================

基于 daily_n 表(N 日线)数据, 构建特征
"""
import numpy as np
import pandas as pd

from utils.utils import load_table, enable_print_all

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)
# 设置value的显示长度为100，默认为50
pd.set_option('display.width', 100000)
np.set_printoptions(threshold=np.inf)


def get_daily_n_line():
    df = load_table(table_name='daily_n', ts_code='000001.SZ')
    enable_print_all()
    print(df)


if __name__ == '__main__':
    get_daily_n_line()
