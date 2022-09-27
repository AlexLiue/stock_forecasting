"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/26 11:21
# @Author  : PcLiu
# @FileName: daily_n.py
===========================
"""

import numpy as np
import pandas as pd
from utils.utils import load_table

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)
# 设置value的显示长度为100，默认为50
pd.set_option('display.width', 100000)
np.set_printoptions(threshold=np.inf)
SH_601699 = load_table('daily_n', ts_code='001330.SZ', start_date='', end_date='')

steps = [1, 3, 5, 7, 9, 11, 13, 15, 21, 31, 45, 61, 123, 187, 365, 731, 1095]
avg_column = ['avg_' + str(x) for x in steps]
avg_n = SH_601699.loc[:, ['ts_code', 'trade_date'] + avg_column]
print(avg_n)
