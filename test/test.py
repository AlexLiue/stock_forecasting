"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/19 16:51
# @Author  : PcLiu
# @FileName: test.py
===========================
"""

import time
from time import sleep

import numpy as np
from scipy.interpolate import splrep, splev
import matplotlib.pyplot as plt

# 原始数据点
x = np.linspace(0, 10, 100)
y = np.sin(x) + np.random.randn(100) * 0.1  # 加入一些噪声

# 对数据点进行平滑处理
tck = splrep(x, y, k=1)  # k是样条的度，可以根据需要调整
x_smooth = np.linspace(0, 10, 1000)
y_smooth = splev(x_smooth, tck)

# 绘图
plt.plot(x, y, 'o', label='Original Data')
plt.plot(x_smooth, y_smooth, '-', label='Smoothed Curve')
plt.legend()
plt.show()

print("SSS")