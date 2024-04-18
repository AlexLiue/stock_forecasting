"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/26 16:40
# @Author  : PcLiu
# @FileName: launch_cash.py
===========================

特征预先处理缓存

"""
import argparse

from daily_n_average import daily_n_average


def exec_features_cash(overwrite):
    daily_n_average.calculate(overwrite)  # 计算 N 日均线写入 daily_n_average 表


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sync mode args')
    parser.add_argument("--overwrite", action='store_true')
    args = parser.parse_args()
    if args.overwrite:
        exec_features_cash(overwrite=True)
    else:
        exec_features_cash(overwrite=False)

