"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/26 16:40
# @Author  : PcLiu
# @FileName: launch_feature_calculate.py
===========================

特征预先处理缓存

"""
import argparse

from daily_n import daily_n


def exec_features_cash(overwrite):
    daily_n.calculate(overwrite)  # 计算 N 日均线写入 daily_n 表


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sync mode args')
    parser.add_argument("--overwrite", action='store_true')
    args = parser.parse_args()
    if args.overwrite:
        exec_features_cash(overwrite=True)
    else:
        exec_features_cash(overwrite=False)

