"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/26 16:40
# @Author  : PcLiu
# @FileName: exec_features_cash.py
===========================

特征预先处理缓存

"""
import argparse

from features_cash.daily_n import daily_n


def init_features_cash():
    """
    初始化特征预处理数据
    :return:
    """
    daily_n.init()  # N 日线特征


def append_features_cash():
    """
    追加特征预处理数据
    :return:
    """
    daily_n.append()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sync mode args')
    parser.add_argument("--mode", type=str, default='', help='同步模式: init(初始化模式), append(增量追加模式)')
    args = parser.parse_args()
    mode = args.mode
    if mode == 'init':
        init_features_cash()
    elif mode == 'append':
        append_features_cash()
    else:
        print('Useage: python data_syn.py --mode [init | append | init_spc | append_spc]')
