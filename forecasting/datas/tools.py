"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 13:56
# @Author  : PcLiu
# @FileName: tools.py
===========================

辅助工具模块, 提供基本的常用函数实现

"""

# 加载配置信息函数
import configparser
import os


def get_cfg():
    cfg = configparser.ConfigParser()
    file_name = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../application.ini'))
    cfg.read(file_name)
    return cfg
