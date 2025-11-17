"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/11/17 12:19
# @Author  : PcLiu
# @FileName: ma_crossover.py
===========================

移动均线交叉策略 (MA Crossover)
思路：短期均线反映当前动量；长期均线反映趋势方向
信号：
买入：MA_short 上穿 MA_long；
卖出：MA_short 下穿 MA_long。
参数示例：20日MA 与 60日MA。
优点：简单稳健、长期有效；
缺点：震荡市假信号多。

"""

