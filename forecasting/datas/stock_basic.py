"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/22 23:09
# @Author  : PcLiu
# @FileName: stock_basic.py
===========================

沪深股票-基础信息-股票列表   stock_basic 表
接口：stock_basic，可以通过数据工具调试和查看数据
描述：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
tushare 接口说明： https://tushare.pro/document/2?doc_id=25
"""

import pandas as pd

from forecasting.utils.utils import get_sql_engine


def load_stock_basic():
    """
    从数据库中获取 沪深股票-基础信息-股票列表
    """
    # 构建查询 Where 条件
    condition = '1=1'

    # 执行 SQL 查询
    engine = get_sql_engine()
    sql = "SELECT `ts_code`,`symbol`,`name`,`area`,`industry`,`fullname`,`enname`,`cnspell`,`market`,`exchange`," \
          "`curr_type`,`list_status`,`list_date`,`delist_date`,`is_hs` " \
          "FROM `stock_basic` sb " \
          "WHERE %s " \
          "ORDER BY sb.ts_code ASC " % condition
    return pd.read_sql(sql, engine)


if __name__ == '__main__':
    df = load_stock_basic()
    print(df)
