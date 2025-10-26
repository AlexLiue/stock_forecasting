"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_sse_summary.py
===========================
描述: 上海证券交易所-股票数据总貌
限量: 单次返回最近交易日的股票数据总貌(当前交易日的数据需要交易所收盘后统计)

目标地址: http://www.sse.com.cn/market/stockdata/statistic/
目标表名:  stock_sse_summary

"""
import datetime
import os
import time

import akshare as ak
import numpy as np
import pandas as pd
from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg

pd.set_option('display.max_columns', None)


def sync(drop_exist):
    cfg = get_cfg()
    logger = get_logger('stock_sse_summary', cfg['sync-logging']['filename'])
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist, logger)
    engine = get_engine()

    query_start_date = "SELECT NVL(MAX(\"报告时间\"), 19900101) as max_date FROM stock_sse_summary"
    logger.info(f"Execute SQL  [{query_start_date}]")
    # 开始日期: 如果为周五，则日期+2 跳转下周一
    start_date = str(pd.read_sql(query_start_date, engine).iloc[0, 0])

    stock_sse_summary_df = ak.stock_sse_summary()
    stock_sse_summary_df = stock_sse_summary_df.set_index("项目")

    summary = stock_sse_summary_df.T
    summary["项目"] = summary.axes[0]

    summary = summary[["项目", "上市股票", "总股本", "流通股本", "总市值", "流通市值", "平均市盈率", "报告时间"]]
    summary = summary.reset_index(drop=True)

    logger.info(f"Execute Filter : 报告时间 > %s" , start_date)
    summary = summary[summary["报告时间"]>start_date]

    summary.to_sql("stock_sse_summary", engine, index=False, if_exists='append', chunksize=5000)
    logger.info(f"Execute Sync stock_sse_summary " +  f"Write[{summary.shape[0]}] Records")

#

# 增量追加表数据, 股票列表不具备增量条件, 全量覆盖
if __name__ == '__main__':
    sync(True)

