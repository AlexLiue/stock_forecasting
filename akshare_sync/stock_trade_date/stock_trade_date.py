"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2024/04/22 21:33
# @Author  : PcLiu
# @FileName: stock_trade_date.py
===========================
描述: 股票交易日历 - A 股票
目标表名:  stock_trade_date
"""

import datetime
import os
import akshare as ak
import pandas as pd

from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg


# 全量初始化表数据
def sync(drop_exist):
    cfg = get_cfg()
    engine = get_engine()
    logger = get_logger('stock_trade_date', cfg['sync-logging']['filename'])
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist, logger)

    max_date_sql = "SELECT IFNULL(MAX(trade_date), DATE('1990-12-19')) as trade_date " \
                   f"FROM {cfg['mysql']['database']}.stock_trade_date"
    logger.info(f"Execute SQL  [{max_date_sql}]")
    max_date = pd.read_sql(max_date_sql, engine).iloc[0, 0]

    if max_date < datetime.datetime.now().date():
        trade_date = ak.tool_trade_date_hist_sina()
        sse = pd.DataFrame({"exchange": "SSE", "trade_date": trade_date["trade_date"]})
        szse = pd.DataFrame({"exchange": "SZSE", "trade_date": trade_date["trade_date"]})
        bse = pd.DataFrame({"exchange": "BSE", "trade_date": trade_date["trade_date"]})
        trade_data_a = pd.concat([sse, szse, bse])
        trade_data_a = trade_data_a[trade_data_a["trade_date"] > max_date]

        trade_data_a.to_sql("stock_trade_date", engine, index=False, if_exists='append', chunksize=5000)
        logger.info(
            f"Execute Sync [trade_date>{str(max_date.strftime('%Y%m%d'))}] Write[{trade_data_a.shape[0]}] Records")


# 增量追加表数据, 股票列表不具备增量条件, 全量覆盖
if __name__ == '__main__':
    sync(False)
