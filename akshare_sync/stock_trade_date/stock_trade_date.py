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
import traceback

import akshare as ak
import pandas as pd

from akshare_sync.sync_logs.sync_logs import query_last_api_sync_date, update_api_sync_date
from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg


# 全量初始化表数据
def sync(drop_exist=False):
    cfg = get_cfg()
    logger = get_logger('stock_trade_date', cfg['sync-logging']['filename'])
    try:
        engine = get_engine()
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        last_date = query_last_api_sync_date('tool_trade_date_hist_sina', 'stock_trade_date')
        cur_date = datetime.datetime.now().date().strftime('%Y%m%d')


        if last_date < cur_date:
            trade_date = ak.tool_trade_date_hist_sina()
            trade_date['trade_date'] = trade_date['trade_date'].apply(lambda d: int(d.strftime('%Y%m%d')))
            sse = pd.DataFrame({"exchange": "SSE", "trade_date": trade_date["trade_date"]})
            szse = pd.DataFrame({"exchange": "SZSE", "trade_date": trade_date["trade_date"]})
            bse = pd.DataFrame({"exchange": "BSE", "trade_date": trade_date["trade_date"]})
            trade_data_a = pd.concat([sse, szse, bse])
            trade_data_a.columns = ["交易所", "交易日期"]
            trade_data_a.to_sql("stock_trade_date", engine, index=False, if_exists='append', chunksize=5000)
            logger.info(f"Execute Sync stock_trade_date Date[{cur_date}]" + f" Write[{trade_data_a.shape[0]}] Records")
            update_api_sync_date('tool_trade_date_hist_sina', 'stock_trade_date', f'{cur_date}')

    except Exception as e:
        logger.error(f"Table [stock_zh_a_hist_weekly_hfq] Sync Failed Cause By [{e.__cause__}] Stack[{traceback.format_exc()}]")

if __name__ == '__main__':
    sync(False)
