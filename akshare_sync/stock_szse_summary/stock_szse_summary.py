"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_szse_summary.py
===========================
描述: 上海证券交易所-股票数据总貌
限量: 单次返回最近交易日的股票数据总貌(当前交易日的数据需要交易所收盘后统计)

目标地址: http://www.szse.cn/market/overview/index.html
目标表名:  stock_szse_summary

"""
import datetime
import os
import time

import akshare as ak
import numpy as np
import pandas as pd

from akshare_sync.global_data.global_data import GlobalData
from akshare_sync.sync_logs.sync_logs import query_last_api_sync_date, update_api_sync_date
from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x) #


def sync(drop_exist):
    cfg = get_cfg()
    logger = get_logger('stock_szse_summary', cfg['sync-logging']['filename'])
    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)
        engine = get_engine()

        # 查询交易日历
        global_data = GlobalData()
        trade_date_set = global_data.trade_date_a
        engine = get_engine()
        query_start_date = query_last_api_sync_date('stock_szse_summary', 'stock_szse_summary')
        start_date = str(max(query_start_date, '20100101'))
        end_date = str(datetime.datetime.now().strftime('%Y%m%d'))
        date_list = [date for date in trade_date_set if start_date < date <= end_date]
        logger.info(f"Execute Sync stock_szse_summary From Date[{start_date}] to Date[{end_date}]")

        for step_date in date_list:
            logger.info(f"Execute Sync stock_szse_summary Date[{step_date}]")
            df = ak.stock_szse_summary(date=step_date)
            if not df.empty:
                df["日期"] = step_date
                df = df[["日期", "证券类别", "数量", "成交金额", "总市值", "流通市值"]]
                df.to_sql("stock_szse_summary", engine, index=False, if_exists='append', chunksize=5000)
                logger.info(f"Execute Sync stock_szse_summary Date[{step_date}]" + f" Write[{df.shape[0]}] Records")
                update_api_sync_date('stock_szse_summary', 'stock_szse_summary', f'{step_date}')
    except Exception as e:
        logger.error(f"Table [stock_szse_summary] Sync Failed Cause By [{e.__cause__}] Traceback[{e.__traceback__}]")




# 增量追加表数据, 股票列表不具备增量条件, 全量覆盖
if __name__ == '__main__':
    sync(False)

