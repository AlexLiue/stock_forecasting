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

from akshare_sync.sync_logs.sync_logs import query_api_sync_date, update_api_sync_date
from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x) #


def sync(drop_exist, max_retry, retry_interval):
    cfg = get_cfg()
    logger = get_logger('stock_szse_summary', cfg['sync-logging']['filename'])
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist, logger)
    engine = get_engine()

    query_start_date = query_api_sync_date('stock_szse_summary', 'stock_szse_summary')
    start_date = str(max(query_start_date, '20100101'))
    logger.info(f"Execute Sync stock_sse_summary Date[{start_date}]")

    end_date = str(datetime.datetime.now().strftime('%Y%m%d'))
    start = datetime.datetime.strptime(start_date, '%Y%m%d') + datetime.timedelta(days=1)
    end = datetime.datetime.strptime(end_date, '%Y%m%d')
    step = start  # 微批开始时间
    while step <= end:
        cur_retry = 1
        while cur_retry <= max_retry:
            try:
                step_date = str(step.strftime('%Y%m%d'))
                logger.info(f"Execute Sync stock_szse_summary Date[{step_date}]")
                df = ak.stock_szse_summary(date=step_date)
                df["日期"] = step_date
                df = df[["日期", "证券类别", "数量", "成交金额", "总市值", "流通市值"]]
                df.to_sql("stock_szse_summary", engine, index=False, if_exists='append', chunksize=5000)
                logger.info(f"Execute Sync stock_szse_summary Date[{step_date}]" + f"Write[{df.shape[0]}] Records")
                step = step + datetime.timedelta(days=1)
                update_api_sync_date('stock_sse_summary', 'stock_sse_summary', f'{step_date}')

                break
            except Exception as e:
                if cur_retry + 1 <= max_retry:
                    logger.error(f"Get Exception[{e.__cause__}] [{e.__traceback__}]")
                    logger.error(f"Retry[{cur_retry}] Failed, Exec Retry After [{cur_retry * retry_interval}] Second")
                    time.sleep(cur_retry * retry_interval)
                    cur_retry += 1
                else:
                    return -1


# 增量追加表数据, 股票列表不具备增量条件, 全量覆盖
if __name__ == '__main__':
    sync(False, 3, 10)

