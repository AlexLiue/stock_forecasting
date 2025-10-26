"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_sse_deal_daily.py
===========================
描述: 深圳证券交易所-市场总貌-地区交易排序

限量: 单次返回指定 date 的市场总貌数据-地区交易排序数据

目标地址: http://www.szse.cn/market/overview/index.html
目标表名:  stock_szse_area_summary

"""
import datetime
from dateutil.relativedelta import relativedelta
import os
import time
import akshare as ak
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
    logger = get_logger('stock_szse_area_summary', cfg['sync-logging']['filename'])
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist, logger)
    engine = get_engine()

    query_start_date = query_api_sync_date('stock_szse_area_summary', 'stock_szse_area_summary')
    start_date = str(max(query_start_date, '20191201'))
    logger.info(f"Execute Sync stock_szse_area_summary Date[{start_date}]")

    end_date = str(datetime.datetime.now().strftime('%Y%m%d'))
    start = datetime.datetime.strptime(start_date, '%Y%m%d') + relativedelta(months=1)
    end = datetime.datetime.strptime(end_date, '%Y%m%d') + relativedelta(months=-1)
    step = start  # 微批开始时间
    while str(step.strftime('%Y%m%d')) <= str(end.strftime('%Y%m%d')):
        cur_retry = 1
        while cur_retry <= max_retry:
            try:
                step_date = str(step.strftime('%Y%m'))
                logger.info(f"Execute Sync stock_szse_area_summary Date[{step_date}]")
                df = ak.stock_szse_area_summary(date=step_date)
                df["日期"] = step_date
                df = df[["日期", "地区", "总交易额", "占市场", "股票交易额", "基金交易额", "债券交易额"]]
                df.to_sql("stock_szse_area_summary", engine, index=False, if_exists='append', chunksize=5000)
                logger.info(f"Execute Sync stock_szse_area_summary Date[{step_date}]" + f"Write[{df.shape[0]}] Records")
                step = step + relativedelta(months=1)
                update_api_sync_date('stock_szse_area_summary', 'stock_szse_area_summary', f'{str(step.strftime('%Y%m%d'))}')
                break
            except Exception as e:
                if cur_retry + 1 <= max_retry:
                    logger.error(f"Get Exception[{e.__cause__}] [{e.__traceback__}]")
                    logger.error(f"Retry[{cur_retry}] Failed, Exec Retry After [{cur_retry * retry_interval}] Second")
                    time.sleep(cur_retry * retry_interval)
                    cur_retry += 1
                else:
                    return -1
    return None


# 增量追加表数据, 股票列表不具备增量条件, 全量覆盖
if __name__ == '__main__':
    sync(False, 3, 5)

