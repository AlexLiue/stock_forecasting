"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_szse_sector_summary.py
===========================
接口: stock_szse_sector_summary

目标地址: http://docs.static.szse.cn/www/market/periodical/month/W020220511355248518608.html

描述: 深圳证券交易所-统计资料-股票行业成交数据

限量: 单次返回指定 symbol 和 date 的统计资料-股票行业成交数据

"""
import datetime
import os
import time
import traceback

import akshare as ak
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from akshare_sync.sync_logs.sync_logs import query_last_api_sync_date, update_api_sync_date
from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x) #


def sync(drop_exist, max_retry, retry_interval):
    cfg = get_cfg()
    logger = get_logger('stock_szse_sector_summary', cfg['sync-logging']['filename'])
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist, logger)
    engine = get_engine()

    query_start_date = query_last_api_sync_date('stock_szse_sector_summary', 'stock_szse_sector_summary')
    start_date = str(max(query_start_date, '20181201'))
    logger.info(f"Execute Sync stock_szse_sector_summary Date[{start_date}]")


    end_date = str(datetime.datetime.now().strftime('%Y%m%d'))
    start = datetime.datetime.strptime(start_date, '%Y%m%d') + relativedelta(months=1)
    end = datetime.datetime.strptime(end_date, '%Y%m%d') + relativedelta(months=-1)
    step = start  # 微批开始时间
    while str(step.strftime('%Y%m%d')) <= str(end.strftime('%Y%m%d')):
        cur_retry = 1
        while cur_retry <= max_retry:
            try:
                step_date = str(step.strftime('%Y%m'))
                logger.info(f"Execute Sync stock_szse_sector_summary Date[{step_date}]")
                df = ak.stock_szse_sector_summary(symbol="当月", date=f'{step_date}')
                df["日期"] = step_date
                df = df[["日期", "项目名称", "项目名称-英文", "交易天数", "成交金额-人民币元", "成交金额-占总计", "成交股数-股数", "成交股数-占总计", "成交笔数-笔", "成交笔数-占总计"]]
                df.columns = ["日期", "名称", "名称英文", "交易天数", "成交金额", "成交金额占比", "成交股数", "成交股数占比", "交笔数", "成交笔数占比"]
                df.to_sql("stock_szse_sector_summary", engine, index=False, if_exists='append', chunksize=5000)
                logger.info(f"Execute Sync stock_szse_sector_summary Date[{step_date}]" + f" Write[{df.shape[0]}] Records")
                step = step + relativedelta(months=1)
                update_api_sync_date('stock_szse_sector_summary', 'stock_szse_sector_summary', f'{str(step.strftime('%Y%m%d'))}')
                break
            except Exception as e:
                if cur_retry + 1 <= max_retry:
                    logger.error(f"Get Exception[{e.__cause__}]]")
                    traceback.print_exc()
                    logger.error(f"Retry[{cur_retry}] Failed, Exec Retry After [{cur_retry * retry_interval}] Second")
                    time.sleep(cur_retry * retry_interval)
                    cur_retry += 1
                else:
                    return -1
    return None


# 增量追加表数据, 股票列表不具备增量条件, 全量覆盖
if __name__ == '__main__':
    sync(True, 3, 10)

