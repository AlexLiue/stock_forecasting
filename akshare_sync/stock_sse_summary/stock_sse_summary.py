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
    logger = get_logger('stock_sse_summary', cfg['sync-logging']['filename'])
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist, logger)
    engine = get_engine()

    query_start_date = query_api_sync_date('stock_sse_summary', 'stock_sse_summary')
    start_date = str(max(query_start_date, '19900101'))
    logger.info(f"Execute Sync stock_sse_summary Date[{start_date}]")

    cur_retry = 1
    while cur_retry <= max_retry:
        try:
            stock_sse_summary_df = ak.stock_sse_summary()
            stock_sse_summary_df = stock_sse_summary_df.set_index("项目")

            summary = stock_sse_summary_df.T
            summary["项目"] = summary.axes[0]

            summary = summary[["报告时间", "项目", "上市股票", "总股本", "流通股本", "总市值", "流通市值", "平均市盈率"]]
            summary.columns = ["日期", "项目", "上市股票", "总股本", "流通股本", "总市值", "流通市值", "平均市盈率"]
            summary = summary.reset_index(drop=True)

            logger.info(f"Execute Filter : 日期 > %s", start_date)
            summary = summary[summary["日期"] > start_date]

            summary.to_sql("stock_sse_summary", engine, index=False, if_exists='append', chunksize=5000)
            logger.info(f"Execute Sync stock_sse_summary " + f"Write[{summary.shape[0]}] Records")

            # 跳出
            break

        except Exception as e:
            if cur_retry + 1 <= max_retry:
                logger.error("Get Exception[%s]" % e.__cause__)
                logger.error(f"Retry[{cur_retry}] Failed, Exec Retry After [{cur_retry * retry_interval}] Second")
                time.sleep(cur_retry * retry_interval)
                cur_retry += 1
            else:
                raise e

    cur_date = str(datetime.datetime.now().strftime('%Y%m%d'))
    update_api_sync_date('stock_sse_summary', 'stock_sse_summary', f'{cur_date}')



# 增量追加表数据, 股票列表不具备增量条件, 全量覆盖
if __name__ == '__main__':
    sync(False, 3, 10)

