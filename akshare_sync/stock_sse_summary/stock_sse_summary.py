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

from akshare_sync.akshare_overwrite.overwrite_function import stock_sse_summary
from akshare_sync.sync_logs.sync_logs import query_last_api_sync_date, update_api_sync_date
from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x) #



# 全量初始化表数据
def sync(drop_exist=False):
    cfg = get_cfg()
    logger = get_logger('stock_sse_summary', cfg['sync-logging']['filename'])

    try:
        start_date = query_last_api_sync_date('stock_sse_summary', 'stock_sse_summary')
        end_date = str(datetime.datetime.now().strftime('%Y%m%d'))
        if start_date < end_date:
            dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
            exec_create_table_script(dir_path, drop_exist, logger)

            # 获取数据
            stock_sse_summary_df = stock_sse_summary()
            stock_sse_summary_df = stock_sse_summary_df.set_index("项目")

            data = stock_sse_summary_df.T
            data["项目"] = data.axes[0]

            data = data[
                ["报告时间", "项目", "上市股票", "总股本", "流通股本", "总市值", "流通市值", "平均市盈率"]]
            data.columns = ["日期", "项目", "上市股票", "总股本", "流通股本", "总市值", "流通市值", "平均市盈率"]
            data = data.reset_index(drop=True)

            logger.info(f"Execute Filter : 日期 > %s", start_date)
            data = data[data["日期"] > start_date]

            # 写入数据库
            connection = get_engine()
            logger.info(f'Write [{data.shape[0]}] records into table [stock_sse_summary] with [{connection.engine}]')
            data.to_sql('stock_sse_summary', connection, index=False, if_exists='append', chunksize=5000)

            update_api_sync_date('stock_sse_summary', 'stock_sse_summary', f'{str(data["日期"].max())}')

        else:
            logger.info(f"Table [stock_sse_summary] Early Synced start_date[{start_date}] end_date[{end_date}], Skip ...")
    except Exception as e:
        logger.error(f"Table [stock_sse_summary] SyncFailed", exc_info=True)



if __name__ == '__main__':
    sync(False)

