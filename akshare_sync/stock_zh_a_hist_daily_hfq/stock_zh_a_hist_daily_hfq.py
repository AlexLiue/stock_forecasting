"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_zh_a_hist_daily_hfq.py
===========================
接口: stock_zh_a_hist

目标地址: https://quote.eastmoney.com/concept/sh603777.html?from=classic(示例)

描述: 东方财富-沪深京 A 股日频率数据; 历史数据按日频率更新, 当日收盘价请在收盘后获取

限量: 单次返回指定沪深京 A 股上市公司、指定周期和指定日期间的历史行情日频率数据
"""
import datetime
import traceback

from dateutil.relativedelta import relativedelta
import os
import time
import pandas as pd

from akshare_sync.akshare_overwrite.overwrite_function import stock_zh_a_hist
from akshare_sync.global_data.global_data import GlobalData
from akshare_sync.sync_logs.sync_logs import update_api_sync_date


from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x) #



def query_last_sync_date(trade_code, engine, logger):
    query_start_date = f"SELECT NVL(MAX(\"日期\"), 19900101) as max_date FROM STOCK_ZH_A_HIST_DAILY_HFQ WHERE \"股票代码\"='{trade_code}'"
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


def sync(drop_exist=False):
    cfg = get_cfg()
    logger = get_logger('stock_zh_a_hist_daily_hfq', cfg['sync-logging']['filename'])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        # 查询交易股票列表
        global_data = GlobalData()
        trade_code_list = global_data.trade_code_a
        # 结束日期: 16:30:00 前取前一天的日期，否则取当天的日期
        end_date = str(datetime.datetime.now().strftime('%Y%m%d')) if datetime.datetime.now().strftime('%H:%M:%S') > "16:30:00" \
            else (datetime.datetime.now() + relativedelta(days=-1)).strftime('%Y%m%d')

        engine = get_engine()
        for row_idx in range(trade_code_list.shape[0]):
            row = trade_code_list.iloc[row_idx]
            trade_code = row.iloc[0]
            trade_name = row.iloc[1]
            last_sync_date = query_last_sync_date(trade_code, engine, logger)
            start_date =  (datetime.datetime.strptime(last_sync_date, '%Y%m%d') + relativedelta(days=1)).strftime('%Y%m%d')

            if start_date <= end_date:
                logger.info(
                    f"Execute Sync stock_zh_a_hist  trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}]")
                df = stock_zh_a_hist(symbol=trade_code, period="daily", start_date=start_date, end_date=end_date,
                                        adjust="hfq", timeout=20)
                if not df.empty:
                    df["日期"] = df["日期"].apply(lambda x: x.strftime('%Y%m%d'))
                    df.to_sql("stock_zh_a_hist_daily_hfq", engine, index=False, if_exists='append', chunksize=20000)
                    logger.info(
                        f"Execute Sync stock_zh_a_hist_daily_hfq trade_code[{trade_code}]" + f" Write[{df.shape[0]}] Records")
            else:
                logger.info(
                    f"Execute Sync stock_zh_a_hist_daily_hfq  trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}], Skip Sync ... ")

        engine.close()
        update_api_sync_date('stock_zh_a_hist', 'stock_zh_a_hist_daily_hfq', f'{str(end_date)}')

    except Exception as e:

        logger.error(f"Table [stock_szse_summary] Sync  Failed", exc_info=True)



if __name__ == '__main__':
    sync(False)

