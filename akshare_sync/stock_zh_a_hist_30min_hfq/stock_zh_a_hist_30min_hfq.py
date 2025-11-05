"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_zh_a_hist_30min_hfq.py
===========================
接口: stock_hk_hist_min_em

目标地址: http://quote.eastmoney.com/hk/00948.html

描述: 东方财富网-行情首页-港股-每日分时行情

限量: 单次返回指定上市公司最近 5 个交易日分钟数据, 注意港股有延时

"""

import datetime

import os
import pandas as pd

from akshare_sync.akshare_overwrite.overwrite_function import stock_zh_a_hist_min_em
from akshare_sync.global_data.global_data import GlobalData
from akshare_sync.sync_logs.sync_logs import update_api_sync_date


from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg, exec_sql

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x) #


def query_last_sync_info(trade_code, engine, logger):
    """
    查询上次同步的数据截止时间
    查询上次同步数据的结果数据，检测数据是否发生变动，来判断是否需要重新同步
    """
    result = []
    query_last_date = f"SELECT NVL(MAX(\"时间\"), TO_DATE('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) as max_date FROM STOCK_ZH_A_HIST_30MIN_HFQ WHERE \"股票代码\"='{trade_code}'"
    logger.info(f"Execute Query SQL  [{query_last_date}]")
    last_date = str(pd.read_sql(query_last_date, engine).iloc[0, 0])
    result.append(last_date)

    query_last_close = f"SELECT \"收盘\" FROM STOCK_ZH_A_HIST_30MIN_HFQ WHERE \"股票代码\"='{trade_code}' AND \"时间\"=TO_DATE('{last_date}', 'YYYY-MM-DD HH24:MI:SS')"
    logger.info(f"Execute Query SQL  [{query_last_close}]")
    last_close = pd.read_sql(query_last_close, engine)
    if last_close.shape[0] > 0:
       result.append(last_close.iloc[0,0])
    else:
       result.append(None)

    return result


def get_end_date():
    end_date = ''
    if datetime.datetime.now().strftime('%H:%M:%S') > "16:30:00":
        end_date = str(datetime.datetime.now().strftime('%Y-%m-%d')) + " 15:00:00"
    else:
        now_date = datetime.datetime.now()
        hour = now_date.hour
        minute = '30' if now_date.minute > 30 else '00'
        end_date = now_date.strftime('%Y-%m-%d') + f" {hour:02}:{minute:02}:00"
    return end_date


def sync(drop_exist=False):
    """
    同步 30分钟级 前复权数据，由于当股票分红后前复权的数据需要重新计算，因此需要检测历史数据是否发生变动，如发生变动则需要重新同步
    """
    cfg = get_cfg()
    logger = get_logger('stock_zh_a_hist_30min_hfq', cfg['sync-logging']['filename'])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        # 查询交易股票列表
        global_data = GlobalData()
        trade_code_list = global_data.trade_code_a
        # 结束日期: 16:30:00 前取前一天的日期，否则取当天的日期
        end_date = get_end_date()

        engine = get_engine()
        for row_idx in range(trade_code_list.shape[0]):
            row = trade_code_list.iloc[row_idx]
            trade_code = row.iloc[0]
            trade_name = row.iloc[1]
            last_sync_info = query_last_sync_info(trade_code, engine, logger)
            last_sync_date = last_sync_info[0]
            last_sync_close = last_sync_info[1]
            start_date = last_sync_date

            if start_date < end_date:
                logger.info( f"Execute Sync stock_zh_a_hist_30min_hfq  trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}]")
                df = stock_zh_a_hist_min_em(symbol=trade_code, start_date=start_date, end_date=end_date, period="30", adjust="hfq")
                if not df.empty:
                    df["股票代码"] = trade_code
                    df["时间"] = pd.to_datetime(df['时间'])
                    """ 判断复权的数据是否发生变动 """
                    if last_sync_close is None or df.loc[df["时间"]==start_date, "收盘"][0] == last_sync_close:
                        df = df.loc[df["时间"]!=start_date]
                        df.to_sql("stock_zh_a_hist_30min_hfq", engine, index=False, if_exists='append', chunksize=20000)
                        logger.info(f"Execute Sync stock_zh_a_hist_30min_hfq trade_code[{trade_code}]" + f" Write[{df.shape[0]}] Records")
                    else:
                        clean_sql = f"DELETE FROM stock_zh_a_hist_30min_hfq WHERE \"股票代码\"='{trade_code}'"
                        logger.info(f"Execute Sync stock_zh_a_hist_30min_hfq, Detect QFQ data updated, Clean History Data With SQL [{clean_sql}], Recall Sync")
                        exec_sql(clean_sql)

                        last_sync_info = query_last_sync_info(trade_code, engine, logger)
                        last_sync_date = last_sync_info[0]
                        start_date = last_sync_date
                        df = stock_zh_a_hist_min_em(symbol=trade_code, start_date=start_date, end_date=end_date, period="30", adjust="hfq")
                        if not df.empty:
                            df["股票代码"] = trade_code
                            df["时间"] = pd.to_datetime(df['时间'])
                            df.to_sql("stock_zh_a_hist_30min_hfq", engine, index=False, if_exists='append',  chunksize=20000)
                            logger.info(  f"Execute Sync stock_zh_a_hist_30min_hfq trade_code[{trade_code}]" + f" Write[{df.shape[0]}] Records")
            else:
                logger.info(f"Execute Sync stock_zh_a_hist  trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}], Skip Sync ... ")

        engine.close()
        update_api_sync_date('stock_zh_a_hist_min_em', 'stock_zh_a_hist_30min_hfq', f'{str(end_date)}')

    except Exception as e:
        logger.error(f"Table [stock_zh_a_hist_30min_hfq] Sync  Failed", exc_info=True)



if __name__ == '__main__':
    sync(False)

