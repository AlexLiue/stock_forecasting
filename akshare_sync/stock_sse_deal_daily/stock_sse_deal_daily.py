"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_sse_deal_daily.py
===========================
接口: stock_sse_deal_daily

目标地址: http://www.sse.com.cn/market/stockdata/overview/day/

描述: 上海证券交易所-数据-股票数据-成交概况-股票成交概况-每日股票情况

限量: 单次返回指定日期的每日概况数据, 当前交易日数据需要在收盘后获取; 注意仅支持获取在 20211227（包含）之后的数据

"""
import datetime
from dateutil.relativedelta import relativedelta
import os
import time
import akshare as ak
import pandas as pd

from akshare_sync.global_data.global_data import GlobalData
from akshare_sync.sync_logs.sync_logs import query_last_api_sync_date, update_api_sync_date
from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x) #


def sync(drop_exist=False):
    cfg = get_cfg()
    logger = get_logger('stock_sse_deal_daily', cfg['sync-logging']['filename'])
    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        query_start_date = query_last_api_sync_date('stock_sse_deal_daily', 'stock_sse_deal_daily')
        start_date = str(max(query_start_date, '20211231'))
        end_date = str(datetime.datetime.now().strftime('%Y%m%d'))

        # 查询交易日历
        global_data = GlobalData()
        trade_date = global_data.trade_date_a
        date_list = [date for date in trade_date if start_date < date <= end_date]
        logger.info(f"Execute Sync stock_szse_summary From Date[{start_date}] to Date[{end_date}]")

        for step_date in date_list:
            logger.info(f"Execute Sync stock_sse_deal_daily Date[{step_date}]")
            df = ak.stock_sse_deal_daily(date=step_date)
            if not df.empty:
                df = df.set_index("单日情况")
                df = df.T
                df["日期"] = step_date
                df["板块"] = df.axes[0]
                df = df.reset_index(drop=True)
                df = df[
                    ["日期", "板块", "挂牌数", "市价总值", "流通市值", "成交金额", "成交量", "平均市盈率", "换手率",
                     "流通换手率"]]

                df.to_sql("stock_sse_deal_daily", engine, index=False, if_exists='append', chunksize=5000)
                logger.info(
                    f"Execute Sync stock_sse_deal_daily Date[{step_date}]" + f" Write[{df.shape[0]}] Records")

                update_api_sync_date('stock_sse_deal_daily', 'stock_sse_deal_daily',
                                     f'{str(step_date)}')


    except Exception as e:
        logger.error(f"Table [stock_sse_deal_daily] Sync Failed Cause By [{e.__cause__}] Traceback[{e.__traceback__.__str__()}]")





if __name__ == '__main__':
    sync(False)

