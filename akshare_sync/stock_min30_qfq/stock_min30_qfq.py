"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2024/04/22 21:33
# @Author  : PcLiu
# @FileName: stock_table_api_summary.py
===========================
描述: 30分行情-前复权
目标表名:  stock_min30_qfq

"""
import datetime
import os
import time

import akshare as ak
import numpy as np
import pandas as pd
from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg

pd.set_option('display.max_columns', None)


def sync(drop_exist):
    cfg = get_cfg()
    logger = get_logger('stock_min30_qfq', cfg['sync-logging']['filename'])
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist, logger)

    engine = get_engine()
    query_sql = f"SELECT sbi.`symbol`, sbi.`name`, sbi.`exchange` " \
                f"FROM {cfg['mysql']['database']}.stock_basic_info sbi " \
                "WHERE name not like 'ST%%' AND name not like '*ST%%'" \
                f"ORDER BY sbi.`exchange` DESC, sbi.`symbol` ASC; "
    logger.info(f"Execute SQL [{query_sql}]")
    basic_info = pd.read_sql(query_sql, engine)

    for index, row in basic_info.iterrows():
        symbol = row.iloc[0]
        name = row.iloc[1]
        exchange = row.iloc[2]

        query_start_date = "SELECT DATE_ADD(IFNULL(MAX(trade_date),DATE('1990-01-01')),INTERVAL 1 DAY) as trade_date " \
                           f"FROM {cfg['mysql']['database']}.stock_min30_qfq WHERE symbol ='{symbol}';"
        logger.info(f"Execute SQL  [{query_start_date}]")
        # 开始日期: 如果为周五，则日期+2 跳转下周一
        start_date = pd.read_sql(query_start_date, engine).iloc[0, 0]
        start_date = start_date + datetime.timedelta(days=2) if start_date.weekday() == 5 else start_date
        start_date = str(start_date.strftime('%Y%m%d'))

        end_date = str(datetime.datetime.now().date().strftime('%Y%m%d'))

        if start_date <= end_date:
            logger.info(f"Execute Sync [{index}/{basic_info.shape[0] - 1}] Symbol[{symbol}] Name[{name}] "
                        f"Exchange[{exchange}] StartDate[{start_date}] EndDate[{end_date}]")

            minutes = None
            # 沪深京 A 股
            if exchange == 'SSE' or exchange == 'SZSE' or exchange == 'BSE':
                minutes = ak.stock_zh_a_hist_min_em(symbol=symbol, period="30", adjust="qfq",
                                                    start_date=f"{start_date} 00:00:00",
                                                    end_date=f"{end_date} 23:59:59")
            # 港股 HK
            elif exchange == 'HKSE':
                minutes = ak.stock_hk_hist_min_em(symbol=symbol, period="30", adjust="qfq",
                                                  start_date=f"{start_date} 00:00:00",
                                                  end_date=f"{end_date} 23:59:59")

            if not minutes.empty:
                minutes["证券代码"] = symbol
                # 修复部分AVG数据存在异常(如停牌时vol=0), 替换为 forward 值
                minutes["均价"] = minutes["成交额"] / minutes["成交量"] / 100
                minutes["均价"] = minutes["均价"].replace([np.inf, -np.inf], np.nan).ffill()
                # 计算交易日期、交易时间
                minutes["时间"] = pd.to_datetime(minutes["时间"], format='%Y-%m-%d %H:%M:%S', errors="coerce")
                minutes["日期"] = minutes["时间"].dt.date
                minutes["时间"] = minutes["时间"].dt.time

                minutes = minutes[["日期", "证券代码", "时间", "开盘", "收盘", "最高", "最低", "成交量", "成交额",
                                   "均价", "振幅", "涨跌幅", "涨跌额", "换手率"]]
                minutes.columns = ["trade_date", "symbol", "trade_time", "open", "close", "high", "low", "vol",
                                   "amount", "avg", "amp", "pct_chg", "pct_amt", "tr"]

                minutes.to_sql("stock_min30_qfq", engine, index=False, if_exists='append', chunksize=5000)
                logger.info(f"Execute Sync [{index}/{basic_info.shape[0] - 1}] Symbol[{symbol}] Name[{name}] "
                            f" Write[{minutes.shape[0]}] Records")
            if int(end_date) - int(start_date) > 30:
                time.sleep(1)
            else:
                time.sleep(0.5)

        else:
            logger.info(f"Execute Sync [{index}/{basic_info.shape[0]}] Symbol[{symbol}] Name[{name}] "
                        f"StartDate[{start_date}] EndDate[{end_date}], Skip Exec Sync ")



if __name__ == '__main__':
    sync(False)
