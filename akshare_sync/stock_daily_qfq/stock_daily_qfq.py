"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2024/04/22 21:33
# @Author  : PcLiu
# @FileName: stock_daily_qfq.py
===========================

历史行情数据-东财
目标表名:  stock_daily_qfq
接口: stock_zh_a_hist

目标地址: https://quote.eastmoney.com/concept/sh603777.html?from=classic(示例)
描述: 东方财富-沪深京 A 股日频率数据; 历史数据按日频率更新, 当日收盘价请在收盘后获取
限量: 单次返回指定沪深京 A 股上市公司、指定周期和指定日期间的历史行情日频率数据

同步前 前复权 的数据
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
    logger = get_logger('stock_daily_qfq', cfg['sync-logging']['filename'])
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
                           f"FROM {cfg['mysql']['database']}.stock_daily_qfq WHERE symbol ='{symbol}';"
        logger.info(f"Execute SQL  [{query_start_date}]")
        # 开始日期: 如果为周五，则日期+2 跳转下周一
        start_date = pd.read_sql(query_start_date, engine).iloc[0, 0]
        start_date = start_date + datetime.timedelta(days=2) if start_date.weekday() == 5 else start_date
        start_date = str(start_date.strftime('%Y%m%d'))

        end_date = str(datetime.datetime.now().date().strftime('%Y%m%d'))

        if start_date <= end_date:
            logger.info(f"Execute Sync [{index}/{basic_info.shape[0] - 1}] Symbol[{symbol}] Name[{name}] "
                        f"Exchange[{exchange}] StartDate[{start_date}] EndDate[{end_date}]")

            daily = None
            # 沪深京 A 股
            if exchange == 'SSE' or exchange == 'SZSE' or exchange == 'BSE':
                daily = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq",
                                           start_date=start_date, end_date=end_date)
            # 港股 HK
            elif exchange == 'HKSE':
                daily = ak.stock_hk_hist(symbol=symbol, period="daily", adjust="qfq",
                                         start_date=start_date, end_date=end_date)

            if not daily.empty:
                daily["证券代码"] = symbol
                daily["均价"] = daily["成交额"] / daily["成交量"] / 100
                daily["均价"] = daily["均价"].replace([np.inf, -np.inf], np.nan).ffill()

                # 修复部分AVG数据存在异常(如停牌时vol=0), 替换为 forward 值
                daily = daily[
                    ["日期", "证券代码", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "均价", "振幅", "涨跌幅",
                     "涨跌额", "换手率"]].replace([np.inf, -np.inf], np.nan).ffill()
                daily.columns = ["trade_date", "symbol", "open", "close", "high", "low", "vol",
                                 "amount", "avg", "amp", "pct_chg", "pct_amt", "tr"]

                daily.to_sql("stock_daily_qfq", engine, index=False, if_exists='append', chunksize=5000)
                logger.info(f"Execute Sync [{index}/{basic_info.shape[0] - 1}] Symbol[{symbol}] Name[{name}] "
                            f"Write[{daily.shape[0]}] Records")
            if int(end_date) - int(start_date) > 30:
                time.sleep(1)
            else:
                time.sleep(0.1)

        else:
            logger.info(f"Execute Sync [{index}/{basic_info.shape[0]}] Symbol[{symbol}] Name[{name}] "
                        f"StartDate[{start_date}] EndDate[{end_date}], Skip Exec Sync ")


# 增量追加表数据, 股票列表不具备增量条件, 全量覆盖
if __name__ == '__main__':
    sync(False)
