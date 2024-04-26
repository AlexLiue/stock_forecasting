"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2024/04/22 21:33
# @Author  : PcLiu
# @FileName: stock_hour_qfq.py
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
import pandas as pd
from akshare_sync.util.tools import exec_create_table_script, get_mock_connection, get_logger, get_cfg


def sync(drop_exist):
    cfg = get_cfg()
    logger = get_logger('stock_daily_qfq', cfg['sync-logging']['filename'])
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist, logger)

    engine = get_mock_connection()
    query_sql = f"SELECT sbi.`symbol`, sbi.`name` " \
                f"FROM {cfg['mysql']['database']}.stock_basic_info sbi " \
                "WHERE exchange!='HKSE' AND name not like 'ST%%' AND name not like '*ST%%'" \
                f"ORDER BY sbi.`symbol` ASC; "
    logger.info(f"Execute SQL [{query_sql}]")
    basic_info = pd.read_sql(query_sql, engine)

    for index, row in basic_info.iterrows():
        symbol = row.iloc[0]
        name = row.iloc[1]

        query_date = "SELECT DATE_FORMAT(DATE_ADD(STR_TO_DATE(CAST(IFNULL(max(trade_date),19900101) AS CHAR)," \
                     "'%%Y%%m%%d'), INTERVAL 1 DAY),'%%Y%%m%%d') as trade_date " \
                     f"FROM {cfg['mysql']['database']}.stock_daily_qfq WHERE symbol ='{symbol}';"
        logger.info(f"Execute SQL  [{query_date}]")
        start_date = pd.read_sql(query_date, engine).iloc[0, 0]
        end_date = str(datetime.datetime.now().strftime('%Y%m%d'))
        if start_date <= end_date:
            logger.info(f"Execute Sync [{index}/{basic_info.shape[0]-1}] Symbol[{symbol}] Name[{name}] "
                        f"StartDate[{start_date}] EndDate[{end_date}]")
            daily = ak.stock_zh_a_hist(symbol=symbol, period="daily",
                                       start_date=start_date, end_date=end_date, adjust="qfq")
            if not daily.empty:
                daily["日期"] = daily["日期"].apply(lambda x: str(x.strftime('%Y%m%d')))
                daily["证券代码"] = symbol

                daily = daily[
                    ["日期", "证券代码", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额",
                     "换手率"]]
                daily.columns = ["trade_date", "symbol", "open", "close", "high", "low", "vol",
                                 "amount", "amp", "pct_chg", "pct_amt", "tr"]
                daily.to_sql("stock_daily_qfq", engine, index=False, if_exists='append', chunksize=5000)
                logger.info(f"Execute Sync [{index}/{basic_info.shape[0]-1}] Symbol[{symbol}] Name[{name}] "
                            f"Write[{daily.shape[0]}] Records")
            if int(end_date) - int(start_date) > 30:
                time.sleep(5)

        else:
            logger.info(f"Execute Sync [{index}/{basic_info.shape[0]}] Symbol[{symbol}] Name[{name}] "
                        f"StartDate[{start_date}] EndDate[{end_date}], Skip Exec Sync ")


# 增量追加表数据, 股票列表不具备增量条件, 全量覆盖
if __name__ == '__main__':
    sync(False)
