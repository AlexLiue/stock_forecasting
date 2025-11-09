"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2024/04/22 21:33
# @Author  : PcLiu
# @FileName: stock_margin.py
===========================
描述:融资融券明细
目标表名:  stock_margin

本日融资余额(元)=前日融资余额＋本日融资买入-本日融资偿还额
本日融券余量(股)=前日融券余量＋本日融券卖出量-本日融券买入量-本日现券偿还量
本日融券余额(元)=本日融券余量×本日收盘价
本日融资融券余额(元)=本日融资余额＋本日融券余额

"""

import datetime
import os

import akshare as ak
import pandas as pd

from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg, save_to_database

pd.set_option('display.max_columns', None)


def sync(drop_exist=False):
    cfg = get_cfg()
    db = cfg['mysql']['database']
    logger = get_logger('stock_margin', cfg['sync-logging']['filename'])
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist, logger)
    engine = get_engine()

    # SSE 融资融券明细
    # 查询交易日历
    trade_date_sql = f"SELECT trade_date FROM {db}.stock_trade_date WHERE exchange ='SSE';"
    logger.info(f"Execute SQL  [{trade_date_sql}]")
    trade_date = pd.read_sql(trade_date_sql, engine)
    # 查询历史最大同步时间
    max_trade_date_sql = f"SELECT IFNULL(MAX(trade_date),DATE('1990-01-01')) as trade_date " \
                         f"FROM {db}.stock_margin WHERE exchange ='SSE';"
    logger.info(f"Execute SQL  [{max_trade_date_sql}]")
    max_rade_date = pd.read_sql(max_trade_date_sql, engine).iloc[0, 0]

    start_date = max(datetime.datetime.strptime("20150101", "%Y%m%d").date(), max_rade_date)
    end_date = datetime.datetime.now().date()

    trade_date = trade_date[(trade_date["trade_date"] > start_date) &
                            (trade_date["trade_date"] < end_date)]

    for index, row in trade_date.iterrows():
        trade_date = row.iloc[0]
        date = str(trade_date.strftime('%Y%m%d'))
        logger.info(f"Execute Sync Exchange[SSE] TradeDate[{date}] By [stock_margin_detail_sse]")
        df = ak.stock_margin_detail_sse(date=date)
        if not df.empty:
            df["交易日期"] = trade_date
            df["交易所"] = "SSE"
            df = df[
                ["交易日期", "标的证券代码", "标的证券简称", "交易所", "融资买入额", "融资余额", "融资偿还额",
                 "融券卖出量", "融券余量", "融券偿还量"]]
            df.columns = ["trade_date", "symbol", "name", "exchange", "buy_value", "buy_balance", "buy_return",
                              "sell_value", "sell_balance_vol", "sell_return"]
            save_to_database(df, "stock_margin", engine, index=False, if_exists='append', chunksize=20000)
            logger.info(f"Execute Sync Exchange[SSE] TradeDate[{date}] Write[{df.shape[0]}] Records")

    # SSE 融资融券明细
    trade_date_sql = f"SELECT trade_date FROM {db}.stock_trade_date WHERE exchange ='SZSE';"
    logger.info(f"Execute SQL  [{trade_date_sql}]")
    trade_date = pd.read_sql(trade_date_sql, engine)
    # 查询历史最大同步时间
    max_trade_date_sql = f"SELECT IFNULL(MAX(trade_date),DATE('1990-01-01')) as trade_date " \
                         f"FROM {db}.stock_margin WHERE exchange ='SZSE';"
    logger.info(f"Execute SQL  [{max_trade_date_sql}]")
    max_rade_date = pd.read_sql(max_trade_date_sql, engine).iloc[0, 0]
    start_date = max(datetime.datetime.strptime("20150101", "%Y%m%d").date(), max_rade_date)
    end_date = datetime.datetime.now().date()
    trade_date = trade_date[(trade_date["trade_date"] > start_date) &
                            (trade_date["trade_date"] < end_date)]

    for index, row in trade_date.iterrows():
        trade_date = row.iloc[0]
        date = str(trade_date.strftime('%Y%m%d'))
        logger.info(f"Execute Sync Exchange[SZSE] TradeDate[{date}] By [stock_margin_detail_szse]")
        df = ak.stock_margin_detail_szse(date=date)
        if not df.empty:
            df["交易日期"] = trade_date
            df["交易所"] = "SZSE"
            df = df[["交易日期", "证券代码", "证券简称", "交易所", "融资买入额", "融资余额",
                             "融券卖出量", "融券余量", "融券余额", "融资融券余额"]]
            df.columns = ["trade_date", "symbol", "name", "exchange", "buy_value", "buy_balance",
                              "sell_value", "sell_balance_vol", "sell_balance_val", "margin_balance"]
            save_to_database(df, "stock_margin", engine, index=False, if_exists='append', chunksize=20000)
            logger.info(f"Execute Sync Exchange[SZSE] TradeDate[{date}] Write[{df.shape[0]}] Records")



if __name__ == '__main__':
    sync(False)
