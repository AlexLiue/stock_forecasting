"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2024/04/22 21:33
# @Author  : PcLiu
# @FileName: stock_margin.py
===========================

历史行情数据-东财
目标表名:  stock_margin
接口:


"""
import datetime
import os
import akshare as ak
import pandas as pd
from akshare_sync.util.tools import exec_create_table_script, get_mock_connection, get_logger, get_cfg

pd.set_option('display.max_columns', None)


def sync(drop_exist):
    cfg = get_cfg()
    logger = get_logger('stock_margin', cfg['sync-logging']['filename'])
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist, logger)

    engine = get_mock_connection()

    #  SSE
    sse_start_date = "SELECT DATE_ADD(" \
                     "IFNULL(MAX(trade_date), DATE('2024-01-01')), INTERVAL 1 DAY) as trade_date " \
                     f"FROM {cfg['mysql']['database']}.stock_margin WHERE exchange ='SSE';"
    sse_start = pd.read_sql(sse_start_date, engine).iloc[0, 0]
    step = sse_start
    while step < datetime.datetime.now().date():
        date = str(step.strftime('%Y%m%d'))
        margin = ak.stock_margin_detail_sse(date=date)
        margin["交易日期"] = step
        margin = margin[["交易日期", "标的证券代码", "标的证券简称", "融资买入额", "融资余额", "融资偿还额",
                         "融券卖出量", "融券余量", "融券偿还量", "融资融券余额"]]
        margin.columns = ["trade_date", "symbol", "name", "buy_value", "buy_balance", "buy_return",
                          "sell_value", "sell_balance_vol", "sell_return", "margin_balance"]
        margin.to_sql("stock_min30_qfq", engine, index=False, if_exists='append', chunksize=5000)
        logger.info(f"Execute Sync  TradeDate[{date}] Write[{margin.shape[0]}] Records")

        # 更新下一次微批时间段
        step = step + datetime.timedelta(days=1)

    szse_start_date = "SELECT DATE_ADD(" \
                      "IFNULL(MAX(trade_date), DATE('2024-01-01')), INTERVAL 1 DAY) as trade_date " \
                      f"FROM {cfg['mysql']['database']}.stock_margin WHERE exchange ='SZSE';"
    szse_start = pd.read_sql(szse_start_date, engine).iloc[0, 0]
    step = szse_start
    while szse_start < datetime.datetime.now().date():
        date = str(step.strftime('%Y%m%d'))
        margin = ak.stock_margin_detail_szse(date=date)
        margin["交易日期"] = date
        margin = margin[["交易日期", "证券代码", "证券简称", "融资买入额", "融资余额",
                         "融券卖出量", "融券余量", "融券余额", "融资融券余额"]]
        margin.columns = ["trade_date", "symbol", "name", "buy_value", "buy_balance",
                          "sell_value", "sell_balance_vol", "sell_balance_val", "margin_balance"]
        margin.to_sql("stock_min30_qfq", engine, index=False, if_exists='append', chunksize=5000)
        logger.info(f"Execute Sync  TradeDate[{date}] Write[{margin.shape[0]}] Records")

        # 更新下一次微批时间段
        step = step + datetime.timedelta(days=1)


# 增量追加表数据, 股票列表不具备增量条件, 全量覆盖
if __name__ == '__main__':
    sync(False)
