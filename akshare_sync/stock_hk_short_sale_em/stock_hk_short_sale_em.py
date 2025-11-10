"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/11/3 21:33
# @Author  : PcLiu
# @FileName: stock_hk_short_sale_em.py
===========================

接口: stock_hk_short_sale_em

描述: 东方财富港股卖空数据

https://hk.eastmoney.com/sellshort.htm算

"""

import datetime
import os
from datetime import timedelta

import pandas as pd
from akshare import stock_hk_short_sale
from dateutil.relativedelta import relativedelta
from pandas import Timestamp

import akshare_sync
from akshare_sync.sync_logs.sync_logs import (
    update_sync_log_date,
    update_sync_log_state_to_failed,
)
from akshare_sync.util.tools import (
    get_cfg,
    get_logger,
    exec_create_table_script,
    get_engine,
    save_to_database,
)


def query_last_sync_date(engine, logger):
    query_start_date = (
        f'SELECT NVL(MAX("日期"), 20120820) as max_date FROM STOCK_HK_SHORT_SALE'
    )
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


def get_last_week_friday_date():
    """
    获取上周五的日期
    """
    now = datetime.datetime.now()
    weekday = now.weekday()
    return (now - datetime.timedelta(days=weekday + 3)).strftime("%Y%m%d")


def get_split_range(start_date, end_date, freq="70D"):
    """
    将时间拆分成若干个区间, 单次执行一个区间的数据同步, 防止单次拉取数据量过大
    """
    # 转换为 Timestamp
    start = pd.to_datetime(start_date, format="%Y%m%d")
    end: Timestamp = pd.to_datetime(end_date, format="%Y%m%d")
    intervals = pd.date_range(start=start, end=end, freq=freq)
    result = []
    for i in range(len(intervals) - 1):
        result.append(
            (
                intervals[i].date().strftime("%Y%m%d"),
                (intervals[i + 1] - timedelta(days=1)).date().strftime("%Y%m%d"),
            )
        )
    if intervals[-1] <= end:
        result.append(
            (intervals[-1].date().strftime("%Y%m%d"), end.date().strftime("%Y%m%d"))
        )
    return result


"""
执行数据下载同步
"""


def sync(drop_exist=False):
    cfg = get_cfg()
    logger = get_logger("stock_hk_short_sale", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        last_sync_date = query_last_sync_date(engine, logger)

        begin_date = (
            datetime.datetime.strptime(last_sync_date, "%Y%m%d")
            + relativedelta(weeks=1)
        ).strftime("%Y%m%d")
        end_date = get_last_week_friday_date()

        if begin_date <= end_date:
            date_ranges = get_split_range(begin_date, end_date, freq="70D")
            for i, (batch_start, batch_end) in enumerate(date_ranges, 1):
                logger.info(
                    f"Exec Sync STOCK_SHORT_SALE_HK Batch[{i}]: StartDate[{batch_start}] EndDate[{batch_end}] "
                )

                df = stock_hk_short_sale(start_date=batch_start, end_date=batch_end)
                logger.info(
                    f"Write [{df.shape[0]}] records into table [stock_hk_short_sale] with [{engine.engine}]"
                )

                number_cols = ["日期", "证券代码", "淡仓股数", "淡仓金额"]
                df[number_cols] = df[number_cols].apply(pd.to_numeric, errors="coerce")

                save_to_database(
                    df,
                    "stock_hk_short_sale",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
                update_sync_log_date(
                    "stock_hk_short_sale", "stock_hk_short_sale", batch_end
                )
        else:
            logger.info("Table [stock_hk_short_sale] Early Synced, Skip ...")
    except Exception:
        logger.error(f"Table [stock_hk_short_sale] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed("stock_hk_short_sale", "stock_hk_short_sale")


if __name__ == "__main__":
    akshare_sync.init_proxy()
    sync(False)
