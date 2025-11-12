"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/11/12 21:33
# @Author  : PcLiu
# @FileName: stock_table_summary.py
===========================

汇总同步的表以及字段信息

"""
import datetime
import os

import pandas as pd

from akshare_sync import get_cfg, get_logger
from akshare_sync.sync_logs.sync_logs import update_sync_log_state_to_failed, update_sync_log_date
from akshare_sync.util.tools import exec_create_table_script

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.float_format", lambda x: "%.2f" % x)  #


def sync(drop_exist=True):
    cfg = get_cfg()
    logger = get_logger("stock_table_summary", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)
        update_sync_log_date(
            "stock_table_summary",
            "stock_table_summary",
            datetime.datetime.now().strftime("%Y%m%d"),
        )
    except Exception:
        logger.error(f"Table [stock_table_summary] Sync Failed", exc_info=True)
        update_sync_log_state_to_failed(
            "stock_table_summary", "stock_table_summary"
        )


if __name__ == "__main__":
    sync()
