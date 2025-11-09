"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_hk_ggt_components_em.py
===========================

接口: stock_hk_ggt_components_em

目标地址: https://quote.eastmoney.com/center/gridlist.html#hk_components

描述: 东方财富网-行情中心-港股市场-港股通成份股

限量: 单次获取所有港股通成份股数据

"""
import datetime
import os

import pandas as pd
from akshare import stock_hk_ggt_components_em

from akshare_sync.sync_logs.sync_logs import (
    query_last_api_sync_date,
    update_sync_log_date,
    update_sync_log_state_to_failed,
)
from akshare_sync.util.tools import (
    exec_create_table_script,
    get_engine,
    get_logger,
    get_cfg,
    save_to_database,
)

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.float_format", lambda x: "%.2f" % x)  #


# 全量初始化表数据
def sync(drop_exist=False):
    cfg = get_cfg()
    logger = get_logger("stock_hk_ggt_components_em", cfg["sync-logging"]["filename"])

    try:
        start_date = query_last_api_sync_date(
            "stock_hk_ggt_components_em", "stock_hk_ggt_components_em"
        )
        end_date = str(datetime.datetime.now().strftime("%Y%m%d"))
        if start_date < end_date:
            dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
            exec_create_table_script(dir_path, drop_exist, logger)

            # 获取数据
            hk_ggt_df = stock_hk_ggt_components_em()
            hk_ggt_df["交易所"] = "HK"
            hk_ggt_df = hk_ggt_df[["代码", "名称", "交易所"]]
            hk_ggt_df.columns = ["证券代码", "证券简称", "交易所"]
            df = hk_ggt_df
            # 写入数据库
            engine = get_engine()
            logger.info(
                f"Write [{df.shape[0]}] records into table [stock_hk_ggt_components_em] with [{engine.engine}]"
            )
            save_to_database(
                df,
                "stock_hk_ggt_components_em",
                engine,
                index=False,
                if_exists="append",
                chunksize=20000,
            )

            update_sync_log_date(
                "stock_hk_ggt_components_em",
                "stock_hk_ggt_components_em",
                f"{end_date}",
            )

        else:
            logger.info(
                f"Table [stock_hk_ggt_components_em] Early Synced start_date[{start_date}] end_date[{end_date}], Skip ..."
            )
    except Exception:
        logger.error(f"Table [stock_hk_ggt_components_em] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed(
            "stock_hk_ggt_components_em", "stock_hk_ggt_components_em"
        )


if __name__ == "__main__":
    sync(False)
