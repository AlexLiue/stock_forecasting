"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/11/3 21:33
# @Author  : PcLiu
# @FileName: stock_board_concept_name_em.py
===========================

接口: stock_board_concept_name_em

目标地址: https://quote.eastmoney.com/center/boardlist.html#concept_board

描述: 东方财富网-行情中心-沪深京板块-概念板块

限量: 单次返回当前时刻所有概念板块的实时行情数据
"""

import datetime
import os

import pandas as pd
from akshare import stock_board_concept_name_em
from akshare.stock_a.stock_board_concept_name_em import stock_board_concept_name_em
from dateutil.relativedelta import relativedelta

from akshare_sync import get_cfg, get_logger
from akshare_sync.sync_logs.sync_logs import (
    update_sync_log_date,
    update_sync_log_state_to_failed,
)
from akshare_sync.util.tools import (
    exec_create_table_script,
    get_engine,
    exec_sql,
    save_to_database,
)


pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.float_format", lambda x: "%.2f" % x)  #


def query_last_sync_date(engine, logger):
    query_start_date = f'SELECT NVL(MAX("日期"), 19700101) as max_date FROM STOCK_BOARD_CONCEPT_NAME_EM'
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


def sync(drop_exist=False, ggt=True):
    cfg = get_cfg()
    logger = get_logger("stock_board_concept_name_em", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()

        start_date = query_last_sync_date(engine, logger)
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        if start_date < end_date:
            # 获取数据
            df = stock_board_concept_name_em()
            df["日期"] = end_date
            df = df[[
                "日期",
                "排名",
                "板块名称",
                "板块代码",
                "最新价",
                "涨跌额",
                "涨跌幅",
                "总市值",
                "换手率",
                "上涨家数",
                "下跌家数",
                "领涨股票",
                "领涨股票-涨跌幅"]
            ]
            df.columns = [
                "日期",
                "排名",
                "板块名称",
                "板块代码",
                "最新价",
                "涨跌额",
                "涨跌幅",
                "总市值",
                "换手率",
                "上涨家数",
                "下跌家数",
                "领涨股票",
                "领涨股票_涨跌幅"]

            if not df.empty:
                # 写入数据库
                save_to_database(
                    df,
                    "stock_board_concept_name_em",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
                logger.info(
                    f"Write [{df.shape[0]}] records into table [stock_board_concept_name_em] with [{engine.engine}]"
                )
                update_sync_log_date(
                    "stock_board_concept_name_em",
                    "stock_board_concept_name_em",
                    end_date,
                )
        else:
            logger.info("Table [stock_board_concept_name_em] Early Synced, Skip ...")
    except Exception:
        logger.error(f"Table [stock_board_concept_name_em] Sync Failed", exc_info=True)
        update_sync_log_state_to_failed(
            "stock_board_concept_name_em", "stock_board_concept_name_em"
        )


if __name__ == "__main__":
    sync(True)
