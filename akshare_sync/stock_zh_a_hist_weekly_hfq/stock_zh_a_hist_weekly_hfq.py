"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_zh_a_hist_weekly_hfq.py
===========================
接口: stock_zh_a_hist

目标地址: https://quote.eastmoney.com/concept/sh603777.html?from=classic(示例)

描述: 东方财富-沪深京 A 股日频率数据; 历史数据按日频率更新, 当日收盘价请在收盘后获取

限量: 单次返回指定沪深京 A 股上市公司、指定周期和指定日期间的历史行情日频率数据
"""
import datetime
import traceback

import requests
from dateutil.relativedelta import relativedelta
import os
import time
import akshare as ak
import pandas as pd

from akshare_sync.global_data.global_data import GlobalData
from akshare_sync.sync_logs.sync_logs import update_api_sync_date


from akshare_sync.util.requests_tool import request_get
from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x) #



def stock_zh_a_hist(
    symbol: str = "000001",
    period: str = "daily",
    start_date: str = "19700101",
    end_date: str = "20500101",
    adjust: str = "",
    timeout: float = None,
) -> pd.DataFrame:
    """
    东方财富网-行情首页-沪深京 A 股-每日行情
    https://quote.eastmoney.com/concept/sh603777.html?from=classic
    :param symbol: 股票代码
    :type symbol: str
    :param period: choice of {'daily', 'weekly', 'monthly'}
    :type period: str
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :param adjust: choice of {"qfq": "前复权", "hfq": "后复权", "": "不复权"}
    :type adjust: str
    :param timeout: choice of None or a positive float number
    :type timeout: float
    :return: 每日行情
    :rtype: pandas.DataFrame
    """
    market_code = 1 if symbol.startswith("6") else 0
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": period_dict[period],
        "fqt": adjust_dict[adjust],
        "secid": f"{market_code}.{symbol}",
        "beg": start_date,
        "end": end_date,
    }
    r = request_get(url, params=params)

    data_json = r.json()
    if not (data_json["data"] and data_json["data"]["klines"]):
        return pd.DataFrame()
    temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
    temp_df["股票代码"] = symbol
    temp_df.columns = [
        "日期",
        "开盘",
        "收盘",
        "最高",
        "最低",
        "成交量",
        "成交额",
        "振幅",
        "涨跌幅",
        "涨跌额",
        "换手率",
        "股票代码",
    ]
    temp_df["日期"] = pd.to_datetime(temp_df["日期"], errors="coerce").dt.date
    temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
    temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
    temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
    temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
    temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
    temp_df = temp_df[
        [
            "日期",
            "股票代码",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
        ]
    ]
    return temp_df



def query_last_sync_date(trade_code, engine, logger):
    query_start_date = f"SELECT NVL(MAX(\"日期\"), 19900101) as max_date FROM stock_zh_a_hist_weekly_hfq WHERE \"股票代码\"='{trade_code}'"
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


def get_last_friday_date():
    """
    获取当前时间的上一个星期五的日期，作为数据的最后周日期
    如果当前日期小于星期五的16:30:00分，则取上周五的日期，否则取这周五的日期
    """
    today = datetime.date.today()
    weekday = today.weekday()
    if weekday < 5 or (weekday == 5 and today.strftime('%H:%M:%S')<'16:30:00'):
        return (today - datetime.timedelta(days=weekday + 3)).strftime('%Y%m%d')
    else:
        return (today - datetime.timedelta(days=weekday - 4)).strftime('%Y%m%d')



def sync(drop_exist):
    cfg = get_cfg()
    logger = get_logger('stock_zh_a_hist_weekly_hfq', cfg['sync-logging']['filename'])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        # 查询交易股票列表
        global_data = GlobalData()
        trade_code_list = global_data.trade_code_a
        # 结束日期: 最近的周五日期
        end_date = get_last_friday_date()

        engine = get_engine()
        for row_idx in range(trade_code_list.shape[0]):
            row = trade_code_list.iloc[row_idx]
            trade_code = row.iloc[0]
            trade_name = row.iloc[1]
            last_sync_date = query_last_sync_date(trade_code, engine, logger)
            start_date =  (datetime.datetime.strptime(last_sync_date, '%Y%m%d') + relativedelta(weeks=1)).strftime('%Y%m%d')

            if start_date < end_date:
                logger.info(f"Execute Sync stock_zh_a_hist  trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}]")
                df = stock_zh_a_hist(symbol=trade_code, period="weekly", start_date=start_date, end_date=end_date, adjust="hfq", timeout=20)
                if not df.empty:
                    df["日期"] = df["日期"].apply(lambda x: x.strftime('%Y%m%d'))
                    df.to_sql("stock_zh_a_hist_weekly_hfq", engine, index=False, if_exists='append', chunksize=5000)
                    logger.info(
                        f"Execute Sync stock_zh_a_hist_weekly_hfq trade_code[{trade_code}]" + f" Write[{df.shape[0]}] Records")
            else:
                logger.info(f"Execute Sync stock_zh_a_hist  trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}], Skip Sync ... ")

        engine.close()
        update_api_sync_date('stock_zh_a_hist', 'stock_zh_a_hist_weekly_hfq', f'{str(end_date)}')

    except Exception as e:

        logger.error(f"Table [stock_zh_a_hist_weekly_hfq] Sync Failed Cause By [{e.__cause__}] Stack[{traceback.format_exc()}]")





if __name__ == '__main__':
    sync(False)

