"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/11/4 18:01
# @Author  : PcLiu
# @FileName: overwrite_function.py
===========================
"""
import warnings
from functools import lru_cache
from io import BytesIO

import pandas as pd
import time
import traceback

import requests
from fake_useragent import UserAgent
from requests import exceptions


from akshare_sync.util.tools import get_cfg



cfg = get_cfg()
proxies = {
    "http": cfg['proxies']['http'],
    "https": cfg['proxies']['https']
}

user_agent = UserAgent(os=["Windows", "Linux", "Ubuntu", "Mac OS X"])

headers = {
    "User-Agent": user_agent.random,
    "Connection": "close",
    "Accept": "text/html,application/xhtml+xml,application/xml",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
}


def request_get(url, params=None, timeout=20):
    max_retry = 5
    cur_retry = 0
    while cur_retry < max_retry:
        try:
            r = requests.get(url, params=params, timeout=timeout, proxies=proxies, headers={
                "User-Agent": user_agent.random,
                "Connection": "close",
                "Accept": "text/html,application/xhtml+xml,application/xml",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
            })

            if r.status_code == 200:
                return r
            break
        except Exception as e:
            print(traceback.format_exc())
            cur_retry += 1
            time.sleep(1)
    if cur_retry == max_retry:
        raise exceptions.RequestException

    return None



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



@lru_cache()
def stock_info_sz_name_code(symbol: str = "A股列表") -> pd.DataFrame:
    """
    深圳证券交易所-股票列表
    https://www.szse.cn/market/product/stock/list/index.html
    :param symbol: choice of {"A股列表", "B股列表", "CDR列表", "AB股列表"}
    :type symbol: str
    :return: 指定 indicator 的数据
    :rtype: pandas.DataFrame
    """
    url = "https://www.szse.cn/api/report/ShowReport"
    indicator_map = {
        "A股列表": "tab1",
        "B股列表": "tab2",
        "CDR列表": "tab3",
        "AB股列表": "tab4",
    }
    params = {
        "SHOWTYPE": "xlsx",
        "CATALOGID": "1110",
        "TABKEY": indicator_map[symbol],
        "random": "0.6935816432433362",
    }
    r = requests.get(url, params=params, timeout=15)
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        temp_df = pd.read_excel(BytesIO(r.content))
    if len(temp_df) > 10:
        if symbol == "A股列表":
            temp_df["A股代码"] = (
                temp_df["A股代码"]
                .astype(str)
                .str.split(".", expand=True)
                .iloc[:, 0]
                .str.zfill(6)
                .str.replace("000nan", "")
            )
            temp_df = temp_df[
                [
                    "板块",
                    "A股代码",
                    "A股简称",
                    "A股上市日期",
                    "A股总股本",
                    "A股流通股本",
                    "所属行业",
                ]
            ]
        elif symbol == "B股列表":
            temp_df["B股代码"] = (
                temp_df["B股代码"]
                .astype(str)
                .str.split(".", expand=True)
                .iloc[:, 0]
                .str.zfill(6)
                .str.replace("000nan", "")
            )
            temp_df = temp_df[
                [
                    "板块",
                    "B股代码",
                    "B股简称",
                    "B股上市日期",
                    "B股总股本",
                    "B股流通股本",
                    "所属行业",
                ]
            ]
        elif symbol == "AB股列表":
            temp_df["A股代码"] = (
                temp_df["A股代码"]
                .astype(str)
                .str.split(".", expand=True)
                .iloc[:, 0]
                .str.zfill(6)
                .str.replace("000nan", "")
            )
            temp_df["B股代码"] = (
                temp_df["B股代码"]
                .astype(str)
                .str.split(".", expand=True)
                .iloc[:, 0]
                .str.zfill(6)
                .str.replace("000nan", "")
            )
            temp_df = temp_df[
                [
                    "板块",
                    "A股代码",
                    "A股简称",
                    "A股上市日期",
                    "B股代码",
                    "B股简称",
                    "B股上市日期",
                    "所属行业",
                ]
            ]
        return temp_df
    else:
        return temp_df

