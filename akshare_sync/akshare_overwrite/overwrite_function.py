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
from io import BytesIO, StringIO

import pandas as pd
import time
import traceback

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from requests import exceptions


from akshare_sync.util.tools import get_cfg

"""
重写 Akshare 函数, 引入 proxies 代理解决 IP 地址被封问题
"""


""" 读取配置文件中的 proxies 信息 """
cfg = get_cfg()
proxies = {
    "http": cfg['proxies']['http'],
    "https": cfg['proxies']['https']
}

""" 使用 fake_useragent  随机生成 UserAgent  """
user_agent = UserAgent(os=["Windows", "Linux", "Ubuntu", "Mac OS X"])

""" 构建 requests 的请求报文头 """
headers = {
    "User-Agent": user_agent.random,
    "Connection": "close",
    "Accept": "text/html,application/xhtml+xml,application/xml",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
}


def request_get(url, params=None, timeout=20):
    """
    基于 requests 库获取网页内容, 考虑到代理的稳定性较低, 引入重试访问, 默认重试 5 次代理访问
    """
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


def stock_szse_summary(date: str = "20240830") -> pd.DataFrame:
    """
    深证证券交易所-总貌-证券类别统计
    https://www.szse.cn/market/overview/index.html
    :param date: 最近结束交易日
    :type date: str
    :return: 证券类别统计
    :rtype: pandas.DataFrame
    """
    url = "http://www.szse.cn/api/report/ShowReport"
    params = {
        "SHOWTYPE": "xlsx",
        "CATALOGID": "1803_sczm",
        "TABKEY": "tab1",
        "txtQueryDate": "-".join([date[:4], date[4:6], date[6:]]),
        "random": "0.39339437497296137",
    }
    r = request_get(url, params=params)
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        temp_df = pd.read_excel(BytesIO(r.content), engine="openpyxl")
    temp_df["证券类别"] = temp_df["证券类别"].str.strip()
    temp_df.iloc[:, 2:] = temp_df.iloc[:, 2:].map(lambda x: x.replace(",", ""))
    temp_df.columns = ["证券类别", "数量", "成交金额", "总市值", "流通市值"]
    temp_df["数量"] = pd.to_numeric(temp_df["数量"], errors="coerce")
    temp_df["成交金额"] = pd.to_numeric(temp_df["成交金额"], errors="coerce")
    temp_df["总市值"] = pd.to_numeric(temp_df["总市值"], errors="coerce")
    temp_df["流通市值"] = pd.to_numeric(temp_df["流通市值"], errors="coerce")
    return temp_df


def stock_sse_summary() -> pd.DataFrame:
    """
    上海证券交易所-总貌
    https://www.sse.com.cn/market/stockdata/statistic/
    :return: 上海证券交易所-总貌
    :rtype: pandas.DataFrame
    """
    url = "http://query.sse.com.cn/commonQuery.do"
    params = {
        "sqlId": "COMMON_SSE_SJ_GPSJ_GPSJZM_TJSJ_L",
        "PRODUCT_NAME": "股票,主板,科创板",
        "type": "inParams",
    }
    headers = {
        "Referer": "http://www.sse.com.cn/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/89.0.4389.90 Safari/537.36",
    }
    r = request_get(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"]).T
    temp_df.reset_index(inplace=True)
    temp_df["index"] = [
        "流通股本",
        "总市值",
        "平均市盈率",
        "上市公司",
        "上市股票",
        "流通市值",
        "报告时间",
        "-",
        "总股本",
        "项目",
    ]
    temp_df = temp_df[temp_df["index"] != "-"].iloc[:-1, :]
    temp_df.columns = [
        "项目",
        "股票",
        "主板",
        "科创板",
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
    r = request_get(url, params=params, timeout=15)
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



def stock_szse_sector_summary(
    symbol: str = "当月", date: str = "202501"
) -> pd.DataFrame:
    """
    深圳证券交易所-统计资料-股票行业成交数据
    https://docs.static.szse.cn/www/market/periodical/month/W020220511355248518608.html
    :param symbol: choice of {"当月", "当年"}
    :type symbol: str
    :param date: 交易年月
    :type date: str
    :return: 股票行业成交数据
    :rtype: pandas.DataFrame
    """
    url = "https://www.szse.cn/market/periodical/month/index.html"
    r =  request_get(url)
    r.encoding = "utf8"
    soup = BeautifulSoup(r.text, features="lxml")
    tags_list = soup.find_all(name="div", attrs={"class": "g-container"})[1].find_all(
        "script"
    )
    tags_dict = [
        eval(
            item.string[item.string.find("{"): item.string.find("}") + 1]
            .replace("\n", "")
            .replace(" ", "")
            .replace("value", "'value'")
            .replace("text", "'text'")
        )
        for item in tags_list
    ]
    date_url_dict = dict(
        zip(
            [item["text"] for item in tags_dict],
            [item["value"][2:] for item in tags_dict],
        )
    )
    date_format = "-".join([date[:4], date[4:]])
    if not date_url_dict.__contains__(date_format): return pd.DataFrame()
    url = f"https://www.szse.cn/market/periodical/month/{date_url_dict[date_format]}"
    r = request_get(url)
    r.encoding = "utf8"
    soup = BeautifulSoup(r.text, features="lxml")
    url = [
        item for item in soup.find_all("a") if item.get_text() == "股票行业成交数据"
    ][0]["href"]

    if symbol == "当月":
        r = request_get(url)
        temp_df = pd.read_html(StringIO(r.text), encoding="gbk")[0]
        temp_df.columns = [
            "项目名称",
            "项目名称-英文",
            "交易天数",
            "成交金额-人民币元",
            "成交金额-占总计",
            "成交股数-股数",
            "成交股数-占总计",
            "成交笔数-笔",
            "成交笔数-占总计",
        ]
    else:
        temp_df = pd.read_html(url, encoding="gbk")[1]
        temp_df.columns = [
            "项目名称",
            "项目名称-英文",
            "交易天数",
            "成交金额-人民币元",
            "成交金额-占总计",
            "成交股数-股数",
            "成交股数-占总计",
            "成交笔数-笔",
            "成交笔数-占总计",
        ]

    temp_df["交易天数"] = pd.to_numeric(temp_df["交易天数"], errors="coerce")
    temp_df["成交金额-人民币元"] = pd.to_numeric(
        temp_df["成交金额-人民币元"], errors="coerce"
    )
    temp_df["成交金额-占总计"] = pd.to_numeric(
        temp_df["成交金额-占总计"], errors="coerce"
    )
    temp_df["成交股数-股数"] = pd.to_numeric(temp_df["成交股数-股数"], errors="coerce")
    temp_df["成交股数-占总计"] = pd.to_numeric(
        temp_df["成交股数-占总计"], errors="coerce"
    )
    temp_df["成交笔数-笔"] = pd.to_numeric(temp_df["成交笔数-笔"], errors="coerce")
    temp_df["成交笔数-占总计"] = pd.to_numeric(
        temp_df["成交笔数-占总计"], errors="coerce"
    )
    return temp_df


def stock_zh_a_hist_min_em(
    symbol: str = "000001",
    start_date: str = "1979-09-01 09:32:00",
    end_date: str = "2222-01-01 09:32:00",
    period: str = "5",
    adjust: str = "",
) -> pd.DataFrame:
    """
    东方财富网-行情首页-沪深京 A 股-每日分时行情
    https://quote.eastmoney.com/concept/sh603777.html?from=classic
    :param symbol: 股票代码
    :type symbol: str
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :param period: choice of {'1', '5', '15', '30', '60'}
    :type period: str
    :param adjust: choice of {'', 'qfq', 'hfq'}
    :type adjust: str
    :return: 每日分时行情
    :rtype: pandas.DataFrame
    """
    market_code = 1 if symbol.startswith("6") else 0
    adjust_map = {
        "": "0",
        "qfq": "1",
        "hfq": "2",
    }
    if period == "1":
        url = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "ndays": "5",
            "iscr": "0",
            "secid": f"{market_code}.{symbol}",
        }
        r = request_get(url, timeout=15, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(
            [item.split(",") for item in data_json["data"]["trends"]]
        )
        temp_df.columns = [
            "时间",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "均价",
        ]
        temp_df.index = pd.to_datetime(temp_df["时间"])
        temp_df = temp_df[start_date:end_date]
        temp_df.reset_index(drop=True, inplace=True)
        temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
        temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
        temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
        temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
        temp_df["均价"] = pd.to_numeric(temp_df["均价"], errors="coerce")
        temp_df["时间"] = pd.to_datetime(temp_df["时间"]).astype(str)
        return temp_df
    else:
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "klt": period,
            "fqt": adjust_map[adjust],
            "secid": f"{market_code}.{symbol}",
            "beg": "0",
            "end": "20500000",
        }
        r = request_get(url, timeout=15, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(
            [item.split(",") for item in data_json["data"]["klines"]]
        )
        temp_df.columns = [
            "时间",
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
        temp_df.index = pd.to_datetime(temp_df["时间"])
        temp_df = temp_df[start_date:end_date]
        temp_df.reset_index(drop=True, inplace=True)
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
        temp_df["时间"] = pd.to_datetime(temp_df["时间"]).astype(str)
        temp_df = temp_df[
            [
                "时间",
                "开盘",
                "收盘",
                "最高",
                "最低",
                "涨跌幅",
                "涨跌额",
                "成交量",
                "成交额",
                "振幅",
                "换手率",
            ]
        ]
        return temp_df
