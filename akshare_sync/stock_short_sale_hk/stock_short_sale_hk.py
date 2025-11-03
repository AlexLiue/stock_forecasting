"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/11/3 21:33
# @Author  : PcLiu
# @FileName: stock_short_sale.py
===========================

接口: stock_short_sale

描述: 指明股份合计须申报淡仓, 港股 HK 淡仓申报 （香港证监会每周更新一次）
    根据于申报日持有须申报淡仓的市场参与者或其申报代理人向证监会所呈交的通知书内的资料而计算

"""
import datetime
import os
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup
import csv
from io import StringIO

from akshare_sync.sync_logs.sync_logs import query_last_api_sync_date
from akshare_sync.util.tools import get_cfg, get_logger, exec_create_table_script

headers = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Referer': 'https://finance.sina.com.cn/',
    'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36'
}

"""
字符串日期格式转换：
如： 2025年7月18日 -> 20250718； 2025年10月10日 -> 20251010
     
"""
def convert_date(str_date):
    item = re.findall("\d+", str_date)
    return f"{item[0]}{item[1]:0>2}{item[2]:0>2}"




def get_stock_short_sale_report_list():
    # 获取转换比例数据
    root_url = "https://sc.sfc.hk/TuniS/www.sfc.hk/TC/Regulatory-functions/Market/Short-position-reporting/Aggregated-reportable-short-positions-of-specified-shares"
    r = requests.get(root_url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.find_all("tr", scope="row")
    url_rows = []
    for row in rows:
        items = row.find_all("td")
        if (len(items) == 3):
            csv_date = convert_date(items[0])
            csv_url = items[2].find("a").get("href")
            url_rows.append([csv_date, csv_url])
    return pd.DataFrame(rows, columns=["报告日期", "文件地址"])




    soup.get("")
    print(soup)



    row = rows[0]

    for row in rows:
        items = row.find_all("td")
        if (len(items) == 3):
            csv_name = items[0]
            csv_url = items[2].find("a").get("href")
            csv_text = requests.get(csv_url, headers=headers).text
            df = pd.read_csv(StringIO(csv_text))
            df["Date"] = df["Date"].apply(lambda d: d.replace('/', ''))
            df["Stock Code"] = df["Stock Code"].apply(lambda d: f"{d:05d}")
            df.columns = ["日期", "证券代码", "证券简称", "淡仓股数", "淡仓金额"]

    soup.get("")
    print(soup)



# 全量初始化表数据
def sync(drop_exist):
    cfg = get_cfg()
    logger = get_logger('stock_basic_info', cfg['sync-logging']['filename'])

    try:
        start_date = query_last_api_sync_date('stock_basic_info', 'stock_basic_info')
        if start_date < str(datetime.datetime.now().strftime('%Y%m%d')):
            dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
            exec_create_table_script(dir_path, drop_exist, logger)

            # 获取数据
            report_list = get_stock_short_sale_report_list()




            # # 清理历史数据
            # clean_sql = f"TRUNCATE TABLE STOCK_BASIC_INFO"
            # logger.info('Execute Clean SQL [%s]' % clean_sql)
            # exec_sql(clean_sql)
            #
            # # 写入数据库
            # connection = get_engine()
            # logger.info(f'Write [{data.shape[0]}] records into table [stock_basic_info] with [{connection.engine}]')
            # data.to_sql('stock_basic_info', connection, index=False, if_exists='append', chunksize=5000)
            #
            # update_api_sync_date('stock_basic_info', 'stock_basic_info',
            #                      f'{str(datetime.datetime.now().strftime('%Y%m%d'))}')

        else:
            logger.info("Table [stock_basic_info] Early Synced, Skip ...")
    except Exception as e:
        logger.error(f"Table [stock_basic_info] Sync Failed Cause By [{e.__cause__}] Traceback[{e.__traceback__}]")



# 增量追加表数据, 股票列表不具备增量条件, 全量覆盖
if __name__ == '__main__':
    sync(False)




