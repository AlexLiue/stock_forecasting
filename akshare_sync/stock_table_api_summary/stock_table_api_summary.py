"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_table_api_summary.py
===========================
描述:  抓取 Akshare 页面的 API 接口概要信息
限量: 每次全量覆盖抓取

目标地址: https://akshare.akfamily.xyz/data/stock/stock.html
目标表名:  stock_table_api_summary

"""
import datetime
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup

from akshare_sync.sync_logs.sync_logs import query_last_api_sync_date, update_api_sync_date
from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg, exec_sql

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x) #

def get_table_api_content():
    url = "https://akshare.akfamily.xyz/data/stock/stock.html"
    params = {
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/89.0.4389.90 Safari/537.36",
    }
    response = requests.get(url, params=params, headers=headers)

    soup = BeautifulSoup(response.text, 'html.parser')
    sections = soup.find_all('section')

    rows = []
    for section in sections:
        tags = section.find_all()
        if len(tags) > 6 and tags[2].text.startswith('接口: stock') and tags[3].text.startswith('目标地址: '):
            name = tags[2].text.split("接口: ")[-1]
            addr = tags[3].text.split("目标地址: ")[-1]
            desc = tags[4].text.split("描述: ")[-1]
            comt = tags[5].text.split("限量: ")[-1]
            rows.append([name, addr, desc, comt])
    return pd.DataFrame(rows, columns=["接口", "地址", "描述", "备注"])



def sync(drop_exist):
    cfg = get_cfg()
    logger = get_logger('stock_table_api_summary', cfg['sync-logging']['filename'])

    try:
        start_date = query_last_api_sync_date('stock_table_api_summary', 'stock_table_api_summary')
        if start_date < str(datetime.datetime.now().strftime('%Y%m%d')):
            dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
            exec_create_table_script(dir_path, drop_exist, logger)

            # 获取数据
            data = get_table_api_content()

            # 清理历史数据
            clean_sql = f"TRUNCATE TABLE stock_table_api_summary"
            logger.info('Execute Clean SQL [%s]' % clean_sql)
            exec_sql(clean_sql)

            # 写入数据库
            connection = get_engine()
            logger.info(
                f'Write [{data.shape[0]}] records into table [stock_table_api_summary] with [{connection.engine}]')
            data.to_sql('stock_table_api_summary', connection, index=False, if_exists='append', chunksize=5000)

            update_api_sync_date('stock_table_api_summary', 'stock_table_api_summary',
                                 f'{str(datetime.datetime.now().strftime('%Y%m%d'))}')

        else:
            logger.info("Table [stock_table_api_summary] Early Synced, Skip ...")
    except Exception as e:
        logger.error(f"Table [stock_table_api_summary] Sync Failed Cause By [{e.__cause__}] Traceback[{e.__traceback__}]")


# 增量追加表数据, 股票列表不具备增量条件, 全量覆盖
if __name__ == '__main__':
    sync(False)
