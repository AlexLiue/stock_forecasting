"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2024/04/22 21:33
# @Author  : PcLiu
# @FileName: stock_basic_info.py
===========================

接口: stock_basic_info
目标地址: 沪深京三个交易所
描述: 沪深京 A 股股票代码和股票简称数据
限量: 单次获取所有 A 股股票代码和简称数据
"""
import datetime
import os
import pandas as pd
from akshare import stock_info_sh_name_code, stock_info_bj_name_code, stock_hk_spot

from akshare_sync.akshare_overwrite.overwrite_function import stock_info_sz_name_code
from akshare_sync.sync_logs.sync_logs import update_api_sync_date, query_last_api_sync_date
from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg, \
    exec_sql


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x) #




def stock_info_code_name() -> pd.DataFrame:
    """
    沪深京 A 股列表
    :return: 沪深京 A 股数据
    :rtype: pandas.DataFrame
    """
    stock_sh_a = stock_info_sh_name_code(symbol="主板A股").dropna()
    stock_sh_a["交易所"] = "SSE"
    stock_sh_a["板块"] = "主板A股"
    stock_sh_a = stock_sh_a[["证券代码", "证券简称", "交易所", "板块", "上市日期"]]
    stock_sh_a.columns = ["symbol", "name", "exchange", "market", "list_date"]

    stock_sh_b = stock_info_sh_name_code(symbol="主板A股").dropna()
    stock_sh_b["交易所"] = "SSE"
    stock_sh_b["板块"] = "主板B股"
    stock_sh_b = stock_sh_b[["证券代码", "证券简称", "交易所", "板块", "上市日期"]]
    stock_sh_b.columns = ["symbol", "name", "exchange", "market", "list_date"]

    stock_sh_kcb = stock_info_sh_name_code(symbol="科创板").dropna()
    stock_sh_kcb["交易所"] = "SSE"
    stock_sh_kcb["板块"] = "科创板"
    stock_sh_kcb = stock_sh_kcb[["证券代码", "证券简称", "交易所", "板块", "上市日期"]]
    stock_sh_kcb.columns = ["symbol", "name", "exchange", "market", "list_date"]

    stock_sz_a = stock_info_sz_name_code(symbol="A股列表").dropna()
    stock_sz_a["A股代码"] = stock_sz_a["A股代码"].astype(str).str.zfill(6)
    stock_sz_a["交易所"] = "SZSE"
    stock_sz_a = stock_sz_a[["A股代码", "A股简称", "交易所", "板块", "A股上市日期"]]
    stock_sz_a.columns = ["symbol", "name", "exchange", "market", "list_date"]

    stock_sz_b = stock_info_sz_name_code(symbol="B股列表").dropna()
    stock_sz_b["B股代码"] = stock_sz_b["B股代码"].astype(str).str.zfill(6)
    stock_sz_b["交易所"] = "SZSE"
    stock_sz_b = stock_sz_b[["B股代码", "B股简称", "交易所", "板块", "B股上市日期"]]
    stock_sz_b.columns = ["symbol", "name", "exchange", "market", "list_date"]


    stock_bse = stock_info_bj_name_code().dropna()
    stock_bse.loc[:, "交易所"] = "BSE"
    stock_bse["板块"] = "北交所"
    stock_bse = stock_bse[["证券代码", "证券简称", "交易所", "板块", "上市日期"]]
    stock_bse.columns = ["symbol", "name", "exchange", "market", "list_date"]

    stock_hk = stock_hk_spot().dropna()
    stock_hk["交易所"] = "HKSE"
    stock_hk["上市日期"] = ""
    stock_hk["板块"] = "港交所"
    stock_hk = stock_hk[["代码", "中文名称", "交易所", "板块", "上市日期"]]
    stock_hk.columns = ["symbol", "name", "exchange", "market", "list_date"]

    big_df = pd.concat([stock_sh_a, stock_sh_kcb, stock_sz_a, stock_sz_b, stock_bse, stock_hk], ignore_index=True)
    big_df["list_date"] = big_df['list_date'].apply(lambda d: str(d).replace('-',''))
    big_df["数据日期"] = str(datetime.datetime.now().strftime('%Y%m%d'))
    big_df.columns = ["证券代码", "证券简称", "交易所", "板块", "上市日期", "数据日期"]

    return big_df


# 全量初始化表数据
def sync(drop_exist=False):
    cfg = get_cfg()
    logger = get_logger('stock_basic_info', cfg['sync-logging']['filename'])

    try:
        start_date = query_last_api_sync_date('stock_basic_info', 'stock_basic_info')
        if start_date < str(datetime.datetime.now().strftime('%Y%m%d')):
            dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
            exec_create_table_script(dir_path, drop_exist, logger)

            # 获取数据
            data = stock_info_code_name()

            # 清理历史数据
            clean_sql = f"TRUNCATE TABLE STOCK_BASIC_INFO"
            logger.info('Execute Clean SQL [%s]' % clean_sql)
            exec_sql(clean_sql)

            # 写入数据库
            connection = get_engine()
            logger.info(f'Write [{data.shape[0]}] records into table [stock_basic_info] with [{connection.engine}]')
            data.to_sql('stock_basic_info', connection, index=False, if_exists='append', chunksize=5000)

            update_api_sync_date('stock_basic_info', 'stock_basic_info',
                                 f'{str(datetime.datetime.now().strftime('%Y%m%d'))}')

        else:
            logger.info("Table [stock_basic_info] Early Synced, Skip ...")
    except Exception as e:
        logger.error(f"Table [stock_basic_info] Sync Failed", exc_info=True)




if __name__ == '__main__':
    sync(False)
