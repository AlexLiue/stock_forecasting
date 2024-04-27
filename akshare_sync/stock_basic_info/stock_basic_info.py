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

import os
import akshare as ak
import pandas as pd
from akshare import stock_info_sh_name_code, stock_info_sz_name_code, stock_info_bj_name_code

from akshare_sync.util.tools import exec_create_table_script, get_mock_connection, get_logger, get_cfg, \
    exec_mysql_sql


def stock_info_code_name() -> pd.DataFrame:
    """
    沪深京 A 股列表
    :return: 沪深京 A 股数据
    :rtype: pandas.DataFrame
    """
    stock_sh_a = stock_info_sh_name_code(symbol="主板A股").dropna()
    stock_sh_a["交易所"] = "SSE"
    stock_sh_a["板块"] = "主板"
    stock_sh_a = stock_sh_a[["证券代码", "证券简称", "交易所", "板块", "上市日期"]]
    stock_sh_a.columns = ["symbol", "name", "exchange", "market", "list_date"]

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

    stock_bse = stock_info_bj_name_code().dropna()
    stock_bse.loc[:, "交易所"] = "BSE"
    stock_bse["板块"] = "北交所"
    stock_bse = stock_bse[["证券代码", "证券简称", "交易所", "板块", "上市日期"]]
    stock_bse.columns = ["symbol", "name", "exchange", "market", "list_date"]

    stock_hk = ak.stock_hk_spot_em().dropna()
    stock_hk["交易所"] = "HKSE"
    stock_hk["上市日期"] = ""
    stock_hk["板块"] = "港交所"
    stock_hk = stock_hk[["代码", "名称", "交易所", "板块", "上市日期"]]
    stock_hk.columns = ["symbol", "name", "exchange", "market", "list_date"]

    big_df = pd.concat([stock_sh_a, stock_sh_kcb, stock_sz_a, stock_bse, stock_hk], ignore_index=True)
    big_df.columns = ["symbol", "name", "exchange", "market", "list_date"]
    return big_df


# 全量初始化表数据
def sync(drop_exist):
    cfg = get_cfg()
    logger = get_logger('stock_basic_info', cfg['sync-logging']['filename'])

    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist, logger)

    # 清理历史数据
    clean_sql = f"TRUNCATE TABLE  {cfg['mysql']['database']}.stock_basic_info"
    logger.info('Execute Clean SQL [%s]' % clean_sql)
    counts = exec_mysql_sql(clean_sql)
    logger.info("Execute Clean SQL Affect [%d] records" % counts)

    data = stock_info_code_name()

    connection = get_mock_connection()
    logger.info(f'Write [{data.shape[0]}] records into table [stock_basic_info] with [{connection.engine}]')
    data.to_sql('stock_basic_info', connection, index=False, if_exists='append', chunksize=5000)


# 增量追加表数据, 股票列表不具备增量条件, 全量覆盖
if __name__ == '__main__':
    sync(False)
