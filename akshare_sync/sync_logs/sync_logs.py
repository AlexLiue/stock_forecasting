"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: sync_logs.py
===========================

描述: 数据同步状态日志，记录最后成功同步的时间日期, 同步失败重试时跳过同步

"""
import datetime
import os

import pandas as pd
from akshare_sync.util.tools import exec_create_table_script, get_engine, get_logger, get_cfg, get_connection

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x) #

# 查询 API 同步的时间
def query_api_sync_date(api_name, table_name):
    cfg = get_cfg()
    logger = get_logger('sync_logs', cfg['sync-logging']['filename'])
    engine = get_engine()
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, False, logger)

    query_start_date = f"SELECT NVL(MAX(\"日期\"), 19900101) as max_date FROM sync_logs WHERE \"接口名\"='{api_name}' AND \"表名\"='{table_name}'"
    logger.info(f"Execute SQL  [{query_start_date}]")
    last_date = str(pd.read_sql(query_start_date, engine).iloc[0, 0])
    cur_date = datetime.datetime.strptime(last_date, '%Y%m%d') + datetime.timedelta(days=1)
    return str(cur_date.strftime('%Y%m%d'))


def update_api_sync_date(api_name, table_name, date):
    cfg = get_cfg()
    logger = get_logger('sync_logs', cfg['sync-logging']['filename'])
    engine = get_engine()
    conn = get_connection()
    cursor = conn.cursor()

    query_exits = f"SELECT COUNT(1) as cnt FROM sync_logs WHERE \"接口名\"='{api_name}' AND \"表名\"='{table_name}'"
    logger.info(f"Execute SQL  [{query_exits}]")
    if int(pd.read_sql(query_exits, engine).iloc[0, 0]) == 0:
        insert_sql = f"INSERT INTO SYNC_LOGS (\"接口名\", \"表名\",\"日期\") VALUES ('{api_name}', '{table_name}', '{date}')"
        logger.info(f"Execute SQL  [{insert_sql}]")
        cursor.execute(insert_sql)

    else:
        update_sql =  f"UPDATE SYNC_LOGS SET \"日期\"='{date}' WHERE \"接口名\"='{api_name}' AND \"表名\"='{table_name}'"
        logger.info(f"Execute SQL  [{update_sql}]")
        cursor.execute(update_sql)
    conn.commit()
    cursor.close()
    conn.close()

# 增量追加表数据, 股票列表不具备增量条件, 全量覆盖
if __name__ == '__main__':
    date = query_api_sync_date('stock_sse_summary', 'stock_sse_summary')
    update_api_sync_date('stock_sse_summary', 'stock_sse_summary', 20251024)

