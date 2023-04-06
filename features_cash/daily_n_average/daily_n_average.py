"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 16:31
# @Author  : PcLiu
# @FileName: daily_n_change.py
===========================

计算 "三日线" 并存储 MySQL 库

"""
import logging
import os
import sys
import time
import datetime

from utils.utils import get_mock_connection, get_logger, exec_mysql_script, load_table, exec_mysql_sql, \
    exec_create_table_script, get_cfg, get_sql_engine, query_last_sync_date
import pandas as pd



package_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append('%s/../../utils' % package_path)
print(sys.path)


def calculate(drop_exist):
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist)

    cfg = get_cfg()
    connection = get_mock_connection()
    engine = get_sql_engine()
    logger = get_logger('daily_n_change', cfg['logging']['filename'])

    # 获取股票列表
    ts_code_sql = 'select ts_code from %s.stock_basic' % cfg['mysql']['database']
    logger.info('Load ts_code from table [stock_basic] with sql [%s]' % ts_code_sql)
    ts_code_list = pd.read_sql(ts_code_sql, engine)['ts_code']

    # 查询 daily 原始数据最早日期数据

    earliest_trade_date_sql = 'select ts_code, min(trade_date) as trade_date from %s.daily group by ts_code' % \
                              cfg['mysql']['database']
    logger.info('Load ts_code earliest trade_date from table [daily] with sql [%s]' % earliest_trade_date_sql)
    earliest_trade_dates = pd.read_sql(earliest_trade_date_sql, engine, index_col='ts_code')

    # 查询历史最大同步日期(构建计算的日期范围)
    trade_date_sql = 'select ts_code, max(trade_date) as trade_date from %s.daily_n_average group by ts_code' % \
                     cfg['mysql']['database']
    logger.info('Load ts_code last calculate date from table [daily_n_average] with sql [%s]' % trade_date_sql)
    trade_dates = pd.read_sql(trade_date_sql, engine, index_col='ts_code')

    steps = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 21, 31, 45, 61, 91, 123, 187, 365, 731, 1095,
             99999]
    ts_code_size = ts_code_list.shape[0]
    for ts_code_index in range(ts_code_size):
        ts_code = ts_code_list[ts_code_index]
        begin_date = str(earliest_trade_dates.loc[ts_code]['trade_date'])
        last_date = begin_date
        if trade_dates.index.__contains__(ts_code):
            last_date = str(trade_dates.loc[ts_code]['trade_date'] + 1)  # 历史计算日期 + 1 断点继续计算
        start = max(last_date, begin_date)
        end = str(datetime.datetime.now().strftime('%Y%m%d'))
        logger.info("Calculate ts_code[%s] start_date[%s] end_date[%s]" % (ts_code, start, end))

        # 循环计算
        start_date = datetime.datetime.strptime(start, '%Y%m%d')
        end_date = datetime.datetime.strptime(end, '%Y%m%d')
        step_date = start_date
        while step_date <= end_date:
            # 读取指定股票数据 并计算
            step_date_str = str(step_date.strftime('%Y%m%d'))
            daily_sql = "select ts_code,trade_date,vol,amount from %s.daily where ts_code = '%s' and trade_date <= %s" \
                        % (cfg['mysql']['database'], ts_code, step_date_str)
            logger.info('Load daily data from table [daily] with sql [%s]' % daily_sql)
            daily = pd.read_sql(daily_sql, engine)

            dic = {'ts_code': ts_code, 'trade_date': step_date_str}
            for step in steps:
                amount = daily['amount'][-step:].sum() * 1000
                vol = daily['vol'][-step:].sum() * 100
                dic['avg_%s' % step] = amount / vol
            res = pd.DataFrame([dic])
            res.to_sql('daily_n_average', connection, index=False, if_exists='append', chunksize=5000)
            step_date += + datetime.timedelta(days=1)


if __name__ == '__main__':

    print(sys.path)
    calculate(False)
