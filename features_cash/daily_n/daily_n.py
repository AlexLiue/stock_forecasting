"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 16:31
# @Author  : PcLiu
# @FileName: daily_n.py
===========================

计算 "三日线" 并存储 MySQL 库

"""
import os
import sys
import time
import datetime

from utils.utils import get_mock_connection, get_logger, exec_mysql_script, load_table, exec_mysql_sql
import pandas as pd

package_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append('%s/../../utils' % package_path)
print(sys.path)


def append_daily_n(df, dic, n=1):
    """
    获取 N 日线数据
    :param df: daily 日线数据
    :param dic: 引用并保存返回结果，数据字典格式
    :param n: 计算 n 日线
    :return: None
    """
    mp = 1
    open_n = df['open'].rolling(n, axis=0, min_periods=mp).agg(lambda rows: rows.iloc[0])
    close_n = df['close'].rolling(n, axis=0, min_periods=mp).agg(lambda rows: rows.iloc[-1])
    change_n = close_n - open_n
    pct_chg_n = change_n / close_n
    vol_n = df['vol'].rolling(n, axis=0, min_periods=mp).agg(lambda rows: rows.sum())
    amount_n = df['amount'].rolling(n, axis=0, min_periods=mp).agg(lambda rows: rows.sum())
    avg_n = (amount_n * 1000) / (vol_n * 100)

    dic['change_%s' % n] = change_n
    dic['pct_chg_%s' % n] = pct_chg_n
    dic['vol_%s' % n] = vol_n
    dic['amount_%s' % n] = amount_n
    dic['avg_%s' % n] = avg_n


def init():
    """
    初始化 N 日线数据
    :return:
    """
    steps = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 21, 31, 45, 61]  # N 日线列表

    # 建表
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_mysql_script(dir_path)

    connection = get_mock_connection()
    logger = get_logger('daily_n', 'data_syn.log')

    # 加载数据
    logger.info('Load source data from table [stock_basic, daily] ...')
    daily = load_table(table_name='daily', ts_code='', start_date='', end_date='')
    ts_code_list = load_table(table_name='stock_basic', ts_code='', start_date='', end_date='')['ts_code']

    # 数据加工计算
    ts_code_size = ts_code_list.shape[0]
    for ts_code_index in range(ts_code_size):
        ts_code = ts_code_list[ts_code_index]
        max_retry = 0
        while max_retry < 3:
            try:
                logger.info('Step ([%d] of [%d]): Get ts_code[%s] daily_n data...'
                            % (ts_code_index, ts_code_size, ts_code))
                df1 = daily[daily['ts_code'] == ts_code].sort_values(by=['trade_date'], ascending=[True], inplace=False)
                dic = {'ts_code': df1['ts_code'], 'trade_date': df1['trade_date']}
                for step in steps:
                    append_daily_n(df=df1, dic=dic, n=step)
                df2 = pd.DataFrame(dic)
                if df2.shape[0] > 0:
                    logger.info(
                        'Step ([%d] of [%d]): Write [%s] records of ts_code[%s] into table [daily_n] with [%s]' %
                        (ts_code_index, ts_code_size, df2.shape[0], ts_code, connection.engine))
                    df2.to_sql('daily_n', connection, index=False, if_exists='append', chunksize=5000)
                break
            except Exception as e:
                logger.error("Try [%d], Get Exception[%s]" % (max_retry, e.__cause__))
                time.sleep(3)
            max_retry += 1
        if max_retry == 3:
            return -1


def append():
    """
    每日增量追加 N 日线数据，考到数据量较大,出于性能考虑, 仅计算一个季度的 N 日线, 并只重写 14 天的数据
    :return:
    """
    steps = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 21, 31, 45, 61]  # N 日线列表
    start_date = int((datetime.datetime.now() + datetime.timedelta(days=-75)).strftime('%Y%m%d'))  # 读取 75 天内的数据
    back_offset_date = int((datetime.datetime.now() + datetime.timedelta(days=-14)).strftime('%Y%m%d'))

    connection = get_mock_connection()
    logger = get_logger('daily_n', 'data_syn.log')

    # 清理历史数据 - 清理 14 天内的历史数据
    clean_sql = "DELETE FROM daily_n WHERE trade_date>=%s" % back_offset_date
    logger.info('Execute Clean SQL [%s]' % clean_sql)
    counts = exec_mysql_sql(clean_sql)
    logger.info("Execute Clean SQL Affect [%d] records" % counts)

    logger.info('Load source data from table [stock_basic, daily] ...')
    daily = load_table(table_name='daily', ts_code='', start_date=start_date, end_date='')  # 读取 75 天内的日线数据
    ts_code_list = load_table(table_name='stock_basic', ts_code='', start_date='', end_date='')['ts_code']  # 读取 ts_code

    # 构建处理结果 DataFrame
    dic = {'ts_code': [], 'trade_date': []}
    for n in steps:
        dic['change_%s' % n] = []
        dic['pct_chg_%s' % n] = []
        dic['vol_%s' % n] = []
        dic['amount_%s' % n] = []
        dic['avg_%s' % n] = []
    res = pd.DataFrame(dic)

    ts_code_size = ts_code_list.shape[0]
    for ts_code_index in range(ts_code_size):
        ts_code = ts_code_list[ts_code_index]
        max_retry = 0
        while max_retry < 3:
            try:
                logger.info('Step ([%d] of [%d]): Get ts_code[%s] daily_n append data...'
                            % (ts_code_index, ts_code_size, ts_code))
                df1 = daily[daily['ts_code'] == ts_code].sort_values(by=['trade_date'], ascending=[True], inplace=False)
                dic = {'ts_code': df1['ts_code'], 'trade_date': df1['trade_date']}
                for step in steps:
                    append_daily_n(df=df1, dic=dic, n=step)
                df2 = pd.DataFrame(dic)
                df3 = df2[df2['trade_date'] >= back_offset_date]
                if df3.shape[0] > 0:
                    res = pd.concat([res, df3], ignore_index=True)
                    print(res)
                    logger.info(
                        'Step ([%d] of [%d]): Append [%s] records of ts_code[%s] into table [daily_n] with [%s]' %
                        (ts_code_index, ts_code_size, df2.shape[0], ts_code, connection.engine))
                break
            except Exception as e:
                logger.error("Try [%d], Get Exception[%s]" % (max_retry, e.__cause__))
                time.sleep(3)
            max_retry += 1
        if max_retry == 3:
            return -1
    logger.info('Append write [%s] records into table [daily_n] with [%s]' % (res.shape[0], connection.engine))
    res.to_sql('daily_n', connection, index=False, if_exists='append', chunksize=5000)


if __name__ == '__main__':
    print(sys.path)
    append()
