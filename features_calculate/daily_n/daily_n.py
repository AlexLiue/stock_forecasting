"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 16:31
# @Author  : PcLiu
# @FileName: daily_n_break.py
===========================

计算 "N日线" 并存储 MySQL 库

1. 根据 daily 表的 交易总金额 amount 和 交易总手数 vol 计算 N 日均值, 最小值, 最大值
2. 初始化过程使用  pandas.DataFrame.rolling() 窗口函数计算
3. 后续追加使用手工计算： N 日总交易金额 / N 日总交易手数 * 100
"""
import datetime
import os
import sys

import pandas as pd

from features_calculate.utils.utils import get_mock_connection, get_logger, exec_create_table_script, get_cfg, \
    get_sql_engine

package_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append('%s/../../utils' % package_path)
print(sys.path)


def daily_n_rolling(daily_df, dic, window, min_periods):
    """
 获取 N 日线数据
    :param daily_df:
    :param dic:
    :param window:
    :param min_periods:
    :return:
    """
    # 窗口总交易手数, 默认 1手 = 100股
    vol_n = daily_df['vol'].rolling(window=window,
                                    min_periods=min_periods,
                                    center=False,
                                    axis=0,
                                    closed='right').agg(lambda rows: rows.sum())
    # 窗口总交易金额， 单位 1000
    amount_n = daily_df['amount'].rolling(window=window,
                                          min_periods=min_periods,
                                          center=False,
                                          axis=0,
                                          closed='right').agg(lambda rows: rows.sum())
    # 窗口最低成交价
    low_n = daily_df['low'].rolling(window=window,
                                    min_periods=min_periods,
                                    center=False,
                                    axis=0,
                                    closed='right').agg(lambda rows: rows.min())
    # 窗口最高成交价
    high_n = daily_df['low'].rolling(window=window,
                                     min_periods=min_periods,
                                     center=False,
                                     axis=0,
                                     closed='right').agg(lambda rows: rows.max())

    avg_n = (amount_n * 1000) / (vol_n * 100)
    dic['avg_%s' % window] = avg_n
    dic['low_%s' % window] = low_n
    dic['high_%s' % window] = high_n


def calculate(drop_exist):
    """
    计算 N 日线
    :param drop_exist: 是否重新计算
    :return:
    """
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist)

    cfg = get_cfg()
    connection = get_mock_connection()
    engine = get_sql_engine()
    logger = get_logger('daily_n', cfg['feature-logging']['filename'])

    # 获取股票列表
    ts_code_sql = 'select distinct ts_code from %s.stock_basic order by ts_code asc' % cfg['mysql']['database']
    logger.info('Load ts_code from table [stock_basic] with sql [%s]' % ts_code_sql)
    ts_code_list = pd.read_sql(ts_code_sql, engine)['ts_code']

    # 查询 daily 原始数据最早日期数据
    earliest_trade_date_sql = 'select ts_code, min(trade_date) as trade_date from %s.daily group by ts_code' % \
                              cfg['mysql']['database']
    logger.info('Load ts_code earliest trade_date from table [daily] with sql [%s]' % earliest_trade_date_sql)
    earliest_trade_dates = pd.read_sql(earliest_trade_date_sql, engine, index_col='ts_code')

    # 查询历史最大同步日期(构建增量计算的日期范围)
    trade_date_sql = 'select ts_code, max(trade_date) as trade_date from %s.daily_n group by ts_code' % \
                     cfg['mysql']['database']
    logger.info('Load ts_code last calculate date from table [daily_n] with sql [%s]' % trade_date_sql)
    trade_dates = pd.read_sql(trade_date_sql, engine, index_col='ts_code')

    # N 日线窗口大小
    windows = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 21, 31, 45, 61, 91, 123, 187, 365, 731, 1095,
               99999]

    # 为降低处理内存占，采用循环依次加载单只股票数据，然后并计算处理
    ts_code_size = ts_code_list.shape[0]
    for ts_code_index in range(ts_code_size):
        ts_code = ts_code_list[ts_code_index]
        # 读取指定股票数据
        end_date_str = str(datetime.datetime.now().strftime('%Y%m%d'))
        daily_sql = f"select ts_code,trade_date,low,high,vol,amount " \
                    f"from {cfg['mysql']['database']}.daily where ts_code = '{ts_code}' order by trade_date asc"
        logger.info('Load daily data from table [daily] with sql [%s]' % daily_sql)
        daily_df = pd.read_sql(daily_sql, engine, index_col='trade_date')

        # 根据是否曾初始化数据, 采用不同的计算方式
        if not trade_dates.index.__contains__(ts_code):
            # 初始化：基于 rolling 计算 N 日均线
            dic = {'ts_code': daily_df['ts_code'], 'trade_date': daily_df.index}
            for window in windows:
                daily_n_rolling(daily_df=daily_df, dic=dic, window=window, min_periods=1)
            daily_n = pd.DataFrame(dic)
            if daily_n.shape[0] > 0:
                daily_n.to_sql('daily_n', connection, index=False, if_exists='append', chunksize=5000)
                logger.info(
                    'Step ([%d] of [%d]): Write [%s] records of ts_code[%s] into table [daily_n] with [%s]' %
                    (ts_code_index, ts_code_size - 1, daily_n.shape[0], ts_code, connection.engine))
        else:
            # 增量追加：手工计算 N 日均线
            last_date_his = datetime.datetime.strptime(str(trade_dates.loc[ts_code]['trade_date']), '%Y%m%d')
            last_date = str((last_date_his + datetime.timedelta(days=1)).strftime('%Y%m%d'))
            # 历史计算日期 + 1 作为断点继续计算的起始日期
            begin_date = str(earliest_trade_dates.loc[ts_code]['trade_date'])
            start_date_str = max(last_date, begin_date)
            logger.info("'Step ([%d] of [%d]): Calculate ts_code[%s] start_date[%s] end_date[%s]" %
                        (ts_code_index, ts_code_size - 1, ts_code, start_date_str, end_date_str))

            # 循环日期计算
            start_date = datetime.datetime.strptime(start_date_str, '%Y%m%d')
            end_date = datetime.datetime.strptime(end_date_str, '%Y%m%d')
            step_date = start_date
            while step_date <= end_date:
                # 筛除日期大于 step_date 的记录
                step_date_int = int(str(step_date.strftime('%Y%m%d')))
                daily = daily_df[daily_df.index <= step_date_int]

                # 如果 step_date 是交易日, 则 daily 必包含数据
                if daily.index.__contains__(step_date_int):
                    dic = {'ts_code': ts_code, 'trade_date': step_date_int}
                    # 计算单日的 N 日线
                    for window in windows:
                        amount_n = daily['amount'][-window:].sum() * 1000
                        vol_n = daily['vol'][-window:].sum() * 100
                        low_n = daily['low'][-window:].min()
                        high_n = daily['high'][-window:].max()
                        dic['avg_%s' % window] = amount_n / vol_n
                        dic['low_%s' % window] = low_n
                        dic['high_%s' % window] = high_n
                    daily_n = pd.DataFrame([dic])
                    rowcount = daily_n.to_sql('daily_n', connection, index=False, if_exists='append', chunksize=5000)
                    logger.info(
                        'Step ([%d] of [%d]): Write [%s] records of ts_code[%s] into table [daily_n] with [%s]' %
                        (ts_code_index, ts_code_size - 1, daily_n.shape[0], ts_code, connection.engine))

                # 计算后一日数据
                step_date += + datetime.timedelta(days=1)


if __name__ == '__main__':
    print(sys.path)
    calculate(False)
