"""
数据同步程序入口
"""
import argparse
import multiprocessing

from akshare_sync.stock_basic_info import stock_basic_info
from akshare_sync.stock_short_sale_hk import stock_short_sale_hk
from akshare_sync.stock_sse_deal_daily import stock_sse_deal_daily
from akshare_sync.stock_szse_area_summary import stock_szse_area_summary
from akshare_sync.stock_szse_sector_summary import stock_szse_sector_summary
from akshare_sync.stock_szse_summary import stock_szse_summary
from akshare_sync.stock_sse_summary import stock_sse_summary
from akshare_sync.stock_table_api_summary import stock_table_api_summary
from akshare_sync.stock_zh_a_hist_daily_hfq import stock_zh_a_hist_daily_hfq
from akshare_sync.stock_zh_a_hist_monthly_hfq import stock_zh_a_hist_monthly_hfq
from akshare_sync.stock_zh_a_hist_weekly_hfq import stock_zh_a_hist_weekly_hfq

import math
from io import StringIO
from concurrent.futures import ProcessPoolExecutor, as_completed

class ProcessPool:
    """
    线程池类, 并发同步多个表的数据, 单表不同股票、日期的数据串行处理
    """
    def __init__(self, max_processes: int):
        self.tasklist = []
        self.process_pool = ProcessPoolExecutor(max_processes)

    def submit_task(self, task, *args):
        """提交任务到进程池"""
        self.tasklist.append(self.process_pool.submit(task, *args))

    def get_results(self):
        """获取所有已完成任务的结果，并清除已处理的任务列表"""
        process_results = [task.result() for task in as_completed(self.tasklist)]
        self.tasklist.clear()
        return process_results



# 全量历史初始化
def sync(max_processes):
    """ 同步的函数列表 """
    functions = [
        stock_table_api_summary.sync,  # 表 API 接口信息
        stock_table_api_summary.sync,  # 表 API 接口信息
        stock_basic_info.sync,  # 股票基本信息: 股票代码、股票名称、交易所、板块
        stock_basic_info.sync,  # 股票基本信息: 股票代码、股票名称、交易所、板块
        stock_short_sale_hk.sync,  # 港股 HK 淡仓申报
        stock_sse_summary.sync,  # 上海证券交易所-股票数据总貌
        stock_szse_summary.sync,  # 深圳证券交易所-市场总貌-证券类别统计
        stock_szse_area_summary.sync,
        stock_szse_sector_summary.sync,
        stock_sse_deal_daily.sync,  # 上海证券交易所-数据-股票数据-成交概况-股票成交概况-每日股票情况
        stock_zh_a_hist_daily_hfq.sync,  # 东方财富-沪深京 A 股日频率数据 - 后复权
        stock_zh_a_hist_weekly_hfq.sync,  # 东方财富-沪深京 A 股周频率数据 - 后复权
        stock_zh_a_hist_monthly_hfq.sync  # 东方财富-沪深京 A 股月频率数据 - 后复权
    ]

    """ 创建执行的线程池对象, 并指定线程池大小, 并提交数据同步task任务  """
    pool = ProcessPool(max_processes=max_processes)
    for func in functions:
        pool.submit_task(func)

    results = pool.get_results()
    print(f"计算结果是：{results}")


def use_age():
    print('Useage: python data_syn.py [--pool_size 4]')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sync mode args')
    parser.add_argument('--pool_size',default=4, type=int, help='同步并发线程池大小')
    args = parser.parse_args()
    pool_size = args.pool_size
    print(f'Exec With Args:--pool_size [{pool_size}]')

    sync(pool_size)





