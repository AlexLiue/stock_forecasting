"""
数据同步程序入口
"""

import argparse
import multiprocessing

from akshare_sync.stock_basic_info import stock_basic_info
from akshare_sync.stock_hk_short_sale import stock_hk_short_sale
from akshare_sync.stock_sse_deal_daily import stock_sse_deal_daily
from akshare_sync.stock_sse_summary import stock_sse_summary
from akshare_sync.stock_szse_area_summary import stock_szse_area_summary
from akshare_sync.stock_szse_sector_summary import stock_szse_sector_summary
from akshare_sync.stock_szse_summary import stock_szse_summary
from akshare_sync.stock_table_api_summary import stock_table_api_summary
from akshare_sync.stock_trade_date import stock_trade_date
from akshare_sync.stock_zh_a_hist_30min_hfq import stock_zh_a_hist_30min_hfq
from akshare_sync.stock_zh_a_hist_30min_qfq import stock_zh_a_hist_30min_qfq
from akshare_sync.stock_zh_a_hist_daily_hfq import stock_zh_a_hist_daily_hfq
from akshare_sync.stock_zh_a_hist_daily_qfq import stock_zh_a_hist_daily_qfq
from akshare_sync.stock_zh_a_hist_monthly_hfq import stock_zh_a_hist_monthly_hfq
from akshare_sync.stock_zh_a_hist_monthly_qfq import stock_zh_a_hist_monthly_qfq
from akshare_sync.stock_zh_a_hist_weekly_hfq import stock_zh_a_hist_weekly_hfq
from akshare_sync.stock_zh_a_hist_weekly_qfq import stock_zh_a_hist_weekly_qfq


# 全量历史初始化
def sync(processes_size):
    stock_trade_date.sync()  # 交易日历
    stock_basic_info.sync()  # 股票基本信息: 股票代码、股票名称、交易所、板块

    """ 同步的函数列表 """
    functions = [
        stock_table_api_summary.sync,  # 表 API 接口信息
        stock_hk_short_sale.sync,  # 港股 HK 淡仓申报
        stock_sse_summary.sync,  # 上海证券交易所-股票数据总貌
        stock_szse_summary.sync,  # 深圳证券交易所-市场总貌-证券类别统计
        stock_szse_area_summary.sync,
        stock_szse_sector_summary.sync,
        stock_sse_deal_daily.sync,  # 上海证券交易所-数据-股票数据-成交概况-股票成交概况-每日股票情况
        stock_zh_a_hist_30min_qfq.sync,  # 东方财富网-行情首页-港股-每日分时行情-30分钟-前复权
        stock_zh_a_hist_30min_hfq.sync,  # 东方财富网-行情首页-港股-每日分时行情-30分钟-后复权
        stock_zh_a_hist_daily_qfq.sync,  # 东方财富-沪深京 A 股日频率数据 - 前复权
        stock_zh_a_hist_daily_hfq.sync,  # 东方财富-沪深京 A 股日频率数据 - 后复权
        stock_zh_a_hist_weekly_qfq.sync,  # 东方财富-沪深京 A 股周频率数据 - 前复权
        stock_zh_a_hist_weekly_hfq.sync,  # 东方财富-沪深京 A 股周频率数据 - 后复权
        stock_zh_a_hist_monthly_qfq.sync,  # 东方财富-沪深京 A 股月频率数据 - 前复权
        stock_zh_a_hist_monthly_hfq.sync,  # 东方财富-沪深京 A 股月频率数据 - 后复权
    ]

    """ 创建执行的线程池对象, 并指定线程池大小, 并提交数据同步task任务  """
    pool = multiprocessing.Pool(processes=processes_size)

    results = [pool.apply_async(function, args=(False,)) for function in functions]

    """ 关闭进程池，不再接受新的任务"""
    pool.close()

    """ 等待所有任务完成 """
    pool.join()

    """ 收集任务结果 """
    for result in results:
        print(result.get())


def use_age():
    print("Useage: python syn_start.py [--processes 4]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="sync mode args")
    parser.add_argument("--processes", default=4, type=int, help="同步并发线程池大小")
    args = parser.parse_args()
    processes = args.processes
    print(f"Exec With Args:--processes [{processes}]")

    sync(processes)
