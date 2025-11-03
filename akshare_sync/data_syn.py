"""
数据同步程序入口
"""
import argparse

import cx_Oracle
import sys

from akshare_sync.stock_basic_info import stock_basic_info
from akshare_sync.stock_short_sale_hk import stock_short_sale_hk
from akshare_sync.stock_sse_deal_daily import stock_sse_deal_daily
from akshare_sync.stock_szse_area_summary import stock_szse_area_summary
from akshare_sync.stock_szse_sector_summary import stock_szse_sector_summary
from akshare_sync.stock_szse_summary import stock_szse_summary
from akshare_sync.stock_sse_summary import stock_sse_summary
from akshare_sync.stock_table_api_summary import stock_table_api_summary
from akshare_sync.stock_zh_a_hist_daily_hfq import stock_zh_a_hist_daily_hfq
from akshare_sync.util.tools import get_cfg


# 全量历史初始化
def sync(drop_exist, max_retry, retry_interval):
    stock_table_api_summary.sync(False)  # 表 API 接口信息
    stock_basic_info.sync(False) # 股票基本信息: 股票代码、股票名称、交易所、板块
    stock_basic_info.sync(False) # 股票基本信息: 股票代码、股票名称、交易所、板块
    stock_short_sale_hk.sync(False)  # 港股 HK 淡仓申报
    stock_sse_summary.sync(drop_exist) #上海证券交易所-股票数据总貌
    stock_szse_summary.sync(drop_exist) # 深圳证券交易所-市场总貌-证券类别统计
    stock_szse_area_summary.sync(drop_exist)
    stock_szse_sector_summary.sync(drop_exist)
    stock_sse_deal_daily.sync(drop_exist) #上海证券交易所-数据-股票数据-成交概况-股票成交概况-每日股票情况
    # stock_zh_a_hist_daily_hfq.sync(drop_exist) #东方财富-沪深京 A 股日频率数据 - 后复权
    # stock_zh_a_hist_daily_hfq.sync(drop_exist, max_retry, retry_interval) # 东方财富-沪深京 A 股日频率数据;




def use_age():
    print('Useage: python data_syn.py --mode [init | append | init_spc | append_spc] [--drop_exist]')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sync mode args')

    parser.add_argument('--drop_exist', action='store_true',
                        help='初始化建表过程如果表已存在 Drop 后再建')

    args = parser.parse_args()
    dropExist = args.drop_exist
    print(f'Exec With Args:--drop_exist [{dropExist}]')


    sync(dropExist, 3, 5)





