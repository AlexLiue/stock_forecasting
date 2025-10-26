"""
数据同步程序入口
"""
import argparse

import cx_Oracle
import sys
from akshare_sync.stock_szse_summary import stock_szse_summary
from akshare_sync.stock_sse_summary import stock_sse_summary
from akshare_sync.util.tools import get_cfg


# 全量历史初始化
def sync(drop_exist, max_retry, retry_interval):
    stock_sse_summary.sync(drop_exist, max_retry, retry_interval)
    stock_szse_summary.sync(drop_exist, max_retry, retry_interval)





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





