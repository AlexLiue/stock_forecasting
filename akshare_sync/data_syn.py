"""
数据同步程序入口
"""
import argparse

from stock_basic_info import stock_basic_info


# 全量历史初始化
def sync(drop_exist):
    stock_basic_info.sync(drop_exist)


def use_age():
    print('Useage: python data_syn.py --mode [init | append | init_spc | append_spc] [--drop_exist]')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='sync mode args')

    parser.add_argument('--drop_exist', action='store_true',
                        help='初始化建表过程如果表已存在 Drop 后再建')

    args = parser.parse_args()
    dropExist = args.drop_exist
    print(f'Exec With Args:--drop_exist [{dropExist}]')
    sync(dropExist)


