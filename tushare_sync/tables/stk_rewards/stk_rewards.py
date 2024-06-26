"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/9 22:50
# @Author  : PcLiu
# @FileName: stk_rewards.py
===========================

沪深股票-基础信息-管理层薪酬和持股（数据量较大,不存在日更新逻辑, 选择性同步）
接口：stk_rewards
描述：获取上市公司管理层薪酬和持股
tushare 接口说明： https://tushare.pro/document/2?doc_id=194
"""


import datetime
import os

from tushare_sync.utils.utils import exec_create_table_script, exec_sync_with_ts_code, \
    query_last_sync_date, max_date, get_cfg


def exec_sync(start_date, end_date):
    exec_sync_with_ts_code(
        table_name='stk_rewards',
        api_name='stk_rewards',
        fields=[
            "ts_code",
            "ann_date",
            "end_date",
            "name",
            "title",
            "reward",
            "hold_vol"
        ],
        date_column='end_date',
        start_date=start_date,
        end_date=end_date,
        date_step=1,
        limit=5000,
        interval=0.3,
        ts_code_limit=1000)


# 全量初始化表数据
def sync(drop_exist):
    # 创建表
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist)

    # 查询历史最大同步日期
    begin_date = '20100101'
    cfg = get_cfg()
    date_query_sql = "select max(end_date) date from %s.stk_rewards" % cfg['mysql']['database']
    last_date = query_last_sync_date(date_query_sql)
    start_date = max_date(last_date, begin_date)
    end_date = str(datetime.datetime.now().strftime('%Y%m%d'))

    exec_sync(start_date, end_date)


if __name__ == '__main__':
    sync(False)
