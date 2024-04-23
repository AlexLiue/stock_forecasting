"""
数据同步共享函数定义
1. 提供配置信息加载函数
1. 提供数据库 Engine or Connection 对象创建函数
2. 提供 tushare DataApi 对象函数
"""

import configparser
import datetime
import logging
import os
import sys
import time

import pandas as pd
import pymysql
import tushare as ts
from sqlalchemy import create_engine


# 加载配置信息函数
def get_cfg():
    cfg = configparser.ConfigParser()
    file_name = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../application.ini'))
    cfg.read(file_name)
    return cfg


# 获取 MySQL Connection 对象
def get_mock_connection():
    cfg = get_cfg()
    db_host = cfg['mysql']['host']
    db_user = cfg['mysql']['user']
    db_password = cfg['mysql']['password']
    db_port = cfg['mysql']['port']
    db_database = cfg['mysql']['database']
    db_url = 'mysql://%s:%s@%s:%s/%s?charset=utf8&use_unicode=1' % (db_user, db_password, db_host, db_port, db_database)
    return create_engine(db_url)


def get_mysql_connection():
    cfg = get_cfg()
    return pymysql.connect(host=cfg['mysql']['host'],
                           port=int(cfg['mysql']['port']),
                           user=cfg['mysql']['user'],
                           passwd=cfg['mysql']['password'],
                           db=cfg['mysql']['database'],
                           charset='utf8')


# 构建 Tushare 查询 API 接口对象
def get_tushare_api():
    cfg = get_cfg()
    token = cfg['tushare']['token']
    return ts.pro_api(token=token, timeout=300)


# 获取日志文件打印输出对象
def get_logger(log_name, file_name):
    cfg = get_cfg()
    log_level = cfg['sync-logging']['level']
    backup_days = int(cfg['sync-logging']['backupDays'])
    logger = logging.getLogger(log_name)
    logger.setLevel(log_level)
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..', 'logs')
    log_file = os.path.join(log_dir, '%s.%s' % (file_name, str(datetime.datetime.now().strftime('%Y-%m-%d'))))
    if file_name != '':
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        clean_file = os.path.join(log_dir, 'file_name.%s' %
                                  str((datetime.datetime.now() +
                                       datetime.timedelta(days=-backup_days)).strftime('%Y-%m-%d'))
                                  )

        if os.path.exists(clean_file):
            os.remove(clean_file)
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_fmt = '[%(asctime)s] [%(levelname)s] [ %(filename)s:%(lineno)s - %(name)s ] %(message)s '
        formatter = logging.Formatter(file_fmt)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_fmt = '[%(asctime)s] [%(levelname)s] [ %(filename)s:%(lineno)s - %(name)s ] %(message)s '
        console_handler = logging.StreamHandler(stream=sys.stdout)
        console_handler.setFormatter(logging.Formatter(fmt=console_fmt))
        logger.addHandler(console_handler)
    return logger


def exec_mysql_sql(sql):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    counts = cursor.execute(sql + ';')
    conn.commit()
    cursor.close()
    conn.close()
    return counts


def exec_create_table_script(script_dir, drop_exist, logger):
    """
    执行 SQL 脚本
    :param script_dir: 脚本路径
    :param drop_exist: 如果表存在是否先 Drop 后再重建
    :param logger: 日志类
    :return:
    """
    table_name = str(script_dir).split('/')[-1]
    table_exist = query_table_is_exist(table_name)
    if (not table_exist) | (table_exist & drop_exist):
        db = get_mysql_connection()
        cursor = db.cursor()
        count = 0
        flt_cnt = 0
        suc_cnt = 0
        str1 = ''
        for home, dirs, files in os.walk(script_dir):
            for filename in files:
                if filename.endswith('.sql'):
                    dir_name = os.path.dirname(os.path.abspath(__file__))
                    full_name = os.path.join(dir_name, script_dir, filename)
                    file_object = open(full_name)
                    for line in file_object:
                        if not line.startswith("--") and not line.startswith('/*'):  # 处理注释
                            str1 = str1 + ' ' + ' '.join(line.strip().split())  # pymysql一次只能执行一条sql语句
                    file_object.close()  # 循环读取文件时关闭文件很重要，否则会引起bug
        for commandSQL in str1.split(';'):
            command = commandSQL.strip()
            if command != '':
                try:
                    logger.info('Execute SQL [%s]' % command.strip())
                    cursor.execute(command.strip() + ';')
                    count = count + 1
                    suc_cnt = suc_cnt + 1
                except db.DatabaseError as e:
                    print(e)
                    print(command)
                    flt_cnt = flt_cnt + 1
                    pass
        logger.info('Execute result: Total [%s], Succeed [%s] , Failed [%s] ' % (count, suc_cnt, flt_cnt))
        cursor.close()
        db.close()
        if flt_cnt > 0:
            raise Exception('Execute SQL script [%s] failed. ' % script_dir)


def query_table_is_exist(table_name):
    sql = "SELECT count(1) from information_schema.TABLES t WHERE t.TABLE_NAME ='%s'" % table_name
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute(sql + ';')
    count = cursor.fetchall()[0][0]
    if int(count) > 0:
        return True
    else:
        return False


def query_last_sync_date(sql):
    """
    查询历史同步数据的最大日期
    :param sql: 执行查询的SQL
    :return: 查询结果
    """
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute(sql + ';')
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    last_date = result[0][0]
    result = "19700101"
    if last_date is not None:
        result = str(last_date)
    return result


# 获取两个日期的最小值
def min_date(date1, date2):
    if date1 <= date2:
        return date1
    else:
        return date2


def max_date(date1, date2):
    if date1 >= date2:
        return date1
    else:
        return date2

#
# if __name__ == '__main__':
#     # ts_codes_1 = get_ts_code_list(0.3, 1000)
#     #
#     # print(ts_codes_1[0:10].str.cat(sep=','))
