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
import re
import sys
import time
from pathlib import Path

import cx_Oracle
import oracledb
import pandas as pd
import pymysql
import sqlparse
import tushare as ts
from sqlalchemy import create_engine


# 加载配置信息函数
def get_cfg():
    cfg = configparser.ConfigParser()
    file_name = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../application.ini'))
    cfg.read(file_name)
    return cfg


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

        console_fmt = '[%(asctime)s] [%(levelname)s] [ %(filename)s:%(lineno)s - %(name)s ] %(message)s '
        console_handler = logging.StreamHandler(stream=sys.stdout)
        console_handler.setFormatter(logging.Formatter(fmt=console_fmt))

        if len(logger.handlers) < 2:
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

        # if not logger.handlers:
        #     logger.addHandler(console_handler)

    return logger


def once_init_decorator(func):
    cfg = get_cfg()
    logger = get_logger('tools', cfg['sync-logging']['filename'])
    called = False
    def wrapper(*args, **kwargs):
        nonlocal called
        if not called:
            called = True
            return func(*args, **kwargs)
        else:
            logger.info("Oracle Client has already been inited.")
            return None
    return wrapper

@once_init_decorator
def init_oracle_client():
    cfg = get_cfg()
    lib_dir = cfg['oracle']['client']
    try:
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    except Exception as err:
        print("Error connecting: cx_Oracle.init_oracle_client()")
        print(err)
        sys.exit(1)


# 获取 MySQL Connection 对象
def get_engine():
    cfg = get_cfg()
    init_oracle_client()
    username = cfg['oracle']['user']
    password = cfg['oracle']['password']
    host = cfg['oracle']['host']
    port = cfg['oracle']['port']
    service_name = cfg['oracle']['service_name']
    dsn = f"oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={service_name}"
    return create_engine(dsn)

# 获取 Oracle Connection 对象
def get_connection():
    cfg = get_cfg()
    params = oracledb.ConnectParams(host=cfg['oracle']['host'], port=int(cfg['oracle']['port']), service_name=cfg['oracle']['service_name'])
    return  oracledb.connect(user=cfg['oracle']['user'], password=cfg['oracle']['password'], params=params)





def exec_sql(sql):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()



def load_sql_script(path):
    file = open(path, 'r', encoding='UTF-8')
    sql_script = file.read().upper()
    stats = sqlparse.split(sql_script)
    res = []
    i = 0
    session_flag = False
    for item in stats:
        format_item = item.strip()
        if format_item.startswith("BEGIN"):
            res.append(format_item)
            session_flag = True
        elif session_flag == False:
             res.append(format_item)
             i += 1
        else:
            res[i] = res[i] + "\n" + format_item

        if format_item.endswith("END") or format_item.endswith("END;"):
            session_flag = False
            i += 1
    file.close()

    for i in range(len(res)):
        res[i] = re.sub(r'\s+', ' ', res[i].replace("\n",""))

    return res


def exec_create_table_script(script_dir, drop_exist, logger):
    """
    执行 SQL 脚本
    :param script_dir: 脚本路径
    :param drop_exist: 如果表存在是否先 Drop 后再重建
    :param logger: 日志类
    :return:
    """
    table_name = Path(script_dir).name
    table_exist = query_table_is_exist(table_name)
    if (not table_exist) | (table_exist & drop_exist):
        conn = get_connection()
        cursor = conn.cursor()

        count = 0
        flt_cnt = 0
        suc_cnt = 0
        str1 = ''
        for home, dirs, files in os.walk(script_dir):
            for filename in files:
                if filename.endswith('.sql'):
                    dir_name = os.path.dirname(os.path.abspath(__file__))
                    full_name = os.path.join(dir_name, script_dir, filename)
                    sql_list = load_sql_script(full_name)
                    for commandSQL in sql_list:
                        command = commandSQL.strip()
                        if command != '':
                            try:
                                if not command.startswith("BEGIN"):
                                    command = command[:-1]
                                logger.info('Execute SQL [%s]' % command)
                                cursor.execute(command)
                                conn.commit()
                                count = count + 1
                                suc_cnt = suc_cnt + 1
                            except Exception as e:
                                print(e)
                                print(command)
                                flt_cnt = flt_cnt + 1
                                logger.info('Execute Failed. ')
                                pass
        logger.info('Execute result: Total [%s], Succeed [%s] , Failed [%s] ' % (count, suc_cnt, flt_cnt))

        # 清理 LOGS 表的记录
        clean_logs_sql = f"DELETE FROM SYNC_LOGS WHERE \"接口名\"='{table_name}'"
        logger.info(f"Execute SQL  [{clean_logs_sql}]")
        cursor.execute(clean_logs_sql)
        conn.commit()

        cursor.close()
        conn.close()
        if flt_cnt > 0:
            raise Exception('Execute SQL script [%s] failed. ' % script_dir)


def query_table_is_exist(table_name):
    sql = "SELECT count(1) from USER_TABLES t WHERE t.TABLE_NAME ='%s'" % table_name.upper()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    count = cursor.fetchall()[0][0]
    cursor.close()
    conn.close()
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
    conn = get_connection()
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
