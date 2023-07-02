#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/6/23 10:52
# @Author  : fuganchen
# @Site    : 
# @File    : store_verify_code.py
# @Project : flask_webhook
# @Software: PyCharm
import os
import sys
import datetime
import pymysql
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(PROJECT_DIR)
sys.path.append(PROJECT_DIR)
from utils.get_ini_config import get_config


def get_conn():
    """
    :return: 连接，游标
    """
    # 创建连接
    host = get_config("database_config", "host")
    port = int(get_config("database_config", "port"))
    user = get_config("database_config", "user")
    password = get_config("database_config", "password")
    db = get_config("database_config", "database")
    minsize = int(get_config("database_config", "minsize"))
    maxsize = int(get_config("database_config", "maxsize"))
    conn = pymysql.connect(host=host,
                           port=port,
                           user=user,
                           password=password,
                           db=db)
    # 创建游标
    cursor = conn.cursor()
    return conn, cursor


def close_conn(conn, cursor):
    """
    关闭连接
    :param conn:
    :param cursor:
    :return:
    """
    if cursor:
        cursor.close()
    if conn:
        conn.close()


def query(sql, *args):
    """
    封装通用查询
    :param sql:
    :param args:
    :return: 返回结果
    """
    conn, cursor = get_conn()
    cursor.execute(sql, args)
    res = cursor.fetchall()
    close_conn(conn, cursor)
    return res


def insert_verify_code(verify_code, phone):
    """
    插入验证码
    :param verify_code:
    :param phone:
    :return:
    """
    conn, cursor = get_conn()
    update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql = "insert into tkVerifyCodeInfos(verifyCode, phone, updateTime) values(%s, %s, %s) on duplicate key update verifyCode=values(verifyCode), updateTime=values(updateTime)"
    cursor.execute(sql, (verify_code, phone, update_time))
    conn.commit()
    close_conn(conn, cursor)





if __name__ == '__main__':
    sql = "select * from tkShopBasicInfoDto"
    # 数据库名有：kScoreInfo，tkCounterpartsRank，userAssets，tkShopMonthlyBillInfos，tkShopDailyBillInfos，tkShopNoClearingInfos，tkShopClearingInfos
    # sql = "select * from kScoreInfo"
    # sql = "select * from tkCounterpartsRank"
    # sql = "select * from userAssets"
    sql = "select * from tkVerifyCodeInfos"
    # sql = "select * from tkShopDailyBillInfos"
    # sql = "select * from tkShopNoClearingInfos"
    # sql = "select * from tkShopClearingInfos"
    insert_verify_code("7153", "16737229683")
    res = query(sql)
    print(res)