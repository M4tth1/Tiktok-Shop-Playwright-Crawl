#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/5/12 01:57
# @Author  : fuganchen
# @Site    : 
# @File    : sql_query_helper.py
# @Project : tiktok_crawl
# @Software: PyCharm
import pymysql
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


if __name__ == '__main__':
    sql = "select * from tkShopBasicInfoDto"
    # 数据库名有：kScoreInfo，tkCounterpartsRank，userAssets，tkShopMonthlyBillInfos，tkShopDailyBillInfos，tkShopNoClearingInfos，tkShopClearingInfos
    # sql = "select * from kScoreInfo"
    # sql = "select * from tkCounterpartsRank"
    # sql = "select * from userAssets"
    sql = "select * from tkShopMonthlyBillInfos"
    # sql = "select * from tkShopDailyBillInfos"
    # sql = "select * from tkShopNoClearingInfos"
    # sql = "select * from tkShopClearingInfos"
    res = query(sql)
    print(res)