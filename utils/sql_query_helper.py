#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/5/12 01:57
# @Author  : fuganchen
# @Site    : 
# @File    : sql_query_helper.py
# @Project : tiktok_crawl
# @Software: PyCharm
import pymysql


def get_conn():
    """
    :return: 连接，游标
    """
    # 创建连接
    conn = pymysql.connect(host='127.0.0.1',
                           port=3306,
                           user='root',
                           password='6468467495',
                           db='mysql')
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