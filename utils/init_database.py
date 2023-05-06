#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/5/6 23:00
# @Author  : fuganchen
# @Site    : 
# @File    : init_database.py
# @Project : tiktok_crawl
# @Software: PyCharm
import pymysql
# 创建数据库表,具体表的字段根据抖店数据字段表来定


def create_table():
    conn = pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           password='6468467495',
                           db='mysql',
                           charset='utf8mb4')
    cursor = conn.cursor()
    sql = '''
    CREATE TABLE IF NOT EXISTS `tiktok_shop_info`(
    '''
