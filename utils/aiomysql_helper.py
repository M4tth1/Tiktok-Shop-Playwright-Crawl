#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/5/10 01:59
# @Author  : fuganchen
# @Site    : 
# @File    : aiomysql_helper.py
# @Project : tiktok_crawl
# @Software: PyCharm
import datetime
import os.path

import aiomysql
import asyncio
import pymysql
from utils.get_ini_config import get_config
from utils.sql_insert_helper import create_pool, close_pool
from constant import PROJECT_DIR

async def get_conn():
    """
    :return: 连接，游标
    """
    # 创建连接
    host = get_config("database_config", "host")
    port = int(get_config("database_config", "port"))
    user = get_config("database_config", "user")
    password = get_config("database_config", "password")
    db = get_config("database_config", "database")
    conn = await aiomysql.connect(host=host,
                                  port=port,
                                  user=user,
                                  password=password,
                                  db=db)
    # 创建游标
    cursor = await conn.cursor()
    return conn, cursor


async def close_conn(conn, cursor):
    """
    关闭连接
    :param conn:
    :param cursor:
    :return:
    """
    if cursor:
        await cursor.close()
    if conn:
        await conn.close()


async def exec_query(pool, sql, *args):
    """
    封装通用查询
    :param sql:
    :param args:
    :return: 返回结果
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(sql, args)
            await conn.commit()
            res = await cursor.fetchall()
            # 将查询结果转换为字典列表
            desc = cursor.description
            column = [col[0] for col in desc]
            res = [dict(zip(column, row)) for row in res]
    return res


async def exec_query_atom(pool, sql, *args):
    """
    查询返回一条结果
    :param sql:
    :param args:
    :return:
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(sql, args)
            await conn.commit()
            res = await cursor.fetchone()
            return res[0]


async def execute(sql, *args):
    """
    封装通用增删改
    :param sql:
    :param args:
    :return: 返回受影响的行数
    """
    conn, cursor = await get_conn()
    await cursor.execute(sql, args)
    await conn.commit()
    await close_conn(conn, cursor)


async def insert(sql, *args):
    """
    封装通用增删改
    :param sql:
    :param args:
    :return: 返回受影响的行数
    """
    conn, cursor = await get_conn()
    await cursor.execute(sql, args)
    await conn.commit()
    await close_conn(conn, cursor)


async def insert_many(sql, args):
    """
    封装通用增删改
    :param sql:
    :param args:
    :return: 返回受影响的行数
    """
    conn, cursor = await get_conn()
    await cursor.executemany(sql, args)
    await conn.commit()
    await close_conn(conn, cursor)


async def test_query():
    time_now = datetime.datetime.now()
    pool = await create_pool()
    while True:
        await asyncio.sleep(5)
        sql = 'select * from tkVerifyCodeInfos where phone = %s '
        verify_code_info = await exec_query(pool, sql, '16737229683')
        verify_code = verify_code_info[0].get('verifyCode')
        print(verify_code)
        update_time = verify_code_info[0].get('updateTime')
        update_time = datetime.datetime.strptime(str(update_time), '%Y-%m-%d %H:%M:%S')
        # 判断update_time是否超过2分钟
        if (update_time - time_now).seconds > 120:
            print('验证码仍未接收到')
        else:
            break
    await close_pool(pool)


if __name__ == '__main__':
    # asyncio.run(test_query())
    pass
