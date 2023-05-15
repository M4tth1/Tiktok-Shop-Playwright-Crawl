#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/5/10 01:59
# @Author  : fuganchen
# @Site    : 
# @File    : aiomysql_helper.py
# @Project : tiktok_crawl
# @Software: PyCharm
import aiomysql
import asyncio
import pymysql

# async def get_conn():
#     """
#     :return: 连接，游标
#     """
#     # 创建连接
#     conn = await aiomysql.connect(host=config_helper.get_config("mysql", "host"),
#                                   port=config_helper.get_config("mysql", "port"),
#                                   user=config_helper.get_config("mysql", "user"),
#                                   password=config_helper.get_config("mysql", "password"),
#                                   db=config_helper.get_config("mysql", "db"),
#                                   charset=config_helper.get_config("mysql", "charset"))
#     # 创建游标
#     cursor = await conn.cursor()
#     return conn, cursor


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


async def query(sql, *args):
    """
    封装通用查询
    :param sql:
    :param args:
    :return: 返回结果
    """
    conn, cursor = await get_conn()
    await cursor.execute(sql, args)
    res = await cursor.fetchall()
    await close_conn(conn, cursor)
    return res


async def get_one(sql, *args):
    """
    查询返回一条结果
    :param sql:
    :param args:
    :return:
    """
    conn, cursor = await get_conn()
    await cursor.execute(sql, args)
    res = await cursor.fetchone()
    await close_conn(conn, cursor)
    return res


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

# 接下来采用连接池的方式来操作数据库
# 1.创建连接池
async def create_pool():
    """
    创建连接池
    :return:
    """
    pool = await aiomysql.create_pool(host=config_helper.get_config("mysql", "host"),
                                      port=config_helper.get_config("mysql", "port"),
                                      user=config_helper.get_config("mysql", "user"),
                                      password=config_helper.get_config("mysql", "password"),
                                      db=config_helper.get_config("mysql", "db"),
                                      charset=config_helper.get_config("mysql", "charset"),
                                      autocommit=config_helper.get_config("mysql", "autocommit"))
    return pool


async def select(sql, args, size=None):
    """
    封装select语句
    :param sql:
    :param args:
    :param size:
    :return:
    """
    logger.info("SQL: %s" % sql)
    logger.info("ARGS: %s" % str(args))
    pool = await create_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                res = await cur.fetchmany(size)
            else:
                res = await cur.fetchall()
    logger.info("RESULT: %s" % str(res))
    return res


async def execute_pool(sql, args):
    """
    封装通用增删改
    :param sql:
    :param args:
    :return: 返回受影响的行数
    """
    logger.info("SQL: %s" % sql)
    logger.info("ARGS: %s" % str(args))
    pool = await create_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            affected = cur.rowcount
    logger.info("AFFECTED: %s" % affected)
    return affected


async def insert_pool(sql, args):
    """
    封装通用增删改
    :param sql:
    :param args:
    :return: 返回受影响的行数
    """
    logger.info("SQL: %s" % sql)
    logger.info("ARGS: %s" % str(args))
    pool = await create_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            affected = cur.rowcount
    logger.info("AFFECTED: %s" % affected)
    return affected


async def insert_many_pool(sql, args):
    """
    封装通用增删改
    :param sql:
    :param args:
    :return: 返回受影响的行数
    """
    logger.info("SQL: %s" % sql)
    logger.info("ARGS: %s" % str(args))
    pool = await create_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.executemany(sql.replace('?', '%s'), args or ())
            affected = cur.rowcount
    logger.info("AFFECTED: %s" % affected)
    return affected


async def select_one(sql, args):
    """
    查询返回一条结果
    :param sql:
    :param args:
    :return:
    """
    res = await select(sql, args, 1)
    if len(res) == 0:
        return None
    return res[0]


async def select_int(sql, args):
    """
    查询返回一个数值
    :param sql:
    :param args:
    :return:
    """
    res = await select_one(sql, args)
    if len(res) == 0:
        return None
    return res[0]


async def select_list(sql, args):
    """
    查询返回一个列表
    :param sql:
    :param args:
    :return:
    """
    res = await select(sql, args)
    return res

if __name__ == '__main__':
    for i in range(0, 2):
        print(i)


