#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/5/5 21:58
# @Author  : fuganchen
# @Site    : 
# @File    : sql_insert_helper.py
# @Project : tiktok_crawl
# @Software: PyCharm
import asyncio
import configparser
import pymysql
import aiomysql
import os
from dbutils.pooled_db import PooledDB
from utils.get_ini_config import get_config
# python连接mysql的一些操作，需要用异步aiomysql实现


# 创建mysql连接池，后面的函数连接都使用连接池
async def create_pool():
    host = get_config("database_config", "host")
    port = int(get_config("database_config", "port"))
    user = get_config("database_config", "user")
    password = get_config("database_config", "password")
    db = get_config("database_config", "database")
    minsize = int(get_config("database_config", "minsize"))
    maxsize = int(get_config("database_config", "maxsize"))
    pool = await aiomysql.create_pool(host=host, port=port, user=user, password=password, db=db, minsize=minsize, maxsize=maxsize)
    return pool


#关闭连接池
async def close_pool(pool):
    pool.close()
    await pool.wait_closed()


# 下面根据数据库建表的sql语句来完成插入数据的函数，数据使用参数化。遇到unique时，使用更新操作，遇到外键时，使用查询操作，遇到其他时，使用插入操作
# 插入店铺基本信息，建表sql如下
# sql = '''
#     CREATE TABLE IF NOT EXISTS tkShopBasicInfoDto (
#     tiktokBindStatus VARCHAR(255),
#     firmApproveStatus VARCHAR(255),
#     tiktokBindDate VARCHAR(255),
#     shopName VARCHAR(255),
#     opeAdminIdNo VARCHAR(255),
#     opeAdminIdDeadline VARCHAR(255),
#     shopStatus VARCHAR(255),
#     seriousIllegalPoints VARCHAR(255),
#     firmName VARCHAR(255),
#     uscCode VARCHAR(255),
#     shopId VARCHAR(255),
#     shopType VARCHAR(255),
#     opeAdminIdType VARCHAR(255),
#     opeAdminName VARCHAR(255),
#     usualIllegalPoints VARCHAR(255),
#     opeAddress VARCHAR(255),
#     PRIMARY KEY (shopId),
#     UNIQUE (shopName)
#     )ENGINE=InnoDB DEFAULT CHARSET=utf8;
#     '''
async def insert_shop_basic_info(pool, data):
    sql = '''
        INSERT INTO tkShopBasicInfoDto (
        tiktokBindStatus,
        firmApproveStatus,
        tiktokBindDate,
        shopName,
        opeAdminIdNo,
        opeAdminIdDeadline,
        shopStatus,
        seriousIllegalPoints,
        firmName,
        uscCode,
        shopId,
        shopType,
        opeAdminIdType,
        opeAdminName,
        usualIllegalPoints,
        opeAddress,
        phone
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        tiktokBindStatus = VALUES(tiktokBindStatus),
        firmApproveStatus = VALUES(firmApproveStatus),
        tiktokBindDate = VALUES(tiktokBindDate),
        opeAdminIdNo = VALUES(opeAdminIdNo),
        opeAdminIdDeadline = VALUES(opeAdminIdDeadline),
        shopStatus = VALUES(shopStatus),
        seriousIllegalPoints = VALUES(seriousIllegalPoints),
        firmName = VALUES(firmName),
        uscCode = VALUES(uscCode),
        shopType = VALUES(shopType),
        opeAdminIdType = VALUES(opeAdminIdType),
        opeAdminName = VALUES(opeAdminName),
        usualIllegalPoints = VALUES(usualIllegalPoints),
        opeAddress = VALUES(opeAddress),
        phone = VALUES(phone)
        '''
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(sql, data)
            await conn.commit()


# 插入店铺体验分sql
# sql = '''
#     CREATE TABLE IF NOT EXISTS kScoreInfo (
#     total VARCHAR(255),
#     service VARCHAR(255),
#     goods VARCHAR(255),
#     logistics VARCHAR(255),
#     scoreTime VARCHAR(255),
#     shopId VARCHAR(255),
#     PRIMARY KEY (shopId),
#     FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
#     )ENGINE=InnoDB DEFAULT CHARSET=utf8;
#     '''
async def insert_shop_score_info(pool, data):
    sql = '''
        INSERT INTO kScoreInfo (
        total,
        service,
        goods,
        logistics,
        scoreTime,
        shopId,
        phone
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        total = VALUES(total),
        service = VALUES(service),
        goods = VALUES(goods),
        logistics = VALUES(logistics),
        scoreTime = VALUES(scoreTime),
        phone = VALUES(phone)
        '''
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(sql, data)
            await conn.commit()

# 插入同行体验分sql
# sql = '''
#     CREATE TABLE IF NOT EXISTS tkCounterpartsRank (
#     total VARCHAR(255),
#     service VARCHAR(255),
#     cxOpeScore VARCHAR(255),
#     goods VARCHAR(255),
#     logistics VARCHAR(255),
#     updateTime VARCHAR(255),
#     szDisputeRate VARCHAR(255),
#     shopId VARCHAR(255),
#     PRIMARY KEY (shopId),
#     FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
#     )ENGINE=InnoDB DEFAULT CHARSET=utf8;
#     '''
async def insert_shop_counterparts_rank(pool, data):
    sql = '''
        INSERT INTO tkCounterpartsRank (
        total,
        service,
        cxOpeScore,
        goods,
        logistics,
        updateTime,
        szDisputeRate,
        shopId,
        phone
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        total = VALUES(total),
        service = VALUES(service),
        cxOpeScore = VALUES(cxOpeScore),
        goods = VALUES(goods),
        logistics = VALUES(logistics),
        updateTime = VALUES(updateTime),
        szDisputeRate = VALUES(szDisputeRate),
        phone = VALUES(phone)
        '''
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(sql, data)
            await conn.commit()

# 插入用户资产sql
# sql = '''
#     CREATE TABLE IF NOT EXISTS userAssets (
#     period VARCHAR(255),
#     amount VARCHAR(255),
#     avgAmt VARCHAR(255),
#     refundUsers VARCHAR(255),
#     userDealCounts VARCHAR(255),
#     visit VARCHAR(255),
#     userCounts VARCHAR(255),
#     refundAmt VARCHAR(255),
#     shopId VARCHAR(255),
#     PRIMARY KEY (shopId),
#     FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
#     )ENGINE=InnoDB DEFAULT CHARSET=utf8;
#     '''
async def insert_user_assets(pool, data):
    sql = '''
        INSERT INTO userAssets (
        period,
        amount,
        avgAmt,
        refundUsers,
        userDealCounts,
        visit,
        userCounts,
        refundAmt,
        shopId,
        phone
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        period = VALUES(period),
        amount = VALUES(amount),
        avgAmt = VALUES(avgAmt),
        refundUsers = VALUES(refundUsers),
        userDealCounts = VALUES(userDealCounts),
        visit = VALUES(visit),
        userCounts = VALUES(userCounts),
        refundAmt = VALUES(refundAmt),
        phone = VALUES(phone)
        '''
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(sql, data)
            await conn.commit()


# 插入店铺月账单sql
# sql = '''
#     CREATE TABLE IF NOT EXISTS tkShopMonthlyBillInfos (
#     totalIncome VARCHAR(255),
#     counts VARCHAR(255),
#     endBalance VARCHAR(255),
#     totalExpenses VARCHAR(255),
#     balanceChange VARCHAR(255),
#     updateTime VARCHAR(255),
#     beginBalance VARCHAR(255),
#     shopId VARCHAR(255),
#     PRIMARY KEY (shopId),
#     FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
#     )ENGINE=InnoDB DEFAULT CHARSET=utf8;
#     '''
async def insert_shop_month_bill(pool, data):
    sql = '''
        INSERT INTO tkShopMonthlyBillInfos (
        totalIncome,
        counts,
        endBalance,
        totalExpenses,
        balanceChange,
        updateTime,
        beginBalance,
        shopId,
        phone
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        totalIncome = VALUES(totalIncome),
        counts = VALUES(counts),
        endBalance = VALUES(endBalance),
        totalExpenses = VALUES(totalExpenses),
        balanceChange = VALUES(balanceChange),
        updateTime = VALUES(updateTime),
        beginBalance = VALUES(beginBalance),
        phone = VALUES(phone)
        '''
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(sql, data)
            await conn.commit()


# 插入店铺日账单sql
# sql = '''
#     CREATE TABLE IF NOT EXISTS tkShopDailyBillInfos (
#     totalIncome VARCHAR(255),
#     counts VARCHAR(255),
#     endBalance VARCHAR(255),
#     totalExpenses VARCHAR(255),
#     balanceChange VARCHAR(255),
#     updateTime VARCHAR(255),
#     beginBalance VARCHAR(255),
#     shopId VARCHAR(255),
#     PRIMARY KEY (shopId),
#     FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
#     )ENGINE=InnoDB DEFAULT CHARSET=utf8;
#     '''
async def insert_shop_daily_bill(pool, data):
    sql = '''
        INSERT INTO tkShopDailyBillInfos (
        totalIncome,
        counts,
        endBalance,
        totalExpenses,
        balanceChange,
        updateTime,
        beginBalance,
        shopId,
        phone
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        totalIncome = VALUES(totalIncome),
        counts = VALUES(counts),
        endBalance = VALUES(endBalance),
        totalExpenses = VALUES(totalExpenses),
        balanceChange = VALUES(balanceChange),
        updateTime = VALUES(updateTime),
        beginBalance = VALUES(beginBalance),
        phone = VALUES(phone)
        '''
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(sql, data)
            await conn.commit()


# 插入待结算订单sql
# sql = '''
#     CREATE TABLE IF NOT EXISTS tkShopNoClearingInfos (
#     paymentAmt DECIMAL(10, 2),
#     preClearingAmt DECIMAL(10, 2),
#     orderNo VARCHAR(255),
#     goodsId VARCHAR(255),
#     ordersSubNo VARCHAR(255),
#     orderStatus VARCHAR(255),
#     refundStatus VARCHAR(255),
#     finishDate VARCHAR(255),
#     updateTime VARCHAR(255),
#     preClearingDate VARCHAR(255),
#     orderDate VARCHAR(255),
#     shopId VARCHAR(255),
#     PRIMARY KEY (orderNo),
#     FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
#     )ENGINE=InnoDB DEFAULT CHARSET=utf8;
#     '''
async def insert_shop_no_clearing(pool, data):
    sql = '''
        INSERT INTO tkShopNoClearingInfos (
        paymentAmt,
        preClearingAmt,
        orderNo,
        goodsId,
        ordersSubNo,
        orderStatus,
        refundStatus,
        finishDate,
        updateTime,
        preClearingDate,
        orderDate,
        shopId,
        phone
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        paymentAmt = VALUES(paymentAmt),
        preClearingAmt = VALUES(preClearingAmt),
        goodsId = VALUES(goodsId),
        ordersSubNo = VALUES(ordersSubNo),
        orderStatus = VALUES(orderStatus),
        refundStatus = VALUES(refundStatus),
        finishDate = VALUES(finishDate),
        updateTime = VALUES(updateTime),
        preClearingDate = VALUES(preClearingDate),
        orderDate = VALUES(orderDate),
        phone = VALUES(phone)
        '''
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(sql, data)
            await conn.commit()


# 插入结算订单sql，根据下面建表写插入
# sql = '''
#     CREATE TABLE IF NOT EXISTS tkShopClearingInfos (
#     amount DECIMAL(10, 2),
#     orderNo VARCHAR(255),
#     orderStatus VARCHAR(255),
#     updateTime VARCHAR(255),
#     afterSalesStatus VARCHAR(255),
#     paymentMethod VARCHAR(255),
#     orderDate VARCHAR(255),
#     shopId VARCHAR(255),
#     PRIMARY KEY (orderNo),
#     FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
#     )ENGINE=InnoDB DEFAULT CHARSET=utf8;
#     '''
async def insert_shop_clearing(pool, data):
    sql = '''
        INSERT INTO tkShopClearingInfos (
        amount,
        orderNo,
        orderStatus,
        updateTime,
        paymentMethod,
        orderDate,
        shopId,
        phone
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        amount = VALUES(amount),
        orderStatus = VALUES(orderStatus),
        updateTime = VALUES(updateTime),
        paymentMethod = VALUES(paymentMethod),
        orderDate = VALUES(orderDate),
        phone = VALUES(phone)
        '''
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(sql, data)
            await conn.commit()


# 插入订单详情sql，根据下面建表写插入
# sql = '''
#     CREATE TABLE IF NOT EXISTS tkOrderDetailInfos (
#     orderNo VARCHAR(255),
#     quantity INTEGER,
#     specification VARCHAR(255),
#     updateTime VARCHAR(255),
#     tags VARCHAR(255),
#     price DECIMAL(10, 2),
#     name VARCHAR(255),
#     category VARCHAR(255),
#     shopId VARCHAR(255),
#     PRIMARY KEY (orderNo),
#     FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
#     )ENGINE=InnoDB DEFAULT CHARSET=utf8;
#     '''
async def insert_order_detail(pool, data):
    sql = '''
        INSERT INTO tkOrderDetailInfos (
        orderNo,
        quantity,
        specification,
        updateTime,
        tags,
        price,
        afterSalesStatus,
        name,
        category,
        shopId,
        phone
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        quantity = VALUES(quantity),
        specification = VALUES(specification),
        updateTime = VALUES(updateTime),
        tags = VALUES(tags),
        price = VALUES(price),
        afterSalesStatus = VALUES(afterSalesStatus),
        name = VALUES(name),
        category = VALUES(category),
        phone = VALUES(phone)
        '''
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(sql, data)
            await conn.commit()


async def insert_error_infos(pool, data):
    sql = '''
        INSERT INTO tkErrorInfos (
        phone,
        errorInfo,
        updateTime
        ) VALUES (%s, %s, %s)
        '''
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(sql, data)
            await conn.commit()



# 测试插入的函数
async def test_insert():
    pool = await create_pool()
    test_data = [29.00, 27.55, '6918452303422559827', '3600144569297152275', '6918452303422559827', '已发货', '', '',
         '2023-05-14 17:48:57', '买家/系统确认收货4天后可结算货款', '2023/05/14 15:27:04', '4018100']
    await insert_shop_no_clearing(pool, test_data)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(create_pool())
    loop.close()
    # asyncio.run(create_pool())