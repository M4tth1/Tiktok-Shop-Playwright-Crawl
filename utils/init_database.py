#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/5/6 23:00
# @Author  : fuganchen
# @Site    : 
# @File    : init_database.py
# @Project : tiktok_crawl
# @Software: PyCharm
import pymysql
from dbutils.pooled_db import PooledDB
from utils.get_ini_config import get_config
# 创建mysql连接池，后面的函数连接都使用连接池
host = get_config("database_config", "host")
port = int(get_config("database_config", "port"))
user = get_config("database_config", "user")
password = get_config("database_config", "password")
db = get_config("database_config", "database")
minsize = int(get_config("database_config", "minsize"))
maxsize = int(get_config("database_config", "maxsize"))
Pool = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=6,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=5,  # 链接池中最多闲置的链接，0和None不限制
    maxshared=3,  # 链接池中最多共享的链接数量，0和None表示全部共享。
    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
    # setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
    # ping=0,  # ping MySQL服务端，检查是否服务可用。
    host=host,
    port=port,
    user=user,
    password=password,
    database=db,
)


# 查数据库所有表名
def show_tables():
    conn = Pool.connection()
    cursor = conn.cursor()
    cursor.execute('show tables')
    tables = cursor.fetchall()
    print(tables)
    cursor.close()
    conn.close()

# 创建抖店店铺信息数据库表,具体表的字段根据抖店数据字段表来定,具体字段名和类型如下，唯一键为shopName和shopId,其他数据库的外键为shopId,记得创建时把字段名改为下划线连接：
# 数据库名：tkShopBasicInfoDto 店铺基本信息
# tiktokBindStatus	抖音账号绑定状态	string
# firmApproveStatus	企业号类型	string
# tiktokBindDate	抖音账号绑定时间	string(date-time)
# shopName	店铺名称	string
# opeAdminIdNo	经营者证件号码	string
# opeAdminIdDeadline	经营者证件截至日期	string
# shopStatus	店铺状态	string
# seriousIllegalPoints	严重违规积分	number
# firmName	公司名称	string
# uscCode	统一社会信用代码	string
# shopId	店铺Id	string
# shopType	店铺类型	string
# opeAdminIdType	经营者证件类型	string
# opeAdminName	经营者姓名	string
# usualIllegalPoints	一般违规积分	number
# opeAddress	经营地址	string

# 数据库名：kScoreInfo	店铺体验分
# total	体验分总分	number
# service	服务体验分	number
# goods	商品体验分	number
# logistics	物流体验分	number
# scoreTime	打分时间	string(date-time)

# 数据库名：tkCounterpartsRank	同行体验分比较
# total	体验分总分比较	number
# service	服务体验分比较	number
# cxOpeScore	持续经营分	number
# goods	商品体验分比较	number
# logistics	物流体验分比较	number
# updateTime	更新时间	string(date-time)
# szDisputeRate	纠纷商责率	number

# 数据库名：userAssets	用户资产
# period	数据周期	string
# amount	用户成交金额	number
# avgAmt	用户客单价	number
# refundUsers	用户退款人数	integer(int32)
# userDealCounts	用户成交人数	integer(int32)
# visit	用户访店人数	integer(int32)
# userCounts	用户总数	integer(int32)
# refundAmt	用户退款金额	number

# 数据库名： tkShopMonthlyBillInfos	店铺月账单
# totalIncome	总收入	number
# counts	明细笔数	integer(int32)
# endBalance	期末余额	number
# totalExpenses	总支出	number
# balanceChange	余额变化	number
# updateTime	更新时间	string
# beginBalance	期初余额	number

# 数据库名： tkShopDailyBillInfos	店铺日账单
# totalIncome	总收入	number
# counts	明细笔数	integer(int32)
# endBalance	期末余额	number
# totalExpenses	总支出	number
# balanceChange	余额变化	number
# updateTime	更新时间	string(date-time)
# beginBalance	期初余额	number

# 还有一个接收验证码用的表，具体字段名和类型如下：
# phone	手机号	string
# code	验证码	string
# send_time	发送时间	string(date-time)
# 接下来根据上述注释创建数据库


# 创建抖店店铺信息数据库表,shopName和shopId为唯一键
def create_tk_shop_basic_info():
    conn = Pool.connection()
    cursor = conn.cursor()
    sql = '''
    CREATE TABLE IF NOT EXISTS tkShopBasicInfoDto (
    tiktokBindStatus VARCHAR(255),
    firmApproveStatus VARCHAR(255),
    tiktokBindDate VARCHAR(255),
    shopName VARCHAR(255),
    opeAdminIdNo VARCHAR(255),
    opeAdminIdDeadline VARCHAR(255),
    shopStatus VARCHAR(255),
    seriousIllegalPoints VARCHAR(255),
    firmName VARCHAR(255),
    uscCode VARCHAR(255),
    shopId VARCHAR(255),
    shopType VARCHAR(255),
    opeAdminIdType VARCHAR(255),
    opeAdminName VARCHAR(255),
    usualIllegalPoints VARCHAR(255),
    opeAddress VARCHAR(255),
    PRIMARY KEY (shopId),
    UNIQUE (shopName)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '''
    cursor.execute(sql)
    cursor.close()
    conn.close()


def create_k_score_info():
    conn = Pool.connection()
    cursor = conn.cursor()
    # 外键为tkShopBasicInfoDto中的shopId
    sql = '''
    CREATE TABLE IF NOT EXISTS kScoreInfo (
    total VARCHAR(255),
    service VARCHAR(255),
    goods VARCHAR(255),
    logistics VARCHAR(255),
    scoreTime VARCHAR(255),
    shopId VARCHAR(255),
    PRIMARY KEY (shopId),
    FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '''
    cursor.execute(sql)
    cursor.close()
    conn.close()


def create_tk_counterparts_rank():
    conn = Pool.connection()
    cursor = conn.cursor()
    # 外键为tkShopBasicInfoDto中的shopId
    sql = '''
    CREATE TABLE IF NOT EXISTS tkCounterpartsRank (
    total VARCHAR(255),
    service VARCHAR(255),
    cxOpeScore VARCHAR(255),
    goods VARCHAR(255),
    logistics VARCHAR(255),
    updateTime VARCHAR(255),
    szDisputeRate VARCHAR(255),
    shopId VARCHAR(255),
    PRIMARY KEY (shopId),
    FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '''
    cursor.execute(sql)
    cursor.close()
    conn.close()


def create_user_assets():
    conn = Pool.connection()
    cursor = conn.cursor()
    # 外键为tkShopBasicInfoDto中的shopId
    sql = '''
    CREATE TABLE IF NOT EXISTS userAssets (
    period VARCHAR(255),
    amount DECIMAL(10, 2),
    avgAmt DECIMAL(10, 2),
    refundUsers INTEGER,
    userDealCounts INTEGER,
    visit INTEGER,
    userCounts INTEGER,
    refundAmt DECIMAL(10, 2),
    shopId VARCHAR(255),
    PRIMARY KEY (shopId),
    FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '''
    cursor.execute(sql)
    cursor.close()
    conn.close()


def create_tk_shop_monthly_bill_infos():
    conn = Pool.connection()
    cursor = conn.cursor()
    # 外键为tkShopBasicInfoDto中的shopId
    sql = '''
    CREATE TABLE IF NOT EXISTS tkShopMonthlyBillInfos (
    totalIncome DECIMAL(10, 2),
    counts VARCHAR(255),
    endBalance DECIMAL(10, 2),
    totalExpenses DECIMAL(10, 2),
    balanceChange DECIMAL(10, 2),
    updateTime VARCHAR(255),
    beginBalance DECIMAL(10, 2),
    shopId VARCHAR(255),
    FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '''
    cursor.execute(sql)
    cursor.close()
    conn.close()


def create_tk_shop_daily_bill_infos():
    conn = Pool.connection()
    cursor = conn.cursor()
    # 外键为tkShopBasicInfoDto中的shopId
    sql = '''
    CREATE TABLE IF NOT EXISTS tkShopDailyBillInfos (
    totalIncome DECIMAL(10, 2),
    counts VARCHAR(255),
    endBalance DECIMAL(10, 2),
    totalExpenses DECIMAL(10, 2),
    balanceChange DECIMAL(10, 2),
    updateTime VARCHAR(255),
    beginBalance DECIMAL(10, 2),
    shopId VARCHAR(255),
    FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '''
    cursor.execute(sql)
    cursor.close()
    conn.close()


# 创建抖店店铺订单信息数据表,具体表的字段根据抖店数据字段表来定,具体字段名和类型如下：
# 数据库名：tkShopNoClearingInfos	待结算订单
# paymentAmt	支付金额	number
# preClearingAmt	预结算金额	number
# orderNo	订单编号	string
# goodsId	商品名称	string
# ordersSubNo	子订单编号	string
# orderStatus	订单状态	string
# refundStatus	退款状态	string
# finishDate	完成时间	string(date-time)
# updateTime	更新时间	string(date-time)
# preClearingDate	预结算时间	string(date-time)
# orderDate	下单时间	string(date-time)
def create_tk_shop_no_clearing_infos():
    conn = Pool.connection()
    cursor = conn.cursor()
    # 外键为tkShopBasicInfoDto中的shopId
    sql = '''
    CREATE TABLE IF NOT EXISTS tkShopNoClearingInfos (
    paymentAmt DECIMAL(10, 2),
    preClearingAmt DECIMAL(10, 2),
    orderNo VARCHAR(255),
    goodsId VARCHAR(255),
    ordersSubNo VARCHAR(255),
    orderStatus VARCHAR(255),
    refundStatus VARCHAR(255),
    finishDate VARCHAR(255),
    updateTime VARCHAR(255),
    preClearingDate VARCHAR(255),
    orderDate VARCHAR(255),
    shopId VARCHAR(255),
    PRIMARY KEY (orderNo),
    FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '''
    cursor.execute(sql)
    cursor.close()
    conn.close()


# 数据库名：tkOrderInfos	订单信息
# amount	支付金额	number
# orderNo	订单编号	string
# orderStatus	订单状态	string
# updateTime	数据更新时间	string(date-time)
# afterSalesStatus	售后状态	string
# paymentMethod	支付方式	string
# orderDate	下单时间	string(date-time)
def create_tk_shop_clearing_infos():
    conn = Pool.connection()
    cursor = conn.cursor()
    # 外键为tkShopBasicInfoDto中的shopId
    sql = '''
    CREATE TABLE IF NOT EXISTS tkShopClearingInfos (
    amount DECIMAL(10, 2),
    orderNo VARCHAR(255),
    orderStatus VARCHAR(255),
    updateTime VARCHAR(255),
    paymentMethod VARCHAR(255),
    orderDate VARCHAR(255),
    shopId VARCHAR(255),
    PRIMARY KEY (orderNo),
    FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '''
    cursor.execute(sql)
    cursor.close()
    conn.close()


# 数据库名：tkOrderDetailInfos	订单商品信息
# orderNo	订单编号	string
# quantity	数量	integer(int32)
# specification	商品规格	string
# updateTime	数据更新时间	string(date-time)
# tags	商品标签	string
# price	单价	number
# name	商品名称	string
# category	商品品类	string
def create_tk_order_detail_infos():
    conn = Pool.connection()
    cursor = conn.cursor()
    # 外键为tkShopBasicInfoDto中的shopId
    sql = '''
    CREATE TABLE IF NOT EXISTS tkOrderDetailInfos (
    orderNo VARCHAR(255),
    quantity INTEGER,
    specification VARCHAR(255),
    updateTime VARCHAR(255),
    tags VARCHAR(255),
    price DECIMAL(10, 2),
    afterSalesStatus VARCHAR(255),
    name VARCHAR(255),
    category VARCHAR(255),
    shopId VARCHAR(255),
    FOREIGN KEY (shopId) REFERENCES tkShopBasicInfoDto(shopId)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '''
    cursor.execute(sql)
    cursor.close()
    conn.close()


def create_tk_verify_code_infos():
    conn = Pool.connection()
    cursor = conn.cursor()
    # 限制phone为主键，且phone唯一
    sql = '''
    CREATE TABLE IF NOT EXISTS tkVerifyCodeInfos (
    phone VARCHAR(255),
    verifyCode VARCHAR(255),
    updateTime VARCHAR(255),
    PRIMARY KEY (phone)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '''
    cursor.execute(sql)
    cursor.close()
    conn.close()



# 删除上述创建的所有表
def drop_table():
    conn = Pool.connection()
    cursor = conn.cursor()
    sql = '''
    DROP TABLE IF EXISTS tkShopBasicInfoDto;
    DROP TABLE IF EXISTS kScoreInfo;
    DROP TABLE IF EXISTS tkCounterpartsRank;
    DROP TABLE IF EXISTS userAssets;
    DROP TABLE IF EXISTS tkShopMonthlyBillInfos;
    DROP TABLE IF EXISTS tkShopDailyBillInfos;
    DROP TABLE IF EXISTS tkShopNoClearingInfos;
    DROP TABLE IF EXISTS tkShopClearingInfos;
    '''
    sql = '''
    DROP TABLE IF EXISTS tkOrderDetailInfos;
    '''
    cursor.execute(sql)
    cursor.close()
    conn.close()


def describe_tables():
    conn = Pool.connection()
    cursor = conn.cursor()
    sql = '''
    describe tkOrderDetailInfos;
    '''
    cursor.execute(sql)
    result = cursor.fetchall()
    print(result)
    cursor.close()
    conn.close()


def out_put_tables():
    conn = Pool.connection()
    cursor = conn.cursor()
    sql = '''
    select * from tkOrderDetailInfos;
    '''
    cursor.execute(sql)
    result = cursor.fetchall()
    print(result)
    cursor.close()
    conn.close()


if __name__ == '__main__':
    show_tables()
    # drop_table()
    # create_table()
    # create_tk_shop_basic_info()
    # create_k_score_info()
    # create_tk_counterparts_rank()
    # create_user_assets()
    # create_tk_shop_monthly_bill_infos()
    # create_tk_shop_daily_bill_infos()
    # create_tk_shop_no_clearing_infos()
    # create_tk_shop_clearing_infos()
    # create_tk_order_detail_infos()
    create_tk_verify_code_infos()
    show_tables()
    describe_tables()


