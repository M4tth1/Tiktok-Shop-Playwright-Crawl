#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/5/6 23:00
# @Author  : fuganchen
# @Site    : 
# @File    : init_database.py
# @Project : tiktok_crawl
# @Software: PyCharm
import pymysql
# 创建抖店店铺信息数据库表,具体表的字段根据抖店数据字段表来定,具体字段名和类型如下，唯一键为shopName和shopId：
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
# total	体验分总分	number
# service	服务体验分	number
# goods	商品体验分	number
# logistics	物流体验分	number
# scoreTime	打分时间	string(date-time)
# total	体验分总分比较	number
# service	服务体验分比较	number
# cxOpeScore	持续经营分	number
# goods	商品体验分比较	number
# logistics	物流体验分比较	number
# updateTime	更新时间	string(date-time)
# szDisputeRate	纠纷商责率	number
# period	数据周期	string
# amount	用户成交金额	number
# avgAmt	用户客单价	number
# refundUsers	用户退款人数	integer(int32)
# userDealCounts	用户成交人数	integer(int32)
# visit	用户访店人数	integer(int32)
# userCounts	用户总数	integer(int32)
# refundAmt	用户退款金额	number

# 创建抖店店铺订单信息数据表,具体表的字段根据抖店数据字段表来定,具体字段名和类型如下：
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
# amount	支付金额	number
# orderNo	订单编号	string
# quantity	数量	integer(int32)
# specification	商品规格	string
# orderStatus	订单状态	string
# updateTime	数据更新时间	string(date-time)
# afterSalesStatus	售后状态	string
# tags	商品标签	string
# price	单价	number
# name	商品名称	string
# paymentMethod	支付方式	string
# category	商品品类	string
# orderDate	下单时间	string(date-time)

# 还有一个接收验证码用的表，具体字段名和类型如下：
# phone	手机号	string
# code	验证码	string
# send_time	发送时间	string(date-time)

def create_table():
    conn = pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           password='6468467495',
                           db='mysql',
                           charset='utf8mb4')
    cursor = conn.cursor()
    shop_info_sql = '''
    CREATE TABLE IF NOT EXISTS `tiktok_shop_info`(
    `id` INT UNSIGNED AUTO_INCREMENT,
    `tiktokBindStatus` VARCHAR(255) NOT NULL,
    `firmApproveStatus` VARCHAR(255) NOT NULL,
    `tiktokBindDate` VARCHAR(255) NOT NULL,
    `shopName` VARCHAR(255) NOT NULL,
    `opeAdminIdNo` VARCHAR(255) NOT NULL,
    `opeAdminIdDeadline` VARCHAR(255) NOT NULL,
    `shopStatus` VARCHAR(255) NOT NULL,
    `seriousIllegalPoints` INT NOT NULL,
    `firmName` VARCHAR(255) NOT NULL,
    `uscCode` VARCHAR(255) NOT NULL,
    `shopId` VARCHAR(255) NOT NULL,
    `shopType` VARCHAR(255) NOT NULL,
    `opeAdminIdType` VARCHAR(255) NOT NULL,
    `opeAdminName` VARCHAR(255) NOT NULL,
    `usualIllegalPoints` INT NOT NULL,
    `opeAddress` VARCHAR(255) NOT NULL,
    `total` INT NOT NULL,
    `service` INT NOT NULL,
    `goods` INT NOT NULL,
    `logistics` INT NOT NULL,
    `scoreTime` VARCHAR(255) NOT NULL,
    `totalCompare` INT NOT NULL,
    `serviceCompare` INT NOT NULL,
    `cxOpeScore` INT NOT NULL,
    `goodsCompare` INT NOT NULL,
    `logisticsCompare` INT NOT NULL,
    `updateTime` DATETIME NOT NULL,
    `szDisputeRate` INT NOT NULL,
    `period` VARCHAR(255) NOT NULL,
    `amount` INT NOT NULL,
    `avgAmt` INT NOT NULL,
    `refundUsers` INT NOT NULL,
    `userDealCounts` INT NOT NULL,
    `visit` INT NOT NULL,
    `userCounts` INT NOT NULL,
    `refundAmt` INT NOT NULL,
    PRIMARY KEY (shopName, shopId)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''
    order_info_sql = '''
    CREATE TABLE IF NOT EXISTS `tiktok_order_info`(
    `id` INT UNSIGNED AUTO_INCREMENT,
    `paymentAmt` INT NOT NULL,
    `preClearingAmt` INT NOT NULL,
    `orderNo` VARCHAR(255) NOT NULL,
    `goodsId` VARCHAR(255) NOT NULL,
    `ordersSubNo` VARCHAR(255) NOT NULL,
    `orderStatus` VARCHAR(255) NOT NULL,
    `refundStatus` VARCHAR(255) NOT NULL,
    `finishDate` VARCHAR(255) NOT NULL,
    `updateTime` VARCHAR(255) NOT NULL,
    `preClearingDate` VARCHAR(255) NOT NULL,
    `orderDate` VARCHAR(255) NOT NULL,
    `amount` INT NOT NULL,
    `quantity` INT NOT NULL,
    `specification` VARCHAR(255) NOT NULL,
    `afterSalesStatus` VARCHAR(255) NOT NULL,
    `tags` VARCHAR(255) NOT NULL,
    `price` INT NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `paymentMethod` VARCHAR(255) NOT NULL,
    `category` VARCHAR(255) NOT NULL,
    PRIMARY KEY (`orderNo`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''
    phone_code_sql = '''
    CREATE TABLE IF NOT EXISTS `tiktok_phone_code`(
    `id` INT UNSIGNED AUTO_INCREMENT,
    `phone` VARCHAR(255) NOT NULL,
    `code` VARCHAR(255) NOT NULL,
    `send_time` VARCHAR(255) NOT NULL,
    PRIMARY KEY (`phone`)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''
    cursor.execute(shop_info_sql)
    cursor.execute(order_info_sql)
    cursor.execute(phone_code_sql)
    conn.commit()
    cursor.close()
    conn.close()

def drop_table():
    conn = pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           password='6468467495',
                           db='mysql',
                           charset='utf8mb4')
    cursor = conn.cursor()
    shop_info_sql = '''
    DROP TABLE IF EXISTS `tiktok_shop_info`;
    '''
    order_info_sql = '''
    DROP TABLE IF EXISTS `tiktok_order_info`;
    '''
    phone_code_sql = '''
    DROP TABLE IF EXISTS `tiktok_phone_code`;
    '''
    cursor.execute(shop_info_sql)
    cursor.execute(order_info_sql)
    cursor.execute(phone_code_sql)
    conn.commit()
    cursor.close()
    conn.close()

# insert ignore into users(user_id,user_name) values("111","naruto"),("222","sasuke") on duplicate key update user_name=values(user_name);根据此sql写出上述三个sql的插入语句

def insert_shop_info(params):
    conn = pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           password='6468467495',
                           db='mysql',
                           charset='utf8mb4')
    cursor = conn.cursor()
    shop_info_sql = '''
    INSERT IGNORE INTO `tiktok_shop_info`(`tiktokBindStatus`, `firmApproveStatus`, `tiktokBindDate`, `shopName`, `opeAdminIdNo`, `opeAdminIdDeadline`, `shopStatus`, `seriousIllegalPoints`, `firmName`, `uscCode`, `shopId`, `shopType`, `opeAdminIdType`, `opeAdminName`, `usualIllegalPoints`, `opeAddress`, `total`, `service`, `goods`, `logistics`, `scoreTime`, `totalCompare`, `serviceCompare`, `cxOpeScore`, `goodsCompare`, `logisticsCompare`, `updateTime`, `szDisputeRate`, `period`, `amount`, `avgAmt`, `refundUsers`, `userDealCounts`, `visit`, `userCounts`, `refundAmt`) VALUES
    ('%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', '%s', %d, '%s', %d, %d, %d, %d, '%s', %d, %d, %d, %d, %d, '%s', %d, '%s', %d, %d, %d, %d, %d, %d, %d) on duplicate key update `tiktokBindStatus`='%s', `firmApproveStatus`='%s', `tiktokBindDate`='%s', `opeAdminIdNo`='%s', `opeAdminIdDeadline`='%s', `shopStatus`='%s', `seriousIllegalPoints`=%d, `firmName`='%s', `uscCode`='%s', `shopType`='%s', `opeAdminIdType`='%s', `opeAdminName`='%s', `usualIllegalPoints`=%d, `opeAddress`='%s', `total`=%d, `service`=%d, `goods`=%d, `logistics`=%d, `scoreTime`='%s', `totalCompare`=%d, `serviceCompare`=%d, `cxOpeScore`=%d, `goodsCompare`=%d, `logisticsCompare`=%d, `updateTime`='%s', `szDisputeRate`=%d, `period`='%s', `amount`=%d, `avgAmt`=%d, `refundUsers`=%d, `userDealCounts`=%d, `visit`=%d, `userCounts`=%d, `refundAmt`=%d;
    '''
    cursor.execute(shop_info_sql, params)
    conn.commit()
    cursor.close()
    conn.close()

def insert_order_info(params):
    conn = pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           password='6468467495',
                           db='mysql',
                           charset='utf8mb4')
    cursor = conn.cursor()
    order_info_sql = '''
    INSERT IGNORE INTO `tiktok_order_info`(`paymentAmt`, `preClearingAmt`, `orderNo`, `goodsId`, `ordersSubNo`, `orderStatus`, `refundStatus`, `finishDate`, `updateTime`, `preClearingDate`, `orderDate`, `amount`, `quantity`, `specification`, `afterSalesStatus`, `tags`, `price`, `name`, `paymentMethod`, `category`) VALUES
    (%d, %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s', '%s', '%s', %d, '%s', '%s', '%s', '%s') on duplicate key update `paymentAmt`=%d, `preClearingAmt`=%d, `goodsId`='%s', `ordersSubNo`='%s', `orderStatus`='%s', `refundStatus`='%s', `finishDate`='%s', `updateTime`='%s', `preClearingDate`='%s', `orderDate`='%s', `amount`=%d, `quantity`=%d, `specification`='%s', `afterSalesStatus`='%s', `tags`='%s', `price`=%d, `name`='%s', `paymentMethod`='%s', `category`='%s';
    '''
    cursor.execute(order_info_sql, params)
    conn.commit()
    cursor.close()
    conn.close()


def insert_phone_code(params):
    conn = pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           password='6468467495',
                           db='mysql',
                           charset='utf8mb4')
    cursor = conn.cursor()
    phone_code_sql = '''
    INSERT IGNORE INTO `tiktok_phone_code`(`phone`, `code`, `send_time`) VALUES
    ('%s', '%s', '%s') on duplicate key update `code`='%s', `send_time`='%s';
    '''
    cursor.execute(phone_code_sql, params)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    create_table()
