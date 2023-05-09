#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/5/10 02:27
# @Author  : fuganchen
# @Site    : 
# @File    : get_ini_config.py
# @Project : tiktok_crawl
# @Software: PyCharm
# 从ini文件中读取配置信息
import configparser
import os

from utils import log_helper

logger = log_helper.get_logger("config_helper")


def get_config(section, key):
    """
    读取配置文件
    :param section: 配置文件中的section
    :param key: 配置文件中的key
    :return: 配置文件中的value
    """
    config = configparser.ConfigParser()
    # 获取当前文件的父目录绝对路径
    path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    # 获取配置文件的路径
    file_path = path + '/config/config.ini'
    config.read(file_path, encoding='utf-8')
    try:
        res = config.get(section, key)
    except Exception as e:
        logger.error(e)
        res = None
    return res


# 修改配置信息
def set_config(section, key, value):
    """
    修改配置文件
    :param section: 配置文件中的section
    :param key: 配置文件中的key
    :param value: 配置文件中的value
    :return: None
    """
    config = configparser.ConfigParser()
    # 获取当前文件的父目录绝对路径
    path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    # 获取配置文件的路径
    file_path = path + '/config/config.ini'
    config.read(file_path, encoding='utf-8')
    try:
        config.set(section, key, value)
        config.write(open(file_path, 'w'))
    except Exception as e:
        logger.error(e)


# ini配置格式
# [mysql]
# host = localhost
# port = 3306
# user = root
# password = 6468467495
# db = mysql
# charset = utf8
#
# [redis]
# host = localhost
# port = 6379