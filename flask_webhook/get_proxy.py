#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/7/3 02:24
# @Author  : fuganchen
# @Site    : 
# @File    : get_proxy.py
# @Project : tiktok_crawl
# @Software: PyCharm
import requests


def get_proxy():
    return requests.get("http://127.0.0.1:5010/get/").json()


def delete_proxy(proxy):
    requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy))

# your spider code


def getHtml():
    # ....
    retry_count = 5
    proxy = get_proxy().get("proxy")
    print(proxy)
    while retry_count > 0:
        try:
            html = requests.get('http://www.baidu.com', proxies={"http": "http://{}".format(proxy)})
            # 使用代理访问
            return html
        except Exception:
            retry_count -= 1
    # 删除代理池中代理
    delete_proxy(proxy)
    return None


def test_proxy():
    retry_count = 5
    while True:
        proxy_info = get_proxy()
        print(proxy_info)
        proxy_addr = proxy_info.get('region')
        if '中国' in proxy_addr:
            proxy = proxy_info.get("proxy")
            break
    for i in range(10):
        while retry_count > 0:
            try:
                # html = requests.get('https://fxg.jinritemai.com/ffa/grs/qualification/shopinfo', proxies={"http": "http://{}".format(proxy)})
                # 使用代理访问
                return proxy
            except Exception:
                retry_count -= 1
    return None


if __name__ == '__main__':
    # print(test_proxy())
    print(get_proxy())