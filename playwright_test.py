#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/5/1 13:31
# @Author  : fuganchen
# @Site    : 
# @File    : playwright_test.py
# @Project : tiktok_crawl
# @Software: PyCharm
import datetime
import os.path
import re
import time
import traceback

import aiohttp
import asyncio
import hashlib
from playwright.async_api import async_playwright
from decimal import Decimal
from opencv_test import get_notch_location, get_track, get_slide_track,get_track_old
from utils.sql_insert_helper import insert_shop_basic_info, insert_shop_score_info, insert_user_assets, insert_shop_counterparts_rank, insert_shop_month_bill, insert_shop_daily_bill, insert_shop_no_clearing, insert_shop_clearing, create_pool, close_pool, insert_order_detail


async def download_image(url, _type):
    if _type == 'bg':
        file_suffix = 'jpeg'
    elif _type == 'hx':
        file_suffix = 'png'
    # file_name = str(time.time()).replace('.', '')
    file_name = hashlib.md5(url.encode('utf-8')).hexdigest()
    file_path = os.path.join('temp', file_name + '.' + file_suffix)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.read()
            with open(file_path, "wb") as f:
                f.write(data)
    return file_path


async def handle_login():
    """
    处理登录
    :return:
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto("https://fxg.jinritemai.com/login")
        await page.wait_for_selector('span.account-center-code-text')
        await page.locator("rect").nth(1).click()
        # await page.get_by_text("邮箱登录", exact=True).click()
        # await page.get_by_placeholder("请输入邮箱").click()
        # await page.get_by_placeholder("请输入邮箱").fill("1208879081@qq.com")
        # await page.get_by_placeholder("密码").click()
        # await page.get_by_placeholder("密码").fill("JIAOkai123.")
        # await page.get_by_role("button", name="登录").click()
        await page.get_by_placeholder("请输入手机号码").click()
        await page.get_by_placeholder("请输入手机号码").fill("15238084961")
        await page.get_by_text("发送验证码").click()
        # todo 等待获取验证码
        for i in range(10):
            try:
                await page.wait_for_selector(selector='img#captcha-verify-image.sc-gqjmRU.cHbGdz.sc-ifAKCX.itlNmx')
                bg_xpath = await page.query_selector('img#captcha-verify-image.sc-gqjmRU.cHbGdz.sc-ifAKCX.itlNmx')
                hx_xpath = await page.query_selector('img.captcha_verify_img_slide.react-draggable.sc-VigVT.ggNWOG')
                bg_src = await bg_xpath.get_attribute('src')
                hx_src = await hx_xpath.get_attribute('src')
                bg_path = await download_image(bg_src, 'bg')  # playwright下载bg_src和hx_src图片
                hx_path = await download_image(hx_src, 'hx')
                # playwright下载bg_src和hx_src图片
                box = await hx_xpath.bounding_box()
                bg_box = await bg_xpath.bounding_box()
                bg_length = bg_box["width"]
                # todo 加一个失败重试机制
                await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                await page.mouse.down()
                x = box["x"] + box["width"] / 2
                y = box["y"] + box["height"] / 2
                tracks = get_track_old(hx_path, bg_path, bg_length)
                # tracks = get_slide_track(hx_path, bg_path, bg_length)
                for track in tracks:
                    # 循环鼠标按照轨迹移动
                    # strps 是控制单次移动速度的比例是1/10 默认是1 相当于 传入的这个距离不管多远0.1秒钟移动完 越大越慢
                    await page.mouse.move(x + track, 0, steps=10)
                    x += track
                # 移动结束鼠标抬起
                await page.wait_for_timeout(500)
                await page.mouse.up()
                await page.wait_for_selector('div.style_businessItem__2ty6K.style_todoItemPromotion__1Uv5C', timeout=3000)
                break
            except Exception as e:
                await page.wait_for_timeout(1000)
                print(traceback.format_exc())
                print(e)
        # todo 接收验证码
        await page.wait_for_timeout(30000)
        storage = await context.storage_state(path=r'G:\workspace\tiktok_crawl\storage\test.json')
        # await page.locator('text="下一步"').click()
        # await page.locator('text="我知道了"').click()
        await context.close()
        await browser.close()


async def handle_shop_info_crawl():
    # 爬取店铺名
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(storage_state=r'G:\workspace\tiktok_crawl\storage\test.json')
        page = await context.new_page()
        page1 = await context.new_page()
        await page.goto('https://fxg.jinritemai.com/ffa/grs/qualification/shopinfo')
        await page.wait_for_selector('div._3duiZdUahHyw8H7MW-2zvW')
        shop_info_list = await page.query_selector_all('div.ant-row._3aJWiCG93IlaU_3qf7a1Uc')
        storage_shop_info_list = list()
        for item in shop_info_list:
            shop_item = await item.query_selector('div.ant-col._2oUK91ircK4AKJrKQ8fyHD')
            shop_item_title = await item.query_selector('div.ant-col._7Qygb3VmS0-7XcXX446U4')
            shop_item_title_text = await shop_item_title.text_content()
            shop_name = await shop_item.text_content()
            # print(shop_item_title_text, shop_name)
            storage_shop_info_list.append(shop_name)
        shopper_info_list = await page.query_selector_all('div.ant-row.index__row--D9_4Q')
        storage_shopper_info_list = list()
        for item in shopper_info_list:
            shopper_item = await item.query_selector('div.ant-col.index__value--24QvX')
            shopper_item_title = await item.query_selector('div.ant-col.index__label--3cO2r')
            shopper_item_title_text = await shopper_item_title.text_content()
            shopper_name = await shopper_item.text_content()
            # print(shopper_item_title_text, shopper_name)
            storage_shopper_info_list.append(shopper_name)
        # print('店铺名:', shop_name)
        await page1.goto('https://fxg.jinritemai.com/ffa/grs/health-center')
        await page1.wait_for_selector('div._2hOhdOZVPf0Qbj42CHxJKp')
        shop_health_list = await page1.query_selector_all('div._2hOhdOZVPf0Qbj42CHxJKp')
        storage_shop_health_list = list()
        for item in shop_health_list:
            shop_health_item = await item.text_content()
            # print(shop_health_item)
            storage_shop_health_list.append(shop_health_item.split('当前违规积分：')[1])
        await context.close()
        await browser.close()
        print(storage_shop_info_list)
        print(storage_shopper_info_list)
        print(storage_shop_health_list)
        # todo excel中还有界面中未找到的数据，爬取的数据，需要补充
        params = [None, None, None, storage_shop_info_list[2], None, None, None, storage_shop_health_list[0], None, None, storage_shop_info_list[0], storage_shop_info_list[1], None, storage_shopper_info_list[0], storage_shop_health_list[1], None]
        pool = await create_pool()
        await insert_shop_basic_info(pool, params)
        await close_pool(pool)


async def handle_shop_basic_info_crawl():
    # 爬取店铺其他基本信息，未完成
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(storage_state=r'G:\workspace\tiktok_crawl\storage/test.json')
        page = await context.new_page()
        await page.goto('https://fxg.jinritemai.com/ffa/mshop/homepage/index')
        await page.wait_for_selector('div.style_businessItem__2ty6K.style_todoItemPromotion__1Uv5C')
        shop_basic_info_list = await page.query_selector_all('div.ant-row._3aJWiCG93IlaU_3qf7a1Uc')
        for item in shop_basic_info_list:
            shop_basic_item = await item.query_selector('div.ant-col._2oUK91ircK4AKJrKQ8fyHD')
            shop_basic_item_title = await item.query_selector('div.ant-col._7Qygb3VmS0-7XcXX446U4')
            shop_basic_item_title_text = await shop_basic_item_title.text_content()
            shop_basic_name = await shop_basic_item.text_content()
            print(shop_basic_item_title_text, shop_basic_name)
        shop_basicper_info_list = await page.query_selector_all('div.ant-row.index__row--D9_4Q')
        for item in shop_basicper_info_list:
            shop_basicper_item = await item.query_selector('div.ant-col.index__value--24QvX')
            shop_basicper_item_title = await item.query_selector('div.ant-col.index__label--3cO2r')
            shop_basicper_item_title_text = await shop_basicper_item_title.text_content()
            shop_basicper_name = await shop_basicper_item.text_content()
            print(shop_basicper_item_title_text, shop_basicper_name)
        # print('店铺名:', shop_basic_name)
        await context.close()
        await browser.close()
        
        
async def handle_score_info_crawl(shop_id):
    # 爬取服务体验分
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(storage_state=r'G:\workspace\tiktok_crawl\storage/test.json')
        page = await context.new_page()
        await page.goto('https://fxg.jinritemai.com/ffa/eco/experience-score')
        await page.wait_for_selector('div.v6_wFQaGBIqvvE-')
        service_score_total = await (await page.query_selector('div.v6_wFQaGBIqvvE-')).text_content()
        # print('服务分总分:', service_score_total)
        score_info_list = await page.query_selector_all('div.v6_zVSomkApGwc-')
        storage_score_info_list = list()
        storage_counter_score = list()
        for item in score_info_list:
            score_item = await item.query_selector('div.v6_EgCX-3RMYjE-')
            score_item_title = await item.query_selector('div.v6_F-mNPOIVxzc-')
            score_item_title_text = await score_item_title.text_content()
            score_name = await score_item.text_content()
            # print(score_item_title_text, score_name)
            storage_score_info_list.append(score_name)
            compare_list = await item.query_selector_all('div.v6_7fsnInXemPI-')
            for i in compare_list:
                compare_text = await i.text_content()
                if compare_text.startswith('超越'):
                    # print(compare_text)
                    storage_counter_score.append(compare_text)
        score_time_list = await page.query_selector_all('div.v6_LHvV1R66e-c-')
        score_time = None
        for item in score_time_list:
            score_time_temp = await item.text_content()
            if score_time_temp.startswith('更新时间'):
                score_time = score_time_temp.split('更新时间: ')[1]
        score_detail_list = await page.query_selector_all('div.v6_Lln7s-CXL3o-')
        dispute_rate = '%0.00'
        for item in score_detail_list:
            score_detail_title = await (await item.query_selector('div.v6_G3qU9-gKqH8-')).text_content()
            score_detail = await (await item.query_selector('div.v6_xNdRJF3ONww-')).text_content()
            # print(score_detail_title, score_detail)
            if score_detail_title.startswith('纠纷伤责率'):
                dispute_rate = score_detail
        # print('店铺名:', score_name)
        await context.close()
        await browser.close()
        print(storage_counter_score)
        score_params = [service_score_total.split('较昨日')[0], storage_score_info_list[2][0:4], storage_score_info_list[0][0:4], storage_score_info_list[1][0:4], score_time, shop_id]
        print(('超越' + service_score_total.split('超越')[1]))
        print(storage_counter_score[2])
        print(storage_counter_score[0])
        print(storage_counter_score[1])
        print(score_time)
        print(dispute_rate)
        print(shop_id)
        counter_params = [('超越' + service_score_total.split('超越')[1]), storage_counter_score[2], None, storage_counter_score[0], storage_counter_score[1], score_time, dispute_rate, shop_id]
        pool = await create_pool()
        await insert_shop_score_info(pool, score_params)
        await insert_shop_counterparts_rank(pool, counter_params)
        await close_pool(pool)

async def shop_user_assets(shop_id):
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=False)
        context = await browser.new_context(storage_state=r'G:\workspace\tiktok_crawl\storage/test.json')
        page = await context.new_page()
        await page.goto('https://fxg.jinritemai.com/ffa/mshop/homepage/index')
        await page.get_by_role("button", name="知道了").click()
        async with page.expect_popup() as page1_info:
            await page.get_by_text("查看更多数据").click()
        page1 = await page1_info.value
        await page1.goto('https://compass.jinritemai.com/shop/business-part')
        await page1.wait_for_selector('div.cardWrapper--FiTGw.enableCheck--u536d.active--fYy_5.active_card.card_item')
        shop_user_assets_seleted_list = await page1.query_selector_all('div.cardWrapper--FiTGw.enableCheck--u536d.active--fYy_5.active_card.card_item')
        storage_shop_user_assets_list = list()
        for item in shop_user_assets_seleted_list:
            shop_user_assets_selected_name = await item.text_content()
            # print(shop_user_assets_selected_name)
            # print(1)
            # storage_shop_user_assets_list.append(shop_user_assets_selected_name)
        shop_user_assets_unseleted_list = await page1.query_selector_all('div.cardWrapper--FiTGw.enableCheck--u536d.card_item')
        for item in shop_user_assets_unseleted_list:
            shop_user_assets_unselected_name = await item.text_content()
            # print(shop_user_assets_unselected_name)
            # print(2)
            storage_shop_user_assets_list.append(shop_user_assets_unselected_name)
        await page1.get_by_text("全店退款分析").click()
        await page1.wait_for_selector('div.cardWrapper--OrRDC.enableCheck--OmTIW.active--uvPee.active_card.card_item.secondary--tJohQ')
        shop_user_unfund_seleted_list = await page1.query_selector_all('div.cardWrapper--OrRDC.enableCheck--OmTIW.active--uvPee.active_card.card_item.secondary--tJohQ')
        for item in shop_user_unfund_seleted_list:
            shop_user_unfund_selected_name = await item.text_content()
            # print(shop_user_unfund_selected_name)
            # print(3)
            # storage_shop_user_assets_list.append(shop_user_unfund_selected_name)
        shop_user_unfund_unseleted_list = await page1.query_selector_all('div.cardWrapper--OrRDC.enableCheck--OmTIW.card_item.secondary--tJohQ')
        for item in shop_user_unfund_unseleted_list:
            shop_user_unfund_unselected_name = await item.text_content()
            # print(shop_user_unfund_unselected_name)
            # print(4)
            storage_shop_user_assets_list.append(shop_user_unfund_unselected_name)
        await context.close()
        await browser.close()
        print(storage_shop_user_assets_list)
        for item in storage_shop_user_assets_list:
            if item.startswith('成交金额'):
                amount = item.split('成交金额¥')[1].split('较上周期')[0].replace(',', '')
                print(amount)
                amount = Decimal(amount).quantize(Decimal('0.00'))
            elif item.startswith('退款金额'):
                refund_amount = item.split('退款金额¥')[1].split('较上周期')[0].replace(',', '')
                print(refund_amount)
                refund_amount = Decimal(refund_amount).quantize(Decimal('0.00'))
            elif item.startswith('成交客单价'):
                average_amount = item.split('成交客单价¥')[1].split('较上周期')[0].replace(',', '')
                print(average_amount)
                average_amount = Decimal(average_amount).quantize(Decimal('0.00'))
            elif item.startswith('退款人数'):
                refund_user_num = item.split('退款人数')[1].split('较上周期')[0]
                if '万' in refund_user_num:
                    num = float(refund_user_num[:-1]) * 10000  # 将字符串转为浮点数并乘以10000
                    refund_user_num = int(num)  # 再将浮点数转为整数并转为字符串
            elif item.startswith('成交人数'):
                user_num = item.split('成交人数')[1].split('较上周期')[0]
                if '万' in user_num:
                    num = float(user_num[:-1]) * 10000  # 将字符串转为浮点数并乘以10000
                    user_num = int(num)  # 再将浮点数转为整数并转为字符串
            elif item.startswith('商品访客数'):
                visitor_num = item.split('商品访客数')[1].split('较上周期')[0]
                if '万' in visitor_num:
                    num = float(visitor_num[:-1]) * 10000  # 将字符串转为浮点数并乘以10000
                    visitor_num = int(num)  # 再将浮点数转为整数并转为字符串
            elif item.startswith('商品曝光次数'):
                exposure_num = item.split('商品曝光次数')[1].split('较上周期')[0]
                if '万' in exposure_num:
                    num = float(exposure_num[:-1]) * 10000  # 将字符串转为浮点数并乘以10000
                    exposure_num = int(num)  # 再将浮点数转为整数并转为字符串
        period = '30'
        params = [period, amount, average_amount, refund_user_num, user_num, visitor_num, exposure_num, refund_amount, shop_id]
        print(params)
        pool = await create_pool()
        await insert_user_assets(pool, params)
        await close_pool(pool)


async def shop_orders_info_crawl(shop_id):
    # 订单详情
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(storage_state=r'G:\workspace\tiktok_crawl\storage\test.json')
        page = await context.new_page()
        await page.goto('https://fxg.jinritemai.com/ffa/morder/order/list')
        await page.get_by_role("button", name="right").click()
        await page.wait_for_timeout(5000)
        await page.wait_for_selector('tr.auxo-table-row.auxo-table-row-level-0.row-vertical-top.index_table-row__ULgxX')
        params_list = list()
        # 标签栏
        head_list = list()
        pre_head_info_list = await page.query_selector_all('span.auxo-pair-head-wrapper')
        for pre_head_info in pre_head_info_list:
            order_no_list = await pre_head_info.query_selector_all('span.index_text__3y09K')
            temp_list = list()
            for order_no in order_no_list:
                order_no = await order_no.text_content()
                order_no = order_no.replace('\xa0', ' ')
                order_no = order_no.replace('/', '-')
                order_no = order_no.split(' ')[1]
                temp_list.append(order_no)
            head_list.append([temp_list[0], temp_list[1]])
        tr_temp_list = list()
        tr_test_list = await page.query_selector_all('tr')
        order_index_list = list()
        for tr_test in tr_test_list:
            tr_test_text = await tr_test.text_content()
            tr_temp_list.append(tr_test_text)
            # 订单详情栏
        tab_name = ['tr.auxo-table-row.auxo-table-row-level-1.auxo-pair-group-row-last.row-vertical-top.index_table-row__ULgxX', 'tr.auxo-table-row.auxo-table-row-level-1.row-vertical-top.index_table-row__ULgxX']
        tab_name = ['tr.auxo-table-row.auxo-table-row-level-1.row-vertical-top.index_table-row__ULgxX']
        payment_method_list = list()
        amount_list = list()
        order_status_list = list()
        after_sales_status_list = list()
        tags_list = list()
        name_list = list()
        specification_list = list()
        quantity_list = list()
        price_list = list()
        for sub_tab in tab_name:
            shop_order_info_list = await page.query_selector_all(sub_tab)
            # print(len(shop_order_info_list))
            for shop_order_info in shop_order_info_list:
                box_text_temp_list = list()
                order_payment_info_list = await shop_order_info.query_selector_all('td.auxo-table-cell')
                sub_box_num = 0
                for box_item in order_payment_info_list:
                    sub_box_num += 1
                    if sub_box_num == 2:
                        # todo 点商品
                        await box_item.click()
                    try:
                        goods_info_list = await box_item.query_selector_all('div.index_ellipsis__29MP5.undefined')
                        tags = await box_item.query_selector('div.style_tags__1hgJ9')
                        if tags:
                            tags_text = await tags.text_content()
                            tags_list.append(tags_text)
                        goods_text_list = list()
                        for goods_info in goods_info_list:
                            goods_info_text = await goods_info.text_content()
                            goods_text_list.append(goods_info_text)
                        if len(goods_text_list) >= 3:
                            name = goods_text_list[0]
                            specification = goods_text_list[1].split('x')[0]
                            quantity = goods_text_list[1].split('x')[1]
                            name_list.append(name)
                            quantity_list.append(int(quantity))
                            specification_list.append(specification)
                    except Exception as e:
                        print(e)
                    box_text = await box_item.text_content()
                    box_text_temp_list.append(box_text)
                    if '¥' in box_text and 'x' not in box_text:
                        # 支付方式
                        # 总金额
                        payment_method = box_text.split('¥')[0]
                        amount = Decimal(box_text.split('¥')[1]).quantize(Decimal('0.00'))
                        payment_method_list.append(payment_method)
                        amount_list.append(amount)
                if len(box_text_temp_list) >= 3:
                    price = Decimal(box_text_temp_list[2].split('¥')[1].split('x')[0]).quantize(Decimal('0.00'))
                    price_list.append(price)
                    after_sales_status = box_text_temp_list[3].replace('-', '')
                    after_sales_status_list.append(after_sales_status)
                if len(box_text_temp_list) > 7:
                    order_status_list.append(box_text_temp_list[-2])
        for index, value in enumerate(tr_temp_list):
            if '订单编号' in value:
                order_index_list.append(index)
        box_quantity_all_list = list()
        for index, value in enumerate(order_index_list):
            box_quantity_all = 0
            if index + 1 < len(order_index_list):
                for i in range(order_index_list[index]+1, order_index_list[index+1]):
                    box_text = tr_temp_list[i]
                    try:
                        sub_box_quantity_all = int(box_text.split('x')[1].split('商家编码')[0])
                        box_quantity_all += sub_box_quantity_all
                    except Exception as e:
                        box_quantity_all += 0
                box_quantity_all_list.append(box_quantity_all)
            else:
                for i in range(order_index_list[index]+1, len(tr_temp_list)):
                    box_text = tr_temp_list[i]
                    try:
                        sub_box_quantity_all = int(box_text.split('x')[1].split('商家编码')[0])
                        box_quantity_all += sub_box_quantity_all
                    except Exception as e:
                        box_quantity_all += 0
                box_quantity_all_list.append(box_quantity_all)
        # # todo 数据更新时间
        update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        goods_index = 0
        pool = await create_pool()
        for index, num in enumerate(box_quantity_all_list):
            for i in range(goods_index, goods_index + num):
                print(after_sales_status_list[i])
                goods_params = [head_list[index][0], quantity_list[i], specification_list[i], update_time, tags_list[i], price_list[i], after_sales_status_list[i], name_list[i], '', shop_id]
                await insert_order_detail(pool, goods_params)
            goods_index += num
            order_params = [amount_list[index], head_list[index][0], order_status_list[index], update_time, payment_method_list[index], head_list[index][1], shop_id]
            await insert_shop_clearing(pool, order_params)
        # print(len(goods_params_list), goods_params_list)
        # print(len(order_params_list), order_params_list)
        # todo 数据入库
        await close_pool(pool)
        await context.close()
        await browser.close()


async def shop_not_clear_orders_info_crawl(shop_id):
    # 待结算订单详情
    # todo 待结算时间选取
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(storage_state=r'G:\workspace\tiktok_crawl\storage\test.json')
        page = await context.new_page()
        await page.goto('https://fxg.jinritemai.com/ffa/morder/finance/order-list')
        await page.wait_for_selector('tr.auxo-table-row.auxo-table-row-level-0.row-vertical-top')
        not_clear_order_info_list = await page.query_selector_all('tr.auxo-table-row.auxo-table-row-level-0.row-vertical-top')
        print(len(not_clear_order_info_list))
        params_list = list()
        for not_clear_order_info in not_clear_order_info_list:
            tab_cell_list = await not_clear_order_info.query_selector_all('td.auxo-table-cell')
            # order_payment_info_text = await (await not_clear_order_info.query_selector('td.auxo-table-cell')).text_content()
            # order_payment_info_text = await not_clear_order_info.text_content()
            # print(order_payment_info_text)
            # order_no = order_payment_info_text[0:19]
            # order_sub_no = order_payment_info_text[19:39]
            # goods_id = order_payment_info_text[39:59]
            temp_params_list = list()
            for tab_cell in tab_cell_list:
                tab_cell_text = await tab_cell.text_content()
                temp_params_list.append(tab_cell_text)
            update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for item in temp_params_list:
                if item == '-':
                    temp_params_list[temp_params_list.index(item)] = ''
            order_date = ''
            finish_date = ''
            if temp_params_list[5] != '':
                date_str = temp_params_list[5][:10]  # 提取日期部分 '2023/05/12'
                time_str = temp_params_list[5][10:]  # 提取时间部分 '21:58:20'
                order_date = date_str + ' ' + time_str
            if temp_params_list[6] != '':
                date_str = temp_params_list[6][:10]  # 提取日期部分 '2023/05/12'
                time_str = temp_params_list[6][10:]  # 提取时间部分 '21:58:20'
                finish_date = date_str + ' ' + time_str
            if temp_params_list[7] != '':
                payment_amt = Decimal(temp_params_list[7].split('¥')[1]).quantize(Decimal('0.00'))
            if temp_params_list[9] != '':
                pre_clear_amt = Decimal(temp_params_list[9].split('¥')[1]).quantize(Decimal('0.00'))
            params_list.append([payment_amt, pre_clear_amt, temp_params_list[0], temp_params_list[2], temp_params_list[1], temp_params_list[3], temp_params_list[4], finish_date, update_time, temp_params_list[8], order_date, shop_id])
        await context.close()
        await browser.close()
        pool = await create_pool()
        for param in params_list:
            print(param)
            await insert_shop_no_clearing(pool, param)
        await close_pool(pool)


async def shop_daily_bill_crawl(shop_id):
    # 待结算订单详情
    # todo 是否需要翻页
    date_pattern = r'\d{4}-\d{2}-\d{2}'  # 匹配日期
    money_pattern = r'(?<=¥)[\d,]+(\.\d{2})?'  # 匹配金额，使用正向零宽度断言 (?<=¥) 查找¥符号后面的数字
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(storage_state=r'G:\workspace\tiktok_crawl\storage\test.json')
        page = await context.new_page()
        await page.goto('https://fxg.jinritemai.com/ffa/p-new/online-bill')
        await page.wait_for_selector('tr.auxo-table-row.auxo-table-row-level-0')
        daily_bill_info_list = await page.query_selector_all('tr.auxo-table-row.auxo-table-row-level-0')
        print(len(daily_bill_info_list))
        params = list()
        for daily_bill_info in daily_bill_info_list:
            # order_payment_info_text = await (await daily_bill_info.query_selector('td.auxo-table-cell')).text_content()
            order_payment_info_text = await daily_bill_info.text_content()
            order_payment_info_text = order_payment_info_text.replace('生成报表明细', '')
            order_payment_info_text = order_payment_info_text.replace(',', '')
            date = re.findall(date_pattern, order_payment_info_text)[0]
            money_list = order_payment_info_text.split(date)[1].split('¥')
            for index, value in enumerate(money_list):
                if index == 0:
                    continue
                money_list[index] = Decimal(value).quantize(Decimal('0.00'))
            params.append([money_list[1], money_list[0], money_list[5], money_list[2], money_list[3], date, money_list[4], shop_id])
        await page.wait_for_timeout(2000)
        await context.close()
        await browser.close()
        pool = await create_pool()
        for param in params:
            print(param)
            await insert_shop_daily_bill(pool, param)
        await close_pool(pool)


async def shop_monthly_bill_crawl(shop_id):
    # 待结算订单详情
    daily_pattern = r'\d{4}-\d{2}-\d{2}'
    date_pattern = r'\d{4}-\d{2}'  # 匹配日期
    money_pattern = r'(?<=¥)[\d,]+(\.\d{2})?'  # 匹配金额，使用正向零宽度断言 (?<=¥) 查找¥符号后面的数字
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(storage_state=r'G:\workspace\tiktok_crawl\storage\test.json')
        page = await context.new_page()
        await page.goto('https://fxg.jinritemai.com/ffa/p-new/online-bill')
        await page.wait_for_selector('tr.auxo-table-row.auxo-table-row-level-0')
        await page.get_by_role("tab", name="月账单").click()
        # await page.wait_for_selector('div.AKUX6LtcTT0YGWqqnWk3')
        await page.wait_for_timeout(7000)
        monthly_bill_info_list = await page.query_selector_all('tr.auxo-table-row.auxo-table-row-level-0')
        print(len(monthly_bill_info_list))
        params = list()
        for monthly_bill_info in monthly_bill_info_list:
            order_payment_info_text = await monthly_bill_info.text_content()
            if re.match(daily_pattern, order_payment_info_text):
                continue
            order_payment_info_text = order_payment_info_text.replace('生成报表明细', '')
            order_payment_info_text = order_payment_info_text.replace(',', '')
            order_payment_info_text = order_payment_info_text.replace(' ', '')
            date = re.findall(date_pattern, order_payment_info_text)[0]
            money_list = order_payment_info_text.split(date)[1].split('¥')
            print(date, money_list)
            for index, value in enumerate(money_list):
                if index == 0:
                    continue
                print(index, value)
                if value.endswith('-'):
                    value = value.replace('-', '')
                    money_list[index+1] = '-' + money_list[index+1]
                money_list[index] = Decimal(value).quantize(Decimal('0.00'))
            params.append([money_list[1], money_list[0], money_list[5], money_list[2], money_list[3], date, money_list[4],shop_id])
            # order_payment_info_text = await (await monthly_bill_info.query_selector('td.auxo-table-cell')).text_content()
        await context.close()
        await browser.close()
        pool = await create_pool()
        for param in params:
            print(param)
            await insert_shop_month_bill(pool, param)
        await close_pool(pool)


if __name__ == '__main__':
    # asyncio.run(handle_login())
    # asyncio.run(handle_shop_info_crawl())
    # asyncio.run(handle_shop_basic_info_crawl()) 不需要
    # asyncio.run(handle_score_info_crawl())
    # asyncio.run(shop_user_assets())
    # asyncio.run(shop_orders_info_crawl())
    # asyncio.run(shop_not_clear_orders_info_crawl())
    # asyncio.run(shop_daily_bill_crawl())
    # asyncio.run(shop_monthly_bill_crawl())


    loop = asyncio.get_event_loop()
    # loop.run_until_complete(handle_login())
    # loop.run_until_complete(handle_shop_info_crawl())
    # loop.run_until_complete(handle_shop_basic_info_crawl()) 不需要
    # loop.run_until_complete(handle_score_info_crawl('4018100'))
    # loop.run_until_complete(shop_user_assets('4018100'))
    loop.run_until_complete(shop_orders_info_crawl('4018100'))
    # loop.run_until_complete(shop_not_clear_orders_info_crawl('4018100'))
    # loop.run_until_complete(shop_daily_bill_crawl('4018100'))
    # loop.run_until_complete(shop_monthly_bill_crawl('4018100'))
    loop.close()
