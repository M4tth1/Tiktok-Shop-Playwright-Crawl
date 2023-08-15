#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/6/13 00:58
# @Author  : fuganchen
# @Site    : 
# @File    : app.py
# @Project : flask_webhook
# @Software: PyCharm
import json
from flask import Flask, request
from store_verify_code import insert_verify_code, insert_sms_content
from start_script import run_script, terminate_process
app = Flask(__name__)


@app.route('/', methods=['GET'])
def hello_world():
    return 'Hello Wor!'


@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()
    text = data.get('text')
    phone = data.get('phone')
    if '【抖店】' in text:
        verify_code = text.split('验证码')[1].split('，')[0]
        print(verify_code)
        phone_number = text.split('\n')[2].split('_')[2]
        print(phone_number)
        insert_verify_code(verify_code, phone_number)
    insert_sms_content(text, phone)
    return 'success'


@app.route('/start_spider', methods=['POST'])
def start_spider():
    data = request.get_json()
    phone = data.get('phone')
    is_alive = run_script(phone)
    if is_alive == 1:
        response_data = {'msg': 'success', 'err': 0, 'status': '启动成功'}
    elif is_alive == 2:
        response_data = {'msg': 'success', 'err': 0, 'status': '已启动，无需重复启动'}
    else:
        response_data = {'msg': 'success', 'err': 0, 'status': '启动失败'}
    response = json.dumps(response_data, ensure_ascii=False)
    return response, 200, {'Content-Type': 'application/json; charset=utf-8'}

@app.route('/stop_spider', methods=['POST'])
def stop_spider():
    success_flag = {'msg': 'success', 'err': 0, 'status': '停止成功'}
    data = request.get_json()
    phone = data.get('phone')
    success_flag = terminate_process(phone)
    if success_flag == 1:
        response_data = {'msg': 'success', 'err': 0, 'status': '停止成功'}
    elif success_flag == 2:
        response_data = {'msg': 'success', 'err': 0, 'status': '未启动，无需停止'}
    response = json.dumps(response_data, ensure_ascii=False)
    return response, 200, {'Content-Type': 'application/json; charset=utf-8'}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8888)
