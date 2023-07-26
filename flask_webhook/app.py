#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/6/13 00:58
# @Author  : fuganchen
# @Site    : 
# @File    : app.py
# @Project : flask_webhook
# @Software: PyCharm
from flask import Flask, request
from store_verify_code import insert_verify_code, insert_sms_content
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8888)
