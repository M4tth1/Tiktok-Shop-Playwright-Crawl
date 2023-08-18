# Tiktok-Shop-Playwright-Crawl

抖店的playwright爬取实现

## 启动步骤（Windows）

注：文档默认根目录为项目根目录

**1. 启动虚拟环境**

   ``` shell
   # 项目根目录打开cmd
   cd ./venv/Scripts/
   activate
   ```

**2. 修改config.ini文件**

   [![image-20230626112904923.png](https://i.postimg.cc/GtmTFMzZ/image-20230626112904923.png)](https://postimg.cc/bD4v897T)

**3. 启动爬虫**

   ``` shell
   python run_crawl.py
   ```

   在这一步骤中，暂未启动flask webhook接收短信转发并入库。

   测试需要手动在数据库填上验证码：

   - 在各表都建立了的情况下，爬虫会读取tkVerifyCodeInfos表内手机号作为爬取启动
   - 在爬虫完成登录滑块验证阶段，手动填入表内验证码verifyCode和updateTime（最好两分钟内，格式"%Y-%m-%d %H:%M:%S")
   - 完成登录，进行后续爬取

**4. 开启flask webhook服务器用于接收验证码**

因验证码和爬虫在同一内网，暂未使用uwsgi和nginx部署

``` shell
# 需重开一个cmd执行1步骤虚拟环境
cd ./flask_webhook/
# --port端口号视实际情况填写，需要打开对应端口防火墙
flask run --host=0.0.0.0 --port=5000
```

- **接收短信接口：**

URL：http://flask_webhook_ip:port/webhook

请求方式: POST

BODY: {"phone": "xxxxxxxxxxxx", "text": "xxxxxxxxxxxx"}


**5. 爬虫启动接口使用方法**

在第4步启动flask webhook服务器后，提供两个接口启动与停止爬虫

- **启动爬虫接口：**

URL：http://flask_webhook_ip:port/start_spider

请求方式: POST

BODY: {"phone": "xxxxxxxxxxxx"}

注：当BODY为空时启动所有账号的爬虫

- **停止爬虫接口：**

URL：http://flask_webhook_ip:port/stop_spider

请求方式: POST

BODY: {"phone": "xxxxxxxxxxxx"}

注：当BODY为空时停止所有账号的爬虫