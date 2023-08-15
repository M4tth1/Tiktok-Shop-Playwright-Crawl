#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/8/16 00:37
# @Author  : fuganchen
# @Site    : 
# @File    : start_script.py
# @Project : tiktok_crawl
# @Software: PyCharm
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import subprocess
import platform
import signal
import time
import datetime
from constant import PROJECT_DIR
from store_verify_code import query, close_conn, get_conn

def run_script(phone):
    # Run the script in the background and get the process ID
    if phone:
        select_sql = "select isAlive from tkPidInfos where phone = %s"
        conn, cursor = get_conn()
        cursor.execute(select_sql, phone)
        print(cursor.fetchone())
        if cursor.fetchone():
            is_active = cursor.fetchone()[0]
            if is_active == 1:
                is_active = 2
                return is_active
        close_conn(conn, cursor)
        script_args = [phone]
        script_path = os.path.join(PROJECT_DIR, 'script', 'start_spider_script.py')
    else:
        script_args = []
        script_path = os.path.join(PROJECT_DIR, "run_crawl.py")
    if platform.system() == "Windows":
        # On Windows, use start /b to run the script in the background
        command = ["start", "/b", "python", script_path] + script_args
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    else:
        # On Linux, use nohup to detach the script from the terminal
        command = ["nohup", "python", script_path] + script_args + ["&"]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    sql = "insert into tkPidInfos values (%s, %s, %s, %s) on duplicate key update pid = values(pid), isAlive = values(isAlive), updateTime = values(updateTime)"
    conn, cursor = get_conn()
    if process.pid:
        is_active = 1
    else:
        is_active = 0
    try:
        cursor.execute(sql, (phone, process.pid, is_active, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
        is_active = 0
    close_conn(conn, cursor)
    return is_active


def terminate_process(phone):
    success_flag = 0
    sql = 'select pid, isAlive from tkPidInfos where phone = %s'
    conn, cursor = get_conn()
    cursor.execute(sql, phone)
    result = cursor.fetchone()
    print(result)
    process_pid = result[0]
    is_alive = result[1]
    if is_alive == 0:
        success_flag = 2
        return success_flag
    # Terminate a process based on its process ID
    if platform.system() == "Windows":
        # On Windows, use taskkill to terminate the process
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(process_pid)])
    else:
        # On Linux, use the kill command to send a SIGTERM signal
        os.kill(process_pid, signal.SIGTERM)
    # 是否有错误情况出现
    update_sql = "update tkPidInfos set isAlive = 0, updateTime = %s where phone = %s"
    try:
        cursor.execute(update_sql, (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), phone))
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
    close_conn(conn, cursor)
    success_flag = 1
    return success_flag


# Example usage
if __name__ == "__main__":
    phone = '18810362350'  # Replace with your actual script path
    process_pid = run_script(phone)
    print(f"Script started with PID: {process_pid}")

    time.sleep(5)  # Let the script run for a while
    terminate_process(phone)
    print("Script terminated.")
