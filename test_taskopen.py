#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 16:55:35 2021

@author: huangboyu
"""

'''
開啟celery指令
celery -A OFAI_Celery_func worker --loglevel=info
開啟redis指令
redis-server
'''
from utils.scheduling_utils_db import *
import celery 
from OFAI_Celery_func import workstation_open ,order_pick,container_operate,arms_store,arms_pick,workstation_get,workstation_workend,order_check,arms_work_transmit
def Celery_all_tasks_delete():
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    all_keys_b = r.keys()
    del_list = []
    for key in all_keys_b:
        content = key.decode("utf-8") 
        result = content.find("celery-task-meta-")
        if result != -1:
            del_list.append(content[17:])
            #celery.task.control.revoke(content[17:], terminate=True, signal='SIGKILL')
            #print(content[17:])
    celery.task.control.revoke(del_list, terminate=True, signal='SIGKILL')
    print(len(del_list))

r = redis.Redis(host='localhost', port=6379, decode_responses=False)
all_keys_b = r.keys()
for key in all_keys_b:
    r.delete(key)
redis_init()
redis_arm_product_update()
with open('參數檔.txt') as f:
    json_data = json.load(f)
index_label = json_data["index_label"]
index = json_data["index"]
num = json_data["num"]
workstation_id = "ws_1"
# index_label = "date"
# index = "20200701"
# num = 20
workstation_open.delay(workstation_id,index_label,index,num)