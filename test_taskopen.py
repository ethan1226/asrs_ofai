#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 16:55:35 2021

@author: huangboyu
"""
from utils.scheduling_utils_db import *
from OFAI_Celery_func import workstation_open ,order_pick,container_operate,arms_store,arms_pick,workstation_get,workstation_workend,order_check,arms_work_transmit
r = redis.Redis(host='localhost', port=6379, decode_responses=False)
all_keys_b = r.keys()
for key in all_keys_b:
    r.delete(key)
redis_init()
redis_arm_product_update()
workstation_id = "ws_1"
index_label = "date"
index = "20200701"
num = 20
workstation_open.delay(workstation_id,index_label,index,num)