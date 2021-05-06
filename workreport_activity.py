#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  4 15:57:25 2021

@author: huangboyu
"""
import cv2
import pandas as pd
import numpy as np
import json
import pymongo
import matplotlib.pyplot as plt

def second2time(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    # print ("%02d:%02d:%02d" % (h, m, s))
    return (h, m, s)
with open('參數檔.txt') as f:
    json_data = json.load(f)
uri = json_data["uri"]
client = pymongo.MongoClient(uri)
db = client['ASRS-Cluster-0']
storage_db = db["Storages"]

file = "workreport_20210504_all_1ws_acc1_allorders.xlsx"
ofai_workreport = pd.read_excel(file , dtype=str,engine='openpyxl')
order_list = list(np.unique(pd.Series(ofai_workreport['訂單ID'])))

order_dict = {}
container_num = len(list(np.unique(pd.Series(ofai_workreport['出庫開始時間']))))
for oi in range(len(ofai_workreport)):
    order_info = ofai_workreport.iloc[oi,:]
    if order_info['訂單ID'] in order_dict:
        start_time = pd.Timestamp(order_info["出庫開始時間"], tz=None)
        end_time = pd.Timestamp(order_info["入庫結束時間"], tz=None)
        order_end_time = pd.Timestamp(order_info["入庫開始時間"], tz=None)
        if (start_time.to_pydatetime() -  order_dict[order_info['訂單ID']]["start_time"].to_pydatetime()).total_seconds() < 0:
            order_dict[order_info['訂單ID']]["start_time"] = start_time
        if (end_time.to_pydatetime() -  order_dict[order_info['訂單ID']]["end_time"].to_pydatetime()).total_seconds() > 0:
            order_dict[order_info['訂單ID']]["end_time"] = end_time
        if (order_end_time.to_pydatetime() -  order_dict[order_info['訂單ID']]["order_end_time"].to_pydatetime()).total_seconds() > 0:
            order_dict[order_info['訂單ID']]["order_end_time"] = order_end_time
        cost_time = (order_dict[order_info['訂單ID']]["order_end_time"].to_pydatetime() - order_dict[order_info['訂單ID']]["start_time"].to_pydatetime()).total_seconds()
        order_dict[order_info['訂單ID']]["cost_time"] = cost_time
        container_id = order_info["箱號"]
        arm_id = storage_db.find_one({"grid_id":int(order_info["出庫開始位置 (grid_id)"])})["arm_id"]
        out_storage_time = pd.Timestamp(order_info["出庫開始時間"], tz=None)
        at_ws_time = pd.Timestamp(order_info["抵達工作站時間"], tz=None)
        out_ws_time = pd.Timestamp(order_info["入庫開始時間"], tz=None)
        order_dict[order_info['訂單ID']]["container"][container_id] = {"arm_id":arm_id,
                                                                      "out_storage_time":out_storage_time,
                                                                      "at_ws_time":at_ws_time,
                                                                      "out_ws_time":out_ws_time}
    else:
        start_time = pd.Timestamp(order_info["出庫開始時間"], tz=None)
        end_time = pd.Timestamp(order_info["入庫結束時間"], tz=None)
        order_end_time = pd.Timestamp(order_info["入庫開始時間"], tz=None)
        cost_time = (order_end_time.to_pydatetime() - start_time.to_pydatetime()).total_seconds()
        order_dict[order_info['訂單ID']] = {"start_time":start_time,
                                           "end_time":end_time,
                                           "order_end_time":order_end_time,
                                           "cost_time":cost_time}
        container_id = order_info["箱號"]
        arm_id = storage_db.find_one({"grid_id":int(order_info["出庫開始位置 (grid_id)"])})["arm_id"]
        out_storage_time = pd.Timestamp(order_info["出庫開始時間"], tz=None)
        at_ws_time = pd.Timestamp(order_info["抵達工作站時間"], tz=None)
        out_ws_time = pd.Timestamp(order_info["入庫開始時間"], tz=None)
        order_dict[order_info['訂單ID']]["container"] = {}
        order_dict[order_info['訂單ID']]["container"][container_id] = {"arm_id":arm_id,
                                                                      "out_storage_time":out_storage_time,
                                                                      "at_ws_time":at_ws_time,
                                                                      "out_ws_time":out_ws_time}
        
print("共 "+str(len(order_dict))+" 訂單")
print("共從倉庫出貨 "+str(container_num)+" 箱container")
#所有訂單花費時間
order_total_time = 0
#訂單數
order_num = len(order_dict)
#計算總花費時間 從出庫開始 到 入庫結束
start = True
#最早工作開始時間
work_start = 0
#最晚箱子放入完成時間
work_end = 0

for order_id,value in order_dict.items():
    if start :
        work_start = value["start_time"]
        work_end = value["end_time"]
        start = False
    else:
        if (work_start.to_pydatetime() -  value["start_time"].to_pydatetime()).total_seconds() > 0:
            work_start = value["start_time"]
        if (work_end.to_pydatetime() -  value["end_time"].to_pydatetime()).total_seconds() < 0:
            work_end = value["end_time"]
    order_total_time += value["cost_time"]
#平均訂單花費時間
avg_time =  order_total_time/order_num
# print("平均訂單花費時間為："+str(avg_time)+" 秒")
print("平均訂單花費時間為："+("%02d:%02d:%02d" % second2time(avg_time)))
total_time = (work_end.to_pydatetime() -  work_start.to_pydatetime()).total_seconds()
t_time = second2time(total_time)
print("總共花費時間為: "+("%02d:%02d:%02d" % t_time))

#初設總秒數動畫數據
move_dict = {}
for i in range(int(total_time)):
    move_dict[i] = {"in_pik":[],"in_container":[],"out_pik":[],"finish_container_num":0,"finish_order_num":0}
#放入訂單完成時間 container完成時間 手臂哪時有工作哪時工作結束
for k,v in order_dict.items():
    finish_work_time = int((v["order_end_time"].to_pydatetime() - work_start.to_pydatetime()).total_seconds())
    move_dict[finish_work_time]["finish_order_num"] += 1
    for container_id,container_value in v["container"].items():
        arm_id = container_value["arm_id"]
        out_storage_time = container_value["out_storage_time"]
        at_ws_time = container_value["at_ws_time"]
        out_ws_time = container_value["out_ws_time"]
        in_time = int((out_storage_time.to_pydatetime() - work_start.to_pydatetime()).total_seconds())
        out_time = int((at_ws_time.to_pydatetime() - work_start.to_pydatetime()).total_seconds())
        finish_container_time = int((out_ws_time.to_pydatetime() - work_start.to_pydatetime()).total_seconds())
        if container_id not in move_dict[in_time]["in_container"]:
            move_dict[in_time]["in_container"].append(container_id)
            move_dict[in_time]["in_pik"].append(arm_id)
            move_dict[out_time]["out_pik"].append(arm_id)
            move_dict[finish_container_time]["finish_container_num"] += 1

# for oi in range(len(ofai_workreport)):
#     order_info = ofai_workreport.iloc[oi,:]
#     arm_id = storage_db.find_one({"grid_id":int(order_info["出庫開始位置 (grid_id)"])})["arm_id"]
#     out_storage_time = pd.Timestamp(order_info["出庫開始時間"], tz=None)
#     at_ws_time = pd.Timestamp(order_info["抵達工作站時間"], tz=None)
#     out_ws_time = pd.Timestamp(order_info["入庫開始時間"], tz=None)
#     in_time = int((out_storage_time.to_pydatetime() - work_start.to_pydatetime()).total_seconds())
#     out_time = int((at_ws_time.to_pydatetime() - work_start.to_pydatetime()).total_seconds())
#     finish_container_time = int((out_ws_time.to_pydatetime() - work_start.to_pydatetime()).total_seconds())
#     move_dict[in_time]["in_pik"].append(arm_id)
#     move_dict[out_time]["out_pik"].append(arm_id)
#     move_dict[finish_container_time]["finish_container_num"] += 1

fourcc = cv2.VideoWriter_fourcc(*'MJPG')
print("建立 VideoWriter 物件，輸出影片至 output.avi")
print("FPS 值為 20.0，解析度為 2000X1200")

out = cv2.VideoWriter('output.avi', fourcc, 20.0, (1200, 2000))

arm_id_index = []
arm_puttext = ""
for x in range(1,8):
    for y in range(1,13):
        arm_id_index.append(str((x,y)))
        arm_puttext = arm_puttext+str((x,y))+"‘\n’"
arm_id_axis = np.array(arm_id_index)
arm_id_use = np.zeros([84])
initial_pt = [100,8]
finish_container_num = 0
finish_order_num = 0
#存成影片
for move in range(len(move_dict)):
    show_arm_usage_rate = np.ones([2000,1200],np.uint8)*255
    for k,v in move_dict[move].items():
        if k == "in_pik":
            for pik in v:
                arm_id_use[arm_id_index.index(pik)] += 1
        if k == "out_pik":
            for pik in v:
                arm_id_use[arm_id_index.index(pik)] -= 1
        if k == "finish_container_num":
            finish_container_num += v
        if k == "finish_order_num":
            finish_order_num += v 
    for arm_index in range(len(arm_id_use)):
        x_start = int(initial_pt[1]+int(arm_index*23))
        x_end = int(initial_pt[1]+int((arm_index+1)*23))
        y_start = int(initial_pt[0])
        y_end = int(initial_pt[0]+arm_id_use[arm_index]*10)
        show_arm_usage_rate[x_start:x_end,y_start:y_end] = 0
    output = cv2.cvtColor(show_arm_usage_rate, cv2.COLOR_GRAY2BGR)
    offset = 0
    for arm_id in arm_id_index:
        cv2.putText(output,arm_id,(0,25+offset*23),cv2.FONT_HERSHEY_COMPLEX, fontScale=0.7,color=(125, 0, 255), thickness=2)
        offset += 1
    time_print = "Elapsed time : "+("%02d:%02d:%02d" % second2time(move)) 
    cv2.putText(output,time_print,(700,30),cv2.FONT_HERSHEY_COMPLEX, fontScale=1,color=(125, 0, 255), thickness=2)
    container_num_print = "pick containers : "+str(finish_container_num)
    cv2.putText(output,container_num_print,(700,80),cv2.FONT_HERSHEY_COMPLEX, fontScale=1,color=(125, 0, 255), thickness=2)
    order_num_print = "finish orders :  "+str(finish_order_num)
    cv2.putText(output,order_num_print,(700,130),cv2.FONT_HERSHEY_COMPLEX, fontScale=1,color=(125, 0, 255), thickness=2)
    # cv2.imshow('frame', output)
    # cv2.waitKey(1)
    out.write(output)
out.release()
    # cv2.imshow("show_arm_usage_rate", show_arm_usage_rate)
    # cv2.waitKey(10)
        
        
        
        
        
        


# 
# FPS 值為 20.0，解析度為 640x360





