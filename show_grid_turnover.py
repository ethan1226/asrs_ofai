#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 18 14:04:10 2021

@author: huangboyu
"""
import cv2
import numpy as np

with open('參數檔.txt') as f:
    json_data = json.load(f)
uri = json_data["uri"]    
client = pymongo.MongoClient(uri)
db = client['ASRS-Cluster-0']
storage_db = db["Storages"]
container_db = db["Containers"]
storage_in_arms = storage_db.find({"arm_id":arm_id})
#此 arm_id 內 storage 資訊
storage_arm = {}
grid_list = []
storage_id_list = []
max_turnover = 0
for sia_info in storage_in_arms:
    storage_id = sia_info["storage_id"]
    k_eval = eval(storage_id)
    grid_list.append(k_eval[0])
    storage_id_list.append(storage_id)
    if sia_info["container_id"] == "":
        container_id = ""
        turnover = 0
        storage_arm[storage_id] = {"container_id":container_id,"turnover":turnover}
    else:
        container_id = sia_info["container_id"]
        turnover = container_db.find_one({"container_id":container_id})["turnover"]
        storage_arm[storage_id] = {"container_id":container_id,"turnover":turnover}
    
    if max_turnover < turnover:
        max_turnover = turnover
grid_list = list(np.unique(grid_list))

grid_list_left = grid_list[0:27]
grid_list_right = grid_list[27:54]

background = np.zeros((1200, 300, 3), np.uint8)
background.fill(255)

container_size_x = 30
container_size_y = 10
x_left = 50
x_right = 160
for storage_id,value in storage_arm.items():
    s_eval = eval(storage_id)
    grid_id = s_eval[0]
    coordinates = s_eval[1]
    turnover = value["turnover"]/max_turnover
    turnover = turnover * 20
    print("turnover: "+str(value["turnover"])+" "+str(turnover)+ " "+ str(255*turnover))
    if grid_id in grid_list_left:
        # grid_id 在左邊
        y = (26 - ( grid_id - grid_list_left[0] )) * 4 * container_size_y + 100
        blank_y = (grid_id - grid_list_left[0]) * 3
        y_offset = coordinates[1] * 10
        x_offset = coordinates[0] * container_size_x
        blank_x = (1 - coordinates[0]) * 5
        if turnover == 0:
            cv2.rectangle(background, (x_left + x_offset - blank_x , y+y_offset - blank_y), 
                                      (x_left + x_offset - blank_x + container_size_x , y + y_offset + container_size_y - blank_y ), 
                                      (0, 0, 0), 
                                      1)
        else:    
            cv2.rectangle(background, (x_left + x_offset - blank_x , y+y_offset - blank_y), 
                                      (x_left + x_offset - blank_x + container_size_x , y + y_offset + container_size_y - blank_y), 
                                      (0, 25*turnover, 0), 
                                      -1)
    else:
        # grid_id 在右邊
        y = (26 - ( grid_id - grid_list_right[0] )) * 4 * 10 + 100
        blank_y = (grid_id - grid_list_right[0]) * 3
        y_offset = coordinates[1] * 10
        x_offset = coordinates[0]*30
        blank_x = (1 - coordinates[0]) * 5
        if turnover == 0 :
            cv2.rectangle(background, (x_right - x_offset + blank_x , y+y_offset - blank_y), 
                                      (x_right - x_offset + blank_x + container_size_x , y + y_offset + container_size_y - blank_y), 
                                      (0, 0, 0), 
                                      1)
        else: 
            cv2.rectangle(background, (x_right - x_offset + blank_x , y+y_offset - blank_y), 
                                      (x_right - x_offset + blank_x + container_size_x , y + y_offset + container_size_y - blank_y),
                                      (0, 25*turnover, 0), 
                                      -1)
        
        
        
        
cv2.imshow(arm_id, background)
cv2.waitKey(0)
cv2.destroyAllWindows()