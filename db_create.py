from utils.scheduling_utils_db import *


from queue import PriorityQueue

import numpy as np
import pandas as pd
import datetime
import itertools
import random
import copy
import dill
import string
import time

import redis
import json
import pymongo

def find_empty_arms_sid_num_db_create(storage_dict, arm_id):
    ##在arm_id找一個可以擺container的sid數量
    num = 0
    for k,v in storage_dict.items():
        if v["arm_id"] == arm_id and v["container_id"] == "":
            num += 1
    return num

file = "yahoo_order_info.xlsx"
yahoo_order_record = pd.read_excel(file , dtype=str)
yahoo_order_record['日期'] = yahoo_order_record['日期'].astype(int).astype(str)

product_call = list(pd.Series(yahoo_order_record['品號']))
product = pd.value_counts(product_call)


product_dict = {}
product_list = []
for pid,pqt in product.items():
    product_dict[pid] = {"product_name":pid,
                         "quantity" :int((1+random.randint(1, 10)/10)*pqt),
                         "turnover":pqt,
                         "container":np.ceil(pqt/482133*29787),
                         "volume":10}
    if product_dict[pid]["container"] <5 :
        product_dict[pid]["container"] += 1
    if product_dict[pid]["container"] <3 :
        product_dict[pid]["container"] += 1
    for i in range(int(product_dict[pid]["container"])):
       product_list.append(pid)
random.shuffle (product_list)

container_call = list(pd.Series(yahoo_order_record['箱號']))
container = pd.value_counts(container_call)
container_list = list(np.unique(pd.Series(yahoo_order_record['箱號'])))

container_dict = {}
for time in range(4):
    for c_id in container_list:
        if len(product_list) >0:
            if str(int(c_id)) not in container_dict:
                container_dict[str(int(c_id))] = {}
                container_dict[str(int(c_id))]["contents"] = {}
            choose = True
            while choose:
                pid_in_container = product_list.pop()
                if pid_in_container not in container_dict[str(int(c_id))]["contents"]:
                    container_dict[str(int(c_id))]["contents"].update({pid_in_container:0})
                    choose = False
                else:
                    product_list.insert(0,pid_in_container)

for k,v in container_dict.items():
    container_dict[k]["turnover"] = 0

for k,v in product_dict.items():
    v["container"] = {}
    
            
for k,v in container_dict.items():
    for v_k,v_v in v["contents"].items():
        product_dict[v_k]["container"].update({k:v_v})
        v["turnover"] += product_dict[v_k]["turnover"]




for k,v in product_dict.items():
    quantity = 0
    for container_id,container_qt in v["container"].items():
        v["container"][container_id] = int(v["turnover"]/len(v["container"])+random.randint(10, 15))
        container_dict[container_id]["contents"][k] = v["container"][container_id]
        quantity += v["container"][container_id]
    v["quantity"] = quantity



uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
client = pymongo.MongoClient(uri)
db = client['ASRS-Cluster-0']
warehouse_db = db["Warehouses"]
storage_db = db["Storages_0201"]
for warehouse in warehouse_db.find():
    nodes = dill.loads(warehouse["nodes"])
    G = dill.loads(warehouse["G"])
    dists = dill.loads(warehouse["dists"])

grid_capacity_x = 2
grid_capacity_y = 4
grid_capacity_z = 1

permu_candidate = [list(range(grid_capacity_x)), list(range(grid_capacity_y)), list(range(grid_capacity_z))]
possible_spot = list(itertools.product(*permu_candidate))
storage_dict = {}
for k, v in nodes.items():
    if v['type'] == 'grid_node':
        grid_id = k # Use the elevator as a starting point of an arm
        coordinates = nodes[k]['coordinates']
        for k_n,v_n in nodes.items():
            if v_n["coordinates"][0] == coordinates[0]+1 and v_n["coordinates"][2] == coordinates[2] and v_n['type'] == "conj_node":
                elevator_id = k_n
                
        for p in list(itertools.product([grid_id], possible_spot)):
            arm_id = (nodes[grid_id]['aisle_index'][0], nodes[grid_id]['aisle_index'][2])
            relative_coords = p[1]
            storage_dict[p] = {'storage_id':str(p),
                                'grid_id':grid_id,
                                "container_id":"",
                                "contents":{},
                                "elevator_id":elevator_id,
                                "arm_id":str(arm_id),
                                "coordinates":{"x":coordinates[0],"y":coordinates[1],"z":coordinates[2]},
                                "relative_coords":{"rx":relative_coords[0],"ry":relative_coords[1],"rz":relative_coords[2]}}
        
for k,v in storage_dict.items():
    v["container_id"] = ""
    v["contents"] = {}

storage_all_key = storage_dict.keys()
storage_all_key_list = []
for key in storage_all_key:
    storage_all_key_list.append(key)
    
storage_0_list = []
storage_1_list = []
for si in storage_all_key_list:
    si_eval  = eval(si)
    if si_eval[1][0] == 0:
        storage_0_list.append(si)
    else:
        storage_1_list.append(si)
 

random.shuffle (storage_0_list)
random.shuffle (storage_1_list)

i = 0
for k,v in container_dict.items():
    if len(storage_0_list)>0:
        storage_id = storage_0_list.pop()
    else:
        storage_id = storage_1_list.pop()
        arm_id = storage_dict[storage_id]['arm_id']
        empty_num = find_empty_arms_sid_num_db_create(storage_dict, arm_id)
        if empty_num == 1:
            continue
    storage_id_eval  = eval(storage_id) 
    container_dict[k]["grid"] = storage_id_eval[0]
    container_dict[k]["relative_coords"] = {"rx" : storage_id_eval[1][0],
                                            "ry" : storage_id_eval[1][0],
                                            "rz" : storage_id_eval[1][0]}
    storage_dict[storage_id]["container_id"] = k
    storage_dict[storage_id]["contents"] = container_dict[k]["contents"]
    i+=1
    print(i)
    


'''update'''
uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
client = pymongo.MongoClient(uri)
db = client['ASRS-Cluster-0']
warehouse_db = db["Warehouses"]
container_db = db["Containers"]
storage_db = db["Storages"]
product_db = db["Products"]

#warehouse_db_cp = db["Warehouses_copy"]
container_db_cp = db["Containers_copy"]
storage_db_cp = db["Storages_copy"]
product_db_cp = db["Products_copy"]

ite = 0
for k,v in product_dict.items():
    print("product",ite)
    myquery = { "product_id": str(k) }
    newvalues = { "$set": {'product_id':str(k),
                           "container":v["container"],
                           "product_name" : str(k),
                           "quantity" : v["quantity"],
                           "turnover" : v["turnover"],
                           "volume" : v["volume"]}} 
    product_db.update_one(myquery, newvalues, upsert=True)
    product_db_cp.update_one(myquery, newvalues, upsert=True)
    ite += 1


ite = 0
for k,v in container_dict.items():
    print("container",ite)
    myquery = { "container_id": str(k) }
    newvalues = { "$set": {'container_id':str(k),
                           "contents":v["contents"],
                           "grid_id" : v["grid"],
                           "relative_coords":v["relative_coords"],
                           "status" : "in grid",
                           "turnover" : v["turnover"],
                           "used_volume" : 0}} 
    container_db.update_one(myquery, newvalues, upsert=True)
    container_db_cp.update_one(myquery, newvalues, upsert=True)

    ite += 1

ite = 0
for k,v in storage_dict.items():
    print("storage",ite)
    myquery = { "storage_id": str(k) }
    newvalues = { "$set": {"storage_id": str(k),
                           "container_id":v["container_id"],
                           "contents":v["contents"],
                           "arm_id":v["arm_id"],
                           "grid_id":v["grid_id"],
                           "relative_coords":v["relative_coords"],
                           "coordinates":v["coordinates"]
                           }} 
    storage_db.update_one(myquery, newvalues, upsert=True)
    storage_db_cp.update_one(myquery, newvalues, upsert=True)

    ite += 1



    