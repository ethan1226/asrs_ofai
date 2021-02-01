from utils.scheduling_utils_db import *
from utils.Arms import Arms
from utils.Containers import Containers

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

storage_dict = {}
for k in storage_db.find():
    storage_dict[ k["storage_id"]] = k
        
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


for k,v in container_dict.items():
    if len(storage_0_list)>0:
        storage_id = storage_0_list.pop()
    else:
        storage_id = storage_1_list.pop()
    storage_id_eval  = eval(storage_id)
    container_dict[k]["grid"] = storage_id_eval[0]
    container_dict[k]["relative_coords"] = {"rx" : storage_id_eval[1][0],
                                            "ry" : storage_id_eval[1][0],
                                            "rz" : storage_id_eval[1][0]}
    storage_dict[storage_id]["container_id"] = k
    storage_dict[storage_id]["contents"] = container_dict[k]["contents"]

    


'''update'''
uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
client = pymongo.MongoClient(uri)
db = client['ASRS-Cluster-0']
warehouse_db = db["Warehouses"]
container_db = db["Container_0201"]
storage_db = db["Storages_0201"]
product_db = db["Products_0201"]

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
    ite += 1

ite = 0
for k,v in storage_dict.items():
    print("storage",ite)
    myquery = { "storage_id": str(k) }
    newvalues = { "$set": {"storage_id": str(k),
                           "container_id":v["container_id"],
                           "contents":v["contents"]}} 
    storage_db.update_one(myquery, newvalues, upsert=True)
    ite += 1



    