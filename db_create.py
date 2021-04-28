


import numpy as np
import pandas as pd

import itertools
import random

import dill

import time
import pymongo

def find_empty_arms_sid_num_db_create(storage_dict, arm_id):
    ##在arm_id找一個可以擺container的sid數量
    num = 0
    for k,v in storage_dict.items():
        if v["arm_id"] == arm_id and v["container_id"] == "":
            num += 1
    return num

#從execl撈訂單資訊
file = "yahoo_order_info.xlsx"
yahoo_order_record = pd.read_excel(file , dtype=str)
yahoo_order_record['日期'] = yahoo_order_record['日期'].astype(int).astype(str)

product_call = list(pd.Series(yahoo_order_record['品號']))
product = pd.value_counts(product_call)


#yahoo資料位置
uri_yahoo = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
client_yahoo = pymongo.MongoClient(uri_yahoo)
yahoo_db = client_yahoo['ASRS-Cluster-0']
yahoo_order_db = yahoo_db["Orders"]

#目標資料存取位置 本地端
target_uri ="mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
client = pymongo.MongoClient(target_uri)
target_db = client['ASRS-Cluster-0']

#order資料
target_order_db = target_db["Orders"]
index_label = "date"
index = "20200701"
order_dict = {}
order_contents = []
for order_i in yahoo_order_db.find():
    order_id = order_i["order_id"]
    date = order_i["date"]
    batch = order_i["batch"]
    contents = order_i['contents']
    # order_id = 
    # contents = []
    # for k,v in order_i['contents'].items():
    #     if k not in order_contents:
    #         order_contents.append(k)
    #     contents.append({"product_id":k,"quantity":str(v)})
    order_dict[order_id] = { "order_id":order_id,
                            "date":date,
                            "batch":batch,
                            "status":"processing",
                            "contents":contents}


#從warehouse db 撈 nodes G dists
yahoo_warehouse_db = yahoo_db["Warehouses"]
for warehouse in yahoo_warehouse_db.find():
    all_warehouse = warehouse
    nodes = dill.loads(warehouse["nodes"])
    G = dill.loads(warehouse["G"])
    dists = dill.loads(warehouse["dists"])


product_dict = {}
product_list = []
ite = 0
for pid,pqt in product.items():
    print("create product_dict",ite)
    ite += 1
    quantity = int((1+random.randint(1, 10)/10)*pqt)
    container_num = np.ceil(pqt/482133*29787)
    product_dict[pid] = {"product_name":pid,
                         "quantity" :quantity,
                         "turnover":pqt,
                         "container":container_num,
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

#分配箱子
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

#分配turnover
for k,v in container_dict.items():
    container_dict[k]["turnover"] = 0

for k,v in product_dict.items():
    v["container"] = {}
    
            
for k,v in container_dict.items():
    for v_k,v_v in v["contents"].items():
        product_dict[v_k]["container"].update({k:v_v})
        v["turnover"] += product_dict[v_k]["turnover"]



#分配箱子內容物
ite = 0
for k,v in product_dict.items():
    quantity = 0
    for container_id,container_qt in v["container"].items():
        print("create container_dict",ite)
        ite += 1
        v["container"][container_id] = int(v["turnover"]/len(v["container"])+random.randint(10, 15))
        container_dict[container_id]["contents"][k] = v["container"][container_id]
        quantity += v["container"][container_id]
    v["quantity"] = quantity





#建立 storage


grid_capacity_x = 2
grid_capacity_y = 4
grid_capacity_z = 1

# 0.25 0.75 1.25 1.75 2.25 2.75 3.25 3.75 4.25 4.75 5.25
layer_height = 10
permu_candidate = [list(range(grid_capacity_x)), list(range(grid_capacity_y)), list(range(grid_capacity_z))]
possible_spot = list(itertools.product(*permu_candidate))
storage_dict = {}
ite = 0
for k, v in nodes.items():
    if v['type'] == 'grid_node':
        #只留layer_height以下的
        if v["coordinates"][2] <= layer_height:
            
            grid_id = k # Use the elevator as a starting point of an arm
            coordinates = nodes[k]['coordinates']
            for k_n,v_n in nodes.items():
                if v_n["coordinates"][0] == coordinates[0]+1 and v_n["coordinates"][2] == coordinates[2] and v_n['type'] == "conj_node":
                    elevator_id = k_n
                    
            for p in list(itertools.product([grid_id], possible_spot)):
                print("create storage_dict",ite)
                ite += 1
                arm_id = (nodes[grid_id]['aisle_index'][0], nodes[grid_id]['aisle_index'][2])
                relative_coords = p[1]
                storage_dict[str(p)] = {'storage_id':str(p),
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
    si_eval = eval(si)
    if si_eval[1][0] == 0:
        storage_0_list.append(str(si))
    else:
        storage_1_list.append(str(si))
 

random.shuffle (storage_0_list)
random.shuffle (storage_1_list)

ite = 0
for k,v in container_dict.items():
    print("put container to storage_dict",ite)
    ite += 1
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
    

workstation_dict = {}

for ws_num in range(1,9):
    workstation_id = "ws_"+str(ws_num)
    workstation_dict[workstation_id] = {"workstation_id":workstation_id,
                                        "work":{},
                                        "workloads":0}


'''update'''
print("update start")

warehouse_db = target_db["Warehouses"]
order_db = target_db["Orders"]
container_db = target_db["Containers"]
storage_db = target_db["Storages"]
product_db = target_db["Products"]
workstation_db = target_db["Workstations"]
#備份

order_db_cp = target_db["Orders_copy"]
container_db_cp = target_db["Containers_copy"]
storage_db_cp = target_db["Storages_copy"]
product_db_cp = target_db["Products_copy"]
workstation_db_cp = target_db["Workstations"]





myquery = { "_id": all_warehouse["_id"] }
newvalues = { "$set":{"nodes":all_warehouse["nodes"],
                      "dists":all_warehouse["dists"],
                      "G":all_warehouse["G"]} } 
warehouse_db.update_one(myquery, newvalues, upsert=True)



ite = 0
for order_i,order_info in order_dict.items():
    print("order",ite)
    myquery = { "order_id": order_i }
    newvalues = { "$set": { "order_id":order_i,
                            "date":order_info["date"],
                            "batch":order_info["batch"],
                            "status":"processing",
                            "contents":order_info["contents"]}}
    order_db.update_one(myquery, newvalues, upsert=True)
    order_db_cp.update_one(myquery, newvalues, upsert=True)
    ite += 1


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
                           "elevator_id":elevator_id,
                           "relative_coords":v["relative_coords"],
                           "coordinates":v["coordinates"]
                           }} 
    storage_db.update_one(myquery, newvalues, upsert=True)
    storage_db_cp.update_one(myquery, newvalues, upsert=True)

    ite += 1

ite = 0
for k,v in workstation_dict.items():
    print("workstation",ite)
    myquery = { "workstation_id": str(k) }
    newvalues = { "$set": {"workstation_id": str(k),
                            "work":{},
                            "workloads":0}} 
    workstation_db.update_one(myquery, newvalues, upsert=True)
    workstation_db_cp.update_one(myquery, newvalues, upsert=True)

    ite += 1

    