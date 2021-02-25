from queue import PriorityQueue
import numpy as np
import pandas as pd
import datetime
import itertools
import random
import copy
import dill
import string
import pymongo
import time
import redis
import json
import uuid
import math
import time

from redis import WatchError
'''
在所有儲格塞入商品資訊
'''
#def Warehouse_items(Warehouse_dict,x,y,z,position,product_id,name,amount,unit):
#    #global Warehouse_dict ????
#    '''
#    整理每個商品的儲格位置資訊
#    '''
#    #position = ",".join([str(x),str(y),str(z),str(position)])
#    position = (x,y,z,position)
#    Warehouse_dict[position] = {"product_id":str(product_id),"name":str(name),"amount":amount,"unit":str(unit)}
#    return Warehouse_dict

# 計算總 Cost 包含移走前方所有物品

def generate_random_unique_id(k):
    alphabet = string.ascii_uppercase.replace('I','').replace('O','') + string.digits.replace('0','').replace('1','')
    return ''.join(random.choices(alphabet, k=k))

def calc_job_cost(target_storage_id, arms_cost,storage_dict):
    
    G = redis_dict_get("G")
    nodes = redis_dict_get("nodes")
    dists = redis_dict_get("dists")
    storage_dict = json.loads(storage_dict)
    
    #計算每一候選container會花費的成本
    grid_id = target_storage_id[0]
    relative_coords = target_storage_id[1]
    container_depth = relative_coords[0]
    
    arm_id = (nodes[grid_id]['aisle_index'][0], nodes[grid_id]['aisle_index'][2])
    grid_candidates = []
    for nv in nodes.values():
        # 找出 Arm 的 id
        if nv['type'] == 'conj_node' and nv['aisle_index'][0] == arm_id[0] and nv['aisle_index'][2] == arm_id[1]:
            arm_node_id = int(nv['id'])
            
        # 找出屬於同一隻 Arm 的所有 grid
        if nv['type'] == 'grid_node' and nv['aisle_index'][0] == arm_id[0] and nv['aisle_index'][2] == arm_id[1]:
            grid_candidates.append(int(nv['id']))
    
    '''todo 修改速度'''
# =============================================================================
#     spot_candidates = []
#     for gc in grid_candidates:
#         for k, v in storage_dict.items():
#             # 屬於同一隻 Arm 且有空位
#             sk = eval(k)
#             sv = v
#             if sk[0] == gc and sv == {}: 
#                 # 同排的空位不能作為當前任務的 buffer
#                 if sk[1][1] == relative_coords[1] and sk[0] == grid_id: 
#                     continue
#                 spot_candidates.append(sk)
# =============================================================================
    
    spot_candidates = []
    for k, v in storage_dict.items():
        sk = json.loads(k.replace('(', '[').replace(')', ']'))
        sv = v['container_id']
        if sk[0] in grid_candidates and sv == "":
            if sk[1][1] == relative_coords[1] and sk[0] == grid_id:
                continue
            spot_candidates.append(sk)
    # 假設皆移動到最近的空位
    major_list = G.shortest_paths(source = grid_id, target = [s[0] for s in spot_candidates], weights = dists)[0] # get traveling distance
    major_list.sort(reverse = False) 
    major_cost = sum(major_list[:container_depth] * 2) 
     
    # 假設皆移動到最近的空位 是位於同一儲格 要多一懲罰項 以歐式距離計
    minor_list = [s[1] for s in spot_candidates if s[0] == grid_id]
    minor_cost = sum([np.linalg.norm(np.array(m)- np.array(relative_coords)) for m in minor_list[:container_depth]])
    
    # 直接取出之 cost
    direct_cost = G.shortest_paths(source = arm_node_id, target = grid_id, weights = dists)[0][0] # get traveling distance
    # TODO：待調整arms_cost倍率
    total_cost = direct_cost + major_cost + minor_cost/5 +arms_cost * 6

    return total_cost





''''''
'''Ethan'''

'''order function'''
def order_product(order_list):
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    order_db = db["Orders"]
    order_prd = []
    for order_content in order_db.find({"order_id":{'$in':order_list}}):
        order_prd.append(order_content["contents"])
    return order_prd

def order_sorting2(index_label,index):
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    
    arms_all_keys = redis_dict_all_keys()
    arms_dict = {}
    ## redis get
    for key in arms_all_keys:
        arms_dict[key] = redis_dict_get(key)
    
    nodes = redis_dict_get("nodes")
    
    db = client['ASRS-Cluster-0']
    order_db = db["Orders"]
    product_db = db["Products"]
    container_db = db["Containers"]
    
    order_dict = {}
    for order_i in order_db.find({index_label:index}):
        order_dict[order_i["order_id"]] = order_i
    product_dict = {}
    for x in product_db.find():
        content = {}
        content = {
            'container':x['container']
            }
        product_dict[x['product_id']] = content
    
    container_dict = {}
    for x in container_db.find():
        content = {}
        content = {
            'grid_id':x['grid_id'],
            'relative_coords':x['relative_coords']
            }
        container_dict[x['container_id']] = content
    
    orders_use_armscost_list = []
    orders_use_armscost_list_tmp = []
    orders_use_armscost_dict = {}
    
    for order_id,order_item in order_dict.items():
        single_order_pid = list(order_item["contents"].keys())
        arms_used = []
        armscost = 0
        for prd in single_order_pid:
            candidate_containers = product_dict[prd]['container']
            arms_used_tmp = []
            for k in candidate_containers.keys():
                container_info = container_dict[k]
                relative_coords = (container_info['relative_coords']['rx'],container_info['relative_coords']['ry'],container_info['relative_coords']['rz'])
                spot = (container_dict[k]['grid_id'], relative_coords)
                grid_id = spot[0]
                arm_id = (nodes[grid_id]['aisle_index'][0], nodes[grid_id]['aisle_index'][2])
                arms_workload = arms_dict[str(arm_id)].get('workload')+1
                armscost = armscost +(arms_workload /len(candidate_containers))
                # used arms id
                arms_used_tmp.append(arm_id)
            #每個container使用的機器手臂ID
            arms_used.append(arms_used_tmp)
        orders_use_armscost_dict[order_id] = armscost
    orders_use_armscost_list_tmp = sorted(orders_use_armscost_dict.items(),key=lambda item:item[1])
    for list_n in range(len(orders_use_armscost_list_tmp)):
        orders_use_armscost_list.append(orders_use_armscost_list_tmp[list_n][0])
    
    return orders_use_armscost_list

# def order_sorting(order_list,date_stores,order_record,arms_dict,nodes):
#     #將訂單以商品為依據依照目前機器手臂工作量進行排序
#     uri = "mongodb+srv://liyiliou:liyiliou@asrs-cluster-0-fj3so.mongodb.net/ASRS-Cluster-0?retryWrites=true&w=majority"
#     client = pymongo.MongoClient(uri)
    
#     db = client['ASRS-Cluster-0']
#      # Retrieve 'storage' from mongodb
#     product_db = db["Products"]
#     container_db = db["Containers"]
#     product_dict = {}
#     for x in product_db.find():
#         content = {}
#         content = {
#             'container':x['container']
#             }
#         product_dict[x['product_id']] = content
    
#     container_dict = {}
#     for x in container_db.find():
#         content = {}
#         content = {
#             'grid_id':x['grid_id'],
#             'relative_coords':x['relative_coords']
#             }
#         container_dict[x['container_id']] = content
#     #arms_dict = db[arms_dict]

#     orders_use_armscost_list = []
#     orders_use_armscost_list_tmp = []
#     orders_use_armscost_dict = {}
    
#     for oi in range(len(order_list)):
#         keys = np.where((order_list[oi] == date_stores))[0]
#         # order pid
#         single_order_pid = order_record['product_id'].iloc[keys]
#         arms_used = []
#         armscost = 0
        
#         for prd in single_order_pid:        
#             # products' container
# # =============================================================================
# #             candidate_containers = product_info[prd]['container']
# # =============================================================================
#             #order_list = order_sorting(order_list,date_stores,order_record,product_info,container_dict,arms_dict,nodes)
#             candidate_containers = product_dict[prd]['container']            

#             # random pick container
# #            ranpic = random.randint(len(candidate_containers)//2,len(candidate_containers))
# #            candidate_containers_random = random.sample(list(candidate_containers),ranpic)
#             arms_used_tmp = []
# #            for k in candidate_containers_random:
#             for k in candidate_containers.keys():
#                 # calculate arms cost
#                 #每一個candidate_containers去計算她可能所以使用的機器手臂
#                 #order_list = order_sorting(order_list,date_stores,order_record,product_info,container_dict,arms_dict,nodes)
#                 container_info = container_dict[k]
#                 relative_coords = (container_info['relative_coords']['rx'],container_info['relative_coords']['ry'],container_info['relative_coords']['rz'])
#                 spot = (container_dict[k]['grid_id'], relative_coords)
#                 grid_id = spot[0]
#                 arm_id = (nodes[grid_id]['aisle_index'][0], nodes[grid_id]['aisle_index'][2])
# #                if arms_dict[arm_id].get('workload')<1:
# #                    arms_workload = 1
# #                else:
# #                    arms_workload = arms_dict[arm_id].get('workload')
#                 arms_workload = arms_dict[str(arm_id)].get('workload')+1
#                 armscost = armscost +(arms_workload /len(candidate_containers))
#                 # used arms id
#                 arms_used_tmp.append(arm_id)
#             #每個container使用的機器手臂ID
#             arms_used.append(arms_used_tmp)
            
#         orders_use_armscost_dict[order_list[oi]] = armscost
        
#     #sort the armscost
#     orders_use_armscost_list_tmp = sorted(orders_use_armscost_dict.items(),key=lambda item:item[1])
#     for list_n in range(len(orders_use_armscost_list_tmp)):
#         orders_use_armscost_list.append(orders_use_armscost_list_tmp[list_n][0])
        
#     return orders_use_armscost_list

def order_info_get(single_order_pid):
    #找出order內全部商品中全部的container與所使用的機器手臂
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    product_db = db["Products"]
    storage_db = db["Storages"]
    products_container = {}
    prd_list = list(single_order_pid)
    prd_list_tuple = tuple(prd_list)
    product_list = product_db.find({"product_id":{'$in':prd_list_tuple}})
    prd_container_in1order = []
    for product_info in product_list:
        products_container[product_info["product_id"]] = product_info['container']
        prd_container_in1order +=list(product_info['container'])
    prd_container_in1order_set = tuple(list(set(prd_container_in1order)))
    arm_list = []
    storage_with_containers = storage_db.find({"container_id":{'$in':prd_container_in1order_set}})
    for storage_info in storage_with_containers:
        arm_list += [storage_info['arm_id']]
    arm_set = tuple(list(set(arm_list)))
    storage_list = storage_db.find({"container_id":"","arm_id":{'$in':arm_set}})
    armid_dict = {}
    for arm_id in arm_set:
        armid_dict[arm_id] = {}
        
    #put arm_id and storage information to arms_dict
    for storage_info in storage_list:
        armid_dict[storage_info["arm_id"]][storage_info["storage_id"]] = storage_info
    
    return products_container,armid_dict

def order_assign(index_label,index,num):
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    order_db = db['Orders']
    order_dict = {}
    for order_i in order_db.find({"$and":[{index_label:index},{"status":"processing"}]}):
        order_dict[order_i["order_id"]] = order_i
    output = []
    for k,v in order_dict.items():
        if num<=0:
            break
        else:
            output.append(k)
            order_dict[k]["status"] = "workstation"
            myquery = { "product_name": k }
            newvalues = { "$set": { "status": "workstation"}}
            order_db.update(myquery,newvalues)
            num -= 1
    return output

def order_pick(workstation_id):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    ws = workstation_db.find_one({'workstation_id':workstation_id})
    ws_works = ws["work"]
    #工作站內的剩餘訂單與訂單還未撿取的商品列表
    ordr_l = []
    ws_order_prd = []
    for order_i,works_value in ws_works.items():
        ordr_l.append(order_i)
        order_unpick = {}
        for prd,qt in works_value["prd"].items():
            order_unpick[prd] = qt["qt"]
        ws_order_prd.append(order_unpick)
    # ordr_l = eval(ordr_l_s)
    #訂單串的商品集合 
    # ws_order_prd = order_product(ordr_l)
    #排序機器手臂工作量
    arm_key_list = arm_work_sort_list()
    #依商品順序處理
    for order_index,order_prd in enumerate(ws_order_prd):
        #時間戳
        oi = get_time_string()
        numbering = 0
        for pid,pqt in order_prd.items():
            isbreak = False
            #先找外層再找內層
            for layer in range(1,-1,-1):
                if not isbreak:
                    #排序機器手臂工作量
                    arm_key_all = copy.deepcopy(arm_key_list) 
                    while len(arm_key_all) > 0:
                        arm_id = arm_key_all[0]
                        arm_key_all.remove(arm_id)
                        #手臂是否有此商品ＩＤ
                        lock_name = arm_id + "_pid"
                        arm_product_lock = acquire_lock_with_timeout(r, lock_name, acquire_timeout=3, lock_timeout=30)
                        if arm_product_lock != False:
                            container_id = arm_product(arm_id,pid,layer)
                            if container_id != "" :
                                #若有
                                arm_id = container_armid(container_id)
                                workstation_addpick(ordr_l[order_index],container_id,pid,pqt)
                                value = (1,oi,numbering,container_id)
                                numbering += 1
                                #更新對應redis
                                redis_data_update(arm_id,value)
                                release_lock(r, lock_name, arm_product_lock)
                                print("arm_id: " + arm_id)
                                isbreak = True
                                print(oi,arm_id,layer,pid,container_id)
                                #改container_db狀態
                                container_waiting(container_id)
                                arms_work_transmit.delay(arm_id)
                                break
                            release_lock(r, lock_name, arm_product_lock)
                        else:
                            arm_key_all.insert(4,arm_id)
    #訂單商品處理結束
    r.delete(workstation_id+"open")

def order_pick_2(workstation_id):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    ws = workstation_db.find_one({'workstation_id':workstation_id})
    ws_works = ws["work"]
    #工作站內的剩餘訂單與訂單還未撿取的商品列表
    ordr_l = []
    ws_order_prd = []
    for order_i,works_value in ws_works.items():
        ordr_l.append(order_i)
        order_unpick = {}
        for prd,qt in works_value["prd"].items():
            order_unpick[prd] = qt["qt"]
        ws_order_prd.append(order_unpick)
    # ordr_l = eval(ordr_l_s)
    #訂單串的商品集合 
    # ws_order_prd = order_product(ordr_l)
    #排序機器手臂工作量
    arm_key_list = arm_work_sort_list()
    #依商品順序處理
    prd_list = []
    prd_content = {}
    for order_index,order_content in enumerate(ws_order_prd):
        for prd,pqt in order_content.items():
            if prd not in prd_content:
                prd_list.append(prd)
                prd_content[prd] = {"qt":pqt,"order":{ordr_l[order_index]:pqt}}
            else:
                prd_content[prd]["qt"] += pqt
                prd_content[prd]["order"].update({ordr_l[order_index]:pqt})
    
    while len(prd_list)>0:
        pid = prd_list[0]
        oi = get_time_string()
        numbering = 0
        prd_qt = prd_content[pid]["qt"]
        isbreak = False
        #先找外層再找內層
        for layer in range(1,-1,-1):
            if not isbreak:
                #排序機器手臂工作量
                arm_key_all = copy.deepcopy(arm_key_list) 
                while len(arm_key_all) > 0:
                    arm_id = arm_key_all[0]
                    arm_key_all.remove(arm_id)
                    #手臂是否有此商品ＩＤ
                    lock_name = arm_id + "_pid"
                    arm_product_lock = acquire_lock_with_timeout(r, lock_name, acquire_timeout=3, lock_timeout=30)
                    if arm_product_lock != False:
                        container_id = arm_product(arm_id,pid,layer)
                        if container_id != "" :
                            #若有
                            #先判斷是否同一container是否有其他商品也在其他訂單商品列表中
                            container_bundle,container_contents = container_otherprd(container_id,prd_list)
                            #container所在的機器手臂ID
                            arm_id = container_armid(container_id)
                            #container 放入 工作站內指定訂單中
                            for bundle_pid in container_bundle:
                                pid_order_dict = prd_content[bundle_pid]["order"]
                                pid_pick_order_list = []
                                for order_id,pqt in pid_order_dict.items():
                                    #若container內pid商品數量還夠
                                    if container_contents[bundle_pid] >= pqt:
                                        #container內pid商品數量檢出
                                        container_contents[bundle_pid] -= pqt
                                        #訂單商品pid數量檢出
                                        prd_qt -= pqt
                                        #工作站增加要撿取之container與商品pid資訊
                                        workstation_addpick(order_id,container_id,bundle_pid,pqt)
                                        pid_pick_order_list.append(order_id)
                                #將檢出的被訂單刪除
                                for pop_order in pid_pick_order_list:
                                    prd_content[bundle_pid]["order"].pop(pop_order,None)
                                #若商品已無訂單需求則刪除商品
                                if prd_content[bundle_pid]["order"] == {}:
                                    prd_list.remove(bundle_pid)
                            value = (1,oi,numbering,container_id)
                            numbering += 1
                            #更新對應redis
                            redis_data_update(arm_id,value)
                            release_lock(r, lock_name, arm_product_lock)
                            
                            print(oi,arm_id,layer,pid,container_id)
                            #改container_db狀態
                            container_waiting(container_id)
                            arms_work_transmit.delay(arm_id)
                            isbreak = True
                            break
                        release_lock(r, lock_name, arm_product_lock)
                    else:
                        arm_key_all.insert(4,arm_id)
    #訂單商品處理結束
    r.delete(workstation_id+"open")
                            

def order_check(workstation_id, order_id):
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    workstation_info = workstation_db.find({'workstation_id':workstation_id})
    for ws_i in workstation_info:
        ws_work = ws_i['work']
    if ws_work[order_id]["prd"] == {}:
        return True
    else:
        return False

def order_count(index_label,index):
    #此條件還有多少張單
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    order_db = db['Orders']
    return order_db.count_documents({index_label:index})

def order_processing_count(index_label,index):
    #此條件還有多少張單屬於processing
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    order_db = db['Orders']
    return order_db.count_documents({"$and":[{index_label:index},{"status":"processing"}]})

'''find function'''

def get_time_string():
    now = time.localtime()
    output = ""
    for i in now[0:6]:
        output = output + str(i)
    return output

# def find_containers_key(grid_id,relative_coords):
#     '''
#     找到container key 利用grid_id 與 relative_coords
#     '''
#     uri = "mongodb+srv://liyiliou:liyiliou@asrs-cluster-0-fj3so.mongodb.net/ASRS-Cluster-0?retryWrites=true&w=majority"
#     client = pymongo.MongoClient(uri)
#     db = client['ASRS-Cluster-0']
#     container_dict = db["Containers"]
    
#     for containers_value in container_dict.find():
#         containers_key = containers_value["container_id"]
#         if containers_value['grid_id'] == grid_id and containers_value['relative_coords'] == list(relative_coords):
#             return containers_key
#     return ''

def find_empty_sid_num():
    #找一個可以擺container的sid數量
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_dict = db["Storages"]

    
    empty_grid = {}
    for storage_info in storage_dict.find({"container_id":""}):
        empty_grid[storage_info["storage_id"]] = {'arm_id':storage_info['arm_id']}
    used_arm = {}
    for eg_i in empty_grid:
        if empty_grid[eg_i]['arm_id'] in used_arm.keys():
            used_arm[empty_grid[eg_i]['arm_id']]['qt'] = used_arm[empty_grid[eg_i]['arm_id']]['qt'] + 1
            used_arm[empty_grid[eg_i]['arm_id']]['storage'].append(eg_i)
        else:
            used_arm[empty_grid[eg_i]['arm_id']] = {'qt':1,'storage':[]}
            used_arm[empty_grid[eg_i]['arm_id']]['storage'].append(eg_i)
    can_put_container = 0
    for arm_i in used_arm:
        if used_arm[arm_i]['qt']>1:
            can_put_container += used_arm[arm_i]['qt']-1
    return can_put_container

def find_empty_sid():
    #找一個可以擺container的sid
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_dict = db["Storages"]
    
    empty_grid = {}
    #找在storage裡container是空的位置 並存下他所在的arm_id
    for storage_info in storage_dict.find({"container_id":""}):
        empty_grid[storage_info["storage_id"]] = {'arm_id':storage_info['arm_id']}
    #以arm_id為key 紀錄每一個arm下有多少位置沒有放入container
    used_arm = {}
    for eg_i in empty_grid:
        if empty_grid[eg_i]['arm_id'] in used_arm.keys():
            used_arm[empty_grid[eg_i]['arm_id']]['qt'] = used_arm[empty_grid[eg_i]['arm_id']]['qt'] + 1
            used_arm[empty_grid[eg_i]['arm_id']]['storage'].append(eg_i)
        else:
            used_arm[empty_grid[eg_i]['arm_id']] = {'qt':1,'storage':[]}
            used_arm[empty_grid[eg_i]['arm_id']]['storage'].append(eg_i)
    #紀錄有大於兩個空位的arm
    can_put_container_arm = []
    for arm_i in used_arm:
        if used_arm[arm_i]['qt']>1:
            can_put_container_arm.append(arm_i)
    #判斷是否還有storage可以放入container
    if len(can_put_container_arm)>0:
        #隨機選擇arm_id
        key_choice = random.choice(can_put_container_arm)
        #隨機選擇storage_id
        #判斷是否在下層
        sid = random.choice(list(used_arm[key_choice]['storage']))
        if det_lower(sid):
            return(sid)
        else:
            sid_lower_tmp = json.loads(sid.replace('(', '[').replace(')', ']'))
            sid_lower = str((sid_lower_tmp[0],(0,sid_lower_tmp[1][1],sid_lower_tmp[1][2])))
            if sid_lower in list(used_arm[key_choice]['storage']):
                return(sid_lower)
            else:
                return(sid)
                
    else:
        return(None)


def find_empty_arms_sid(arm_id):
    ##在arm_id下找一個空的sid
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_dict = db["Storages"]
    
    empty_grid = []
    #找在storage裡container是空的位置並且arm_id為需求者
    for storage_info in storage_dict.find({"container_id":"","arm_id":arm_id}):
        empty_grid.append(storage_info["storage_id"])
    #判斷空位是否大於1,須保留一個buffer才可塞入新的container
    if len(empty_grid)>1:
        #隨機選擇storage_id
        sid = random.choice(empty_grid)
        if det_lower(sid):
            return(sid)
        else:
            sid_lower_tmp = json.loads(sid.replace('(', '[').replace(')', ']'))
            sid_lower = str((sid_lower_tmp[0],(0,sid_lower_tmp[1][1],sid_lower_tmp[1][2])))
            if sid_lower in empty_grid:
                return(sid_lower)
            else:
                return(sid)
    else:
        return(None)
    
def find_empty_arms_sid_num(arm_id):
    ##在arm_id找一個可以擺container的sid數量
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_dict = db["Storages"]
    return(storage_dict.count_documents({"container_id":"","arm_id":arm_id}))

# def find_empty_container(container_dict):
#     ##找一個空的container
#     uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
#     client = pymongo.MongoClient(uri)
#     db = client['ASRS-Cluster-0']##找一個空的grid
#     container_db = db[container_dict]
#     warehouse_db = db["Warehouses"]
#     for warehouse in warehouse_db.find():
#         nodes = dill.loads(warehouse["nodes"])
    
#     empty_container_list = []
#     empty_container = {}
#     for container_info in container_db.find():
#         container_key = container_info["container_id"]
#         if container_db.find_one({"container_id":container_key})['contents'] == {} : 
#             empty_container_list.append(container_key)
#             grid_id = container_db.find_one({"container_id":container_key})['grid_id']
#             arm_id = (nodes[grid_id]['aisle_index'][0], nodes[grid_id]['aisle_index'][2])
#             empty_container[container_key] = {'arm_id':arm_id}
    
#     #同一arm之空的container 
#     used_arm = {}
#     for ec_i in empty_container:
#         if empty_container[ec_i]['arm_id'] in used_arm.keys():
#             used_arm[empty_container[ec_i]['arm_id']]['qt'] = used_arm[empty_container[ec_i]['arm_id']]['qt'] + 1
#             used_arm[empty_container[ec_i]['arm_id']]['container'].append(ec_i)
#         else:
#             used_arm[empty_container[ec_i]['arm_id']] = {'qt':1,'container':[]}
#             used_arm[empty_container[ec_i]['arm_id']]['container'].append(ec_i)
    
#     can_put_container_arm = []
#     for arm_i in used_arm:
#         if used_arm[arm_i]['qt']>1:
#             can_put_container_arm.append(arm_i)
#     ##選擇適合的arm 隨機選擇
#     if len(can_put_container_arm)>0:
#         key_choice = random.choice(can_put_container_arm)
#         return(random.choice(list(used_arm[key_choice]['container'])))
#     else:
#         return(None)
    ##return 塞入arm中的container 隨機選擇 

def det_lower(storage_id):
    #判斷storage_id是否在下層
    sid = json.loads(storage_id.replace('(', '[').replace(')', ']'))
    if sid[1][0] == 0:
        return(True)
    else:
        return(False)

def det_pick_put(conainer_info):
    det = eval(conainer_info)
    if det[0] == 1:
        return True
    else:
        return False
    
    
'''arms function'''

def arms_pick_update(arms_dict,arms_containers):
    '''
    更新arms dict
    將container 放入正確的arms 並更新arm's workload
    '''
    containers = []
    containers_info = {}
    for a_c_i in arms_containers:
        containers.append(a_c_i[3])
        containers_info[a_c_i[3]] = {"info":a_c_i}
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_db = db["Storages"]
    updated_arm_dict = {}
    container_storage_ptr = storage_db.find({'container_id':{'$in':containers}})
    for container_storage_info in container_storage_ptr:
        arm_id = container_storage_info['arm_id']
        containers_key = container_storage_info['container_id']
        arms_dict[str(arm_id)]['works'].put(containers_info[containers_key]['info'])
        arms_dict[str(arm_id)] = {'workload':arms_dict[str(arm_id)]['works'].qsize(),'works':arms_dict[str(arm_id)]['works']}
        updated_arm_dict[str(arm_id)] = arms_dict[str(arm_id)]
    return updated_arm_dict




def arms_pick(container_id):
    #機器手臂撿取container 並會判斷他上方是否有阻礙的container會先行移開在撿取目標container並更新資料庫
    
    ##redis get
    G = redis_dict_get("G")
    nodes = redis_dict_get("nodes")
    dists = redis_dict_get("dists")
    
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    storage_db = db["Storages"]
   
    container_info = container_db.find_one({"container_id":container_id})
    grid_id = container_info['grid_id']
    if container_info['relative_coords']['rx'] == 1:
        #在上層直接取出
        container_status(container_id,'on_conveyor')
        container_grid(container_id,-1)
    else:
        #在下層先,判斷是否上層有東西
        #上層物品
        upper = storage_db.find({'grid_id':grid_id,
                                 'relative_coords.ry':container_info['relative_coords']['ry'],
                                 'relative_coords.rx':1,
                                 'container_id':{'$ne':""}})
        
        if upper.count() == 0 :
            #上層沒有東西,直接取出
            container_status(container_id,'on_conveyor')
            container_grid(container_id,-1)
        else:
            #上層有東西,先移開後取出
            for u_i in upper:
                upper_container = u_i['container_id']
                upper_storage_id = u_i['storage_id']
            arm_id = (nodes[grid_id]['aisle_index'][0], nodes[grid_id]['aisle_index'][2])
            spot_candidates_ptr = storage_db.find({"arm_id":str(arm_id),'container_id':""})
            spot_candidates = []
            for sc in spot_candidates_ptr:
                if sc['grid_id'] == grid_id and container_info['relative_coords']['ry'] == sc['relative_coords']['ry']:
                    continue
                sc_key = json.loads(sc['storage_id'].replace('(', '[').replace(')', ']'))
                spot_candidates.append(sc_key)
            major_list = G.shortest_paths(source = grid_id, target = [s[0] for s in spot_candidates], weights = dists)[0]
            moveto = spot_candidates[major_list.index(min(major_list))]
            moveto = str((moveto[0],tuple(moveto[1])))
            #將upper_container 移到 moveto 修改資料庫
            container_moveto(upper_container,moveto)
            storage_interchange(upper_storage_id,moveto)
            #再將 container_id放到conveyor
            container_status(container_id,'on_conveyor')
            container_grid(container_id,-1)
            storage_pop(container_id)

def arms_store_update(arms_dict,arm_id,arms_containers):
    '''
    更新arms dict
    將container 放入正確的arms 並更新arm's workload
    '''
    
    updated_arm_dict = {}
    arms_dict[str(arm_id)]['works'].put(arms_containers)
    arms_dict[str(arm_id)] = {'workload':arms_dict[str(arm_id)]['works'].qsize(),'works':arms_dict[str(arm_id)]['works']}
    updated_arm_dict[str(arm_id)] = arms_dict[str(arm_id)]
    
    return updated_arm_dict

def arms_store(container_id,arm_id):
    #先從arm_id找出可放入的位置在將container放入並更新資料庫
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    product_db = db["Products"]
    container_db = db["Containers"]
    storage_db = db["Storages"]
    #找arm_id上的可放入的空格
    storage_id = find_empty_arms_sid(arm_id)
    #storage＿id 放入 container_id
    storage_push(storage_id,container_id)
    #container_id 修改資訊(移動到storage_id & status to in grid)
    container_moveto(container_id,storage_id)
    container_status(container_id,"in_grid")
    #將 container_id 內商品更新 product
    product_push_container(container_id)

def arm_product(arm_id,product_id,layer):
    #查詢 arm_id這隻手臂內 layer層 有無 product_id商品
    #arm_id 哪隻機器手臂  layer 內層外層 0外 1內 ,product_id 目標商品
    
    arm_dict = redis_dict_get(arm_id)
    arm_content = arm_dict[str(layer)]
    if product_id in arm_content:
        return arm_content[product_id]["container_id"]
    else:
        return ""

def arm_work_sort_list():
    arm_key_all = redis_dict_all_keys()
    arm_workloads = {}
    for arms_key in arm_key_all:
        arms_data = redis_dict_get(arms_key)
        arm_workloads[arms_key] = arms_data["workload"]
    arm_workloads_list = sorted(arm_workloads.items(),key=lambda item:item[1])
    output_arm_workloads_list = []
    for list_n in range(len(arm_workloads_list)):
        output_arm_workloads_list.append(arm_workloads_list[list_n][0])
    
    return output_arm_workloads_list
    
'''product function'''
'''
def product_push(container_id,push_products):
    #將 push_products 商品放入 container_id 並更新數量
    uri = "mongodb+srv://liyiliou:liyiliou@asrs-cluster-0-fj3so.mongodb.net/ASRS-Cluster-0?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    product_db = db["Products"]
    for products_name in push_products:
        product_dict_container = product_db.find_one({"product_name":products_name})['container']
        if container_id in product_dict_container:
            product_dict_container[container_id] += push_products[products_name]
        else:    
            product_dict_container.update({container_id :push_products[products_name] })
        product_dict_qt = sum(product_dict_container.values())
        myquery = { "product_name": products_name }
        newvalues = { "$set": { "container": product_dict_container,"quantity": product_dict_qt} }
        product_db.update_one(myquery, newvalues)
'''

def product_push_container(container_id):
    #將 container_id 內商品更新至 product
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    product_db = db["Products"]
    container_db = db["Containers"]
    
    container_info = container_db.find({"container_id":container_id})
    for c_i in container_info:
        contents = c_i['contents']
    for k,v in contents.items():
        product_info = product_db.find_one({"product_name":k})
        product_container = product_info["container"]
        product_container.update({k:v})
        product_qt = sum(product_container.values())
        myquery = { "product_name": k }
        newvalues = { "$set": { "container": product_container,"quantity": product_qt} }
        product_db.update_one(myquery, newvalues)

def product_pop_container(container_id):
    #將product有container_id的商品扣掉 container_id 並更新數量
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']##找一個空的grid
    product_dict = db["Products"]
    #找有container_id的商品
    find_container_id = "container." + container_id
    product_have_container = product_dict.find({find_container_id:{'$exists':1}})
    for phc in product_have_container:
        products_name = phc['product_name']
        product_dict_container = phc['container']
        product_dict_container.pop(container_id,None)
        product_dict_qt = sum(product_dict_container.values())
        myquery = { "product_name": products_name }
        newvalues = { "$set": { "container": product_dict_container,"quantity":product_dict_qt}}
        product_dict.update(myquery,newvalues)





'''storage function'''
def storage_pop(container_id):
    #將container_id 從在storage位置上清空
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_dict = db["Storages"]
    storage_info = storage_dict.find({'container_id':container_id})
    for s_i in storage_info:
        storage_id = s_i['storage_id']
    myquery = { "storage_id": storage_id }
    newvalues = { "$set": { "container_id": ""}}
    storage_dict.update(myquery,newvalues) 
    

def storage_push(storage_id,container_id):
    #將container_id放入至 storage的storage_id位置上
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_dict = db["Storages"]
    myquery = { "storage_id": storage_id }
    newvalues = { "$set": { "container_id": container_id}}
    storage_dict.update(myquery,newvalues)  
    return storage_dict

def storage_interchange(src_storage_id,dst_storage_id):
    #將src_storage_id與dst_storage_id上的container_id互換
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_dict = db["Storages"]
    #find src  container_id
    src = storage_dict.find({'storage_id':src_storage_id})
    for src_i in src:
        src_container_id = src_i['container_id']
    #find dst container_id
    dst = storage_dict.find({'storage_id':dst_storage_id})
    for dst_i in dst:
        dst_container_id = dst_i['container_id']
    #put dst_container_id in src_storage
    myquery = { "storage_id": src_storage_id }
    newvalues = { "$set": { "container_id": dst_container_id}}
    storage_dict.update(myquery,newvalues)
    #put src_container_id in dst_storage
    myquery = { "storage_id": dst_storage_id }
    newvalues = { "$set": { "container_id": src_container_id}}
    storage_dict.update(myquery,newvalues)  
    


'''container function'''

def container_pick(container_id,pick):
    #將container_id內撿出pick, pick為str '{'pid':qt}'
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    container_info = container_db.find({"container_id": container_id})
    for c_i in container_info:
        contents = c_i['contents']
    pick = eval(pick)
    for k,v in pick.items():
        if k in contents:
            contents[k] -= v
    myquery = { "container_id": container_id }
    newvalues = { "$set": { "contents": contents}} 
    container_db.update(myquery,newvalues)

def container_putin(container_id,putin):
    #將container_id內存入putin, putin為str '{'pid':qt}'
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    container_info = container_db.find({"container_id": container_id})
    for c_i in container_info:
        contents = c_i['contents']
    putin = eval(putin)
    for k,v in putin.items():
        if k not in contents:
            contents[k] = 0
        contents[k] += v
    myquery = { "container_id": container_id }
    newvalues = { "$set": { "contents": contents}} 
    container_db.update(myquery,newvalues)

    
def container_pop(container_id):
    #將container_id移出container_dict
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_dict = db["Containers"]
    myquery = { "container_id": container_id }
    container_dict.delete_one(myquery)
    #container_dict.pop(container_id,None)

def container_status(container_id,status):
    #將container_id 的狀態修改為 status
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_dict = db["Containers"]
    myquery = { "container_id": container_id }
    newvalues = { "$set": { "status": status}} 
    container_dict.update(myquery,newvalues)  

def container_moveto(container_id,moveto):
    #將container_id 移動至 moveto moveto為storage_id
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_dict = db["Containers"]
    storage_dict = db["Storages"]
    storage_id_list= json.loads(moveto.replace('(', '[').replace(')', ']'))
    myquery = { "container_id": container_id }
    relative_coords = {"rx":storage_id_list[1][0],"ry":storage_id_list[1][1],"rz":storage_id_list[1][2]}
    newvalues = { "$set": { "grid_id": storage_id_list[0],'relative_coords':relative_coords}} 
    container_dict.update(myquery,newvalues)

def container_grid(container_id,new_grid_id):
    #將container_id 的grid_id修改為 grid_id
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_dict = db["Containers"]
    myquery = { "container_id": container_id }
    newvalues = { "$set": { "grid_id": new_grid_id}} 
    container_dict.update(myquery,newvalues)  
    
def container_waiting(container_id):
    #container 狀態改為等待被撿取 並更新資料庫（刪除container_id在product內資訊）
    #刪除container_id在product內資訊
    product_pop_container(container_id)
    #container狀態修改
    container_status(container_id,'waiting')
    
def container_armid(container_id):
    #container 的 arm_id
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_db = db["Storages"]

    for container_storage_info in storage_db.find({'container_id':container_id}):
        arm_id = str(container_storage_info['arm_id'])
    return arm_id
    

def container_putback(container_id):
    print("in container_putback container_id : "+ container_id)
    #container找到一個適當的arm_id位置回去存取
    #判斷 arm id 內 workload , turnover ,目前商品總數
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    container_info = container_db.find({"container_id":container_id})
    for c_i in container_info:
        contents = c_i['contents']
    arm_key_all = redis_dict_all_keys()
    arm_workloads = {}
    arm_prdreserve = {}
    arm_turnover = {}
    arm_score = {}
    for arms_key in arm_key_all:
        # print(arms_key)
        arms_data = redis_dict_get(arms_key)
        arm_workloads[arms_key] = arms_data["workload"]
        arm_turnover[arms_key] = arms_data["turnover"]
        arm_prdreserve[arms_key] = len(arms_data["0"])+len(arms_data["1"])
    # arm_workloads_list = sorted(arm_workloads.items(),key=lambda item:item[1])
    # arm_turnover_list = sorted(arm_turnover.items(),key=lambda item:item[1])
    # arm_prdreserve_list = sorted(arm_prdreserve.items(),key=lambda item:item[1])
    for arms_key in arm_key_all:
        if arm_workloads[max(arm_workloads, key=arm_workloads.get)] == 0:
            arm_score_workloads = arm_workloads[arms_key]
        else:
            arm_score_workloads = arm_workloads[arms_key]/arm_workloads[max(arm_workloads, key=arm_workloads.get)]
            
        if arm_turnover[max(arm_turnover, key=arm_turnover.get)] == 0:
            arm_score_turnover = arm_turnover[arms_key]
        else:
            arm_score_turnover = arm_turnover[arms_key]/arm_turnover[max(arm_turnover, key=arm_turnover.get)]
            
        if arm_prdreserve[max(arm_prdreserve, key=arm_prdreserve.get)] == 0:
            arm_score_prdreserve = arm_prdreserve[arms_key]
        else:
            arm_score_prdreserve = arm_prdreserve[arms_key]/arm_prdreserve[max(arm_prdreserve, key=arm_prdreserve.get)]
        
        arm_score[arms_key] = arm_score_workloads + arm_score_turnover + arm_score_prdreserve
    arm_score_list = sorted(arm_score.items(),key=lambda item:item[1])
    for list_n in range(len(arm_score_list)):
        arms_data = redis_dict_get(arm_score_list[list_n][0])
        clear_space = True
        for k,v in contents.items():
            if k in arms_data["0"] or k in arms_data["1"]:
                clear_space = False
        if clear_space and find_empty_arms_sid_num(arm_score_list[list_n][0])>1:
            return arm_score_list[list_n][0]
    
    return arm_score_list[0][0]

def coutainer_go_storage(container_id):
    #container找到一個適當的arm_id位置回去存取
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_dict = db["Containers"]
    #container內的商品
    for container in container_dict.find({"container_id":container_id}):
        contents = list(container["contents"].keys())
    arm_key_all = redis_dict_all_keys() 
    arm_score = {}
     #判斷是否商品內有在arm_id道上與目前工作量排序
    for arms_key in arm_key_all:
        score = 0
        arms_data = redis_dict_get(arms_key)
        for pid in contents:
            if pid in arms_data["0"]:
                score += 1
            if pid in arms_data["1"]:
                score += 1
        score += arms_data["workload"]
        arm_score[arms_key] = score
    
    
    arm_score_list = sorted(arm_score.items(),key=lambda item:item[1])
    arm_score_sort = []
    for list_n in range(len(arm_score_list)):
        arm_score_sort.append(arm_score_list[list_n][0])
    index = 0
    arm_id = arm_score_sort[index]
    #判斷是否有空間可以存入
    while find_empty_arms_sid(arm_id) == False and index <= len(arm_score_sort):
        index += 1
        arm_id = arm_score_sort[index]
        
    return arm_id

def container_otherprd(container_id,prd_list):
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    container_info = container_db.find_one({"container_id": container_id})
    container_bundle = []
    for pid in prd_list:
        if pid in container_info['contents']:
            container_bundle.append(pid)
    return container_bundle,container_info['contents']


'''workstation function'''
def workstation_assign():
    #指派工作站ＩＤ (選擇工作量最少的工作站)
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    workstation_pick = workstation_db.find().sort('workloads').limit(1)
    for workstation_info in workstation_pick:
        workstation_id = workstation_info['workstation_id']
    return workstation_id

# def workstation_newwork(workstation_id,order):
#     #新order工作產生
#     uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
#     client = pymongo.MongoClient(uri)
#     db = client['ASRS-Cluster-0']
#     workstation_db = db["Workstations"]
    
#     ws = workstation_db.find({'workstation_id':workstation_id})
#     for ws_i in ws:
#         ws_workstation_id = ws_i['workstation_id']
#         ws_work = ws_i['work']
#         ws_workloads = ws_i['workloads']
#     order_work = {}
#     order_work[order] = {}
    
#     ws_work.update(order_work)
#     ws_workloads += 1
#     myquery = { "workstation_id": workstation_id }
#     newvalues = { "$set": { "work": ws_work,"workloads":ws_workloads}}
#     workstation_db.update(myquery,newvalues)
    
# def workstation_newwork_prd(workstation_id,order):
#     #新order工作產生
#     uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
#     client = pymongo.MongoClient(uri)
#     db = client['ASRS-Cluster-0']
#     workstation_db = db["Workstations"]
#     order_db = db["Orders"]
#     order_contents = order_db.find_one({'order_id':order})["contents"]
#     ws = workstation_db.find({'workstation_id':workstation_id})
#     for ws_i in ws:
#         ws_workstation_id = ws_i['workstation_id']
#         ws_work = ws_i['work']
#         ws_workloads = ws_i['workloads']
#     order_work = {}
#     order_work[order] = {}
#     for k,v in order_contents.items():
#        order_work[order]= {"prd":{k:{"qt":v}},"container":{}}
    
#     ws_work.update(order_work)
#     ws_workloads += 1
#     myquery = { "workstation_id": workstation_id }
#     newvalues = { "$set": { "work": ws_work,"workloads":ws_workloads}}
#     workstation_db.update(myquery,newvalues)
def workstation_newwork_prd(workstation_id,order_l):
    #新order工作產生
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    order_db = db["Orders"]
    ws = workstation_db.find({'workstation_id':workstation_id})
    for ws_i in ws:
        ws_workstation_id = ws_i['workstation_id']
        ws_work = ws_i['work']
        ws_workloads = ws_i['workloads']
    order_dict = order_db.find({'order_id':{'$in':order_l}})
    order_work = {}
    for oi in order_dict:
        order_contents = oi["contents"]
        order = oi["order_id"]
        order_work[order] = {}
        order_work[order]["prd"] = {}
        order_work[order]["container"] = {}
        for k,v in order_contents.items():
           order_work[order]["prd"].update({k:{"qt":v}})
        ws_work.update(order_work)
        ws_workloads += 1
    myquery = { "workstation_id": workstation_id }
    newvalues = { "$set": { "work": ws_work,"workloads":ws_workloads}}
    workstation_db.update(myquery,newvalues)
    

# def workstation_addpick(workstation_id,order,container_id,prd,pqt):
#     #指派order 撿取container的contents與數量
#     uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
#     client = pymongo.MongoClient(uri)
#     db = client['ASRS-Cluster-0']
#     workstation_db = db["Workstations"]
    
    
#     ws = workstation_db.find({'workstation_id':workstation_id})
#     for ws_i in ws:
#         ws_work = ws_i['work']
#     insert_pick = {}
#     insert_pick[container_id] = {prd:pqt}
#     ws_work[order]["container"].update(insert_pick) 
        
    
    
#     myquery = { "workstation_id": workstation_id }
#     newvalues = { "$set": { "work": ws_work}}
#     workstation_db.update(myquery,newvalues)
def workstation_addpick(order,container_id,prd,pqt):
    #指派order 撿取container的contents與數量
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    work_info = workstation_db.aggregate([
                     {'$match': { 'work.'+order:{'$exists':"true"}}}])
    for ws_i in work_info:
        workstation_id = ws_i["workstation_id"]
        ws_work = ws_i['work']
    insert_pick = {}
    insert_pick[container_id] = {prd:pqt}
    ws_work[order]["container"].update(insert_pick)
    myquery = { "workstation_id": workstation_id }
    newvalues = { "$set": { "work."+order+".container": ws_work[order]["container"]}}
    workstation_db.update(myquery,newvalues)
def workstation_get(container_id):
    # 工作站收到container
    container_status(container_id,'in_workstation')

def workstation_pick_info(container_id):
    #工作站以從container撿取 order所需物品資訊
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    work_info = workstation_db.aggregate([
                         {'$addFields': {"workTransformed": {'$objectToArray': "$work"}}},
                         {'$match': { 'workTransformed.v.'+container_id: {'$exists':1} }}
                                     ])
    for w_1 in work_info:
        workstation_id = w_1['workstation_id']
        order_id = w_1['workTransformed'][0]['k']
        pick_info = w_1['workTransformed'][0]['v']
        for k,v in pick_info.items():
            pick = v
    
    return workstation_id,order_id,pick

def workstation_pick(container_id):
    #工作站以從container撿取order所需物品
    #刪除 workstation work內工作 container 資訊
    print("in workstation_pick contaioner_id : "+container_id)
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    work_info = workstation_db.aggregate([
                         {'$addFields': {"workTransformed": {'$objectToArray': "$work"}}},
                         {'$match': { 'workTransformed.v.container.'+container_id: {'$exists':1} }}
                                     ])
    for w_1 in work_info:
        print("in workstation_pick contaioner_id : ",container_id," w_1: ",w_1)
        ws_work = w_1['work']
        workstation_id = w_1['workstation_id']
    print("in workstation_pick contaioner_id : ",container_id," ws_work: ",ws_work)
    output_order_id_list = []
    for order_id,order_pickup in ws_work.items():
        for pick_container_id in order_pickup["container"]:
            if container_id == pick_container_id:
                #找出container在哪些張訂單號
                output_order_id_list.append(order_id)
    #刪除 container內要撿的商品
    for output_order_id in output_order_id_list:
        for prd,pqt in ws_work[output_order_id]["container"][container_id].items():
            ws_work[output_order_id]["prd"][prd]["qt"] -= pqt
            #需求量撿完刪除訂單商品
            if ws_work[output_order_id]["prd"][prd]["qt"] == 0:
                ws_work[output_order_id]["prd"].pop(prd,None)
    #撿完container後刪除
    for output_order_id in output_order_id_list:
        ws_work[output_order_id]["container"].pop(container_id,None)
    #更新db
    for output_order_id in output_order_id_list:
        myquery = { "workstation_id": workstation_id}
        newvalues = { "$set": { "work."+output_order_id: ws_work[output_order_id]}}
        workstation_db.update(myquery,newvalues)
    #TODO container送回倉
    return output_order_id,workstation_id
    

def workstation_workend(workstation_id,order):
    #工作站檢取完後 刪除工作
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    ws = workstation_db.find({'workstation_id':workstation_id})
    for ws_i in ws:
        ws_workloads = ws_i['workloads']
    ws_workloads -= 1
    myquery = { "workstation_id": workstation_id}
    #減工作量
    newvalues = { "$set": {"workloads":ws_workloads}}
    workstation_db.update(myquery,newvalues)
    #刪訂單
    newvalues = { "$unset": {"work."+order:{}}}
    workstation_db.update(myquery,newvalues)
    

def workstation_order(workstation_id):
    #工作站內訂單列表
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    ws = workstation_db.find({'workstation_id':workstation_id})
    order_l =[]
    for ws_contents in ws:
        works = ws_contents["work"]
    for order_id,contents in works.items():
        order_l.append(order_id)
    return (str(order_l))

def workstation_free(workstation_id):
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    if len(workstation_db.find_one({"workstation_id":workstation_id})["work"])>0:
        return False
    else:
        return True
 
'''redis function'''
def redis_init():
    #從storage裡面撈出資訊，並初始化redis內資訊
    
    arm_product_ptr = {}
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    warehouse_db = db["Warehouses"]
    for warehouse in warehouse_db.find():
        nodes = dill.loads(warehouse["nodes"])
        G = dill.loads(warehouse["G"])
        dists = dill.loads(warehouse["dists"])
    redis_dict_set("nodes",nodes)
    redis_dict_set("G",G)
    redis_dict_set("dists",dists)
    
    storage_db = db["Storages"]
    for si in storage_db.find():
        storage_id = si['storage_id']
        sid = json.loads(storage_id.replace('(', '[').replace(')', ']'))
        layer = str(sid[1][0])
        if si["arm_id"] not in arm_product_ptr:
            works = PriorityQueue()
            arm_product_ptr[si["arm_id"]] = {'workload':works.qsize(),'works':works}
            arm_product_ptr[si["arm_id"]]["0"] = {}
            arm_product_ptr[si["arm_id"]]["1"] = {}
            for k,v in si["contents"].items():
                arm_product_ptr[si["arm_id"]][layer][k] = {"qt":v,"container_id":si["container_id"]}
            
            
        else:
            for k,v in si["contents"].items():
                if k not in arm_product_ptr[si["arm_id"]][layer]:
                    arm_product_ptr[si["arm_id"]][layer][k] = {"qt":v,"container_id":si["container_id"]}
                else:
                    arm_product_ptr[si["arm_id"]][layer][k] = {"qt":arm_product_ptr[si["arm_id"]][layer][k]["qt"]+v,"container_id":si["container_id"]}
    
    for key, value in arm_product_ptr.items():
        redis_dict_set(key,value)

def redis_arm_product_update():
    #從storage裡面撈出資訊，並初始化redis內資訊
    arm_product_ptr = {}
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_db = db["Storages"]
    product_db = db["Products"]
    product_dict = {}
    #商品清單
    for pi in product_db.find():
        product_dict[pi["product_id"]] = pi
    #將storage分類
    for si in storage_db.find():
        storage_id = si['storage_id']
        sid = json.loads(storage_id.replace('(', '[').replace(')', ']'))
        layer = str(sid[1][0])
        if si["arm_id"] not in arm_product_ptr:
            works = PriorityQueue()
            arm_product_ptr[si["arm_id"]] = {'workload':works.qsize(), 'works':works, "turnover":0}
            arm_product_ptr[si["arm_id"]]["0"] = {}
            arm_product_ptr[si["arm_id"]]["1"] = {}
            for k,v in si["contents"].items():
                arm_product_ptr[si["arm_id"]][layer][k] = {"qt":v,"container_id":si["container_id"]}
                arm_product_ptr[si["arm_id"]]["turnover"] += product_dict[k]['turnover']
            
        else:
            for k,v in si["contents"].items():
                if k not in arm_product_ptr[si["arm_id"]][layer]:
                    arm_product_ptr[si["arm_id"]][layer][k] = {"qt":v,"container_id":si["container_id"]}
                    arm_product_ptr[si["arm_id"]]["turnover"] += product_dict[k]['turnover']
                else:
                    arm_product_ptr[si["arm_id"]][layer][k] = {"qt":arm_product_ptr[si["arm_id"]][layer][k]["qt"]+v,"container_id":si["container_id"]}
                    arm_product_ptr[si["arm_id"]]["turnover"] += product_dict[k]['turnover']
    
    for key, value in arm_product_ptr.items():
        redis_dict_set(key,value)
    
def redis_data_update(key,value):
    container_id =value[3]
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    one_dict = {}
    content = r.get(key)
    if content != None:
        one_dict = dill.loads(content)
        #刪除 redis裡面的機器手臂可撿商品資訊
        for layer in range(2):
            layer = str(layer)
            pop_k = []
            for content_k,content_v in one_dict[layer].items():
                if container_id in content_v["container_id"]:
                    pop_k.append(content_k)
            for popk in pop_k:
                one_dict[layer].pop(popk)
        #更正工作量跟工作
        one_dict['workload'] += 1
        one_dict["works"].put(value)
    r.set(key,dill.dumps(one_dict))
    

def redis_dict_get(key):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)  
    one_dict = {}
    content = r.get(key)
    if content != None:
        one_dict = dill.loads(content)
    return one_dict
def redis_dict_get_work(key):
    value = redis_dict_get(key)
    container_info = value['works'].get()
    redis_dict_set(key, value)
    return container_info

def redis_dict_set(key, value):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)  
    r.set(key,dill.dumps(value))
    
def redis_dict_all_keys():
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    all_keys_b = r.keys()
    all_keys = []
    for key in all_keys_b:
        if "(" in key.decode("utf-8")and "_pid"not in key.decode("utf-8"):
            all_keys.append(key.decode("utf-8"))
    return all_keys

def acquire_lock_with_timeout(conn, lock_name, acquire_timeout=3, lock_timeout=2):
    """
    基于 Redis 实现的分布式锁
    
    :param conn: Redis 连接
    :param lock_name: 锁的名称
    :param acquire_timeout: 获取锁的超时时间，默认 3 秒
    :param lock_timeout: 锁的超时时间，默认 2 秒
    :return:
    """

    identifier = str(uuid.uuid4())
    lockname = f'lock:{lock_name}'
    lock_timeout = int(math.ceil(lock_timeout))

    end = time.time() + acquire_timeout

    while time.time() < end:
        # 如果不存在这个锁则加锁并设置过期时间，避免死锁
        if conn.set(lockname, identifier, ex=lock_timeout, nx=True):
            return identifier

        time.sleep(0.001)

    return False

def acquire_lock_with_timeout_r(lock_name, acquire_timeout=3, lock_timeout=2):
    """
    基于 Redis 实现的分布式锁
    
    :param conn: Redis 连接
    :param lock_name: 锁的名称
    :param acquire_timeout: 获取锁的超时时间，默认 3 秒
    :param lock_timeout: 锁的超时时间，默认 2 秒
    :return:
    """
    conn = redis.Redis(host='localhost', port=6379, decode_responses=False)  
    identifier = str(uuid.uuid4())
    lockname = f'lock:{lock_name}'
    lock_timeout = int(math.ceil(lock_timeout))

    end = time.time() + acquire_timeout

    while time.time() < end:
        # 如果不存在这个锁则加锁并设置过期时间，避免死锁
        if conn.set(lockname, identifier, ex=lock_timeout, nx=True):
            return identifier

        time.sleep(0.001)

    return False

def release_lock(conn, lock_name, identifier):
    """
    释放锁
    
    :param conn: Redis 连接
    :param lockname: 锁的名称
    :param identifier: 锁的标识
    :return:
    """
    # python中redis事务是通过pipeline的封装实现的
    with conn.pipeline() as pipe:
        lockname = f'lock:{lock_name}'
        # lockname = 'lock:' + lockname

        while True:
            try:
                # watch 锁, multi 后如果该 key 被其他客户端改变, 事务操作会抛出 WatchError 异常
                pipe.watch(lockname)
                iden = pipe.get(lockname)
                if iden and iden.decode('utf-8') == identifier:
                    # 事务开始
                    pipe.multi()
                    pipe.delete(lockname)
                    pipe.execute()
                    return True

                pipe.unwatch()
                break
            except WatchError:
                pass
        return False
    
def release_lock_r(lock_name, identifier):
    """
    释放锁
    
    :param conn: Redis 连接
    :param lockname: 锁的名称
    :param identifier: 锁的标识
    :return:
    """
    # python中redis事务是通过pipeline的封装实现的
    conn = redis.Redis(host='localhost', port=6379, decode_responses=False)  
    with conn.pipeline() as pipe:
        lockname = f'lock:{lock_name}'
        # lockname = 'lock:' + lockname

        while True:
            try:
                # watch 锁, multi 后如果该 key 被其他客户端改变, 事务操作会抛出 WatchError 异常
                pipe.watch(lockname)
                iden = pipe.get(lockname)
                if iden and iden.decode('utf-8') == identifier:
                    # 事务开始
                    pipe.multi()
                    pipe.delete(lockname)
                    pipe.execute()
                    return True

                pipe.unwatch()
                break
            except WatchError:
                pass
        return False