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
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]    
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    order_db = db["Orders"]
    order_prd = []
    for order_content in order_db.find({"order_id":{'$in':order_list}}):
        order_prd.append(order_content["contents"])
    return order_prd



def order_info_get(single_order_pid):
    #找出order內全部商品中全部的container與所使用的機器手臂
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
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
    '''
    指派order根據index_label:index
    num數量
    
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    order_db = db['Orders']
    container_db = db['Containers']
    order_dict = {}
    for order_i in order_db.find({"$and":[{index_label:index},{"status":"processing"}]}):
        order_dict[order_i["order_id"]] = order_i
    output = []
    for order_id,order_info in order_dict.items():
        if num<=0:
            break
        else:
            #判斷訂單商品目前是否在倉庫裡有貨
            satisfaction = True
            for pid,qt in order_info["contents"].items():
                if container_db.count_documents({"contents."+pid:{'$exists':"true"},"status":"in grid"}) == 0:
                    print_string = "In order_assign order_id: "+str(order_id)+" pid: "+str(pid)+" not exists in grid"
                    print_coler(print_string,"g")
                    satisfaction = False
                    break
            if satisfaction:
                output.append(order_id)
                order_dict[order_id]["status"] = "workstation"
                myquery = { "order_id": order_id }
                newvalues = { "$set": { "status": "workstation"}}
                order_db.update(myquery,newvalues)
                num -= 1
    if num >0:
        print_string = "只有 "+str(num)+ " 張訂單滿足分配條件"
        print_coler(print_string,"g")
    return output

def order_assign_crunch(index_label,index,num):
    '''
    會將有同一個商品的訂單優先分配出去
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    order_db = db['Orders']
    order_dict = {}
    for order_i in order_db.find({"$and":[{index_label:index},{"status":"processing"}]}):
        order_dict[order_i["order_id"]] = order_i
    #統計訂單相同物品個數
    pid_order = {}
    pid_amount = {}
    for order_id,order_info in order_dict.items():
        for pid,pqt in order_info["contents"].items():
            if pid not in pid_order:
                pid_order[pid] = {order_id}
                pid_amount[pid] = 1
            else:
                pid_order[pid].update({order_id})
                pid_amount[pid] += 1
    pid_amount_list = sorted(pid_amount.items(),key=lambda item:item[1],reverse=True)
    full = False
    output = []
    #優先將有同樣商品的進行配單
    for pid in pid_amount_list:
        if not full:
            for order_id in pid_order[pid[0]]:
                if num<=0:
                    full = True
                    break
                else:
                    if order_id not in output:
                        output.append(order_id)
                        order_dict[order_id]["status"] = "workstation"
                        myquery = { "order_id": order_id }
                        newvalues = { "$set": { "status": "workstation"}}
                        order_db.update(myquery,newvalues)
                        num -= 1
        else:
            break
    return output


              

def order_check(workstation_id, order_id):
    '''
    判斷工作站內order_id是否已完成
    '''
    print("in order check workstation id: "+str(workstation_id)+" order id: "+str(order_id))
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    workstation_info = workstation_db.find({'workstation_id':workstation_id})
    for ws_i in workstation_info:
        ws_work = ws_i['work']
    if order_id in ws_work:
        if ws_work[order_id]["prd"] == {}:
            print("workstation id: "+str(workstation_id)+" in order_check order id : "+str(order_id)+" in workstation is finished")
            return True
        else:
            print("workstation id: "+str(workstation_id)+" in order_check order id : "+str(order_id)+" in workstation is not finished")
            return False
    else:
        print_string ="workstation id: "+str(workstation_id)+" in order_check order id : "+str(order_id)+" not in workstation maybe is finished"
        print_coler(print_string,"g")
        return False

def order_count(index_label,index):
    '''
    此條件還有多少張單
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    order_db = db['Orders']
    return order_db.count_documents({index_label:index})

def order_processing_count(index_label,index):
    '''
    此條件還有多少張單屬於processing
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    order_db = db['Orders']
    return order_db.count_documents({"$and":[{index_label:index},{"status":"processing"}]})

'''find function'''

def get_time_string():
    '''
    給予時間係數
    '''
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
    '''
    找一個可以擺container的sid數量
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
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
    '''
    找一個可以擺container的sid
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
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

def find_empty_arms_sid_by_turnover(arm_id,container_id):
    '''
    在arm_id下找一個空的sid
    '''
    
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_db = db["Storages"]
    
    #此手臂上的全部 grid_id
    grid_list = []
    total_storage = 0
    for si in storage_db.find({"arm_id":arm_id}):
        grid_id = si["grid_id"]
        total_storage += 1
        if grid_id not in grid_list:
            grid_list.append(grid_id)
    #分成左右邊
    grid_l = grid_list[0:27]
    grid_r = grid_list[27:54]
    #此手臂上目前的空位
    empty_grid = []
    #找在storage裡container是空的位置並且arm_id為需求者
    for storage_info in storage_db.find({"container_id":"","arm_id":arm_id}):
        empty_grid.append(storage_info["storage_id"])
    #將空位給分數
    storage_score = {}
    for storage_i in empty_grid:
        storage_eval =  eval(storage_i)
        grid = storage_eval[0]
        #0是下層 1是上層
        layer = storage_eval[1][0]
        if grid < grid_r[0]:
            #在左側
            dist= grid - grid_l[0]
            score = dist + abs(layer - 1)*np.sqrt(dist)
        else:
            #在右側
            dist= grid - grid_r[0]
            score = dist + abs(layer - 1)*np.sqrt(dist)
        storage_score[storage_i] = score
    #在排序分數
    storage_score_list = sorted(storage_score.items(),key=lambda item:item[1],reverse=True)
    #比較turnover 來決定放的區間
    container_db = db["Containers"]
    container_turnover = container_db.find_one({"container_id":container_id})["turnover"]
    arm_turnover = redis_dict_get_turnover(arm_id)
    avg_turnover = arm_turnover/(total_storage - len(empty_grid))
    quarter_turnover = avg_turnover/2
    at_range = container_turnover//quarter_turnover
    if at_range > 3:
        at_range = 3
    #判斷空位是否大於1,須保留一個buffer才可塞入新的container
    if len(empty_grid)>1:
        #隨機選擇storage_id
        sid = random.choice(storage_score_list[0:int(len(empty_grid)/4*(at_range+1))])[0]
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
        return("")


def find_empty_arms_sid(arm_id):
    '''
    在arm_id下找一個空的sid
    '''
    
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
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
        return("")
    
def find_empty_arms_sid_num(arm_id):
    '''
    在arm_id找一個可以擺container的sid數量
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
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
    '''
    判斷storage_id是否在下層
    '''
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
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
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






def arm_work_sort_list():
    '''
    arm工作量排序
    '''
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

def arm_workloads(arm_id):
    '''
    回傳手臂工作量
    '''
    arms_data = redis_dict_get(arm_id)
    return arms_data["workload"]

def arm_work_status(arm_id):
    '''
    return目前arm已使用的箱數與空箱數
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_db = db["Storages"]
    total = storage_db.count_documents({"arm_id":arm_id})
    arms_status = {}
    arms_status[arm_id] = {}
    arms_status[arm_id]["container"] = storage_db.count_documents({"$and":[{"arm_id":arm_id},{"contents":{"$ne":{}}}]})
    arms_status[arm_id]["empty"] = total - arms_status[arm_id]["container"]
    return arms_status



def elevator_workloads(arm_id):
    '''
    arm_id 的elevator workloads
    '''
    arm_id_eval = eval(arm_id)
    arm_key_all = redis_dict_all_keys()
    elevator_workloads = {}
    arm_elevator = 0
    arm_workloads = {}
    for arms_key in arm_key_all:
        arms_key_id = eval(arms_key)
        if arm_id_eval[0] ==arms_key_id[0]:
            arms_data = redis_dict_get(arms_key)
            arm_elevator += arms_data["workload"]
    return arm_elevator

'''product function'''

def product_push_container(container_id):
    '''
    將 container_id 內商品更新至 product
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    product_db = db["Products"]
    container_db = db["Containers"]
    container_info = container_db.find({"container_id":container_id})
    for c_i in container_info:
        contents = c_i['contents']
    for pid,pqt in contents.items():
        myquery = { "product_name": pid }
        newvalues = { "$set": { "container."+container_id:pqt},"$inc": { "quantity":pqt} }
        product_db.update_one(myquery, newvalues)

# def product_pop_container(container_id):
#     #將product有container_id的商品扣掉 container_id 並更新數量
#     with open('參數檔.txt') as f:
#         json_data = json.load(f)
#     uri = json_data["uri"]
#     client = pymongo.MongoClient(uri)
#     db = client['ASRS-Cluster-0']
#     product_dict = db["Products"]
#     #找有container_id的商品
#     find_container_id = "container." + container_id
#     product_have_container = product_dict.find({find_container_id:{'$exists':1}})
#     for phc in product_have_container:
#         products_name = phc['product_name']
#         product_dict_container = phc['container']
#         product_dict_container.pop(container_id,None)
#         product_dict_qt = sum(product_dict_container.values())
#         myquery = { "product_name": products_name }
#         newvalues = { "$set": { "container": product_dict_container,"quantity":product_dict_qt}}
#         product_dict.update(myquery,newvalues)
def product_pop_container(container_id):
    '''
    將product有container_id的商品扣掉 container_id 並更新數量
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    product_db = db["Products"]
    container_db = db["Containers"]
    container_info = container_db.find({"container_id":container_id})
    for c_i in container_info:
        contents = c_i['contents']
    #找有container_id的商品
    for pid,pqt in contents.items():
        myquery = { "product_name": pid }
        newvalues = { "$unset": { "container."+container_id:""},"$inc": { "quantity":-pqt}}
        product_db.update(myquery,newvalues)




'''storage function'''
def storage_pop(container_id):
    '''
    將container_id 從在storage位置上清空
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_dict = db["Storages"]
    myquery = { "container_id": container_id }
    newvalues = { "$set": { "container_id": "","contents" : {}}}
    storage_dict.update(myquery,newvalues) 
    

def storage_push(storage_id,container_id):
    '''
    將container_id放入至 storage的storage_id位置上
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_db = db["Storages"]
    container_db = db["Containers"]
    myquery = { "storage_id": storage_id }
    contents = container_db.find_one({"container_id":container_id})["contents"]
    newvalues = { "$set": { "container_id": container_id,"contents":contents}}
    storage_db.update(myquery,newvalues)  
    

def storage_interchange(src_storage_id,dst_storage_id):
    '''
    將src_storage_id與dst_storage_id上的container_id互換
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
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
    

def storage_empty(storage_id,ban = []):
    '''
    將目標storage_id 位置清空 清除container不放入ban位置中
    '''
    print("in storage_empty storage_id: "+str(storage_id)+" will be empty")
    ##redis get
    G = redis_dict_get("G")
    nodes = redis_dict_get("nodes")
    dists = redis_dict_get("dists")
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]    
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_db = db["Storages"]
    storage_id_eval = eval(storage_id)
    grid_id = storage_id_eval[0]
    relative_coords = storage_id_eval[1]
    #目標storage_id資訊
    storage_target = storage_db.find_one({"storage_id":storage_id})
    container_id = storage_target["container_id"]
    if container_id != "":
        #若有container_id表示還未清空
        arm_id = storage_target['arm_id']
        #找同一機器手臂沒有放container的位置
        spot_candidates_ptr = storage_db.find({"arm_id":str(arm_id),'container_id':""})
        spot_candidates = []
        if relative_coords[0] == 1:
            #在上層直接將目標storage_id移出
            #收集所有可以放入的空位
            for sc in spot_candidates_ptr:
                if (sc['grid_id'] == grid_id and relative_coords[1] == sc['relative_coords']['ry']) or sc['storage_id'] in ban :
                    #若grid_id一樣又'ry'一樣表示是他上層
                    continue
                sc_key = json.loads(sc['storage_id'].replace('(', '[').replace(')', ']'))
                spot_candidates.append(sc_key)
            #計算最短路徑並排序
            major_list = G.shortest_paths(source = grid_id, target = [s[0] for s in spot_candidates], weights = dists)[0]
            #排序各點所需移動的路徑長
            spot_candidates_sort = []
            for spot_i in range(len(major_list)):
                spot_candidates_sort.append([spot_candidates[spot_i],major_list[spot_i]])
            spot_candidates_sort = sorted(spot_candidates_sort, key=lambda s: s[1])
            #選擇距離最短的位置當作移動目標
            moveto = spot_candidates_sort[0][0]
            moveto_storage_id = str((moveto[0],tuple(moveto[1])))
            #判斷空位是上層還是下層
            if moveto[1][0] == 1:
                #空位在上層，判斷下層是否有東西
                lower = storage_db.count_documents({'grid_id':moveto[0],
                                                    'relative_coords.ry':moveto[1][1],
                                                    'relative_coords.rx':0,
                                                    'container_id':{'$ne':""}})
                if lower == 0:
                    #空位下層有東西，放入下層
                    moveto[1][0] = 0
                    moveto_storage_id = str((moveto[0],tuple(moveto[1])))
            ##將目標storage_id 移到 moveto 修改資料庫
            container_moveto(container_id,moveto_storage_id)
            storage_interchange(storage_id,moveto_storage_id)
            
        else:
            #在下層先判斷是否上層有東西
            #上層物品
            upper_num = storage_db.count_documents({'grid_id':grid_id,
                                                    'relative_coords.ry':relative_coords[1],
                                                    'relative_coords.rx':1,
                                                    'container_id':{'$ne':""}})
            if upper_num == 0 :
                #上層沒有東西,直接將目標storage_id移出
                #收集所有可以放入的空位
                for sc in spot_candidates_ptr:
                    if (sc['grid_id'] == grid_id and relative_coords[1] == sc['relative_coords']['ry']) or sc['storage_id'] in ban:
                        #若grid_id一樣又'ry'一樣表示是他上層
                        continue
                    sc_key = json.loads(sc['storage_id'].replace('(', '[').replace(')', ']'))
                    spot_candidates.append(sc_key)
                #計算最短路徑並排序
                major_list = G.shortest_paths(source = grid_id, target = [s[0] for s in spot_candidates], weights = dists)[0]
                #排序各點所需移動的路徑長
                spot_candidates_sort = []
                for spot_i in range(len(major_list)):
                    spot_candidates_sort.append([spot_candidates[spot_i],major_list[spot_i]])
                spot_candidates_sort = sorted(spot_candidates_sort, key=lambda s: s[1])
                #選擇距離最短的位置當作移動目標
                moveto = spot_candidates_sort[0][0]
                moveto_storage_id = str((moveto[0],tuple(moveto[1])))
                #判斷空位是上層還是下層
                if moveto[1][0] == 1:
                    #空位在上層，判斷下層是否有東西
                    lower = storage_db.count_documents({'grid_id':moveto[0],
                                                        'relative_coords.ry':moveto[1][1],
                                                        'relative_coords.rx':0,
                                                        'container_id':{'$ne':""}})
                    if lower == 0:
                        #空位下層有東西，放入下層
                        moveto[1][0] = 0
                        moveto_storage_id = str((moveto[0],tuple(moveto[1])))
                ##將目標storage_id 移到 moveto 修改資料庫
                container_moveto(container_id,moveto_storage_id)
                storage_interchange(storage_id,moveto_storage_id)
            else:
                #上層有東西,先移開上層後在移出
                upper = storage_db.find({'grid_id':grid_id,
                                         'relative_coords.ry':relative_coords[1],
                                         'relative_coords.rx':1,
                                         'container_id':{'$ne':""}})
                #上層資訊
                for u_i in upper:
                    upper_container = u_i['container_id']
                    upper_storage_id = u_i['storage_id']
                #收集所有可以放入的空位
                for sc in spot_candidates_ptr:
                    if (sc['grid_id'] == grid_id and relative_coords[1] == sc['relative_coords']['ry']) or sc['storage_id'] in ban:
                        #若grid_id一樣又'ry'一樣表示是他上層
                        continue
                    sc_key = json.loads(sc['storage_id'].replace('(', '[').replace(')', ']'))
                    spot_candidates.append(sc_key)
                #計算最短路徑並排序
                major_list = G.shortest_paths(source = grid_id, target = [s[0] for s in spot_candidates], weights = dists)[0]
                #排序各點所需移動的路徑長
                spot_candidates_sort = []
                for spot_i in range(len(major_list)):
                    spot_candidates_sort.append([spot_candidates[spot_i],major_list[spot_i]])
                spot_candidates_sort = sorted(spot_candidates_sort, key=lambda s: s[1])
                #選擇距離最短的位置當作移動目標
                moveto = spot_candidates_sort[0][0]
                moveto_storage_id = str((moveto[0],tuple(moveto[1])))
                #判斷空位是上層還是下層
                if moveto[1][0] == 1:
                    #空位在上層，判斷下層是否有東西
                    lower = storage_db.count_documents({'grid_id':moveto[0],
                                                        'relative_coords.ry':moveto[1][1],
                                                        'relative_coords.rx':0,
                                                        'container_id':{'$ne':""}})
                    if lower == 0:
                        #空位下層有東西，放入下層
                        moveto[1][0] = 0
                        moveto_storage_id = str((moveto[0],tuple(moveto[1])))
                
                #將upper_container 移到 moveto 修改資料庫
                container_moveto(upper_container,moveto_storage_id)
                storage_interchange(upper_storage_id,moveto_storage_id)
                #選擇將目標 storage_id 移出的 moveto
                i = 1
                moveto = spot_candidates_sort[i][0]
                moveto_storage_id = str((moveto[0],tuple(moveto[1])))
                while storage_db.find_one({"storage_id":moveto_storage_id})["container_id"] != "" and i<len(spot_candidates_sort):
                    i += 1
                    moveto = spot_candidates_sort[i][0]
                    moveto_storage_id = str((moveto[0],tuple(moveto[1])))
                #判斷空位是上層還是下層
                if moveto[1][0] == 1:
                    #空位在上層，判斷下層是否有東西
                    lower = storage_db.count_documents({'grid_id':moveto[0],
                                                        'relative_coords.ry':moveto[1][1],
                                                        'relative_coords.rx':0,
                                                        'container_id':{'$ne':""}})
                    if lower == 0:
                        #空位下層有東西，放入下層
                        moveto[1][0] = 0
                        moveto_storage_id = str((moveto[0],tuple(moveto[1])))
                
                #將upper_container 移到 moveto 修改資料庫
                container_moveto(container_id,moveto_storage_id)
                storage_interchange(storage_id,moveto_storage_id)
        print("in storage_empty storage_id: "+storage_id+" is empty")
    else:
        #若無container_id表示已清空
        print("in storage_empty storage_id: "+storage_id+" is empty")

'''container function'''

def container_pick(container_id,pick):
    '''
    將container_id內撿出pick, pick為str '{'pid':qt}'
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    product_db = db["Products"]
    container_info = container_db.find({"container_id": container_id})
    for c_i in container_info:
        contents = c_i['contents']
        turnover = c_i['turnover']
    newvalues = {}
    for pid,pqt in pick.items():
        contents[pid] -= pqt
        if contents[pid] == 0:
            pid_turnover = product_db.find_one({"product_id":pid})["turnover"]
            contents.pop(pid,None)
            newvalues.update({"$inc": { "turnover":-pid_turnover}})
    myquery = { "container_id": container_id }
    newvalues.update({ "$set": { "contents": contents}} )
    container_db.update(myquery,newvalues)


    

def container_putin(container_id,putin):
    '''
    將container_id內存入putin, putin為str '{'pid':qt}' 若為新商品則增加container的turnover
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    product_db = db["Products"]
    container_info = container_db.find_one({"container_id": container_id})
    contents = container_info['contents']
    #putin = eval(putin)
    myquery = { "container_id": container_id }
    newvalues = {}
    for k,v in putin.items():
        if k not in contents:
            contents[k] = 0
            turnover = product_db.find_one({"product_id": k})["turnover"]
            newvalues.update({ "$inc": { "turnover": turnover}})
        contents[k] += v
    newvalues.update({ "$set": { "contents": contents}})
    container_db.update(myquery,newvalues)

    
def container_pop(container_id):
    '''
    將container_id移出container_dict
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    myquery = { "container_id": container_id }
    container_db.delete_one(myquery)
    #container_dict.pop(container_id,None)

def container_status(container_id):
    '''
    將container_id 的狀態為 
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    return container_db.find_one({"container_id":container_id})["status"]

def container_set_status(container_id,status):
    '''
    將container_id 的狀態修改為 status
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    myquery = { "container_id": container_id }
    newvalues = { "$set": { "status": status}} 
    container_db.update(myquery,newvalues)  

def container_moveto(container_id,moveto):
    '''
    將container_id 移動至 moveto moveto為storage_id
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    storage_db = db["Storages"]
    storage_id_list= json.loads(moveto.replace('(', '[').replace(')', ']'))
    myquery = { "container_id": container_id }
    relative_coords = {"rx":storage_id_list[1][0],"ry":storage_id_list[1][1],"rz":storage_id_list[1][2]}
    newvalues = { "$set": { "grid_id": storage_id_list[0],'relative_coords':relative_coords}} 
    container_db.update(myquery,newvalues)

def container_grid(container_id,new_grid_id):
    '''
    將container_id 的grid_id修改為 grid_id
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    # storage_db = db["Storages"]
    myquery = { "container_id": container_id }
    newvalues = { "$set": { "grid_id": new_grid_id}} 
    container_db.update(myquery,newvalues)
    # storage_db.update(myquery,newvalues)
    
def container_waiting(container_id):
    '''
    container 狀態改為等待被撿取 並更新資料庫（刪除container_id在product內資訊）
    '''
    #刪除container_id在product內資訊
    product_pop_container(container_id)
    #container狀態修改
    container_set_status(container_id,'waiting')
    
def container_armid(container_id):
    '''
    container 的 arm_id
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_db = db["Storages"]
    arm_id = ""
    for container_storage_info in storage_db.find({'container_id':container_id}):
        arm_id = str(container_storage_info['arm_id'])
    # print("in container_armid container_id: "+container_id+" arm_id: "+arm_id)
    return arm_id
    

def container_putback(container_id):
    '''
    計算一個最好的arm_id給container_id去放入
    '''
    print("in container_putback container_id : "+ container_id)
    #container找到一個適當的arm_id位置回去存取
    #判斷 arm id 內 workload , turnover ,目前商品總數
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    container_info = container_db.find({"container_id":container_id})
    for c_i in container_info:
        contents = c_i['contents']
    arm_key_all = redis_dict_all_keys()
    #arm工作量
    arm_workloads = {}
    #arm商品量
    # arm_prdreserve = {}
    #arm週轉率
    arm_turnover = {}
    #elevator工作量
    elevator_workloads = {}
    #arm elevator 使用率
    arm_elevator = {}
    #arm空位數
    arm_fillment_score = {}
    #arm分數
    arm_score = {}
    #分數佔例
    coefficient_arms_workloads = 1.0
    coefficient_arms_turnover = 0.5
    coefficient_arms_elevator = 2.0
    coefficient_arms_fillment = 1.0
    coefficient_total = coefficient_arms_workloads + coefficient_arms_turnover + coefficient_arms_elevator + coefficient_arms_fillment
    for arms_key in arm_key_all:
        # 存取目前手臂工作量 周轉數 跟移動電梯使用工作量
        arms_data = redis_dict_get(arms_key)
        arm_workloads[arms_key] = arms_data["workload"]
        arm_turnover[arms_key] = arms_data["turnover"]
        arm_fillment_score[arms_key] = find_empty_arms_sid_num(arms_key)
        arm_id = eval(arms_key)
        if arm_id[0] not in elevator_workloads:
            elevator_workloads[arm_id[0]] = arm_workloads[arms_key]
        else:
            elevator_workloads[arm_id[0]]+= arm_workloads[arms_key]
        # arm_prdreserve[arms_key] = len(arms_data["0"])+len(arms_data["1"])
    # arm_workloads_list = sorted(arm_workloads.items(),key=lambda item:item[1])
    # arm_turnover_list = sorted(arm_turnover.items(),key=lambda item:item[1])
    # arm_prdreserve_list = sorted(arm_prdreserve.items(),key=lambda item:item[1])
    for arms_key in arm_key_all:
        # 將移動電梯使用工作量轉移至手臂電梯使用率
        arm_id = eval(arms_key)
        arm_elevator[arms_key] = elevator_workloads[arm_id[0]]
    
    for arms_key in arm_key_all:
        #統計各手臂相關分數
        #手臂工作量分數正規化
        if arm_workloads[max(arm_workloads, key=arm_workloads.get)] == 0:
            arm_score_workloads = arm_workloads[arms_key]
        else:
            arm_score_workloads = arm_workloads[arms_key]/arm_workloads[max(arm_workloads, key=arm_workloads.get)]
        #手臂內商品週轉率分數正規化
        if arm_turnover[max(arm_turnover, key=arm_turnover.get)] == 0:
            arm_score_turnover = arm_turnover[arms_key]
        else:
            arm_score_turnover = arm_turnover[arms_key]/arm_turnover[max(arm_turnover, key=arm_turnover.get)]
        #手臂電梯分數正規化
        if arm_elevator[max(arm_elevator, key=arm_elevator.get)] == 0:
            arm_score_elevator = arm_elevator[arms_key]
        else:
            arm_score_elevator = arm_elevator[arms_key]/arm_elevator[max(arm_elevator, key=arm_elevator.get)]
        #手臂空位分數正規化
        if arm_fillment_score[max(arm_fillment_score, key=arm_fillment_score.get)] == 0:
            arm_score_fillment = arm_fillment_score[arms_key]
        else:
            arm_score_fillment = arm_fillment_score[arms_key]/arm_fillment_score[max(arm_fillment_score, key=arm_fillment_score.get)]
        
        #手臂總分數 = 手臂工作量 + 手臂內空位數 + 手臂內商品週轉率 + 手臂電梯目前使用量
        arm_score[arms_key] = (coefficient_arms_workloads*(1-arm_score_workloads) + \
                               coefficient_arms_fillment*(arm_score_fillment) + \
                               coefficient_arms_turnover*(1-arm_score_turnover) + \
                               coefficient_arms_elevator*(1-arm_score_elevator))/ coefficient_total
        # if arm_prdreserve[max(arm_prdreserve, key=arm_prdreserve.get)] == 0:
        #     arm_score_prdreserve = arm_prdreserve[arms_key]
        # else:
        #     arm_score_prdreserve = arm_prdreserve[arms_key]/arm_prdreserve[max(arm_prdreserve, key=arm_prdreserve.get)]
        
        # arm_score[arms_key] = arm_score_workloads + arm_score_turnover + arm_score_prdreserve
        
    arm_score_list = sorted(arm_score.items(),key=lambda item:item[1],reverse=True)
    #找適當的手臂
    for list_n in range(len(arm_score_list)):
        arms_data = redis_dict_get(arm_score_list[list_n][0])
        
        clear_space = True
        #判斷container內商品是否有存在於此手臂內
        for k,v in contents.items():
            if k in arms_data["0"] or k in arms_data["1"]:
                clear_space = False
        #若不存在於此手臂內又剩餘空間還大於25個
        if clear_space and find_empty_arms_sid_num(arm_score_list[list_n][0])>20:
            return arm_score_list[list_n][0]
    return arm_score_list[0][0]

def coutainer_go_storage(container_id):
    '''
    將container_putback給予的位置放入工作列表中
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
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
    '''
    將container_id內的商品有與prd_list內重複的輸出於container_bundle
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    container_info = container_db.find_one({"container_id": container_id})
    container_bundle = []
    for pid in prd_list:
        if pid in container_info['contents']:
            container_bundle.append(pid)
    return container_bundle,container_info['contents']


def container_movement():
    '''
    將狀態為 on_conveyor與in_workstation與waiting數量輸出
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    return container_db.count_documents({"$or":[{"status":"ws_to_conveyor"},{"status":"conveyor_to_ws"},
                                                {"status":"in_workstation"},{"status":"waiting"}]})

def container_goto(container_id,storage_id):
    '''
    將 container_id 移至 storage_id ,storage_id是已清空的狀態
    會判斷準備移動container位置可能會移動上方物品至最近的位置，但會避開目標storage_id的位置
    '''
    print("in container_goto container_id: "+str(container_id)+" will goto storage_id: "+str(storage_id))
    ##redis get
    G = redis_dict_get("G")
    nodes = redis_dict_get("nodes")
    dists = redis_dict_get("dists")
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]    
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    storage_db = db["Storages"]
    #先判斷目標container_id上層還是下層
    ctr_storage_info = storage_db.find_one({"container_id":container_id})
    ctr_storage_id = ctr_storage_info["storage_id"]
    ctr_storage_id_eval = eval(ctr_storage_id)
    ctr_relative_coords = ctr_storage_id_eval[1]
    grid_id = ctr_storage_id_eval[0]
    arm_id = ctr_storage_info["arm_id"]
    if ctr_relative_coords[0] == 1:
        #目標container_id為上層，直接移至 storage_id
        print("container_id為上層，直接移至storage_id")
        container_moveto(container_id,storage_id)
        storage_interchange(ctr_storage_id,storage_id)
    else:
        #目標container_id為下層，判斷是否上層有container
        upper_num = storage_db.count_documents({'grid_id':grid_id,
                                                'relative_coords.ry':ctr_relative_coords[1],
                                                'relative_coords.rx':1,
                                                'container_id':{'$ne':""}})
        if upper_num == 0:
            #若無直接移至 storage_id
            container_moveto(container_id,storage_id)
            storage_interchange(ctr_storage_id,storage_id)
        else:
            #若有先移開上層container，再將目標container_id移至 storage_id
            upper = storage_db.find({'grid_id':grid_id,
                                     'relative_coords.ry':ctr_relative_coords[1],
                                     'relative_coords.rx':1,
                                     'container_id':{'$ne':""}})
            ##上層container info
            for u_i in upper:
                upper_container = u_i['container_id']
                upper_storage_id = u_i['storage_id']
            #將storage_id加入 ban 並判斷storage_id 是上層或下層 
            storage_id_eval = eval(storage_id)
            if storage_id_eval[1][0] == 1:  
                #storage_id在上層所以ban只有storage_id
                ban = [storage_id]
            else:
                #storage_id在下層所以ban有storage_id與storage_id的上層位置
                ban = [storage_id,str((storage_id_eval[0],(1,storage_id_eval[1][1],storage_id_eval[1][2])))]
            #收集所有可以放入的空位
            spot_candidates_ptr = storage_db.find({"arm_id":str(arm_id),'container_id':""})
            spot_candidates = []
            for sc in spot_candidates_ptr:
                if sc['storage_id'] in ban:
                    continue
                sc_key = json.loads(sc['storage_id'].replace('(', '[').replace(')', ']'))
                spot_candidates.append(sc_key)
            #計算最短路徑並排序
            major_list = G.shortest_paths(source = grid_id, target = [s[0] for s in spot_candidates], weights = dists)[0]
            spot_candidates_sort = []
            for spot_i in range(len(major_list)):
                spot_candidates_sort.append([spot_candidates[spot_i],major_list[spot_i]])
            spot_candidates_sort = sorted(spot_candidates_sort, key=lambda s: s[1])
            #選擇距離最短的位置當作移動目標
            moveto = spot_candidates_sort[0][0]
            moveto_storage_id = str((moveto[0],tuple(moveto[1])))
            #判斷空位是上層還是下層
            if moveto[1][0] == 1:
                #空位在上層，判斷下層是否有東西
                lower = storage_db.count_documents({'grid_id':moveto[0],
                                                    'relative_coords.ry':moveto[1][1],
                                                    'relative_coords.rx':0,
                                                    'container_id':{'$ne':""}})
                if lower == 0:
                    #空位下層有東西，放入下層
                    moveto[1][0] = 0
                    moveto_storage_id = str((moveto[0],tuple(moveto[1])))
            #將upper_container 移到 moveto 修改資料庫
            container_moveto(upper_container,moveto_storage_id)
            storage_interchange(upper_storage_id,moveto_storage_id)
            
            #將container_id移至 storage_id
            container_moveto(container_id,storage_id)
            storage_interchange(ctr_storage_id,storage_id)
    print("in container_goto container_id: "+str(container_id)+" is in storage_id: "+str(storage_id))

def container_exception():
    '''
    將containerdb進行例外處理
    狀態為waiting的改回db內 
    狀態為in_workstation的送回db 
    狀態為on_conveyor的等到打後送回倉庫
    '''
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    container_db = db["Containers"]
    container_candidates = container_db.find({"status":{"$ne":"in grid"}})
    exception_container_number = container_db.count_documents({"status":{"$ne":"in grid"}})
    print_string = str(exception_container_number)+" containers in exception "
    print_coler(print_string,"g")
    for container_info in container_candidates:
        container_id = container_info["container_id"]
        status = container_info["status"]
        #status : waiting , in_workstation , ws_to_conveyor , conveyor_to_ws
        if status == "waiting":
            #status 為 waiting
            product_push_container(container_id)
            container_set_status(container_id,"in grid")
        elif status == "in_workstation":
            #status 為 in_workstation
            #選擇放回去的arm_id
            index_putback = 1
            while index_putback:
                try:
                    arm_id = container_putback(container_id)
                    index_putback = 0
                except:
                    print_string = "restart find arm_id to putback container"
                    print_coler(print_string,"g")
                    index_putback = 1
                
            oi = get_time_string()
            value = (0,oi,0,container_id)
            lock_name = arm_id+ "_pid"
            lock_val = 1
            while lock_val:
                lock_id = acquire_lock_with_timeout(r, lock_name, acquire_timeout= 2, lock_timeout= 100)
                print("workstation_operate: waiting lock release " + lock_name)
                if lock_id != False:
                    lock_val = 0
            redis_data_update_db(arm_id,value)
            redis_dict_work_assign(arm_id)
            release_lock(r, lock_name, lock_id)
            arms_work_transmit.delay(arm_id)
        elif status == "ws_to_conveyor":
             #status 為 ws_to_conveyor
             #至電梯排隊存入
             print("ws_to_conveyor")
        else:
            #status 為 conveyor_to_ws
            #先到工作站再送回
            workstation_get.delay(container_id)
            #選擇放回去的arm_id
            index_putback = 1
            while index_putback:
                try:
                    arm_id = container_putback(container_id)
                    index_putback = 0
                except:
                    print_string = "restart find arm_id to putback container"
                    print_coler(print_string,"g")
                    index_putback = 1
                
            oi = get_time_string()
            value = (0,oi,0,container_id)
            lock_name = arm_id+ "_pid"
            lock_val = 1
            while lock_val:
                lock_id = acquire_lock_with_timeout(r, lock_name, acquire_timeout= 2, lock_timeout= 100)
                print("workstation_operate: waiting lock release " + lock_name)
                if lock_id != False:
                    lock_val = 0
            redis_data_update_db(arm_id,value)
            redis_dict_work_assign(arm_id)
            release_lock(r, lock_name, lock_id)
            arms_work_transmit.delay(arm_id)

'''workstation function'''
def workstation_assign():
    '''
    指派工作站ＩＤ (選擇工作量最少的工作站)
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
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
    '''
    新order工作產生
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
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
def workstation_addpick(order_id,container_id,prd,pqt):
    '''
    指派order 撿取container的contents與數量
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    work_info = workstation_db.aggregate([
                     {'$match': { 'work.'+order_id:{'$exists':"true"}}}])
    for ws_i in work_info:
        workstation_id = ws_i["workstation_id"]
        ws_work = ws_i['work']
    insert_pick = {}
    insert_pick[container_id] = {prd:pqt}
    ws_work[order_id]["container"].update(insert_pick)
    myquery = { "workstation_id": workstation_id }
    newvalues = { "$set": { "work."+order_id+".container."+container_id+"."+prd:pqt}}
    workstation_db.update(myquery,newvalues)
def workstation_get(container_id):
    '''
    工作站收到container
    '''
    # 
    container_set_status(container_id,'in_workstation')
    


def workstation_pick_info(container_id):
    '''
    工作站以從container撿取 order所需物品資訊
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
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
    

def workstation_workend(self, workstation_id,order_id):
    '''
    工作站workstation_id 內的 order_id刪除工作與工作站工作量減1
    '''
    print("In workstation workend workstation_id: "+workstation_id+" order_id: "+order_id)
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    r.set("celery-task-meta-" + self.request.id, self.request.id)
    #工作站檢取完後 刪除工作
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]    
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    r.set("celery-task-meta-" + self.request.id, self.request.id)
    try:
        client = pymongo.MongoClient(uri)
        db = client['ASRS-Cluster-0']
        workstation_db = db["Workstations"]
        ws = workstation_db.find({'workstation_id':workstation_id})
        myquery = { "workstation_id": workstation_id}
        #減工作量
        newvalues = {"$inc": { "workloads":-1}}
        workstation_db.update(myquery,newvalues)
        #刪訂單
        newvalues = { "$unset": {"work."+order_id:{}}}
        workstation_db.update(myquery,newvalues)
        if workstation_free(workstation_id):
            with open('參數檔.txt') as f:
                json_data = json.load(f)
            index_label = json_data["index_label"]
            index = json_data["index"]
            num = json_data["num"]
            workstation_open.delay(workstation_id,index_label,index,num)
        
    except:
        print_string = "restart workstation workend"
        print_coler(print_string,"g")
        workstation_workend.delay(workstation_id, order_id)
        Sigkill_func(self.request.id)
        return True
    

def workstation_order(workstation_id):
    '''
    工作站內訂單列表
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
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
    '''
    判斷工作站工作是否已完成
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    if len(workstation_db.find_one({"workstation_id":workstation_id})["work"])>0:
        return False
    else:
        return True

def workstation_exception():
    '''
    工作站例外處理
    會將還未到達的container先清空，以利後續重新撿取
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    workstation_candidates = workstation_db.find({"work":{"$ne":{}}})
    print_string = "workstation in exception "
    print_coler(print_string,"g")
    for wi in workstation_candidates:
        workstation_id = wi["workstation_id"]
        ws_work = wi["work"]
        for order_id , pick_content in ws_work.items():
            ws_work[order_id]["container"] = {}
        ws_workloads = len(ws_work)
        myquery = { "workstation_id": workstation_id }
        newvalues = { "$set": { "work": ws_work,"workloads":ws_workloads}}
        workstation_db.update(myquery,newvalues)

'''redis function'''
def redis_init():
    '''
    從storage裡面撈出資訊，並初始化redis內資訊
    '''
    arm_product_ptr = {}
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
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
                arm_product_ptr[si["arm_id"]][layer][k] = {si["container_id"]:v}
            
            
        else:
            for k,v in si["contents"].items():
                if k not in arm_product_ptr[si["arm_id"]][layer]:
                    arm_product_ptr[si["arm_id"]][layer][k] = {si["container_id"]:v}
                else:
                    arm_product_ptr[si["arm_id"]][layer][k].update({si["container_id"]:v})
    
    for key, value in arm_product_ptr.items():
        redis_dict_set(key,value)
# def redis_init():
#     #從storage裡面撈出資訊，並初始化redis內資訊
    
#     arm_product_ptr = {}
#     with open('參數檔.txt') as f:
#         json_data = json.load(f)
#     uri = json_data["uri"]
#     client = pymongo.MongoClient(uri)
#     db = client['ASRS-Cluster-0']
#     warehouse_db = db["Warehouses"]
#     for warehouse in warehouse_db.find():
#         nodes = dill.loads(warehouse["nodes"])
#         G = dill.loads(warehouse["G"])
#         dists = dill.loads(warehouse["dists"])
#     redis_dict_set("nodes",nodes)
#     redis_dict_set("G",G)
#     redis_dict_set("dists",dists)
    
#     storage_db = db["Storages"]
#     for si in storage_db.find():
#         storage_id = si['storage_id']
#         sid = json.loads(storage_id.replace('(', '[').replace(')', ']'))
#         layer = str(sid[1][0])
#         if si["arm_id"] not in arm_product_ptr:
#             works = PriorityQueue()
#             arm_product_ptr[si["arm_id"]] = {'workload':works.qsize(),'works':works}
#             arm_product_ptr[si["arm_id"]]["0"] = {}
#             arm_product_ptr[si["arm_id"]]["1"] = {}
#             for k,v in si["contents"].items():
#                 arm_product_ptr[si["arm_id"]][layer][k] = {"qt":v,"container_id":si["container_id"]}
            
            
#         else:
#             for k,v in si["contents"].items():
#                 if k not in arm_product_ptr[si["arm_id"]][layer]:
#                     arm_product_ptr[si["arm_id"]][layer][k] = {"qt":v,"container_id":si["container_id"]}
#                 else:
#                     arm_product_ptr[si["arm_id"]][layer][k] = {"qt":arm_product_ptr[si["arm_id"]][layer][k]["qt"]+v,"container_id":si["container_id"]}
    
#     for key, value in arm_product_ptr.items():
#         redis_dict_set(key,value)

def redis_arm_product_update():
    '''
    從storage裡面撈出資訊，並初始化redis內資訊
    '''
    arm_product_ptr = {}
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
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
                arm_product_ptr[si["arm_id"]][layer][k] = {si["container_id"]:v}
                arm_product_ptr[si["arm_id"]]["turnover"] += product_dict[k]['turnover']
            
        else:
            for k,v in si["contents"].items():
                if k not in arm_product_ptr[si["arm_id"]][layer]:
                    arm_product_ptr[si["arm_id"]][layer][k] = {si["container_id"]:v}
                    arm_product_ptr[si["arm_id"]]["turnover"] += product_dict[k]['turnover']
                else:
                    arm_product_ptr[si["arm_id"]][layer][k].update({si["container_id"]:v})
                    arm_product_ptr[si["arm_id"]]["turnover"] += product_dict[k]['turnover']
    
    for key, value in arm_product_ptr.items():
        redis_dict_set(key,value)


    
def redis_data_update_db(key,value):
    container_id =value[3]
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    one_dict = {}
    content = r.get(key)
    if content != None:
        one_dict = dill.loads(content)
        #更正工作量跟工作
        # one_dict['workload'] += 1
        one_dict["works"].put(value)
    r.set(key,dill.dumps(one_dict))

def redis_dict_work_assign(key):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    one_dict = {}
    content = r.get(key)
    if content != None:
        one_dict = dill.loads(content)
        #更正工作量跟工作
        one_dict['workload'] += 1
    r.set(key,dill.dumps(one_dict))



# def redis_pick_data_update(key,value,layer):
#     container_id =value[3]
#     r = redis.Redis(host='localhost', port=6379, decode_responses=False)
#     one_dict = {}
#     content = r.get(key)
#     if content != None:
#         one_dict = dill.loads(content)
#         #刪除 redis裡面的機器手臂可撿商品資訊
#         pop_k = []
#         for pid,pid_container_qt in one_dict[layer].items():
#             for pid_container,pid_qt in pid_container_qt.items():
#                 if container_id == pid_container:
#                     pop_k.append(pid)
#         for pid_pop in pop_k:
#             one_dict[layer][pid_pop].pop(container_id)
#             if one_dict[layer][pid_pop] == {}:
#                 one_dict[layer].pop(pid_pop)
#         #更正工作量跟工作
#         one_dict['workload'] += 1
#         one_dict["works"].put(value)
#     r.set(key,dill.dumps(one_dict))
    
# def redis_store_work_update(key,value):
#     container_id =value[3]
#     r = redis.Redis(host='localhost', port=6379, decode_responses=False)
#     one_dict = {}
#     content = r.get(key)
#     if content != None:
#         one_dict = dill.loads(content)        
#         #更正工作量跟工作
#         one_dict['workload'] += 1
#         one_dict["works"].put(value)
#     r.set(key,dill.dumps(one_dict))

# def redis_store_data_update(key,container_id,layer):
#     with open('參數檔.txt') as f:
#         json_data = json.load(f)
#     uri = json_data["uri"]    
#     r = redis.Redis(host='localhost', port=6379, decode_responses=False)
#     try:
#         client = pymongo.MongoClient(uri)
#         db = client['ASRS-Cluster-0']
#     except:
#         Sigkill_func(self.request.id)
#     container_db = db["Containers"]
#     container_content = container_db.find_one({"container_id":container_id})['contents']
#     one_dict = {}
#     content = r.get(key)
#     layer = str(layer)
#     if content != None:
#         one_dict = dill.loads(content)
#         #新增 redis裡面的機器手臂可撿商品資訊
#         for content_k,content_v in container_content.items():
#             if content_k in one_dict[layer]:
#                 one_dict[layer][content_k].update({container_id:content_v})
#             else:
#                 one_dict[layer][content_k] = {container_id:content_v}
#     r.set(key,dill.dumps(one_dict))

# def redis_data_update(key,value,layer):
#     container_id =value[3]
#     r = redis.Redis(host='localhost', port=6379, decode_responses=False)
#     one_dict = {}
#     content = r.get(key)
#     if content != None:
#         one_dict = dill.loads(content)
#         #刪除 redis裡面的機器手臂可撿商品資訊
#         pop_k = []
#         for content_k,content_v in one_dict[layer].items():
#             if container_id in content_v["container_id"]:
#                 pop_k.append(content_k)
#         for popk in pop_k:
#             one_dict[layer].pop(popk)
#         #更正工作量跟工作
#         one_dict['workload'] += 1
#         one_dict["works"].put(value)
#     r.set(key,dill.dumps(one_dict))

def redis_data_interchange(src_storage_id,dst_storage_id,container_id):
    #移動箱子更新redis data
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]    
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    try:
        client = pymongo.MongoClient(uri)
        db = client['ASRS-Cluster-0']
    except:
        Sigkill_func(self.request.id)
    container_db = db["Containers"]
    storage_db = db["Storages"]
    container_content = container_db.find_one({"container_id":container_id})['contents']
    arm_id = storage_db.find_one({"storage_id":src_storage_id})['arm_id']
    src_storage_id_eval = eval(src_storage_id)
    dst_storage_id_eval = eval(dst_storage_id)
    layer_src = src_storage_id_eval[1][0]
    layer_dst = dst_storage_id_eval[1][0]
    one_dict = {}
    content = r.get(arm_id)
    if content != None:
        one_dict = dill.loads(content)
        #刪除 redis裡面的機器手臂可撿商品資訊
        pop_k = []
        for pid,pid_container_qt in one_dict[layer_src].items():
            for pid_container,pid_qt in pid_container_qt.items():
                if container_id == pid_container:
                    pop_k.append(pid)
        for pid_pop in pop_k:
            one_dict[layer_src][pid_pop].pop(container_id)
            if one_dict[layer_src][pid_pop] == {}:
                one_dict[layer_src].pop(pid_pop)
        for content_k,content_v in container_content.items():
            if content_k in one_dict[layer_dst]:
                one_dict[layer_dst][content_k].update({container_id:content_v})
            else:
                one_dict[layer_dst][content_k] = {container_id:content_v}
    r.set(key,dill.dumps(one_dict))
    
def redis_dict_turnover_pop(key,container_id):    
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]    
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    try:
        client = pymongo.MongoClient(uri)
        db = client['ASRS-Cluster-0']
    except:
        Sigkill_func(self.request.id)
    container_db = db["Containers"]
    value = redis_dict_get(key)
    container_turnover = container_db.find_one({"container_id":container_id})['turnover']
    value["turnover"] -= container_turnover
    redis_dict_set(key, value)

def redis_dict_turnover_push(key,container_id):    
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]    
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    try:
        client = pymongo.MongoClient(uri)
        db = client['ASRS-Cluster-0']
    except:
        Sigkill_func(self.request.id)
    container_db = db["Containers"]
    value = redis_dict_get(key)
    container_turnover = container_db.find_one({"container_id":container_id})['turnover']
    value["turnover"] += container_turnover
    redis_dict_set(key, value)
    
def redis_dict_get(key):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)  
    one_dict = {}
    content = r.get(key)
    if content != None:
        one_dict = dill.loads(content)
    return one_dict
def redis_dict_get_work(key):
    value = redis_dict_get(key)
    if value['works'].empty():
        return "empty"
    else:
        before = value['works'].qsize()
        container_info = value['works'].get(timeout = 3.0)
        redis_dict_set(key, value)
        after = value['works'].qsize()
        if after<before:
            print("redis_dict_get_work is good")
        else:
            print("redis_dict_get_work is bad")
        return container_info
def redis_dict_get_turnover(key):
    value = redis_dict_get(key)
    return value["turnover"]

def redis_dict_work_over(key):
    value = redis_dict_get(key)
    value['workload'] -= 1
    redis_dict_set(key, value)

def redis_dict_set(key, value):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)  
    r.set(key,dill.dumps(value))
    
def redis_dict_all_keys():
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    all_keys_b = r.keys()
    all_keys = []
    for key in all_keys_b:
        if "(" in key.decode("utf-8")and "_pid"not in key.decode("utf-8")and "_arm2transmit"not in key.decode("utf-8")and "lock" not in key.decode("utf-8") and "_worklock" not in key.decode("utf-8"):
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





def Sigkill_func(self, task_id):
    celery.task.control.revoke(task_id, terminate=True, signal='SIGKILL')
    return True

def print_coler(string,color):
    if color == "r":
        print("\033[1;31m"+string+"\033[0m")
    elif color == "g":
        print("\033[1;32m"+string+"\033[0m")
    elif color == "b":
        print("\033[1;34m"+string+"\033[0m")
    else:
        print("\033[30m"+string+"\033[0m")

def replenish_need_count():
    '''
    計算需 replenish 的商品數
    '''
    with open('參數檔.txt') as f:
            json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    product_db = db["Products"]
    need = product_db.find({"$and":[{"$where" : "this.quantity < this.replenish"},{"status":"normal"}] })
    return need.count()

def replenish_assign(num):
    '''
    分配 replenish 的商品
    '''
    with open('參數檔.txt') as f:
            json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    product_db = db["Products"]
    
    need_replenish_product_candidates = {}
    for pi in product_db.find({"$and":[{"$where" : "this.quantity < this.replenish"},{"status":"normal"}] }):
        need_replenish_product_candidates[pi["product_id"]] = pi["replenish"] - pi["quantity"]
    
    need_replenish_product_score_list = sorted(need_replenish_product_candidates.items(),key=lambda item:item[1],reverse=True)
    output = []
    for replenish_product in need_replenish_product_score_list:
        if num<=0:
            break
        else:
            product_id = replenish_product[0]
            myquery = { "product_id": product_id }
            newvalues = { "$set": { "status": "replenishing"}}
            product_db.update(myquery,newvalues)
            output.append(product_id)
            num -= 1
    if num >0:
        print_string = "只有 "+str(20 - num)+ " 需要補貨"
        print_coler(print_string,"g")
    return output
    
def replenish_id_get(a):
    '''
    給予時間係數
    '''
    now = time.localtime()
    output = "r"+str(now[0]) + str(now[1]).zfill(2) + str(now[2]).zfill(2) + \
            str(now[3]).zfill(2) + str(now[4]).zfill(2) + str(now[5]).zfill(2) + str(a).zfill(2) + "-1"
    return output

def workstation_new_replenish(workstation_id,product_list):
    '''
    新replenish工作產生
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    product_db = db["Products"]
    replenish_product_dict ={}
    for pi in product_db.find({"product_id":{"$in":product_list}}):
        replenish_product_dict[pi["product_id"]] = pi
    ws = workstation_db.find({'workstation_id':workstation_id})
    for ws_i in ws:
        ws_workstation_id = ws_i['workstation_id']
        ws_work = ws_i['work']
        ws_workloads = ws_i['workloads']
    num = 0
    replenish_id_list = []
    replenish_dict = {}
    #依據商品補至replenish的1.3倍
    for product_id in product_list:
        replenish_dict[replenish_id_get(num)] = {"product_id":product_id,
                                                 "qt":int(replenish_product_dict[product_id]["replenish"]*1.3 - replenish_product_dict[product_id]["quantity"]) }
        num += 1
    replenish_work = {}
    for replenish_id,replenish_info in replenish_dict.items():
        replenish_work[replenish_id] = {}
        replenish_work[replenish_id]["prd"] = {}
        replenish_work[replenish_id]["prd"][replenish_info["product_id"]] = {"qt" : replenish_info["qt"]}
        replenish_work[replenish_id]["container"] = {}
        ws_work.update(replenish_work)
        ws_workloads += 1
    myquery = { "workstation_id": workstation_id }
    newvalues = { "$set": { "work": ws_work,"workloads":ws_workloads}}
    workstation_db.update(myquery,newvalues)
    

    
def workstation_add_replenish(replenish_id,container_id,prd,pqt):
    '''
    指派order 撿取container的contents與數量
    '''
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    work_info = workstation_db.aggregate([
                     {'$match': { 'work.'+replenish_id:{'$exists':"true"}}}])
    for ws_i in work_info:
        workstation_id = ws_i["workstation_id"]
        ws_work = ws_i['work']
    insert_pick = {}
    insert_pick[container_id] = {prd:pqt}
    ws_work[replenish_id]["container"].update(insert_pick)
    myquery = { "workstation_id": workstation_id }
    newvalues = { "$set": { "work."+replenish_id+".container."+container_id+"."+prd:pqt}}
    workstation_db.update(myquery,newvalues)
    

