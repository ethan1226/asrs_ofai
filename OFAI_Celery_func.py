# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 14:51:55 2021

@author: WeiKangLiang
"""

from utils.scheduling_utils_db import *

from queue import PriorityQueue
from celery import Celery
import redis
import json
import copy
import pymongo
#import time
#import dill

broker = 'redis://127.0.0.1:6379'  
backend = 'redis://127.0.0.1:6379/0'  
OFAI_Celery_func = Celery('OFAI_Celery_func', broker=broker, backend=backend)

'''
workstation_tasks
'''
@OFAI_Celery_func.task(bind=True)
def redis_arm_product_update(self):
    arm_product_ptr = {}
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    try:
        client = pymongo.MongoClient(uri)
        db = client['ASRS-Cluster-0']
    except:
        Sigkill_func(self.request.id)
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

@OFAI_Celery_func.task(bind=True)
def order_assign(self, index_label,index,num):
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    try:
        client = pymongo.MongoClient(uri)
        db = client['ASRS-Cluster-0']
    except:
        Sigkill_func(self.request.id)
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
            myquery = { "order_id": k }
            newvalues = { "$set": { "status": "workstation"}}
            order_db.update(myquery,newvalues)
            num -= 1
    return output




@OFAI_Celery_func.task(bind=True)
def order_pick(self, workstation_id):
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
                    
                        
                        
                        
                        

# @OFAI_Celery_func.task
# def workstation_open(workstation_id,index_label,index,num):
#     r = redis.Redis(host='localhost', port=6379, decode_responses=False)  
#     lock_name = "order_assignment"
#     #此訂單池還有訂單
#     while order_processing_count(index_label,index)>0:
#         print("訂單持還有訂單")
#         #工作站是否還有工作
#         if workstation_free(workstation_id):
#             print("工作站id: ",workstation_id," 沒有訂單")
#             #取得訂單池ＤＢ鑰匙
#             order_lock = acquire_lock_with_timeout(r,lock_name, acquire_timeout=3, lock_timeout=30)
#             #取得失敗
#             if order_lock != False:
#                 #分配訂單
#                 order_l = str(order_assign(index_label,index,num))
#                 print("訂單池給予訂單")
#                 order_l_eval = eval(order_l)
#                 #釋放訂單池ＤＢ
#                 release_lock(r, lock_name, order_lock)
#                 workstation_newwork_prd(workstation_id,order_l_eval)
#                 print("工作站id: ",workstation_id," 撿取訂單項目輸入完成")
#                 print("工作站id: ",workstation_id," 撿取開始")
#                 #訂單商品選取撿出container號
#                 order_pick.delay(order_l)
#                 print("工作站id: ",workstation_id," 撿取結束")
#         else:
#             print("工作站id: ",workstation_id," 還有訂單")
#             order_l = workstation_order(workstation_id)
#             print("工作站id: ",workstation_id," 撿取開始")
#             order_pick.delay(order_l)
#             print("工作站id: ",workstation_id," 撿取結束")
#     print("訂單池沒有訂單")

@OFAI_Celery_func.task(bind=True)
def workstation_open(self, workstation_id,index_label,index,num):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    lock_name = "order_assignment"
    if r.exists(workstation_id+"open") == 0:
        r.set(workstation_id+"open","")
        #此訂單池還有訂單
        if order_processing_count(index_label,index)>0:
            print("訂單池還有訂單")
            #工作站是否還有工作
            if workstation_free(workstation_id):
                print("工作站id: ",workstation_id," 沒有訂單")
                #取得訂單池ＤＢ鑰匙
                order_lock = acquire_lock_with_timeout(r,lock_name, acquire_timeout=3, lock_timeout=30)
                #取得失敗
                if order_lock != False:
                    #分配訂單
                    order_l = str(order_assign(index_label,index,num))
                    print("訂單池給予訂單")
                    order_l_eval = eval(order_l)
                    #釋放訂單池ＤＢ
                    release_lock(r, lock_name, order_lock)
                    workstation_newwork_prd(workstation_id,order_l_eval)
                    print("工作站id: ",workstation_id," 撿取訂單項目輸入完成")
                    print("工作站id: ",workstation_id," 撿取開始")
                    #訂單商品選取撿出container號
                    order_pick.delay(workstation_id)
                    workstation_open.delay(workstation_id,index_label,index,num)
            else:
                print("工作站id: ",workstation_id," 還有訂單")
                # order_l = workstation_order(workstation_id)
                print("工作站id: ",workstation_id," 撿取開始")
                order_pick.delay(workstation_id)
                workstation_open.delay(workstation_id,index_label,index,num)
        else:
            print("訂單池沒有訂單")
            r.delete(workstation_id+"open")
    else:
        print("工作站id: ",workstation_id,"調配商品中")
        if workstation_free(workstation_id):
            r.delete(workstation_id+"open")
            print("工作站id: ",workstation_id," 完成 補新訂單")
        workstation_open.delay(workstation_id,index_label,index,num)
'''
workstation_tasks
'''

'''
robot_arm_tasks
'''
@OFAI_Celery_func.task(bind=True)
def arms_store(self, container_id,arm_id):
    #先從arm_id找出可放入的位置在將container放入並更新資料庫
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    try:
        client = pymongo.MongoClient(uri)
        db = client['ASRS-Cluster-0']
    except:
        Sigkill_func(self.request.id)
    product_db = db["Products"]
    container_db = db["Containers"]
    storage_db = db["Storages"]
    #找arm_id上的可放入的空格
    print("container_id",container_id,"arm_id",arm_id)
    arm_id = str(arm_id)
    storage_id = find_empty_arms_sid(arm_id)
    #storage＿id 放入 container_id
    storage_push(storage_id,container_id)
    #container_id 修改資訊(移動到storage_id & status to in grid)
    # print("container_id: "+container_id+" storage_id: "+storage_id)
    container_moveto(container_id,storage_id)
    container_status(container_id,"in_grid")
    #將 container_id 內商品更新 product
    product_push_container(container_id)
    print("Storaging container: container_id: " + container_id + "'s state is changed to in_grid")

    
@OFAI_Celery_func.task(bind=True)
def arms_pick(self, container_id):
    #機器手臂撿取container 並會判斷他上方是否有阻礙的container會先行移開在撿取目標container並更新資料庫
    
    ##redis get
    G = redis_dict_get("G")
    nodes = redis_dict_get("nodes")
    dists = redis_dict_get("dists")
    
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    try:
        client = pymongo.MongoClient(uri)
        db = client['ASRS-Cluster-0']
    except:
        Sigkill_func(self.request.id)
    container_db = db["Containers"]
    storage_db = db["Storages"]
   
    container_info = container_db.find_one({"container_id":container_id})
    grid_id = container_info['grid_id']
    if container_info['relative_coords']['rx'] == 1:
        print("在上層,直接取出")
        #在上層直接取出
        container_status(container_id,'on_conveyor')
        container_grid(container_id,-1)
    else:
        print("在下層")
        #在下層先判斷是否上層有東西
        #上層物品
        upper = storage_db.find({'grid_id':grid_id,
                                 'relative_coords.ry':container_info['relative_coords']['ry'],
                                 'relative_coords.rx':1,
                                 'container_id':{'$ne':""}})
        
        if upper.count() == 0 :
            print("上層沒有東西,直接取出")
            #上層沒有東西,直接取出
            container_status(container_id,'on_conveyor')
            container_grid(container_id,-1)
        else:
            print("上層有東西,先移開後取出")
            #上層有東西,先移開後取出
            for u_i in upper:
                upper_container = u_i['container_id']
                upper_storage_id = u_i['storage_id']
            print("container_id: "+str(container_id)+" grid_id: "+str(grid_id)+" arm_id: "+str((nodes[grid_id]['aisle_index'][0], nodes[grid_id]['aisle_index'][2])))
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
    container_operate.delay(container_id)
    print("Picking container: container_id: " + container_id + "'s state is changed to on_conveyor")


@OFAI_Celery_func.task(bind=True)
def arms_work_transmit(self, arm_id):
    # arms_dict = {}
    # arms_dict[arm_id] = redis_dict_get(arm_id)    
    '''
    取手臂鎖
    '''
    conn = redis.Redis(host='localhost', port=6379, decode_responses=False)  
    lock_name = arm_id
    lock_val = 1
    while lock_val:
        lock_id = acquire_lock_with_timeout(conn, lock_name, acquire_timeout= 2, lock_timeout= 100)
        if lock_id != False:
            lock_val = 0
    '''
    取container資訊
    '''
    #print(arms_dict[str(arm_id)]['works'].get())
    #print(eval(arms_dict[str(arm_id)]['works'].get()))
    #container_info = eval(arms_dict[str(arm_id)]['works'].get())
    container_info = redis_dict_get_work(arm_id)
    #result = det_pick_put.delay(container_info) #True: pick; False: store
    '''
    判斷是要撿取還是存取container並執行
    '''
    if container_info[0] == 1:
        arms_pick.delay(container_info[3])
    else:
        arms_store.delay(container_info[3],arm_id)
    
    '''
    釋放手臂鎖    
    '''
    release_lock(conn, lock_name, lock_id)
    return True
'''
robot_arm_tasks
'''

'''
robot_arm_tasks
'''
@OFAI_Celery_func.task(bind=True)
def workstation_get(self, container_id):
    # 工作站收到container
    container_status(container_id,'in_workstation')

@OFAI_Celery_func.task(bind=True) 
def order_check(self, workstation_id, order_id):
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    try:
        client = pymongo.MongoClient(uri)
        db = client['ASRS-Cluster-0']
    except:
        Sigkill_func(self.request.id)
    workstation_db = db["Workstations"]
    workstation_info = workstation_db.find({'workstation_id':workstation_id})
    for ws_i in workstation_info:
        ws_work = ws_i['work']
    if ws_work[order_id]["prd"] == {}:
        return True
    else:
        return False
    
@OFAI_Celery_func.task(bind=True)
def workstation_workend(self, workstation_id,order):
    #工作站檢取完後 刪除工作
    uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
    try:
        client = pymongo.MongoClient(uri)
        db = client['ASRS-Cluster-0']
    except:
        Sigkill_func(self.request.id)
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

@OFAI_Celery_func.task(bind=True)
def container_operate(self, container_id):
    workstation_get.delay(container_id)
    order_id, workstation_id = workstation_pick(container_id)
    if order_check(workstation_id, order_id):
        workstation_workend.delay(workstation_id, order_id)
    arm_id = container_putback(container_id)
    oi = get_time_string()
    value = (0,oi,0,container_id)
    redis_data_update(arm_id,value)
    arms_work_transmit.delay(arm_id)
    return True
'''
robot_arm_tasks
'''

@OFAI_Celery_func.task(bind=True)
def Sigkill_func(self, task_id):
    celery.task.control.revoke(task_id, terminate=True, signal='SIGKILL')
    return True
