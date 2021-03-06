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
@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def order_pick(self, workstation_id):
    '''
    會先收集工作站內訂單
    先將訂單商品先合併商品資訊
    在找到適當的container放入工作站
    搜尋方式從redis改成直接搜尋db
    '''
    print("workstation id: "+str(workstation_id)+"start order_pick")
    # print("\033[1;34m Ethansay order_pick start \033[0m")
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    container_db = db["Containers"]
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
    #排序機器手臂工作量
    arm_key_list = arm_work_sort_list()
    #依商品順序處理 內容包含數量與需求訂單
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
        print("Order picking workstation id: "+str(workstation_id)+" 剩餘訂單商品數量: "+str(len(prd_list))+" 準備撿取pid: "+str(pid))
        oi = get_time_string()
        numbering = 0
        isbreak = False
        #內外層搜尋
        for layer in range(1,-1,-1):
            #是否已找到商品container
            if not isbreak:
                print("workstation id: "+str(workstation_id)+" pid: "+pid+" 搜尋 "+str(layer)+" 層")
                #找有pid的container 在layer層
                container_candidates = container_db.aggregate([{"$match": { "relative_coords.rx":layer,
                                                                            "contents."+pid:{'$exists':"true"},
                                                                            "status":"in grid"}}])
                layer_container_workloads_list = []
                #排序找到的container所在的arm workloads
                for ci in container_candidates:
                    arm_id = container_armid(ci["container_id"])
                    if arm_id != "":
                        layer_container_workloads_list.append([ci["container_id"],arm_id,arm_workloads(arm_id),elevator_workloads(arm_id)])
                    else:
                        # print("error no container in storage container_id is "+str(ci["container_id"]))
                        print_string = "workstation id: "+str(workstation_id)+" Ethansay no container in storage container_id is " +str(ci["container_id"])
                        print_coler(print_string,"g")
                layer_container_workloads_list_sort = sorted(layer_container_workloads_list, key=lambda s: s[2])
                #若有適合的container則進行撿取
                if layer_container_workloads_list_sort  != []:
                    #依序使用適當的container
                    while len(layer_container_workloads_list_sort) > 0:
                        container_choosed = layer_container_workloads_list_sort[0]
                        layer_container_workloads_list_sort.remove(container_choosed)
                        arm_id = container_choosed[1]
                        container_id = container_choosed[0]
                        #防止container被同時多個工作站選取
                        container_lock_name = container_id + "_pid"
                        container_lock = acquire_lock_with_timeout(r, container_lock_name, acquire_timeout=2, lock_timeout=172800)
                        if container_lock != False:
                            if container_status(container_id)=='in grid':
                                #改container_db狀態
                                container_waiting(container_id)
                                #release container lock
                                release_lock(r, container_lock_name, container_lock)
                                #先判斷是否同一container是否有其他商品也在其他訂單商品列表中
                                container_bundle,container_contents = container_otherprd(container_id,prd_list)
                                #是否有撿取此container
                                pick_container = 0
                                #將有在同一工作站的同一箱訂單商品進行加入工作站工作
                                for bundle_pid in container_bundle:
                                    #訂單商品bundle_pid需求資訊
                                    pid_order_dict = prd_content[bundle_pid]["order"]
                                    print("pid: "+str(bundle_pid)+"  order: "+str(pid_order_dict))
                                    pid_pick_order_list = []
                                    for order_id,pqt in pid_order_dict.items():
                                        if container_contents[bundle_pid] >= pqt and container_contents[bundle_pid] > 0:
                                            #若container內pid商品數量還夠
                                            #container內pid商品數量撿出
                                            container_contents[bundle_pid] -= pqt
                                            #工作站增加要撿取之container與商品pid資訊
                                            print("workstation id: "+str(workstation_id)+" order picked order id: "+str(order_id)+" container_id: "+str(container_id)+
                                                  " pid: " + str(bundle_pid)+" qt: "+str(pqt))
                                            workstation_addpick(order_id,container_id,bundle_pid,pqt)
                                            pick_container += 1
                                            #撿完的訂單刪除
                                            pid_pick_order_list.append(order_id)
                                            #刪除訂單需求商品數量
                                            prd_content[bundle_pid]["order"][order_id] -= pqt
                                            prd_content[bundle_pid]["qt"] -= pqt
                                        elif container_contents[bundle_pid] < pqt and container_contents[bundle_pid] > 0:
                                            #若container內pid商品數量不足訂單需求數量則撿出鄉內全部給此訂單
                                            pic_qt = container_contents[bundle_pid]
                                            container_contents[bundle_pid] -= container_contents[bundle_pid]
                                            print_string = "workstation id: "+str(workstation_id)+ " container qt not enough for order picked order id: "+\
                                                            str(order_id)+" container_id: "+str(container_id)+" pid: " + str(bundle_pid)+" qt: "+str(pic_qt)
                                            print_coler(print_string,"b")
                                            workstation_addpick(order_id,container_id,bundle_pid,pic_qt)
                                            pick_container += 1
                                            #刪除訂單需求商品數量
                                            prd_content[bundle_pid]["order"][order_id] -= pic_qt
                                            prd_content[bundle_pid]["qt"] -= pic_qt
                                        else:
                                            print_string = "workstation id: "+str(workstation_id)+ "container_id: "+str(container_id)+" pid: "+\
                                                            str(bundle_pid)+" is empty"
                                            print_coler(print_string,"b")
                                            
                                    #將撿出的被訂單刪除
                                    for pop_order in pid_pick_order_list:
                                        print("workstation id: "+str(workstation_id)+" 已撿取訂單: "+pop_order+" pid: "+bundle_pid)
                                        prd_content[bundle_pid]["order"].pop(pop_order,None)
                                    #若商品已無訂單需求則刪除商品
                                    if prd_content[bundle_pid]["order"] == {}:
                                        prd_list.remove(bundle_pid)
                                if pick_container >0:
                                    print("workstation id: "+str(workstation_id)+" this container to pick for "+str(pick_container)+" orders")
                                    value = (1,oi,numbering,container_id)
                                    numbering += 1
                                    #更新對應redis
                                    arms_data_lock = arm_id+ "_pid"
                                    lock_val = 1
                                    while lock_val:
                                        lock_id = acquire_lock_with_timeout(r, arms_data_lock, acquire_timeout= 2, lock_timeout= 172800)
                                        print("更新對應redis waiting lock release " + arms_data_lock)
                                        if lock_id != False:
                                            lock_val = 0
                                    print("order_pick get _pid key " + arms_data_lock + " " + lock_id)
                                    
                                    redis_data_update_db(arm_id,value)
                                    print("in order_pick redis_data_update arm_id: "+str(arm_id)+" update value: "+str(value))
                                    redis_dict_work_assign(arm_id)
                                    print("order_pick assign arm_id: "+str(arm_id)+" workload +1")
                                    result = release_lock(r, arms_data_lock, lock_id)
                                    if result:
                                        print("order_pick release _pid lock" + arms_data_lock)
                                    else:
                                        print_string = "order_pick release _pid lock fail" + arms_data_lock
                                        print_coler(print_string,"g")
                                    #有訂單需求才送出工作
                                    print("workstation id: "+str(workstation_id)+" oi: "+str(oi)+" arm_id: "+str(arm_id)+" layer: "+str(layer)+
                                          " pid: "+str(pid)+" container_id: "+container_id)
                                    arms_work_transmit.apply_async(args = [arm_id], priority = medium_priority)
                                else:
                                    #container 內沒有訂單需求
                                    container_set_status(container_id,'in grid')
                                    print_string = "container_id: " + str(container_id)+" 內沒有訂單需求狀態改回in grid"
                                    print_coler(print_string,"g")
                                # release_lock(r, lock_name, arm_product_lock)
                                
                        else:
                            layer_container_workloads_list_sort.insert(4,container_choosed)
                            release_lock(r, container_lock_name, container_lock)
                        if pid not in prd_list:
                            print("workstation id: "+str(workstation_id)+" pid: "+str(pid)+" out of prd_list")
                            isbreak = True
                            break
            else:
                print("workstation id: "+str(workstation_id)+" 商品: "+str(pid)+" 搜尋完畢")
    #訂單商品處理結束
    print("workstation id: "+str(workstation_id)+" order pick finished wait next task")
    r.delete(workstation_id+"open")
                    
                        
                        
                        
                        



@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def workstation_open(self, workstation_id,index_label,index,num,work_type):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    r.set("celery-task-meta-" + self.request.id, self.request.id)
    lock_name = "order_assignment"
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
    uri = json_data["uri"]
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]

    if work_type == "pick":
        myquery = { "workstation_id": workstation_id }
        newvalues = { "$set": { "type":"pick"}}
        workstation_db.update(myquery,newvalues)
        print("workstation_id : "+str(workstation_id)+" is picking work")
        if r.exists(workstation_id+"open") == 0:
            r.set(workstation_id+"open","")
            #此訂單池還有訂單
            if order_processing_count(index_label,index)>0:
                print("訂單池還有訂單")
                #工作站是否還有工作
                if workstation_free(workstation_id):
                    print("工作站id: "+str(workstation_id)+" 沒有訂單")
                    #取得訂單池ＤＢ鑰匙
                    order_lock = acquire_lock_with_timeout(r,lock_name, acquire_timeout=3, lock_timeout=172800)
                    #取得失敗
                    if order_lock != False:
                        #分配訂單 
                        if container_movement()<30:
                            #箱子移動數少依時間配單
                            order_l = str(order_assign(index_label,index,num))
                            print("目前 container 移動正常 使用common order assign ")
                        else:
                            #若目前箱子正在移動的數量太多會先分配商品在同一箱的訂單ｓ
                            order_l = str(order_assign_crunch(index_label,index,num))
                            print("目前 container 移動擁擠 使用crunch order assign ")
                        print("分配訂單為："+order_l)
                        print("訂單池給予訂單數為: "+str(len(eval(order_l))))
                        order_l_eval = eval(order_l)
                        #釋放訂單池ＤＢ
                        release_lock(r, lock_name, order_lock)
                        workstation_newwork_prd(workstation_id,order_l_eval)
                        print("工作站id: "+str(workstation_id)+" 撿取訂單項目輸入完成")
                        print("工作站id: "+str(workstation_id)+" 撿取開始")
                        #訂單商品選取撿出container號
                        order_pick.apply_async(args=[workstation_id], priority = low_priority)
                        #workstation_open.delay(workstation_id,index_label,index,num)
                else:
                    print("工作站id: "+str(workstation_id)+" 還有訂單")
                    # order_l = workstation_order(workstation_id)
                    print("工作站id: "+str(workstation_id)+" 撿取開始")
                    order_pick.apply_async(args=[workstation_id], priority = low_priority)
                    #workstation_open.delay(workstation_id,index_label,index,num)
            else:
                print("訂單池沒有訂單")
                myquery = { "workstation_id": workstation_id }
                newvalues = { "$set": { "type":"free"}}
                workstation_db.update(myquery,newvalues)
                r.delete(workstation_id+"open")
        else:
            print("工作站id: "+str(workstation_id)+"調配商品中")
            if workstation_free(workstation_id):
                r.delete(workstation_id+"open")
                print("工作站id: "+str(workstation_id)+" 完成 補新訂單")
    elif work_type == "replenish":
        print("workstation_id : "+str(workstation_id)+" is replenishing work")
        myquery = { "workstation_id": workstation_id }
        newvalues = { "$set": { "type":"replenish"}}
        workstation_db.update(myquery,newvalues)
        if r.exists(workstation_id+"open") == 0:
            r.set(workstation_id+"open","")
            #此訂單池還有訂單
            if replenish_need_count() > 0:
                print("還有商品需進行補貨")
                #工作站是否還有工作
                if workstation_free(workstation_id):
                    print("工作站id: "+str(workstation_id)+" 沒有補貨單")
                    #取得訂單池ＤＢ鑰匙
                    order_lock = acquire_lock_with_timeout(r,lock_name, acquire_timeout=3, lock_timeout=172800)
                    #取得失敗
                    if order_lock != False:
                        #分配補貨單 
                        replenish_l = str(replenish_assign(num))
                        print("分配補貨商品為："+replenish_l)
                        print("需補貨商品數為: "+str(len(eval(replenish_l))))
                        replenish_l_eval = eval(replenish_l)
                        #釋放訂單池ＤＢ
                        release_lock(r, lock_name, order_lock)
                        workstation_new_replenish(workstation_id,replenish_l_eval)
                        print("工作站id: "+str(workstation_id)+" 補貨項目輸入完成")
                        print("工作站id: "+str(workstation_id)+" 補貨開始")
                        #訂單商品選取撿出container號
                        replenish_pick.apply_async(args=[workstation_id], priority = low_priority)
                        #workstation_open.delay(workstation_id,index_label,index,num)
                else:
                    print("工作站id: "+str(workstation_id)+" 還有商品需補貨" )
                    # order_l = workstation_order(workstation_id)
                    print("工作站id: "+str(workstation_id)+" 補貨開始")
                    replenish_pick.apply_async(args=[workstation_id], priority = low_priority)
                    #workstation_open.delay(workstation_id,index_label,index,num)
            else:
                print("沒有商品需補貨")
                r.delete(workstation_id+"open")
                myquery = { "workstation_id": workstation_id }
                newvalues = { "$set": { "type":"free"}}
                workstation_db.update(myquery,newvalues)
        else:
            print("工作站id: "+str(workstation_id)+"商品補貨中")
            if workstation_free(workstation_id):
                r.delete(workstation_id+"open")
                print("工作站id: "+str(workstation_id)+" 完成 補新商品並進行補貨")
    else:
        print("undefine work type")
        # workstation_open.delay(workstation_id,index_label,index,num)
'''
workstation_tasks
'''

'''
robot_arm_tasks
'''
@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800, priority = 0)
def arms_store(self, container_id,arm_id):
    #先從arm_id找出可放入的位置在將container放入並更新資料庫
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    acc_rate = json_data["acc_rate"]
    elevator_speed = json_data["elevator_speed"]
    arm_speed = json_data["arm_speed"]
    uri = json_data["uri"]
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    #r.set("celery-task-meta-" + self.request.id, self.request.id)
    G = redis_dict_get("G")
    nodes = redis_dict_get("nodes")
    dists = redis_dict_get("dists")
    #抓手臂鎖，確定目前沒工作後開始做這一項工作
    arm_lock_name = str(arm_id) + "_worklock"
    arm_lock = False
    waiting_time = 1
    while not arm_lock:
        arm_lock = acquire_lock_with_timeout(r, arm_lock_name, acquire_timeout=2, lock_timeout=172800)
        start_time = datetime.datetime.now()
        result = waiting_func(waiting_time, start_time)
        while not result[0]:
            result = waiting_func(waiting_time, start_time)
    print("arms_store 搶到手臂鎖" + str(arm_id) + "，將揀取container_id: " + container_id)
    #手臂結束工作
    start_work_time = datetime.datetime.now()
    try:
        client = pymongo.MongoClient(uri)
        db = client['ASRS-Cluster-0']
    except:
        Sigkill_func(self.request.id)
    product_db = db["Products"]
    container_db = db["Containers"]
    storage_db = db["Storages"]
    #找arm_id上的可放入的空格
    print("arms_store container_id: "+str(container_id)+" arm_id: "+str(arm_id))
    arm_id = str(arm_id)
    storage_id = find_empty_arms_sid_by_turnover(arm_id,container_id)
    if storage_id == "":
        print_string = " in arm store storage_id is empty"
        print_coler(print_string,"g")
    
    #手臂去電梯口等電梯
    grid_id = eval(storage_id)[0]
    elevator_grid_id = db["Storages"].find_one({"arm_id":arm_id})["elevator_id"]
    elevator_number = eval(str(arm_id))[0]
    elevator_lock_name = "ElevatorIn" + str(elevator_number)
    container_height = eval(arm_id)[1]
    arm_distance = G.shortest_paths(source = grid_id, target = elevator_grid_id, weights = dists)[0]
    arm_moving_time = arm_distance[0] / arm_speed / acc_rate
    print("手臂"+ arm_id + " " + str(arm_moving_time) +"秒後去電梯取貨 container_id:" + container_id)
    r.set("arms_store_" + elevator_lock_name +"_content", container_id)

    start_time = datetime.datetime.now()
    result = waiting_func(arm_moving_time, start_time)
    while not result[0]:
        result = waiting_func(arm_moving_time, start_time)
    print("手臂" + arm_id + "抵達電梯")
    
    #送到目標樓層上，加入存儲工作制手臂中
    elevator_number = elevator_lock_name[-1]
    elevator_content_key = elevator_lock_name + "_content" + str(eval(str(arm_id))[1])
    lock_val = 1
    while lock_val:
        lock_id = acquire_lock_with_timeout(r, elevator_content_key, acquire_timeout= 2, lock_timeout= 172800)
        print("抓取電梯補貨平台鎖中: " + elevator_content_key)
        if lock_id != False:
            lock_val = 0
    
    if r.exists(elevator_content_key):
        elevator_content = dill.loads(r.get(elevator_content_key))
        if container_id in elevator_content:
            elevator_content.pop(container_id)
        else:
            print_string = "arms_store 多送一次 container " + str(container_id)+" 關掉arms_store程序"
            print_coler(print_string,"g")

            result = release_lock(r, arm_lock_name, arm_lock)
            release_lock(r, elevator_content_key, lock_id)
            return True


    else:
        print("elevator_content_key "+ elevator_content_key +" 錯誤")
        print("container_id: " + container_id + "不在補貨平台中!!")
    r.set(elevator_content_key, dill.dumps(elevator_content))
    release_lock(r, elevator_content_key, lock_id)

            
    print("手臂" + arm_id + "已取到貨 container_id " + container_id)
    print("手臂"+ arm_id + " " + str(arm_moving_time) +"秒後將 container_id:" + container_id + "放回倉庫")
    start_time = datetime.datetime.now()
    result = waiting_func(arm_moving_time, start_time)
    while not result[0]:
        result = waiting_func(waiting_time, start_time)    
    #storage＿id 放入 container_id
    storage_push(storage_id,container_id)
    #container_id 修改資訊(移動到storage_id & status to in grid)
    # print("container_id: "+container_id+" storage_id: "+storage_id)
    container_moveto(container_id,storage_id)
    #container_work_append 登入container 儲存位置 儲存時間
    value_name = "container_store_end_time"
    content = datetime.datetime.now()
    container_report_append(container_id, value_name, content)
    value_name = "container_store_position"
    content = grid_id
    container_report_append(container_id, value_name, content)
    container_set_status(container_id,"in grid")
    
    #check container_id 工作記錄是否儲存完整，等到該存的的存好了就傳出紀錄並刪除此container_id 在 redis 工作記錄
    container_key = "report_" + container_id
    container_report = r.get(container_key)
    container_report = dill.loads(container_report)
# =============================================================================
#     check_component = False
#     while not check_component:
#         container_report = r.get(container_key)
#         container_report = dill.loads(container_report)
#         '''
#         workreport = {
#             date,
#             order_id,
#             container_id,
#             container_start_position, 起始揀取container位置
#             container_pick_time, 取出立體倉時間
#             elevator_platform_arrive_time, 抵達電梯取貨平台時間
#             container_end_position,抵達哪個工作站
#             container_arrive_time, 抵達工作站時間
#             product_pick_time, 開始揀取時間
#             product_id, 揀取商品id
#             pick_num, 揀取數量
#             container_store_start_time,
#             container_store_end_time
#         }
#         '''
#         if len(container_report) == 8:
#             check_component = True
#         else:
#             print("waiting container working record " + str(container_key))
#             start_time = datetime.datetime.now()
#             result = waiting_func(2, start_time)
#             while not result[0]:
#                 result = waiting_func(2, start_time)
# =============================================================================
    print("workend_record: " + container_id)
    print(container_report)
    if False:
        workreport_append_container(container_report, container_id)
    r.delete(container_key)
    print("workend delete key" + container_key)
    #將 container_id 內商品更新 product
    product_push_container(container_id)
    print("Storaging container: container_id: " + container_id + "'s state is changed to in grid")
    
    arm_id = str(arm_id)
    arms_data_lock = arm_id+ "_pid"
    lock_val = 1
    while lock_val:
        arms_data_lock_id = acquire_lock_with_timeout(r, arms_data_lock, acquire_timeout= 2, lock_timeout= 172800)
        print("arms_store: waiting lock release " + arms_data_lock)
        if arms_data_lock_id != False:
            lock_val = 0
    print("arms_store get _pid key " + arms_data_lock + " " + arms_data_lock_id)
    redis_dict_work_over(str(arm_id))
    result = release_lock(r, arms_data_lock, arms_data_lock_id)
    if result:
        print("arms_store release _pid lock " + arms_data_lock +" finished")
    else:
        print_string = "arms_store release _pid lock fail" + arms_data_lock
        print_coler(print_string,"g")
        
    #手臂結束工作
    end_work_time = datetime.datetime.now()
    arm_report_append(arm_id, start_work_time, end_work_time, container_id, 0, elevator_grid_id, elevator_grid_id, grid_id)
    #釋放手臂鎖
    result = release_lock(r, arm_lock_name, arm_lock)
    print("arms_store 釋放手臂鎖")

    
@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800, priority = 1)
def arms_pick(self, container_id):
    #機器手臂撿取container 並會判斷他上方是否有阻礙的container會先行移開在撿取目標container並更新資料庫
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    #r.set("celery-task-meta-" + self.request.id, self.request.id)
    ##redis get
    G = redis_dict_get("G")
    nodes = redis_dict_get("nodes")
    dists = redis_dict_get("dists")
    
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    acc_rate = json_data["acc_rate"]
    elevator_speed = json_data["elevator_speed"]
    arm_speed = json_data["arm_speed"]
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
    try:
        client = pymongo.MongoClient(uri)
        db = client['ASRS-Cluster-0']
    except:
        Sigkill_func(self.request.id)
    container_db = db["Containers"]
    storage_db = db["Storages"]
    
    container_info = container_db.find_one({"container_id":container_id})
    grid_id = container_info['grid_id']
    arm_id = (nodes[grid_id]['aisle_index'][0], nodes[grid_id]['aisle_index'][2])
    elevator_grid_id = db["Storages"].find_one({"grid_id":grid_id})["elevator_id"]
    elevator_number = str(eval(str(arm_id))[0])
    elevator_lock_name = "ElevatorOut" + elevator_number
    container_height = arm_id[1]
    elevator_content_key = elevator_lock_name + "_content" + str(eval(str(arm_id))[1])
    if r.exists(elevator_content_key):
        elevator_content = dill.loads(r.get(elevator_content_key))
        if len(elevator_content) >= 2:
            print(elevator_content)
            print(elevator_content_key)
            print_string = "arms_pick揀貨平台上已有2箱，container_id "+ container_id +"重啟程序"
            print_coler(print_string,"b")
            waiting_time = 2
            start_time = datetime.datetime.now()
            result = waiting_func(waiting_time, start_time)
            while not result[0]:
                result = waiting_func(waiting_time, start_time)    
            arms_pick.apply_async(args=[container_id], priority = low_priority)
            return True

    #抓手臂鎖，確定目前沒工作後開始做這一項工作
    arm_lock_name = str(arm_id) + "_worklock"
    arm_lock = False
    while not arm_lock:
        arm_lock = acquire_lock_with_timeout(r, arm_lock_name, acquire_timeout=2, lock_timeout=172800)
    print("arms_pick 搶到手臂鎖" + str(arm_id) + "，將揀取container_id: " + container_id)
    
    #手臂開始工作
    start_work_time = datetime.datetime.now()
    
    #container_work_append 登入container 起始位置 揀取時間
    workreport_key = container_id
    value_name = "container_pick_time"
    content = datetime.datetime.now()
    container_report_append(container_id, value_name, content)
    if container_info['relative_coords']['rx'] == 1:
        print("在上層,直接取出" + str(container_id))
        #在上層直接取出
        container_set_status(container_id,'conveyor_to_ws')
        container_grid(container_id,-1)
        storage_pop(container_id)
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
            container_set_status(container_id,'conveyor_to_ws')
            container_grid(container_id,-1)
            storage_pop(container_id)
        else:
            print("上層有東西,先移開後取出")
            #上層有東西,先移開後取出
            for u_i in upper:
                upper_container = u_i['container_id']
                upper_storage_id = u_i['storage_id']
            print("container_id: "+str(container_id)+" grid_id: "+str(grid_id)+" arm_id: "+str((nodes[grid_id]['aisle_index'][0], nodes[grid_id]['aisle_index'][2])))
            arm_id = (nodes[grid_id]['aisle_index'][0], nodes[grid_id]['aisle_index'][2])
            spot_candidates_ptr = storage_db.find({"arm_id":str(arm_id),'container_id':""})
            #收集所有可以放入的空位
            spot_candidates = []
            for sc in spot_candidates_ptr:
                if sc['grid_id'] == grid_id and container_info['relative_coords']['ry'] == sc['relative_coords']['ry']:
                    #若grid_id一樣又'ry'一樣表示是他上層
                    continue
                sc_key = json.loads(sc['storage_id'].replace('(', '[').replace(')', ']'))
                spot_candidates.append(sc_key)
            #計算最短路徑
            major_list = G.shortest_paths(source = grid_id, target = [s[0] for s in spot_candidates], weights = dists)[0]
            #選擇距離最短的位置當作移動目標
            moveto = spot_candidates[major_list.index(min(major_list))]
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
            print(moveto[0])
            arm_distance = G.shortest_paths(source = grid_id, target = int(moveto[0]), weights = dists)[0]
            arm_moving_time = arm_distance[0] / arm_speed / acc_rate
            waiting_time = arm_moving_time * 2 #來回兩趟
            print("手臂" + str(arm_id)+" "+ str(waiting_time) + " 秒後將container前面障礙移至領一個儲格")
            start_time = datetime.datetime.now()
            result = waiting_func(arm_moving_time, start_time)
            while not result[0]:
                result = waiting_func(arm_moving_time, start_time)
            
            #將upper_container 移到 moveto 修改資料庫
            container_moveto(upper_container,moveto_storage_id)
            storage_interchange(upper_storage_id,moveto_storage_id)
            #再將 container_id放到conveyor
            container_set_status(container_id,'conveyor_to_ws')
            container_grid(container_id,-1)
            storage_pop(container_id)
            
    #container_work_append 登入container 起始位置 揀取時間
    value_name = "container_start_position"
    content = grid_id
    container_report_append(container_id, value_name, content)
    value_name = 'date'
    content = json_data['index']
    container_report_append(container_id, value_name, content)

    arm_distance = G.shortest_paths(source = grid_id, target = elevator_grid_id, weights = dists)[0]
    arm_moving_time = arm_distance[0] / arm_speed / acc_rate
    waiting_time = arm_moving_time
    print("手臂" + str(arm_id)+" "+ str(waiting_time) + " 秒後將container_id: "+ container_id +"移至揀貨平台上")
    start_time = datetime.datetime.now()
    result = waiting_func(arm_moving_time, start_time)
    while not result[0]:
        result = waiting_func(arm_moving_time, start_time)
# =============================================================================
#     elevator_moving_time = float(container_height) / elevator_speed / acc_rate
#     arm_distance = G.shortest_paths(source = grid_id, target = elevator_grid_id, weights = dists)[0]
#     arm_moving_time = arm_distance[0] / arm_speed / acc_rate
#     waiting_time = max(arm_moving_time, elevator_moving_time)
#     print("連接到電梯" + str(elevator_lock_name))
#     print(elevator_lock)
#     print("等電梯"+ str(waiting_time) +"秒後來取貨 " +  str(arm_id) + " container_id:" + container_id)
#     start_time = datetime.datetime.now()
#     result = waiting_func(waiting_time, start_time)
#     while not result[0]:
#         result = waiting_func(waiting_time, start_time)
#         
#     print(elevator_lock_name, elevator_lock, container_id, container_height)
#     #r.set(elevator_lock_name + "_task", elevator_moving_time)
# =============================================================================
    #將貨物送到電梯揀貨平台上
    elevator_grid_id = db["Storages"].find_one({"grid_id":grid_id})["elevator_id"]
    elevator_number = str(eval(str(arm_id))[0])
    elevator_lock_name = "ElevatorOut" + elevator_number
    container_height = arm_id[1]
    elevator_content_key = elevator_lock_name + "_content" + str(eval(str(arm_id))[1])
    
    #抓取揀貨平台鎖
    lock_val = 1
    while lock_val:
        lock_id = acquire_lock_with_timeout(r, elevator_content_key, acquire_timeout= 2, lock_timeout= 172800)
        print("抓取電梯揀貨平台鎖中: " + elevator_content_key)
        if lock_id != False:
            lock_val = 0
    
    if r.exists(elevator_content_key):
        elevator_content = dill.loads(r.get(elevator_content_key))
        elevator_content[container_id] = datetime.datetime.now()
    else:    
        elevator_content = {}
        elevator_content[container_id] = datetime.datetime.now()
        
    r.set(elevator_content_key, dill.dumps(elevator_content))
    
    #container_work_append 貨物抵達取貨平台
    value_name = "elevator_platform_arrive_time"
    content = datetime.datetime.now()
    container_report_append(container_id, value_name, content)
    
    release_lock(r, elevator_content_key, lock_id)
    
    arm_id = str(arm_id)
    arms_data_lock = arm_id+ "_pid"
    lock_val = 1
    while lock_val:
        arms_data_lock_id = acquire_lock_with_timeout(r, arms_data_lock, acquire_timeout= 2, lock_timeout= 172800)
        print("arms_pick: waiting lock release " + arms_data_lock)
        if arms_data_lock_id != False:
            lock_val = 0
    print("arms_pick get _pid key " + arms_data_lock + " " + arms_data_lock_id)
    redis_dict_work_over(str(arm_id))
    result = release_lock(r, arms_data_lock, arms_data_lock_id)
    if result:
        print("arms_pick release _pid lock " + arms_data_lock +" finished")
    else:
        print_string = "arms_pick release _pid lock fail" + arms_data_lock
        print_coler(print_string,"g")
    
    #手臂結束工作
    end_work_time = datetime.datetime.now()
    arm_report_append(arm_id, start_work_time, end_work_time, container_id, 1, elevator_grid_id, elevator_grid_id, grid_id)
    result = release_lock(r, arm_lock_name, arm_lock)
    print("arms_pick 釋放手臂鎖")
    
    elevator_pick_task = elevator_pick_move.apply_async(args = [elevator_lock_name, container_id, str(container_height)], priority = high_priority)
    print("貨送到電梯平台上 container_id:" + container_id)
    #r.set("arms_pick_task_" + elevator_lock_name, arms_pick_task.id)
    



@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def arms_work_transmit(self, arm_id):
    # arms_dict = {}
    # arms_dict[arm_id] = redis_dict_get(arm_id)    
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
    #r.set("celery-task-meta-" + self.request.id, self.request.id)
    '''
    取手臂鎖
    '''
    arms_data_lock = arm_id+ "_pid"
    lock_val = 1
    while lock_val:
        lock_id = acquire_lock_with_timeout(r, arms_data_lock, acquire_timeout= 2, lock_timeout= 172800)
        print("arms_work_transmit: waiting lock release " + arms_data_lock)
        if lock_id != False:
            lock_val = 0
    print("arms_work_transmit get _pid key " + arms_data_lock + " " + lock_id)
    '''
    取container資訊
    '''
    print("in arms work transmit get works from redis")
    #print(arms_dict[str(arm_id)]['works'].get())
    #print(eval(arms_dict[str(arm_id)]['works'].get()))
    #container_info = eval(arms_dict[str(arm_id)]['works'].get())
    
    container_info = redis_dict_get_work(arm_id)
    if container_info == "empty":
        result = release_lock(r, arms_data_lock, lock_id)
        if result:
            print_string ="no work in arm_id: "+str(arm_id)+" arms_work_transmit release _pid lock " + arms_data_lock +" finished"
            print_coler(print_string,"g")
        else:
            print_string = "arms_work_transmit release _pid lock fail" + arms_data_lock
            print_coler(print_string,"g")
        return True
        
    #result = det_pick_put.delay(container_info) #True: pick; False: store
    '''
    判斷是要撿取還是存取container並執行
    '''
    
    container_id = container_info[3]
    print("判斷是要撿取還是存取container並執行 container_id: "+str(container_id)+" container_info: "+str(container_info))
    if container_info[0] == 1:
        if container_status(container_id) == "waiting":
            print("arms pick container id: "+str(container_id))
            arms_pick.apply_async(args = [container_id], priority = low_priority)
            #arms_pick.apply_async((container_info[3]), priority = 5)
            redis_dict_turnover_pop(arm_id,container_id)
        else:
            print_string = "container_id: "+str(container_id)+" not in waiting for arm_pick with arm_id: "+str(arm_id)
            print_coler(print_string,"g")
    else:
        if container_status(container_id) == "ws_to_conveyor":
            print("arms store container id: "+str(container_id)+" on arm id: "+str(arm_id))
            arms_store.apply_async(args = [container_id,arm_id], priority = high_priority)
            #arms_store.apply_async((container_info[3],arm_id), priority = 0)
            redis_dict_turnover_push(arm_id,container_id)
        else:
            print_string = "container_id: "+str(container_id)+" not in waiting for arm_store with arm_id: "+str(arm_id)
            print_coler(print_string,"g")
    
    '''
    釋放手臂鎖    
    '''
    result = release_lock(r, arms_data_lock, lock_id)
    if result:
        print("arms_work_transmit release _pid lock " + arms_data_lock +" finished")
    else:
        print_string = "arms_work_transmit release _pid lock fail" + arms_data_lock
        print_coler(print_string,"g")
    return True
'''
robot_arm_tasks
'''

'''
robot_arm_tasks
'''
# @OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
# def workstation_get(self, container_id):
#     r = redis.Redis(host='localhost', port=6379, decode_responses=False)
#     r.set("celery-task-meta-" + self.request.id, self.request.id)
#     # 工作站收到container
#     container_set_status(container_id,'in_workstation')

# def order_check(self, workstation_id, order_id):
#     uri = "mongodb+srv://liyiliou:liyiliou@cluster-yahoo-1.5gjuk.mongodb.net/Cluster-Yahoo-1?retryWrites=true&w=majority"
#     try:
#         client = pymongo.MongoClient(uri)
#         db = client['ASRS-Cluster-0']
#     except:
#         Sigkill_func(self.request.id)
#     workstation_db = db["Workstations"]
#     workstation_info = workstation_db.find({'workstation_id':workstation_id})
#     for ws_i in workstation_info:
#         ws_work = ws_i['work']
#     if ws_work[order_id]["prd"] == {}:
#         return True
#     else:
#         return False
    
@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def workstation_workend(self, workstation_id,order_id):
    print("In workstation workend workstation_id: "+workstation_id+" order_id: "+order_id)
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    r.set("celery-task-meta-" + self.request.id, self.request.id)
    #工作站檢取完後 刪除工作
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    r.set("celery-task-meta-" + self.request.id, self.request.id)
    try:
        client = pymongo.MongoClient(uri)
        db = client['ASRS-Cluster-0']
        workstation_db = db["Workstations"]
        if workstation_db.count_documents({"workstation_id": workstation_id,"work."+order_id: {'$exists':1}}) == 1:
            print("workstation_id: "+str(workstation_id)+" order id: "+str(order_id)+" work end")
            #若工作站有這張order id
            myquery = { "workstation_id": workstation_id}
            #刪訂單
            newvalues = { "$unset": {"work."+order_id:{}}}
            workstation_db.update(myquery,newvalues)
            #減工作量
            newvalues = {"$inc": { "workloads":-1}}
            workstation_db.update(myquery,newvalues)
            #判斷是否工作站工作沒有工作
            if workstation_free(workstation_id):                      
                with open('參數檔.txt') as f:
                    json_data = json.load(f)
                num = json_data["num"]
                print(f"已揀完 {num} 單")
                index_label = json_data["index_label"]
                index = json_data["index"]
                num = json_data["num"]
                workstation_open.apply_async(args = [workstation_id,index_label,index,num,"pick"], priority = low_priority)
        else:
            print_string = "workstation_id: "+str(workstation_id)+" no order id: "+str(order_id)
            print_coler(print_string,"g")
    except:
        print_string = "restart workstation workend"
        print_coler(print_string,"g")
        workstation_workend.apply_async(args = [workstation_id, order_id], priority = low_priority)
        Sigkill_func(self.request.id)
        return True
    

@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def workstation_operate(self, container_id, elevator_number):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    r.set("celery-task-meta-" + self.request.id, self.request.id)
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    acc_rate = json_data["acc_rate"]
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
    uri = json_data["uri"]

    conveyor_speed = json_data['conveyor_speed']
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    #計算運輸時間
    transport_time = (int(elevator_number) * 2 + conveyor_speed) / acc_rate
    #模擬運輸時間
    start_time = datetime.datetime.now()
    result = waiting_func(transport_time, start_time)
    while not result[0]:
        result = waiting_func(transport_time, start_time)
    print("conveyor 運送 container " + container_id + "中，" + str(transport_time) + "秒後到工作站")


    print("Picking container: container_id: " + container_id + "'s state is on_workstation")
        
    #工作站收到container_id
    if container_status(container_id) == "in_workstation":
        print_string = "container_id: "+str(container_id)+" 重複到工作站 container data to workstation fail"
        print_coler(print_string,"g")
    else:
        print("workstation_get 取得 " + container_id)
    workstation_get(container_id)
    
    #container_work_append 登入container 抵達工作站時間
    value_name = "container_arrive_time"
    content = datetime.datetime.now()
    container_report_append(container_id, value_name, content)
    
    '''
    取工作站鎖
    '''
    work_info = workstation_db.aggregate([
                         {'$addFields': {"workTransformed": {'$objectToArray': "$work"}}},
                         {'$match': { 'workTransformed.v.container.'+container_id: {'$exists':1} }}
                                     ])
    ws_work = ""
    for w_1 in work_info:
        # print("in workstation_pick contaioner_id : ",container_id," w_1: ",w_1)
        ws_work = w_1['work']
        workstation_id = w_1['workstation_id']
    #判斷工作站工作項目
    work_type = workstation_db.find_one({"workstation_id":workstation_id})["type"]
    print("此工作站id: " + str(workstation_id) + " 工作項目為： "+str(work_type))
    if work_type == "pick" :
# =============================================================================
#         workstation_lock = workstation_id+ "_pick_lock"
#         workstation_lock_val = 1
#         while workstation_lock_val and False:
#             workstation_lock_id = acquire_lock_with_timeout(r, workstation_lock, acquire_timeout= 2, lock_timeout= 172800)
#             print("workstation_operate: waiting lock release " + workstation_lock)
#             if workstation_lock_id != False:
#                 workstation_lock_val = 0
#         print("workstation_operate get workstation_lock key " + workstation_lock + " " + workstation_lock_id)
# =============================================================================
        #工作站撿取container_id需求內容物
        order_id_list_str, workstation_id = workstation_pick(container_id)
        order_id_list = eval(order_id_list_str)
        #依序判斷order_list內的order_id是否完成
        for order_id in order_id_list:
            if order_check(workstation_id, order_id):
                print("workstation_id: "+str(workstation_id)+" finished order: "+str(order_id))
                workstation_workend.apply_async(args = [workstation_id, order_id], priority = low_priority)
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
    elif work_type == "replenish" :
# =============================================================================
#         workstation_lock = workstation_id+ "_replenish_lock"
#         workstation_lock_val = 1
#         while workstation_lock_val and False:
#             workstation_lock_id = acquire_lock_with_timeout(r, workstation_lock, acquire_timeout= 2, lock_timeout= 172800)
#             print("workstation_operate: waiting lock release " + workstation_lock)
#             if workstation_lock_id != False:
#                 workstation_lock_val = 0
#         print("workstation_operate get workstation_lock key " + workstation_lock + " " + workstation_lock_id)
# =============================================================================
        #工作站撿取container_id需求內容物
        replenish_id_list_str, workstation_id = workstation_replenish(container_id)
        replenish_id_list = eval(replenish_id_list_str)
        #依序判斷order_list內的order_id是否完成
        for replenish_id in replenish_id_list:
            if order_check(workstation_id, replenish_id):
                print("workstation_id: "+str(workstation_id)+" finished  replenish order: "+str(replenish_id))
                workstation_workend.apply_async(args = [workstation_id, replenish_id], priority = low_priority)
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
    else:
        print("workstation_operate fail undefined work type")
    
    '''
    取手臂鎖
    '''
    arms_data_lock = arm_id+ "_pid"
    lock_val = 1
    while lock_val:
        lock_id = acquire_lock_with_timeout(r, arms_data_lock, acquire_timeout= 2, lock_timeout= 172800)
        print("arms_work_transmit: waiting lock release " + arms_data_lock)
        if lock_id != False:
            lock_val = 0
    print("workstation_operate get _pid key " + arms_data_lock + " " + lock_id)
    
    
    print("workstation_operate assign arm_id: "+str(arm_id)+" work and workload +1")
    redis_dict_work_assign(arm_id)
    
    '''
    釋放手臂鎖    
    '''
    result = release_lock(r, arms_data_lock, lock_id)
    if result:
        print("workstation_operate release _pid lock " + arms_data_lock +" finished")
    else:
        print_string = "workstation_operate release _pid lock fail" + arms_data_lock
        print_coler(print_string,"g")
    
    
    '''
    釋放工作站鎖    
    '''
# =============================================================================
#     result = release_lock(r, workstation_lock, workstation_lock_id)
#     if result:
#         print("workstation_operate release workstation_lock " + workstation_lock +" finished")
#     else:
#         print_string = "workstation_operate release workstation_lock fail" + workstation_lock
#         print_coler(print_string,"g")
# =============================================================================
    start_time = datetime.datetime.now()
    
    #將 container 送回輸送帶上，從工作站返回倉庫
    container_set_status(container_id,'ws_to_conveyor')
    #container_work_append 登入container 入庫開始時間
    value_name = "container_store_start_time"
    content = datetime.datetime.now()
    container_report_append(container_id, value_name, content)
    
    #計算運輸時間
    elevator_number = eval(str(arm_id))[0]
    transport_time = (int(elevator_number) * 2 + conveyor_speed) / acc_rate
    
    #模擬運輸時間
    start_time = datetime.datetime.now()
    result = waiting_func(transport_time, start_time)
    while not result[0]:
        result = waiting_func(transport_time, start_time)

    print("Storaging container: container_id: " + container_id + "'s state arrived elevator" + str(elevator_number))
    #送到電梯的container
    container_height = eval(str(arm_id))[1]
    elevator_lock_name = "ElevatorIn" + str(elevator_number)
    print(elevator_lock_name, container_id, container_height)
    elevator_store_move.apply_async(args = [elevator_lock_name, container_id, container_height, arm_id], priority = low_priority)
    #elevator_store_move.apply_async((elevator_lock_name, container_id, container_height, arm_id), priority = 3)
    print("container: " + container_id + " 抵達電梯 " + elevator_lock_name + " 啟動 elevator_store_move")
    return True
'''
robot_arm_tasks
'''

@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def Sigkill_func(self, task_id):
    celery.task.control.revoke(task_id, terminate=True, signal='SIGKILL')
    return True

@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def elevator_pick_move(self,elevator_lock_name, container_id, container_height):
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=False)
        with open('參數檔.txt') as f:
            json_data = json.load(f)
        acc_rate = json_data["acc_rate"]
        elevator_speed = json_data["elevator_speed"]
        low_priority = json_data['low_priority']
        medium_priority = json_data['medium_priority']
        high_priority = json_data['high_priority']
        moving_time = float(container_height) / elevator_speed / acc_rate
        
        #呼叫電梯，如果連接到電梯則設置電梯鎖
        waiting_time = 1
        start_time = datetime.datetime.now()
        result = waiting_func(waiting_time, start_time)
        elevator_lock = False
        while not elevator_lock:
            print("elevator_pick_move 連接電梯中..." + elevator_lock_name)
            elevator_lock = acquire_lock_with_timeout(r, elevator_lock_name, acquire_timeout=2, lock_timeout=172800)
            start_time = datetime.datetime.now()
            while not result[0]:
                result = waiting_func(waiting_time, start_time)
        
        #電梯開始工作
        start_work_time = datetime.datetime.now()
        print("elevator_pick_move 連接到電梯..." + elevator_lock_name)
        print(" 電梯" + elevator_lock_name + "前往取貨中，此任務耗時"+ str(moving_time) +"秒")
        start_time = datetime.datetime.now()
        result = waiting_func(moving_time, start_time)
        while not result[0]:
            result = waiting_func(moving_time, start_time)
            #r.set(elevator_lock_name + "_task", result[1])
        
        #抓取揀貨工作平台鎖
        elevator_number = elevator_lock_name[-1]
        elevator_content_key = elevator_lock_name + "_content" + str(container_height)
        lock_val = 1
        while lock_val:
            lock_id = acquire_lock_with_timeout(r, elevator_content_key, acquire_timeout= 2, lock_timeout= 172800)
            print("抓取電梯揀貨平台鎖中: " + elevator_content_key)
            if lock_id != False:
                lock_val = 0
                
        if r.exists(elevator_content_key):
            elevator_content = dill.loads(r.get(elevator_content_key))
            elevator_content.pop(container_id)
        else:
            print("elevator_content_key "+ elevator_content_key +" 錯誤")
            print("container_id: " + container_id + "不在揀貨平台中!!")
        r.set(elevator_content_key, dill.dumps(elevator_content))
        result = release_lock(r, elevator_content_key, lock_id)

        
        print("elevator_pick_move收到container " + str(container_id))
        print(" 電梯" + elevator_lock_name + "運送中，此任務耗時"+ str(moving_time) +"秒")
        start_time = datetime.datetime.now()
        result = waiting_func(moving_time, start_time)
        while not result[0]:
            result = waiting_func(moving_time, start_time)
            #r.set(elevator_lock_name + "_task", result[1])
        print(" 電梯" + elevator_lock_name + "運送完成，此任務耗時"+ str(moving_time) +"秒")

        #送到輸送帶上，更改container
        elevator_number = elevator_lock_name[-1]
        workstation_operate.apply_async(args = [container_id, elevator_number], priority = low_priority)


        #電梯開始工作
        end_work_time = datetime.datetime.now()
        #儲存電梯工作記錄
        elevator_report_append(elevator_lock_name, start_work_time, end_work_time, container_id, 1, int(container_height))
        
        #解除電梯鎖
        result = release_lock(r, elevator_lock_name, elevator_lock)
        print("解除電梯鎖: " + str(elevator_lock) + " 電梯編號：" + elevator_lock_name)
        print("Picking container: container_id: " + container_id + "'s state is changed to on_conveyor")
    except:
        print_string = "elevator_pick_move error"
        print_coler(print_string,"r")

@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800, priority = 2)
def elevator_store_move(self, elevator_lock_name, container_id, container_height, arm_id):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
    elevator_number = elevator_lock_name[-1]
    elevator_content_key = elevator_lock_name + "_content" + str(eval(str(arm_id))[1])
    if r.exists(elevator_content_key):
        elevator_content = dill.loads(r.get(elevator_content_key))
        if len(elevator_content) >= 2:
            print(elevator_content)
            print(elevator_content_key)
            print_string = "elevator_store_move揀貨平台上已有2箱，container_id "+ container_id +"重啟程序"
            print_coler(print_string,"b")
            waiting_time = 20
            start_time = datetime.datetime.now()
            result = waiting_func(waiting_time, start_time)
            while not result[0]:
                result = waiting_func(waiting_time, start_time)
            
            elevator_store_move.apply_async(args = [elevator_lock_name, container_id, container_height, arm_id], priority = low_priority)
            #elevator_store_move.apply_async((elevator_lock_name, container_id, container_height, arm_id), priority = 3)
            return True
    
    
    #呼叫電梯，如果連接到電梯則設置電梯鎖
    waiting_time = 1
    start_time = datetime.datetime.now()
    result = waiting_func(waiting_time, start_time)
    elevator_lock = False
    while not elevator_lock:
        print("elevator_store_move 連接電梯中..." + elevator_lock_name)
        elevator_lock = acquire_lock_with_timeout(r, elevator_lock_name, acquire_timeout=2, lock_timeout=172800)
        start_time = datetime.datetime.now()
        while not result[0]:
            result = waiting_func(waiting_time, start_time)
            
    #電梯開始工作
    start_work_time = datetime.datetime.now()
    print(elevator_lock_name +"收到container " + str(container_id))
    
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    acc_rate = json_data["acc_rate"]
    elevator_speed = json_data["elevator_speed"]
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
    moving_time = float(container_height) / elevator_speed / acc_rate
    print(" 電梯" + elevator_lock_name + "運送中，此任務耗時"+ str(moving_time * 2) +"秒")
    start_time = datetime.datetime.now()
    result = waiting_func(moving_time, start_time)
    while not result[0]:
        result = waiting_func(moving_time, start_time)
        #因為是要存進去，所以不知道要等多久，先卡著
        #r.set(elevator_lock_name + "_task", result[1] + moving_time)

    #送到目標樓層上，加入存儲工作制手臂中
    elevator_number = elevator_lock_name[-1]
    elevator_content_key = elevator_lock_name + "_content" + str(eval(str(arm_id))[1])
    lock_val = 1
    while lock_val:
        lock_id = acquire_lock_with_timeout(r, elevator_content_key, acquire_timeout= 2, lock_timeout= 172800)
        print("抓取電梯補貨平台鎖中: " + elevator_content_key)
        if lock_id != False:
            lock_val = 0
    
    if r.exists(elevator_content_key):
        elevator_content = dill.loads(r.get(elevator_content_key))
        elevator_content[container_id] = datetime.datetime.now()

# =============================================================================
#         if len(elevator_content) < 2:
#             elevator_content[container_id] = datetime.datetime.now()
# 
#         else:
#             print("電梯補貨平台超過兩箱: " + elevator_content_key)
#             print("重新排序此任務")
#             result = release_lock(r, elevator_content_key, lock_id)
#             result = release_lock(r, elevator_lock_name, elevator_lock)
# =============================================================================
    else:    
        elevator_content = {}
        elevator_content[container_id] = datetime.datetime.now()
        
    r.set(elevator_content_key, dill.dumps(elevator_content))
    release_lock(r, elevator_content_key, lock_id)

    #將徂存工作塞入機械手臂中
    try:
        oi = get_time_string()
        value = (0,oi,0,container_id)
        arms_data_lock = arm_id+ "_pid"
        lock_val = 1
        while lock_val:
            lock_id = acquire_lock_with_timeout(r, arms_data_lock, acquire_timeout= 2, lock_timeout= 172800)
            print("elevator_store_move: waiting _pid lock release " + arms_data_lock)
            if lock_id != False:
                lock_val = 0
            waiting_time = 2
            start_time = datetime.datetime.now()
            result = waiting_func(waiting_time, start_time)
            while not result[0]:
                result = waiting_func(waiting_time, start_time)
                
        print("elevator_store_move get _pid key " + arms_data_lock + " " + lock_id)
        redis_data_update_db(arm_id,value)
        #redis_dict_work_assign(arm_id)
        print("in elevator_store_move redis_data_update arm_id: "+str(arm_id)+" update value: "+str(value))
        result = release_lock(r, arms_data_lock, lock_id)
        if result:
            print("elevator_store_move release pid lock: " +  str(arms_data_lock))
        else:
            print_string = "elevator_store_move release _pid lock fail: " +  str(arms_data_lock)
            print_coler(print_string,"g")
    except:
        print_string = "elevator_store_move release _pid lock fail!!!!!"
        print_coler(print_string,"r")
    arms_work_transmit.apply_async(args = [arm_id], priority = medium_priority)
    
    print("已放置貨物至平台上，電梯" + elevator_lock_name + "回程中，此任務耗時"+ str(moving_time) +"秒")
    start_time = datetime.datetime.now()
    result = waiting_func(moving_time, start_time)
    while not result[0]:
        result = waiting_func(moving_time, start_time)
        #因為是要存進去，所以不知道要等多久，先卡著
        #r.set(elevator_lock_name + "_task", result[1])
    
    #電梯開始工作
    end_work_time = datetime.datetime.now()
    #儲存電梯工作記錄
    elevator_report_append(elevator_lock_name, start_work_time, end_work_time, container_id, 1, int(container_height))
    
    #解除電梯鎖
    result = release_lock(r, elevator_lock_name, elevator_lock)
    print("解除電梯鎖: " + str(elevator_lock) + " 電梯編號：" + elevator_lock_name)
    
@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def waiting_func(self, waiting_secs, start_time):
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    acc_rate = json_data["acc_rate"]
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
    #start_time = datetime.datetime.now()
    end_time = datetime.datetime.now()
    remain_time = (waiting_secs/acc_rate) - (end_time - start_time).total_seconds()
    if remain_time > 0:
        return [False, remain_time]
    else:
        return [True, remain_time]



@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def container_report_append(self, container_id, value_name, content):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    lock_name = "container_report" + container_id
    lock_id = acquire_lock_with_timeout(r, lock_name, acquire_timeout=2, lock_timeout= 172800)
    while not lock_id:
        lock_id = acquire_lock_with_timeout(r, lock_name, acquire_timeout=2, lock_timeout= 172800)
    print("container_report_append locked " + lock_id)
    if r.exists("report_" + container_id) == 1:
        container_report = r.get("report_" + container_id)
        container_report = dill.loads(container_report)
        if value_name != 'order_info':
            container_report[value_name] = content
        else:
            if "order_info" in container_report:
                order_info = container_report["order_info"]
                order_info.append(content)
                container_report["order_info"] = order_info
            else:
                order_info = []
                order_info.append(content)                
                container_report["order_info"] = order_info
    else:
        container_report = {}
        if value_name != 'order_info':
            report_content = {}
            container_report[value_name] = content
        
            
    print("set container_report")
    print(container_id)
    print(container_report)
    r.set("report_" + container_id,dill.dumps(container_report))
    print("container_report_append release_lock " + lock_id)
    release_lock(r, lock_name, lock_id)
    
@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def elevator_report_append(self, elevator_id, start_work_time, end_work_time, container_id, start_layer, end_layer):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    key = "report_" + elevator_id
    if r.exists(key) == 1:
        elevator_report = r.get("report_" + elevator_id)
        elevator_report = dill.loads(elevator_report)
    else:
        elevator_report = []
    single_record = [start_work_time, end_work_time, container_id, start_layer, end_layer]
    elevator_report.append(single_record)
    r.set(key , dill.dumps(elevator_report))
    print("set elevator_report")
    print(elevator_id)
    print(elevator_report)
    
@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def arm_report_append(self, arm_id, start_work_time, end_work_time, container_id, work_type, start_position, elevator_position, container_position):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    arm_id = str(arm_id).replace("(", "[")
    arm_id = arm_id.replace(")", "]")
    key = "report_" + arm_id
    if r.exists(key) == 1:
        arm_report = r.get("report_" + arm_id)
        arm_report = dill.loads(arm_report)
    else:
        arm_report = []
    single_record = [start_work_time, end_work_time, container_id, work_type, start_position, elevator_position, container_position]
    arm_report.append(single_record)
    r.set(key , dill.dumps(arm_report))
    print("set arm_report")
    print(arm_id)
    print(arm_report)    

@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def workreport_append_container(self, container_report, container_id):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    workreport_key = "workreport_container_" + container_id
    if r.exists(workreport_key) == 1:
        workreport = r.get(workreport_key)
        workreport = dill.loads(workreport)
    else:
        #workreport = {}
        workreport = []
        
    order_id_list = container_report["order_info"]
    print(container_report)
    for order_info in order_id_list:
        order_id = order_info[0]
        product_id = order_info[1]
        pick_num = order_info[2]
        pick_time = order_info[3]
        workstation_id = order_info[4]
        report_key = order_info
        content = container_report
        content["container_id"] = container_id
        print(order_info)
        #content.pop('order_info')
        #workreport[str(report_key)] = content
        workreport.append([report_key, content])
    r.set(workreport_key, dill.dumps(workreport))
# =============================================================================
#     r = redis.Redis(host='localhost', port=6379, decode_responses=False)
#     lock_name = "workreport_append_container_recorder"
#     lock_id = acquire_lock_with_timeout(r, lock_name, acquire_timeout=2, lock_timeout= 172800)
#     while not lock_id:
#         lock_id = acquire_lock_with_timeout(r, lock_name, acquire_timeout=2, lock_timeout= 172800)
#     print("workreport_append_container locked " + lock_id)
#     if r.exists("workreport_container") == 1:
#         workreport = r.get("workreport_container")
#         workreport = dill.loads(workreport)
#     else:
#         #workreport = {}
#         workreport = []
#         
#     order_id_list = container_report["order_info"]
#     print(container_report)
#     for order_info in order_id_list:
#         order_id = order_info[0]
#         product_id = order_info[1]
#         pick_num = order_info[2]
#         pick_time = order_info[3]
#         workstation_id = order_info[4]
#         report_key = order_info
#         content = container_report
#         content["container_id"] = container_id
#         print(order_info)
#         #content.pop('order_info')
#         #workreport[str(report_key)] = content
#         workreport.append([report_key, content])
# 
# 
#     r.set("workreport_container", dill.dumps(workreport))
#     release_lock(r, lock_name, lock_id)
# =============================================================================
    print("workreport_append_container order_id_list: " + str(order_id_list) + " with container_id: " + container_id)
    
@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def workreport_append_elevator(self):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    elevator_number = 7
    elevator_type = ["In", "Out"]
    workreport = {}
    for number in range(elevator_number):
        for etype in elevator_type:
            key = "Elevator" + etype + str(number + 1)
            content = r.get("report_" + key)
            if content != None:
                workreport[key] = dill.loads(content)
            else:
                workreport[key] = None
    r.set("workreport_elevator", dill.dumps(workreport))
    print("workreport_append_elevator recorded")
    
@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def workreport_append_arm(self):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    arm_product_ptr = {}
    workreport = {}
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
                    
    for key, val in arm_product_ptr.items():
        arm_id = key
        arm_id = str(arm_id).replace("(", "[")
        arm_id = arm_id.replace(")", "]")
        key = "report_" + arm_id
        content = r.get(key)
        if content != None:
            workreport[arm_id] = dill.loads(content)
        else:
            workreport[arm_id] = None
    r.set("workreport_arm", dill.dumps(workreport))
    print("workreport_append_arm recorded")
    
def workstation_pick(container_id):
    '''
    工作站以從container撿取order所需物品
    刪除 workstation work內工作 container 資訊
    修改container_db內容
    '''
    print("in workstation_pick contaioner_id : "+container_id)
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    container_db = db["Containers"]
    work_info = workstation_db.aggregate([
                         {'$addFields': {"workTransformed": {'$objectToArray': "$work"}}},
                         {'$match': { 'workTransformed.v.container.'+container_id: {'$exists':1} }}
                                     ])
    ws_work = ""
    for w_1 in work_info:
        # print("in workstation_pick contaioner_id : ",container_id," w_1: ",w_1)
        ws_work = w_1['work']
        workstation_id = w_1['workstation_id']
        
    if ws_work == "":
        print_string = "fail, In workstation_pick ws_work == None!!!"
        print_coler(print_string,"b")
        return True
    # 找有哪些訂單有container
    output_order_id_list = []
    for order_id,order_pickup in ws_work.items():
        for pick_container_id in order_pickup["container"]:
            if container_id == pick_container_id:
                #找出container在哪些張訂單號
                output_order_id_list.append(order_id)
    #刪除 container內要撿的商品並更新db
    container_content_pick = {}
    myquery = { "workstation_id": workstation_id}
    print("in workstation_pick 刪除 container內要撿的商品並更新db")
    for output_order_id in output_order_id_list:
        for prd,pqt in ws_work[output_order_id]["container"][container_id].items():
            print("in workstation_pick from container_id: "+str(container_id)+" pick order_id: "+str(output_order_id)+" pick pid: "+str(prd)+" pqt: "+str(pqt))
            # ws_work[output_order_id]["prd"][prd]["qt"] -= pqt
            #用於 修改container_db 內容
            if prd in container_content_pick:
                container_content_pick[prd] += pqt
            else:
                container_content_pick[prd] = pqt
            newvalues = { "$inc": { "work."+output_order_id+".prd."+prd+".qt":-pqt}}
            workstation_db.update(myquery,newvalues)
            #container_work_append 登入container 訂單編號 揀取商品 揀取數量
            workreport_key = container_id
            value_name = "order_info"
            content = (output_order_id, prd, pqt, str(datetime.datetime.now()), workstation_id)
            if content != None:
                container_report_append(container_id, value_name, content)
            else:
                print("workstation_pick content == None")
            
            '''
            記錄有被撿到的單
            '''
            r = redis.Redis(host='localhost', port=6379, decode_responses=False)
            if r.exists("order_list_record_" + output_order_id):
                pass
            else:
                r.set("order_list_record_" + output_order_id, 1)
            #根據揀取商品數量模擬揀取時間
            '''
            模擬揀單個商品花3秒
            '''
            waiting_time = int(pqt) * 3
            start_time = datetime.datetime.now()
            result = waiting_func(waiting_time, start_time)
            while not result[0]:
                result = waiting_func(waiting_time, start_time)
            
            #需求量撿完刪除訂單商品並更新db
            print("in workstation_pick from container_id: "+str(container_id)+" order id: "+str(output_order_id)+" container撿完刪除訂單商品並更新db")
            # if ws_work[output_order_id]["prd"][prd]["qt"] == 0:
            ws_pick_work = workstation_db.find_one({"workstation_id":workstation_id})["work"]
            if output_order_id in ws_pick_work:
                print("準備刪除訂單商品需求量")
                if prd in ws_pick_work[output_order_id]["prd"]:
                    if ws_pick_work[output_order_id]["prd"][prd]["qt"] == 0:
                        print("order id: "+str(output_order_id)+" pid: "+str(prd)+" 需求量已滿足")
                        # ws_work[output_order_id]["prd"].pop(prd,None)
                        newvalues = { "$unset": { "work."+output_order_id+".prd."+prd:""}}
                        workstation_db.update(myquery,newvalues)
                    else:
                        print("order id: "+str(output_order_id)+" pid: "+str(prd)+" 需求量未滿足")
                else:
                    print_string = "in workstation_pick workstation_id: "+str(workstation_id)+" in order id: "+str(output_order_id)+" prd: "+str(prd)+" 已被刪除"
                    print_coler(print_string,"b")
            else:
                print_string = "order id :"+str(output_order_id)+" was deleted in workstation_id: "+str(workstation_id)
                print_coler(print_string,"b")
            
    #撿完container後刪除並更新workstation_db
    for output_order_id in output_order_id_list:
        print("in workstation_pick order_id: "+str(output_order_id)+" pop container_id:"+str(container_id))
        # ws_work[output_order_id]["container"].pop(container_id,None)
        newvalues = { "$unset": { "work."+output_order_id+".container."+container_id:""}}
        workstation_db.update(myquery,newvalues)
    #更新container_db
    container_pick(container_id, container_content_pick)
    #TODO container送回倉
    print(" workstation_id: "+workstation_id+" order_id: "+str(output_order_id_list))
    return str(output_order_id_list),workstation_id


@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def arms_arrange(self,arm_id):
    '''
    arm_id 上的container重新排序
    '''
    
    G = redis_dict_get("G")
    nodes = redis_dict_get("nodes")
    dists = redis_dict_get("dists")
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
    #排序 container 的 turnover 使用
    container_turnover = []
    #統計 container 數量
    container_num = 0
    print("in arms_arrange arm_id: "+str(arm_id)+" 整理storage in arm的 資訊 ")
    for sia_info in storage_in_arms:
        storage_id = sia_info["storage_id"]
        storage_id_eval = eval(storage_id)
        storage_arm[storage_id] = sia_info
        grid_id = storage_id_eval[0]
        storage_arm[storage_id]["grid_id"] = grid_id
        coordinates = nodes[grid_id]['coordinates']
        storage_arm[storage_id]["coordinates"] = {"x":coordinates[0],"y":coordinates[1],"z":coordinates[2]}
        if storage_arm[storage_id]["container_id"] == "":
            storage_arm[storage_id]["turnover"] = 0
        else:
            container_id = storage_arm[storage_id]["container_id"]
            storage_arm[storage_id]["turnover"] = container_db.find_one({"container_id":container_id})["turnover"]
            container_turnover.append([container_id,storage_arm[storage_id]["turnover"]])
            container_num += 1
    #arm_id上所有的 grid
    grid_list = []
    storage_id_list = []
    for k,v in storage_arm.items():
        k_eval = eval(k)
        grid_list.append(k_eval[0])
        storage_id_list.append(k)
    grid_list = list(np.unique(grid_list))
    
    #照位置排序 先排內層在排外層 依序由門口近到遠
    print("in arms_arrange arm_id: "+str(arm_id)+" 將grid排順序依序由門口近到遠")
    storage_location = []
    for layer in range(1,-1,-1):
        for n in range(int(len(grid_list)/2)):
            for i in range(4):
                storage_location.append(str((grid_list[n],(layer,i,0))))
                storage_location.append(str((grid_list[n+int(len(grid_list)/2)],(layer,i,0))))
    
    #排序 container 的 turnover
    container_turnover_sort = sorted(container_turnover, key=lambda s: s[1],reverse = True)
    
    #將排序老的container放入 先放前兩個的 1/4 在內層 再放剩餘的在外層 保證外層比內層多 
    total_row = len(container_turnover_sort)//4
    #上層位置擺放的container
    upper_container = []
    #下層位置擺放的container
    lower_container = []
    print("in arms_arrange arm_id: "+str(arm_id)+" 將依照container turnover擺放")
    for container in container_turnover_sort[0:total_row*2]:
        upper_container.append(container[0])
    for container in container_turnover_sort[total_row*2:]:
        lower_container.append(container[0])
    #整理後順序 先放下層在放上層
    arrange_location = []
    print("in arms_arrange arm_id: "+str(arm_id)+" 整理順序設定")
    for n in range(len(upper_container)):
        arrange_location.append([storage_location[n],upper_container[n]])
    for n in range(len(lower_container)):
        arrange_location.append([storage_location[n+int(len(storage_location)/2)],lower_container[n]])
    print("in arms_arrange arm_id: "+str(arm_id)+" 整理開始")
    while len(arrange_location)>0:
        arrange_info = arrange_location.pop()
        storage_id = arrange_info[0]
        container_id = arrange_info[1]
        print("in arms_arrange arm_id: "+str(arm_id)+" 剩餘待整理數量為: "+str(len(arrange_location))+
              " 準備整理 contianer id: "+str(container_id)+" 放入 storage_id: "+str(storage_id))
        if storage_db.find_one({"storage_id":storage_id})["container_id"] != container_id:
            print("in arms_arrange arm_id: "+str(arm_id)+" 準備清空 storage_id: "+str(storage_id))
            storage_empty(storage_id)
            print("in arms_arrange arm_id: "+str(arm_id)+" 已清空 storage_id: "+str(storage_id))
            print("in arms_arrange arm_id: "+str(arm_id)+" 將 container_id: "+str(container_id)+" 放入 storage_id: "+str(storage_id))
            container_goto(container_id,storage_id)
        print("in arms_arrange arm_id: "+str(arm_id)+" 完成storage_id: "+str(storage_id)+" 放入container_id: "+str(container_id))
            
    print("in arms_arrange arm_id: "+str(arm_id)+" 整理結束")


'''補貨'''
@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def replenish_pick(self, workstation_id):
    '''
    會先收集工作站內補貨單
    先將補貨單商品先合併商品資訊
    在找到適當的container放入工作站
    搜尋方式從redis改成直接搜尋db
    '''
    print("workstation id: "+str(workstation_id)+"start replenish_pick")
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    container_db = db["Containers"]
    ws = workstation_db.find_one({'workstation_id':workstation_id})
    ws_works = ws["work"]
    #工作站內的剩餘補貨單與補貨單還未撿取的商品列表
    replenish_l = []
    ws_replenish_prd = []
    for replenish_i,works_value in ws_works.items():
        replenish_l.append(replenish_i)
        replenish_unpick = {}
        for prd,qt in works_value["prd"].items():
            replenish_unpick[prd] = qt["qt"]
        ws_replenish_prd.append(replenish_unpick)
    #排序機器手臂工作量
    arm_key_list = arm_work_sort_list()
    #依商品順序處理 內容包含數量與需求訂單
    prd_list = []
    prd_content = {}
    for replenish_index,replenish_content in enumerate(ws_replenish_prd):
        for prd,pqt in replenish_content.items():
            if prd not in prd_content:
                prd_list.append(prd)
                prd_content[prd] = {"qt":pqt,"order":{replenish_l[replenish_index]:pqt}}
            else:
                prd_content[prd]["qt"] += pqt
                prd_content[prd]["order"].update({replenish_l[replenish_index]:pqt})
    #開始選擇補貨container並開始補貨
    while len(prd_list)>0:
        pid = prd_list[0]
        print("workstation id: "+str(workstation_id)+" 剩餘補貨商品數量: "+str(len(prd_list))+" 準備補貨pid: "+str(pid))
        oi = get_time_string()
        numbering = 0
        isbreak = False
        print("Replenishing workstation id: "+str(workstation_id)+" pid: "+pid+" 搜尋 ")
        #搜尋還有空間(sku < 4)的container 內不含pid
        container_candidates = container_db.aggregate([{"$match": { "sku":{"$lt":4},
                                                                    "contents."+pid:{"$not":{"$exists":"true"}},
                                                                    "status":"in grid"}}])
        container_candidates_list = []
        for ci in container_candidates:
            container_candidates_list.append(ci["container_id"])
        #隨機選取數量
        container_candidates_random_num = 10
        #隨機選取放入containers
        if container_candidates_random_num <= len(container_candidates_list):
            container_candidates_random_list = list(np.random.choice(container_candidates_list,replace=False,size=container_candidates_random_num))
        else:
            container_candidates_random_list = container_candidates_list
        #排序找到的container所在的arm workloads
        layer_container_workloads_list = []
        for ci in container_candidates_random_list:
            arm_id = container_armid(ci)
            if arm_id != "":
                layer_container_workloads_list.append([ci,arm_id,arm_workloads(arm_id),elevator_workloads(arm_id)])
            else:
                # print("error no container in storage container_id is "+str(ci["container_id"]))
                print_string = "workstation id: "+str(workstation_id)+" Ethansay no container in storage container_id is " +str(ci)
                print_coler(print_string,"g")
        layer_container_workloads_list_sort = sorted(layer_container_workloads_list, key=lambda s: s[2])
        #若有適合的container則進行撿取
        if layer_container_workloads_list_sort  != []:
            #依序使用適當的container
            while len(layer_container_workloads_list_sort) > 0:
                container_choosed = layer_container_workloads_list_sort[0]
                layer_container_workloads_list_sort.remove(container_choosed)
                arm_id = container_choosed[1]
                container_id = container_choosed[0]
                #防止container被同時多個工作站選取
                container_lock_name = container_id + "_pid"
                container_lock = acquire_lock_with_timeout(r, container_lock_name, acquire_timeout=2, lock_timeout=172800)
                if container_lock != False:
                    if container_status(container_id)=='in grid':
                        #改container_db狀態
                        container_waiting(container_id)
                        #release container lock
                        release_lock(r, container_lock_name, container_lock)
                        #補貨單商品 pid 需求資訊
                        pid_order_dict = prd_content[pid]["order"]
                        print("pid: "+str(pid)+" replenish order: "+str(pid_order_dict))
                        pid_pick_order_list = []
                        pick_container = 0
                        #container可補貨空間
                        container_replenish_capacity = 10  + random.randint(0, 5)
                        for replenish_id,pqt in pid_order_dict.items():
                            if pqt > container_replenish_capacity and pqt > 0:
                                #若補貨單pid數量大於一次可以補貨的量
                                #工作站加入補貨商品與數量
                                workstation_add_replenish(replenish_id,container_id,pid,container_replenish_capacity)
                                 #刪除補貨單需求商品數量
                                prd_content[pid]["order"][replenish_id] -= container_replenish_capacity
                                prd_content[pid]["qt"] -= container_replenish_capacity
                                pick_container += 1
                            elif pqt <= container_replenish_capacity and pqt > 0:
                                #若補貨單pid數量小於一次可以補貨的量
                                #工作站加入補貨商品與數量
                                workstation_add_replenish(replenish_id,container_id,pid,container_replenish_capacity)
                                 #刪除補貨單需求商品數量
                                prd_content[pid]["order"][replenish_id] -= container_replenish_capacity
                                prd_content[pid]["qt"] -= container_replenish_capacity
                                pick_container += 1
                                #補貨完的補貨單刪除
                                pid_pick_order_list.append(replenish_id)
                            else:
                                #若補貨單pid數量為負的
                                print_string = "workstation id: "+str(workstation_id)+ "container_id: "+str(container_id)+" pid: "+\
                                                str(pid)+"not replenish in"
                                print_coler(print_string,"b")
                        #將撿出的被訂單刪除
                        for pop_order in pid_pick_order_list:
                            print("workstation id: "+str(workstation_id)+" 補貨單號: "+pop_order+" 已完成 pid: "+pid+" 的補貨指派")
                            prd_content[pid]["order"].pop(pop_order,None)
                        #若商品已無訂單需求則刪除商品
                        if prd_content[pid]["order"] == {}:
                            print("刪除 pid: "+ str(pid))
                            prd_list.remove(pid)
                        if pick_container >0:
                            print("workstation id: "+str(workstation_id)+" this container to putin for "+str(pick_container)+" replenish order")
                            value = (1,oi,numbering,container_id)
                            numbering += 1
                            #更新對應redis
                            arms_data_lock = arm_id+ "_pid"
                            lock_val = 1
                            while lock_val:
                                lock_id = acquire_lock_with_timeout(r, arms_data_lock, acquire_timeout= 2, lock_timeout= 172800)
                                print("更新對應redis waiting lock release " + arms_data_lock)
                                if lock_id != False:
                                    lock_val = 0
                            print("order_pick get _pid key " + arms_data_lock + " " + lock_id)
                            
                            redis_data_update_db(arm_id,value)
                            print("in replenish_pick redis_data_update arm_id: "+str(arm_id)+" update value: "+str(value))
                            redis_dict_work_assign(arm_id)
                            print("replenish_pick assign arm_id: "+str(arm_id)+" workload +1")
                            result = release_lock(r, arms_data_lock, lock_id)
                            if result:
                                print("replenish_pick release _pid lock" + arms_data_lock)
                            else:
                                print_string = "replenish_pick release _pid lock fail" + arms_data_lock
                                print_coler(print_string,"g")
                            #有訂單需求才送出工作
                            print("workstation id: "+str(workstation_id)+" oi: "+str(oi)+" arm_id: "+str(arm_id)+
                                  " pid: "+str(pid)+" container_id: "+container_id)
                            arms_work_transmit.apply_async(args = [arm_id], priority = medium_priority)
                        else:
                            #container 內沒有訂單需求
                            container_set_status(container_id,'in grid')
                            print_string = "container_id: " + str(container_id)+" 內沒有訂單需求狀態改回in grid"
                            print_coler(print_string,"g")
                else:
                    layer_container_workloads_list_sort.insert(4,container_choosed)
                    release_lock(r, container_lock_name, container_lock)
                if pid not in prd_list:
                    print("workstation id: "+str(workstation_id)+" pid: "+str(pid)+" out of prd_list")
                    isbreak = True
                    break
    #補貨商品處理結束
    print("workstation id: "+str(workstation_id)+" replenish finished wait next task")
    r.delete(workstation_id+"open")

def workstation_replenish(container_id):
    '''
    工作站補貨所需商品進container內
    刪除 workstation work內工作 container 資訊
    修改container_db內容
    '''
    print("in workstation_replenish contaioner_id : "+container_id)
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    uri = json_data["uri"]
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
    client = pymongo.MongoClient(uri)
    db = client['ASRS-Cluster-0']
    workstation_db = db["Workstations"]
    container_db = db["Containers"]
    work_info = workstation_db.aggregate([
                         {'$addFields': {"workTransformed": {'$objectToArray': "$work"}}},
                         {'$match': { 'workTransformed.v.container.'+container_id: {'$exists':1} }}
                                     ])
    ws_work = ""
    for w_1 in work_info:
        # print("in workstation_replenish contaioner_id : ",container_id," w_1: ",w_1)
        ws_work = w_1['work']
        workstation_id = w_1['workstation_id']
        
    if ws_work == "":
        print_string = "fail, In workstation_replenish ws_work == None!!!"
        print_coler(print_string,"b")
        return True
    # 找有哪些補貨單有container
    output_replenish_id_list = []
    for replenish_id,order_putin in ws_work.items():
        for put_container_id in order_putin["container"]:
            if container_id == put_container_id:
                #找出container在哪些張訂單號
                output_replenish_id_list.append(replenish_id)
    #放入 container內要放入的商品並更新db
    container_content_put = {}
    myquery = { "workstation_id": workstation_id}
    print("in workstation_replenish 放入 container內要放入的商品並更新db")
    for output_replenish_id in output_replenish_id_list:
        for prd,pqt in ws_work[output_replenish_id]["container"][container_id].items():
            print("in workstation_replenish from container_id: "+str(container_id)+" replenish replenish_id: "+str(output_replenish_id)+" put pid: "+str(prd)+" pqt: "+str(pqt))
            # ws_work[output_order_id]["prd"][prd]["qt"] -= pqt
            #用於 修改container_db 內容
            if prd in container_content_put:
                container_content_put[prd] += pqt
            else:
                container_content_put[prd] = pqt
            #刪除工作站內補貨單上工作項目
            newvalues = { "$inc": { "work."+output_replenish_id+".prd."+prd+".qt":-pqt}}
            workstation_db.update(myquery,newvalues)
            '''改成補貨
            #container_work_append 登入container 訂單編號 揀取商品 揀取數量
            workreport_key = container_id
            value_name = "order_info"
            content = (output_replenish_id, prd, pqt, str(datetime.datetime.now()), workstation_id)
            if content != None:
                container_report_append(container_id, value_name, content)
            else:
                print("workstation_replenish content == None")
            
            '''
            #記錄有被撿到的單
            '''
            r = redis.Redis(host='localhost', port=6379, decode_responses=False)
            if r.exists("order_list_record_" + output_replenish_id):
                pass
            else:
                r.set("order_list_record_" + output_replenish_id, 1)
            #根據揀取商品數量模擬揀取時間
            '''
            #模擬揀單個商品花3秒
            '''
            waiting_time = int(pqt) * 3
            start_time = datetime.datetime.now()
            result = waiting_func(waiting_time, start_time)
            while not result[0]:
                result = waiting_func(waiting_time, start_time)
           ''' 
            #需求量補充完刪除訂單商品並更新db
            print("in workstation_replenish from container_id: "+str(container_id)+" order id: "+str(output_replenish_id)+" container撿完刪除訂單商品並更新db")
            # if ws_work[output_order_id]["prd"][prd]["qt"] == 0:
            ws_put_work = workstation_db.find_one({"workstation_id":workstation_id})["work"]
            if output_replenish_id in ws_put_work:
                print("準備刪除訂單商品需求量")
                if prd in ws_put_work[output_replenish_id]["prd"]:
                    if ws_put_work[output_replenish_id]["prd"][prd]["qt"] == 0:
                        print("replenish id: "+str(output_replenish_id)+" pid: "+str(prd)+" 需求量已滿足")
                        # ws_work[output_order_id]["prd"].pop(prd,None)
                        newvalues = { "$unset": { "work."+output_replenish_id+".prd."+prd:""}}
                        workstation_db.update(myquery,newvalues)
                    else:
                        print("replenish id: "+str(output_replenish_id)+" pid: "+str(prd)+" 需求量未滿足")
                else:
                    print_string = "in workstation_replenish workstation_id: "+str(workstation_id)+" in replenish id: "+str(output_replenish_id)+" prd: "+str(prd)+" 已被刪除"
                    print_coler(print_string,"b")
            else:
                print_string = "replenish id :"+str(output_replenish_id)+" was deleted in workstation_id: "+str(workstation_id)
                print_coler(print_string,"b")
            
    #撿完container後刪除並更新workstation_db
    for output_replenish_id in output_replenish_id_list:
        print("in workstation_replenish replenish_id: "+str(output_replenish_id)+" pop container_id:"+str(container_id))
        # ws_work[output_order_id]["container"].pop(container_id,None)
        newvalues = { "$unset": { "work."+output_replenish_id+".container."+container_id:""}}
        workstation_db.update(myquery,newvalues)
    #更新container_db
    container_putin(container_id, container_content_put)
    #TODO container送回倉
    print(" workstation_id: "+workstation_id+" order_id: "+str(output_replenish_id_list))
    return str(output_replenish_id_list),workstation_id
    
    
