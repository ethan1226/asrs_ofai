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
        print("workstation id: "+str(workstation_id)+" 剩餘訂單商品數量: "+str(len(prd_list))+" 準備撿取pid: "+str(pid))
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
                                    result = release_lock(r, arms_data_lock, lock_id)
                                    if result:
                                        print("order_pick release _pid lock" + arms_data_lock)
                                    else:
                                        print_string = "order_pick release _pid lock fail" + arms_data_lock
                                        print_coler(print_string,"g")
                                else:
                                    #container 內沒有訂單需求
                                    container_set_status(container_id,'in grid')
                                # release_lock(r, lock_name, arm_product_lock)
                                print("workstation id: "+str(workstation_id)+" oi: "+str(oi)+" arm_id: "+str(arm_id)+" layer: "+str(layer)+
                                      " pid: "+str(pid)+" container_id: "+container_id)
                                arms_work_transmit.apply_async(args = [arm_id], priority = medium_priority)
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
def workstation_open(self, workstation_id,index_label,index,num):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    r.set("celery-task-meta-" + self.request.id, self.request.id)
    lock_name = "order_assignment"
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
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
                    if container_movement()<20:
                        #箱子移動數少依時間配單
                        order_l = str(order_assign(index_label,index,num))
                    else:
                        #若目前箱子正在移動的數量太多會先分配商品在同一箱的訂單ｓ
                        order_l = str(order_assign_crunch(index_label,index,num))
                    print("分配訂單為："+order_l)
                    print("訂單池給予訂單")
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
            r.delete(workstation_id+"open")
    else:
        print("工作站id: "+str(workstation_id)+"調配商品中")
        if workstation_free(workstation_id):
            r.delete(workstation_id+"open")
            print("工作站id: "+str(workstation_id)+" 完成 補新訂單")
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
    storage_id = find_empty_arms_sid(arm_id)
    
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
            print("arms_store 多送一次 container " + str(container_id))
            print("關掉arms_store程序")

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
    container_set_status(container_id,"in grid")
    
    #check container_id 工作記錄是否儲存完整，等到該存的的存好了就傳出紀錄並刪除此container_id 在 redis 工作記錄
    container_key = "report_" + container_id
    check_component = False
    while not check_component:
        container_report = r.get(container_key)
        container_report = dill.loads(container_report)
        '''
        workreport = {
            date,
            order_id,
            container_id,
            container_start_position, 起始揀取container位置
            container_pick_time, 取出立體倉時間
            container_end_position,抵達哪個工作站
            container_arrive_time, 抵達工作站時間
            product_pick_time, 開始揀取時間
            product_id, 揀取商品id
            pick_num, 揀取數量
            container_store_start_time,
            container_store_end_time
        }
        '''
        if len(container_report) == 7:
            check_component = True
        else:
            print("waiting container working record " + str(container_key))
            start_time = datetime.datetime.now()
            result = waiting_func(2, start_time)
            while not result[0]:
                result = waiting_func(2, start_time)
    print("workend_record: " + container_id)
    print(container_report)
    workreport_append(container_report, container_id)
    r.delete(container_key)
    print("workend delete key" + container_key)
    #將 container_id 內商品更新 product
    product_push_container(container_id)
    print("Storaging container: container_id: " + container_id + "'s state is changed to in grid")
    redis_work_over(str(arm_id))
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
    
    if container_info['relative_coords']['rx'] == 1:
        print("在上層,直接取出" + str(container_id))
        #在上層直接取出
        container_set_status(container_id,'on_conveyor')
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
            container_set_status(container_id,'on_conveyor')
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
            moveto = str((moveto[0],tuple(moveto[1])))
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
            container_moveto(upper_container,moveto)
            storage_interchange(upper_storage_id,moveto)
            #再將 container_id放到conveyor
            container_set_status(container_id,'on_conveyor')
            container_grid(container_id,-1)
            storage_pop(container_id)
            
    #container_work_append 登入container 起始位置 揀取時間
    workreport_key = container_id
    value_name = "container_pick_time"
    content = datetime.datetime.now()
    container_report_append.apply_async(args = [container_id, value_name, content], priority = medium_priority)
    value_name = "container_start_position"
    content = grid_id
    container_report_append.apply_async(args = [container_id, value_name, content], priority = medium_priority)
    value_name = 'date'
    content = json_data['index']
    container_report_append.apply_async(args = [container_id, value_name, content], priority = medium_priority)


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
    release_lock(r, elevator_content_key, lock_id)
    
    redis_work_over(str(arm_id))
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
    if container_info == "":
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
    print("判斷是要撿取還是存取container並執行")
    if container_info[0] == 1:
        print("arms pick container id: "+str(container_info[3]))
        arms_pick.apply_async(args = [container_info[3]], priority = low_priority)
        #arms_pick.apply_async((container_info[3]), priority = 5)
        redis_dict_turnover_pop(arm_id,container_info[3])
    else:
        print("arms store container id: "+str(container_info[3])+" on arm id: "+str(arm_id))
        arms_store.apply_async(args = [container_info[3],arm_id], priority = high_priority)
        #arms_store.apply_async((container_info[3],arm_id), priority = 0)
        redis_dict_turnover_push(arm_id,container_info[3])
    
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
                workstation_open.apply_async(args = [workstation_id,index_label,index,num], priority = low_priority)
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
def container_operate(self, container_id, elevator_number):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    r.set("celery-task-meta-" + self.request.id, self.request.id)
    with open('參數檔.txt') as f:
        json_data = json.load(f)
    acc_rate = json_data["acc_rate"]
    low_priority = json_data['low_priority']
    medium_priority = json_data['medium_priority']
    high_priority = json_data['high_priority']
    #計算運輸時間
    transport_time = (int(elevator_number) * 2 + 10) / acc_rate
    #模擬運輸時間
    start_time = datetime.datetime.now()
    result = waiting_func(transport_time, start_time)
    while not result[0]:
        result = waiting_func(transport_time, start_time)
    print("conveyor 運送 container " + container_id + "中，" + str(transport_time) + "秒後到工作站")


    print("Picking container: container_id: " + container_id + "'s state is on_workstation")
        
    #工作站收到container_id
    if container_status(container_id) == "in_workstation":
        print_string = "箱子重複到工作站 container data to workstation fail"
        print_coler(print_string,"g")
    else:
        print("workstation_get 取得 " + container_id)
    workstation_get(container_id)
    
    #container_work_append 登入container 抵達工作站時間
    value_name = "container_arrive_time"
    content = datetime.datetime.now()
    container_report_append.apply_async(args = [container_id, value_name, content], priority = medium_priority)
    
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

    start_time = datetime.datetime.now()
    waiting_time = 10 / acc_rate
    result = waiting_func(waiting_time, start_time)
    while not result[0]:
        result = waiting_func(waiting_time, start_time)
        #r.set(elevator_lock_name + "_task", result[1])
# =============================================================================
#     oi = get_time_string()
#     value = (0,oi,0,container_id)
#     lock_name = arm_id+ "_pid"
#     lock_val = 1
#     while lock_val:
#         lock_id = acquire_lock_with_timeout(r, lock_name, acquire_timeout= 2, lock_timeout= 100)
#         print("container_operate: waiting lock release " + lock_name)
#         if lock_id != False:
#             lock_val = 0
#     redis_data_update_db(arm_id,value)
#     release_lock(r, lock_name, lock_id)
#     arms_work_transmit.delay(arm_id)
# =============================================================================
    
    #將 container 送回輸送帶上，從工作站返回倉庫
    container_set_status(container_id,'on_conveyor')
    #container_work_append 登入container 入庫開始時間
    value_name = "container_store_start_time"
    content = datetime.datetime.now()
    container_report_append.apply_async(args = [container_id, value_name, content], priority = medium_priority)
    
    #計算運輸時間
    elevator_number = eval(str(arm_id))[0]
    transport_time = (int(elevator_number) * 2 + 10) / acc_rate
    
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
        container_operate.apply_async(args = [container_id, elevator_number], priority = low_priority)

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
    remain_time = waiting_secs - (end_time - start_time).total_seconds()
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
        else:
            order_info = []
            order_info.append(content)
            container_report["order_info"] = order_info
            
    print("set container_report")
    print(container_id)
    print(container_report)
    r.set("report_" + container_id,dill.dumps(container_report))
    print("container_report_append release_lock " + lock_id)
    release_lock(r, lock_name, lock_id)

@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def workreport_append(self, container_report, container_id):
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
# =============================================================================
#     lock_id = acquire_lock_with_timeout(r, "workreport_append", acquire_timeout=2, lock_timeout= 86400)
#     while not lock_id:
#         lock_id = acquire_lock_with_timeout(r, "workreport_append", acquire_timeout=2, lock_timeout= 86400)
# =============================================================================
    if r.exists("workreport") == 1:
        workreport = r.get("workreport")
        workreport = dill.loads(workreport)
    else:
        workreport = {}
        
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
        workreport[str(report_key)] = content

    r.set("workreport", dill.dumps(workreport))
    #release_lock(r, "workreport_append", lock_id)
    print("workreport_append order_id_list: " + str(order_id_list) + " with container_id: " + container_id)
    
def workstation_pick(container_id):
    #工作站以從container撿取order所需物品
    #刪除 workstation work內工作 container 資訊
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
    work_info = workstation_db.aggregate([
                         {'$addFields': {"workTransformed": {'$objectToArray': "$work"}}},
                         {'$match': { 'workTransformed.v.container.'+container_id: {'$exists':1} }}
                                     ])
    for w_1 in work_info:
        # print("in workstation_pick contaioner_id : ",container_id," w_1: ",w_1)
        ws_work = w_1['work']
        workstation_id = w_1['workstation_id']
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
    for output_order_id in output_order_id_list:
        for prd,pqt in ws_work[output_order_id]["container"][container_id].items():
            ws_work[output_order_id]["prd"][prd]["qt"] -= pqt
            container_content_pick[prd] = pqt
            newvalues = { "$inc": { "work."+output_order_id+".prd."+prd+".qt":-pqt}}
            
            #container_work_append 登入container 訂單編號 揀取商品 揀取數量
            workreport_key = container_id
            value_name = "order_info"
            content = (output_order_id, prd, pqt, str(datetime.datetime.now()), workstation_id)
            if content != None:
                container_report_append.apply_async(args = [container_id, value_name, content], priority = medium_priority)
            else:
                print("workstation_pick content == None")
            
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
            if ws_work[output_order_id]["prd"][prd]["qt"] == 0:
                ws_work[output_order_id]["prd"].pop(prd,None)
                newvalues = { "$unset": { "work."+output_order_id+".prd."+prd:""}}
            workstation_db.update(myquery,newvalues)
    #撿完container後刪除並更新workstation_db
    for output_order_id in output_order_id_list:
        ws_work[output_order_id]["container"].pop(container_id,None)
        newvalues = { "$unset": { "work."+output_order_id+".container."+container_id:""}}
        workstation_db.update(myquery,newvalues)
    #更新container_db
    container_pick(container_id, container_content_pick)
    #TODO container送回倉
    print(" workstation_id: "+workstation_id+" order_id: "+str(output_order_id_list))
    return str(output_order_id_list),workstation_id

@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def add_a(self):
    for i in range(100000000):
        pass
    print("a")
    
@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def add_b(self):
    print("b")
    
@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def add_c(self):
    print("c")
    
@OFAI_Celery_func.task(bind=True, soft_time_limit= 172800, time_limit= 172800)
def add_d(self):
    print("d")