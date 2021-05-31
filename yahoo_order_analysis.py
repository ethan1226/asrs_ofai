#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 20201003

@author: huangboyu
"""
from datetime import datetime
import pandas as pd
import numpy as np
import openpyxl 
from openpyxl import Workbook

def time_convert(time_str):
    output_time = time_str[0:4] +"-" +time_str[4:6] + "-" + time_str[6:8] +" "+ time_str[8:10]+":"+time_str[10:12]+":"+time_str[12:14]
    # format = '%Y-%m-%d %H:%M:%S'
    # return time.strptime(output_time , format)
    return (output_time)

format = '%Y-%m-%d %H:%M:%S'
file = "yahoo_order_info.xlsx"
yahoo_order_record = pd.read_excel(file , dtype=str,engine='openpyxl')
yahoo_order_record['日期'] = yahoo_order_record['日期'].astype(int).astype(str)
order_list = list(np.unique(pd.Series(yahoo_order_record['訂單'])))
order_date = list(np.unique(pd.Series(yahoo_order_record['日期'])))
order_batch = list(np.unique(pd.Series(yahoo_order_record['批次'])))

order_dict = {}
for oi in range(len(yahoo_order_record)):
    order_info = yahoo_order_record.iloc[oi,:]
    print("日期",order_info['日期'],"批次",order_info['批次'],"訂單",order_info['訂單'],"品號",order_info['品號'],"出庫開始時間",order_info['出庫開始時間'],"出庫結束時間",order_info['出庫結束時間'])
    if order_info['出庫結束時間'] is not np.nan and order_info['出庫開始時間'] is not np.nan:
        #若有出庫時間資料
        if order_info['日期'] in order_dict:
            if order_info['批次'] in order_dict[order_info['日期']]["works"]:
                if order_info['訂單'] in order_dict[order_info['日期']]["works"][order_info['批次']]:
                    if order_info['箱號'] in order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"]:
                        if order_info['品號'] in order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"][order_info['箱號']]:
                            order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"][order_info['箱號']][order_info['品號']] += 1
                        else:
                            if order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["訂單開始時間"] > time_convert(order_info['出庫開始時間']):
                                order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["訂單開始時間"] = time_convert(order_info['出庫開始時間'])
                            if order_info['出庫結束時間'] is not np.nan:    
                                if order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["訂單結束時間"] < time_convert(order_info['出庫結束時間']):
                                    order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["訂單結束時間"] = time_convert(order_info['出庫結束時間'])
                            start_time = datetime.strptime(order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["訂單開始時間"], format)
                            end_time = datetime.strptime(order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["訂單結束時間"], format)
                            order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["訂單花費時間"] = end_time - start_time
                            order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"][order_info['箱號']][order_info['品號']] = 1
                    else:
                        order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"][order_info['箱號']]= {}
                        order_dict[order_info['日期']]["containers_num"] += 1
                        if time_convert(order_info['出庫開始時間']) > time_convert(order_info['日期']+"050000"):
                            if order_dict[order_info['日期']]["cost_time"]["work_start_time_2"] > time_convert(order_info['出庫開始時間']):
                                order_dict[order_info['日期']]["cost_time"]["work_start_time_2"] = time_convert(order_info['出庫開始時間'])
                            if order_dict[order_info['日期']]["cost_time"]["work_end_time_2"] < time_convert(order_info['入庫開始時間']):
                                order_dict[order_info['日期']]["cost_time"]["work_end_time_2"] = time_convert(order_info['入庫開始時間'])
                            if order_dict[order_info['日期']]["cost_time"]["end_time_2"] < time_convert(order_info['入庫結束時間']):
                                order_dict[order_info['日期']]["cost_time"]["end_time_2"] = time_convert(order_info['入庫結束時間'])
                        else:
                            if order_dict[order_info['日期']]["cost_time"]["work_start_time_1"] > time_convert(order_info['出庫開始時間']):
                                order_dict[order_info['日期']]["cost_time"]["work_start_time_1"] = time_convert(order_info['出庫開始時間'])
                            if order_dict[order_info['日期']]["cost_time"]["work_end_time_1"] < time_convert(order_info['入庫開始時間']):
                                order_dict[order_info['日期']]["cost_time"]["work_end_time_1"] = time_convert(order_info['入庫開始時間'])
                            if order_dict[order_info['日期']]["cost_time"]["end_time_1"] < time_convert(order_info['入庫結束時間']):
                                order_dict[order_info['日期']]["cost_time"]["end_time_1"] = time_convert(order_info['入庫結束時間'])
                        time1 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_end_time_1"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_1"], format)
                        time2 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_end_time_2"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_2"], format)
                        order_dict[order_info['日期']]["cost_time"]["order_over"] = time1 + time2
                        time1 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["end_time_1"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_1"], format)
                        time2 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["end_time_2"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_2"], format)
                        order_dict[order_info['日期']]["cost_time"]["day_over"] = time1 + time2
                        order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]['訂單開始時間'] = time_convert(order_info['出庫開始時間'])
                        if order_info['出庫結束時間'] is np.nan:
                            order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]['訂單結束時間'] = time_convert(order_info['出庫開始時間'])
                        else:  
                            order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]['訂單結束時間'] = time_convert(order_info['出庫結束時間'])
                        start_time = datetime.strptime(order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["訂單開始時間"], format)
                        end_time = datetime.strptime(order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["訂單結束時間"], format)
                        order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["訂單花費時間"] = end_time - start_time
                        order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"][order_info['箱號']][order_info['品號']] = 1
                else:
                    order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']] = {}
                    order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"] = {}
                    order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"][order_info['箱號']]= {}
                    order_dict[order_info['日期']]["orders_num"] += 1
                    order_dict[order_info['日期']]["containers_num"] += 1
                    if time_convert(order_info['出庫開始時間']) > time_convert(order_info['日期']+"050000"):
                        if order_dict[order_info['日期']]["cost_time"]["work_start_time_2"] > time_convert(order_info['出庫開始時間']):
                            order_dict[order_info['日期']]["cost_time"]["work_start_time_2"] = time_convert(order_info['出庫開始時間'])
                        if order_dict[order_info['日期']]["cost_time"]["work_end_time_2"] < time_convert(order_info['入庫開始時間']):
                            order_dict[order_info['日期']]["cost_time"]["work_end_time_2"] = time_convert(order_info['入庫開始時間'])
                        if order_dict[order_info['日期']]["cost_time"]["end_time_2"] < time_convert(order_info['入庫結束時間']):
                            order_dict[order_info['日期']]["cost_time"]["end_time_2"] = time_convert(order_info['入庫結束時間'])
                    else:
                        if order_dict[order_info['日期']]["cost_time"]["work_start_time_1"] > time_convert(order_info['出庫開始時間']):
                            order_dict[order_info['日期']]["cost_time"]["work_start_time_1"] = time_convert(order_info['出庫開始時間'])
                        if order_dict[order_info['日期']]["cost_time"]["work_end_time_1"] < time_convert(order_info['入庫開始時間']):
                            order_dict[order_info['日期']]["cost_time"]["work_end_time_1"] = time_convert(order_info['入庫開始時間'])
                        if order_dict[order_info['日期']]["cost_time"]["end_time_1"] < time_convert(order_info['入庫結束時間']):
                            order_dict[order_info['日期']]["cost_time"]["end_time_1"] = time_convert(order_info['入庫結束時間'])
                    time1 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_end_time_1"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_1"], format)
                    time2 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_end_time_2"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_2"], format)
                    order_dict[order_info['日期']]["cost_time"]["order_over"] = time1 + time2
                    time1 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["end_time_1"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_1"], format)
                    time2 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["end_time_2"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_2"], format)
                    order_dict[order_info['日期']]["cost_time"]["day_over"] = time1 + time2
                    order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]['訂單開始時間'] = time_convert(order_info['出庫開始時間'])
                    if order_info['出庫結束時間'] is np.nan:
                        order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]['訂單結束時間'] = time_convert(order_info['出庫開始時間'])
                    else:  
                        order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]['訂單結束時間'] = time_convert(order_info['出庫結束時間'])
                    start_time = datetime.strptime(order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["訂單開始時間"], format)
                    end_time = datetime.strptime(order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["訂單結束時間"], format)
                    order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["訂單花費時間"] = end_time - start_time
                    order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"][order_info['箱號']][order_info['品號']] = 1
            else:
                order_dict[order_info['日期']]["works"][order_info['批次']] = {}
                order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']] = {}
                order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"] = {}
                order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"][order_info['箱號']]= {}
                order_dict[order_info['日期']]["orders_num"] += 1
                order_dict[order_info['日期']]["containers_num"] += 1
                if time_convert(order_info['出庫開始時間']) > time_convert(order_info['日期']+"050000"):
                    if order_dict[order_info['日期']]["cost_time"]["work_start_time_2"] > time_convert(order_info['出庫開始時間']):
                        order_dict[order_info['日期']]["cost_time"]["work_start_time_2"] = time_convert(order_info['出庫開始時間'])
                    if order_dict[order_info['日期']]["cost_time"]["work_end_time_2"] < time_convert(order_info['入庫開始時間']):
                        order_dict[order_info['日期']]["cost_time"]["work_end_time_2"] = time_convert(order_info['入庫開始時間'])
                    if order_dict[order_info['日期']]["cost_time"]["end_time_2"] < time_convert(order_info['入庫結束時間']):
                        order_dict[order_info['日期']]["cost_time"]["end_time_2"] = time_convert(order_info['入庫結束時間'])
                else:
                    if order_dict[order_info['日期']]["cost_time"]["work_start_time_1"] > time_convert(order_info['出庫開始時間']):
                        order_dict[order_info['日期']]["cost_time"]["work_start_time_1"] = time_convert(order_info['出庫開始時間'])
                    if order_dict[order_info['日期']]["cost_time"]["work_end_time_1"] < time_convert(order_info['入庫開始時間']):
                        order_dict[order_info['日期']]["cost_time"]["work_end_time_1"] = time_convert(order_info['入庫開始時間'])
                    if order_dict[order_info['日期']]["cost_time"]["end_time_1"] < time_convert(order_info['入庫結束時間']):
                        order_dict[order_info['日期']]["cost_time"]["end_time_1"] = time_convert(order_info['入庫結束時間'])
                time1 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_end_time_1"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_1"], format)
                time2 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_end_time_2"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_2"], format)
                order_dict[order_info['日期']]["cost_time"]["order_over"] = time1 + time2
                time1 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["end_time_1"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_1"], format)
                time2 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["end_time_2"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_2"], format)
                order_dict[order_info['日期']]["cost_time"]["day_over"] = time1 + time2
                order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]['訂單開始時間'] = time_convert(order_info['出庫開始時間'])
                if order_info['出庫結束時間'] is np.nan:
                    order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]['訂單結束時間'] = time_convert(order_info['出庫開始時間'])
                else:  
                    order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]['訂單結束時間'] = time_convert(order_info['出庫結束時間'])
                order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]['訂單花費時間'] = datetime.strptime(time_convert(order_info['出庫結束時間']), format) - datetime.strptime(time_convert(order_info['出庫開始時間']), format)
                order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"][order_info['箱號']][order_info['品號']] = 1
                
        else:
            order_dict[order_info['日期']] = {}
            order_dict[order_info['日期']]["works"] = {}
            order_dict[order_info['日期']]["works"][order_info['批次']] = {}
            order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']] = {}
            order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"] = {}
            order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"][order_info['箱號']]= {}
            order_dict[order_info['日期']]["containers_num"] = 1
            order_dict[order_info['日期']]["orders_num"] = 1
            order_dict[order_info['日期']]["cost_time"] = {"work_start_time_1":time_convert(order_info['出庫開始時間']),
                                                          "work_end_time_1":time_convert(order_info['入庫開始時間']),
                                                          "end_time_1":time_convert(order_info['入庫結束時間']),
                                                          "work_start_time_2":time_convert(order_info['日期']+"100000"),
                                                          "work_end_time_2":time_convert(order_info['入庫開始時間']),
                                                          "end_time_2":time_convert(order_info['入庫結束時間'])}
            time1 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_end_time_1"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_1"], format)
            time2 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_end_time_2"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_2"], format)
            order_dict[order_info['日期']]["cost_time"]["order_over"] = time1 + time2
            time1 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["end_time_1"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_1"], format)
            time2 = datetime.strptime(order_dict[order_info['日期']]["cost_time"]["end_time_2"], format) - datetime.strptime(order_dict[order_info['日期']]["cost_time"]["work_start_time_2"], format)
            order_dict[order_info['日期']]["cost_time"]["day_over"] = time1 + time2
            order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]['訂單開始時間'] = time_convert(order_info['出庫開始時間'])
            if order_info['出庫結束時間'] is np.nan:
                order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]['訂單結束時間'] = time_convert(order_info['出庫開始時間'])
            else:  
                order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]['訂單結束時間'] = time_convert(order_info['出庫結束時間'])
            order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]['訂單花費時間'] = datetime.strptime(time_convert(order_info['出庫結束時間']), format) - datetime.strptime(time_convert(order_info['出庫開始時間']), format)
            order_dict[order_info['日期']]["works"][order_info['批次']][order_info['訂單']]["containers"][order_info['箱號']][order_info['品號']] = 1
            



excel_file = Workbook() 
# 建立一個工作中表 
sheet = excel_file.active
sheet['A1'] = '日期'
sheet['B1'] = '批次'
sheet['C1'] = '訂單號'
sheet['D1'] = '訂單開始時間'
sheet['E1'] = '訂單結束時間'
sheet['F1'] = '訂單花費時間'


for k,v in order_dict.items():

    A = k
    for k_i,v_i in v.items(): 
        B = k_i
        for k_j,v_j in v_i.items():    
            C = k_j
            D = v_j['訂單開始時間']
            E = v_j['訂單結束時間']
            F = v_j['訂單花費時間']
            sheet.append([A,B,C,D,E,F])

excel_file.save('yahoo_order_cost_time.xlsx')

# "批次開始時間":time_convert(order_info['出庫開始時間']),
#                                                              "批次結束時間":time_convert(order_info['出庫結束時間']),
#                                                              "批次花費時間":datetime.strptime(time_convert(order_info['出庫結束時間']), format) - 
#                                                                           datetime.strptime(time_convert(order_info['出庫開始時間']), format)