#!/usr/bin/python3.6
# -*- coding:UTF-8 -*-
import numpy as np
from copy import deepcopy
import basic_lch.basicdatas as bd
import os
import nmc_vf_base.basicdatatrans.sta_sta_function as ssf
from collections import OrderedDict
import struct
from collections import OrderedDict
from pandas import DataFrame
import pandas as pd
import json
import urllib3

def read_from_micaps1_2_8(filename,column,station = None):
    if os.path.exists(filename):
        sta1 = pd.read_csv(filename, skiprows=2, sep="\s+", header=None, usecols= [0,1,2,column],index_col=0)
        #index_str = np.array(df.index.tolist()).astype("str")
        #df = pd.DataFrame(df.values,index=index_str)
        #sta1 = bd.sta_data(df, column)
        sta1.columns = ['lon', 'lat', 'data']
        if(station is None):
            return sta1
        else:
            sta = ssf.recover(sta1, station)
            return sta
    else:
        return None


def read_from_micaps3(filename,station = None):
    try:
        if os.path.exists(filename):
            file = open(filename,'r')
            skip_num = 0
            strs = []
            nline = 0
            nregion = 0
            nstart = 0
            while 1>0:
                skip_num += 1
                str1 = file.readline()
                strs.extend(str1.split())
                if(len(strs)>8):
                    nline = int(strs[8])
                if(len(strs)>11 + nline):
                    nregion = int(strs[11 + nline])
                    nstart = nline + 2 * nregion + 14
                    if(len(strs) == nstart):
                        break

            file.close()

            sta1 = pd.read_csv(filename, skiprows=skip_num, sep="\s+", header=None, usecols=[0, 1, 2,4], index_col=0)
            sta1.columns = ['lon','lat','data']
            sta1.drop_duplicates(keep='first', inplace=True)
            if (station is None):
                return sta1
            else:
                sta = ssf.recover(sta1,station)
                return sta
        else:
            return None
    except:
        return None

def read_from_micaps16(filename):
    if os.path.exists(filename):
        file = open(filename,'r')
        head = file.readline()
        head = file.readline()
        stationids = []
        row1 = []
        row2 = []
        while(head is not None and head.strip() != ""):
            strs = head.split()
            stationids.append(strs[0])
            a = int(strs[1])
            b = a // 100 + (a % 100) /60
            row1.append(b)
            a = int(strs[2])
            b = a // 100 + (a % 100) /60
            row2.append(b)
            head =  file.readline()
        row1 = np.array(row1)
        row2 = np.array(row2)
        ids = np.array(stationids)
        dat = np.zeros((len(row1),3))
        if(np.max(row2) > 90 or np.min(row2) <-90):
            dat[:,0] = row2[:]
            dat[:,1] = row1[:]
        else:
            dat[:,0] = row1[:]
            dat[:,1] = row2[:]
        station = DataFrame(dat, index=ids, columns=['lon', 'lat', 'dat'])
        return station
    else:
        print(filename +" not exist")
        return None


def read_station(filename,skip = 0):
    if os.path.exists(filename):
        file = open(filename,'r')
        for i in range(skip):
            head = file.readline()
        head = file.readline()
        stationids = []
        row1 = []
        row2 = []
        while(head is not None and head.strip() != ""):
            strs = head.split()
            stationids.append(strs[0])
            a = float(strs[1])
            if(a >1000):
                a = a // 100 + (a % 100) /60
            row1.append(a)
            a = float(strs[2])
            if(a >1000):
                a = a // 100 + (a % 100) /60
            row2.append(a)
            head =  file.readline()
        row1 = np.array(row1)
        row2 = np.array(row2)
        ids = np.array(stationids)
        dat = np.zeros((len(row1),3))
        if(np.max(row2) > 90 or np.min(row2) <-90):
            dat[:,0] = row2[:]
            dat[:,1] = row1[:]
        else:
            dat[:,0] = row1[:]
            dat[:,1] = row2[:]
        station = DataFrame(dat, index=ids, columns=['lon', 'lat', 'data'])
        return station
    else:
        print(filename +" not exist")
        return None


def read_from_cimiss_surface(interface_id,time_str,data_code, element_name,sta_levels =None):
    """
        Retrieve station records from CIMISS by time and station ID.
    >>> time_range = "[20180219000000,20180219010000]"
    >>> data_code = "SURF_CHN_MUL_DAY"
    >>> elements = "Station_Id_C,Lat,Lon,PRE_1h"
    >>> print "retrieve successfully" if data is not None else "failed"
    retrieve successfully
    """
    params = {'dataCode': data_code,
              'elements': "Station_Id_d,Lon,Lat,"+element_name,
              'times': time_str,
              "orderby": "Station_ID_d"}
    if(sta_levels is not None):
        params["staLevels"] = sta_levels

    # set  MUSIC server dns and user information
    dns = "10.20.76.55"
    user_id = "NMC_YBS_liucouhua"
    pwd = "20130913"
    # construct url
    url = 'http://' + dns + '/cimiss-web/api?userId=' + user_id + \
          '&pwd=' + pwd + '&interfaceId=' + interface_id

    # params
    for key in params:
        url += '&' + key + '=' + params[key]

    # data format
    url += '&dataFormat=' + 'json'

    # request http contents
    http = urllib3.PoolManager()
    req = http.request('GET', url)
    if req.status != 200:
        print('Can not access the url: ' + url)
        return None
    contents = json.loads(req.data.decode('utf-8'))
    if contents['returnCode'] != '0':
        return None
    # construct pandas DataFrame
    data = pd.DataFrame(contents['DS'])
    sta = data.set_index(("Station_Id_d"))[['Lon','Lat',element_name]]
    sta.columns = ['lon', 'lat', 'data']
    return sta
