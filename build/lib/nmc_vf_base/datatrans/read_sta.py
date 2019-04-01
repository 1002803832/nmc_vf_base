#!/usr/bin/python3.6
# -*- coding:UTF-8 -*-
import numpy as np
from copy import deepcopy
import basic_lch.basicdatas as bd
import os
import basic_lch.basicdatafunctions.sta_sta_function as ssf
from collections import OrderedDict
import basic_lch.ioapi.DataBlock_pb2 as DataBlock_pb2
import basic_lch.ioapi.GDS_data_service as GDS_data_service
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
        sta1.columns = ['lon', 'lat', 'dat']
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

            #str1 = file.read()
            file.close()

            '''
            strs = str1.split()
            nline = int(strs[8])
            nregion = int(strs[11+nline])
            nstart=nline+2*nregion+14
            nsta=int((len(strs)-nstart)/5)

            str_array =np.delete(np.array(strs[nstart:]).reshape((nsta,5)),3,axis = 1)
            ids = str_array[:,0]
            dat = str_array[:,1:].astype("float32")
            sta1 = DataFrame(dat,index = ids,columns=['lon','lat','dat'])
            '''
            sta1 = pd.read_csv(filename, skiprows=skip_num, sep="\s+", header=None, usecols=[0, 1, 2,4], index_col=0)
            sta1.columns = ['lon','lat','dat']
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

def read_from_gds(filename,element_id = None,station = None,service = None):
    try:
        if(service is None):service = GDS_data_service.service
        directory,fileName = os.path.split(filename)
        status, response = byteArrayResult = service.getData(directory, fileName)
        ByteArrayResult = DataBlock_pb2.ByteArrayResult()
        if status == 200:
            ByteArrayResult.ParseFromString(response)
            if ByteArrayResult is not None:
                byteArray = ByteArrayResult.byteArray
                nsta = struct.unpack("i", byteArray[288:292])[0]
                id_num = struct.unpack("h", byteArray[292:294])[0]
                id_tpye = {}
                for i in range(id_num):
                    element_id0 = struct.unpack("h", byteArray[294 + i * 4:296 + i * 4])[0]
                    id_tpye[element_id0] = struct.unpack("h", byteArray[296 + i * 4:298 + i * 4])[0]
                    if(element_id is None and element_id0 > 200 and element_id0 % 2 == 1):
                        element_id = element_id0
                station_data_dict = OrderedDict()
                index = 294 + id_num * 4
                type_lenght_dict = {1: 1, 2: 2, 3: 4, 4: 4, 5: 4, 6: 8, 7: 1}
                type_str_dict = {1: 'b', 2: 'h', 3: 'i', 4: 'l', 5: 'f', 6: 'd', 7: 'c'}

                for i in range(nsta):
                    one_station_dat = {}
                    one_station_id =str(struct.unpack("i", byteArray[index: index + 4])[0])
                    index += 4
                    one_station_dat['lon'] =struct.unpack("f", byteArray[index: index + 4])[0]
                    index += 4
                    one_station_dat['lat'] =(struct.unpack("f", byteArray[index: index + 4])[0])
                    index += 4
                    value_num = struct.unpack("h", byteArray[index:index + 2])[0]
                    index += 2
                    values = {}
                    for j in range(value_num):
                        id = struct.unpack("h", byteArray[index:index + 2])[0]
                        index += 2
                        id_tpye0 = id_tpye[id]
                        dindex = type_lenght_dict[id_tpye0]
                        type_str = type_str_dict[id_tpye0]
                        value = struct.unpack(type_str, byteArray[index:index + dindex])[0]
                        index += dindex
                        values[id] = value
                    if(element_id in values.keys()):
                        one_station_dat['dat'] =(values[element_id])
                        station_data_dict[one_station_id] = one_station_dat
                sta = pd.DataFrame(station_data_dict).T.ix[:,['lon','lat','dat']]
                return sta
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
        station = DataFrame(dat, index=ids, columns=['lon', 'lat', 'dat'])
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
    sta.columns = ['lon', 'lat', 'dat']
    return sta
