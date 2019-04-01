#!/usr/bin/python3.6
# -*- coding:UTF-8 -*-

import numpy as np
from copy import deepcopy
import nmc_vf_base.basicdatas as bd
from scipy.spatial import cKDTree
import math
from collections import OrderedDict
import pandas as pd

def copy(sta):
    return deepcopy(sta)

def recover(sta_from,sta_to):
    ids = sta_to.index.intersection(sta_from.index)
    sta = sta_to.copy()
    sta_from.drop_duplicates(keep='first',inplace=True)
    sta.ix[ids,'data'] = sta_from.ix[ids,'data']
    return sta

def add(sta1,sta2):
    sta = sta1.copy()
    sta.ix[:,'data'] = sta1.ix[:,'data'] + sta2.ix[:,'data']
    return sta

def mutiply(sta1,sta2):
    sta = sta1.copy()
    sta.ix[:,'data'] = sta1.ix[:,'data'] * sta2.ix[:,'data']
    return sta

def sqrt(sta_from):
    sta = sta_from.copy()
    sta['data'] = sta['data'].map(lambda  x: x **0.5)
    return sta

def power(sta_from,a):
    sta = sta_from.copy()
    sta['data'] = sta['data'].map(lambda  x: x **a)
    return sta


def get_both_having_station(sta1,sta2):
    ids = sta1.index.intersection(sta2.index)
    sta = sta1.ix[ids,:]
    return sta

def get_one_having_station(sta1,sta2):
    ids = sta1.index.intersection(sta2.index)
    sta =pd.concat([sta1.drop(ids),sta2])
    return sta

def set_IV(sta_from,min_value,max_value):
    sta = sta_from.copy()
    sta.ix[sta['data'] < min_value,2] = bd.IV
    sta.ix[sta['data'] > max_value, 2] = bd.IV
    return sta

def reset_IV_value(sta_from,value):
    sta = sta_from.reindex(sta_from.index,fill_value= value)
    sta.ix[sta['data'] == bd.IV, 2] = value

    return sta

def remove_IV(sta):
    sta1 = sta[sta['data'] != bd.IV]
    return sta1

def get_sta_in_grid(sta,grid):
    grid.reset()
    sta1 = sta[sta['lon']>=grid.slon]
    sta1 = sta1[sta1['lon'] <grid.elon]
    sta1 = sta1[sta1['lat'] >= grid.slat]
    sta1 = sta1 [sta1['lat'] < grid.elat]
    return sta1

def get_sta_in_value_range(sta,start,end):
    sta1 = sta[sta['data'] >= start]
    sta1 = sta1[sta1['data'] <end]
    return sta1

def remove_both_station(sta,station):
    ids = sta.index.intersection(station.index)
    sta1 = sta.drop(ids)
    return sta1

def add_lon_lat(station,series):
    ids = series.index.intersection(station.index)
    df = series.to_frame()
    df1 = df.ix[ids,:]
    sta = station.ix[ids, 0:2]
    sta1 = sta.join(df1)
    sta1.columns = ['lon','lat','data']
    return sta1