# -*- coding: utf-8 -*-
"""
Created on Sat Jul 23 23:43:15 2022

Search chartevents, outputevents, procedureevents,and datetimeevents for 
dialysis information in the first 24 hours. 

Runtime: 8 minutes

@author: Kirby
"""

#%% Setup
import pandas as pd
import numpy as np
from pathlib import Path
from time import time

start = time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
ext_valid_path = file_path.parent.parent.parent.joinpath('Ext Validation 2')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

#%% Load in data.

comp = pd.read_csv(dataset_path.joinpath("MIMICIV_complete_dataset.csv"),
                   usecols=['stay_id','intime'],
                   parse_dates=['intime'])

items = pd.read_csv(mimic_path.joinpath('icu', 'd_items.csv.gz'),
                    usecols=['itemid','label','linksto']).dropna()
rel_items = items[items['label'].str.contains('dialysis', case=False)]

#Get Chart Events data. 
chart = pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'),
                       nrows=0,
                       usecols=['stay_id','itemid', 'charttime', 'value', 
                                'valuenum', 'valueuom'],
                       parse_dates=['charttime'])
for chunk in pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'),
                         chunksize=1000000,
                         usecols=['stay_id', 'itemid', 'charttime', 
                                  'value', 'valuenum', 'valueuom'],
                         parse_dates=['charttime']):
    temp_rows = chunk.merge(rel_items, on = 'itemid', how = 'inner')
    chart = pd.concat([chart,temp_rows])
    
#Get outputevents and remove erroneous rows. 
output = pd.read_csv(mimic_path.joinpath('icu', 'outputevents.csv.gz'),
                     usecols=['stay_id', 'charttime', 'itemid',
                              'value', 'valueuom'],
                     parse_dates=['charttime'])

#Get procedure events data and remove erroneous rows. 
proc = pd.read_csv(mimic_path.joinpath('icu', 'procedureevents.csv.gz'),
                   usecols=['stay_id', 'starttime', 'endtime', 'itemid',
                            'value', 'valueuom'],
                   parse_dates=['starttime','endtime'])

#Get datetimeevents data and remove erroneous rows.
datetime = pd.read_csv(mimic_path.joinpath('icu', 'datetimeevents.csv.gz'),
                       usecols=['stay_id', 'itemid', 'charttime','value', 
                                'valueuom'],
                       parse_dates=['charttime'])

#%% Pre-processing

for data in [output,proc,datetime]:
    #Just get our patients, and dialysis info. 
    data.drop((data[~data['itemid'].isin(rel_items['itemid'])]).index,
              inplace=True)
    data.drop((data[~data['stay_id'].isin(comp['stay_id'])]).index,
              inplace=True)

#Output has no relevant data.

#Tack on intime information, get offsets.
chart = chart.merge(comp,on='stay_id',how='inner')
chart.loc[:, 'charttime'] = pd.to_datetime(chart['charttime'], errors = 'coerce')
chart['offset'] = (chart['charttime'] - chart['intime']).dt.total_seconds()/60

proc = proc.merge(comp,on='stay_id',how='inner')
proc['startoffset'] = (proc['starttime'] - proc['intime']).dt.total_seconds()/60
proc['endoffset'] = (proc['endtime'] - proc['intime']).dt.total_seconds()/60

datetime = datetime.merge(comp,on='stay_id',how='inner')
datetime['offset'] = (datetime['charttime'] - datetime['intime']
                      ).dt.total_seconds()/60

#%% Get the feature.
#Get the first 24 hours of data.
chart = chart[(chart['offset']>=0) & (chart['offset']<=1440)]
datetime = datetime[(datetime['offset']>=0) & (datetime['offset']<=1440)]
proc = proc[(proc['endoffset']>=0) & (proc['startoffset']<=1440)]

#CHARTEVENTS has rows labeled 'Dialysis patient' which must be 1 to count.
chart = chart[(chart['itemid']!=225126) | (chart['valuenum']==1)]

#Attach label for readability.
chart = chart.merge(rel_items,on='itemid',how='left')
proc = proc.merge(rel_items,on='itemid',how='left')
datetime = datetime.merge(rel_items,on='itemid',how='left')

#Get label counts and run them by Dr. Stevens. 
# chart_counts = chart['label'].value_counts()
# proc_counts = proc['label'].value_counts()
# datetime_counts = datetime['label'].value_counts()

#Get the stays that had dialysis in the first 24 hours. 
dialysis = chart['stay_id']
dialysis = pd.concat([dialysis, datetime['stay_id'], proc['stay_id']])
dialysis.drop_duplicates(inplace=True)

comp['first_24hr_dialysis'] = comp['stay_id'].isin(dialysis).astype(int)
comp = comp[['stay_id','first_24hr_dialysis']]
comp.to_csv('first_24hr_dialysis_feature_MIMICIV.csv',index=False)

bal = comp['first_24hr_dialysis'].value_counts()

calc = time() - start