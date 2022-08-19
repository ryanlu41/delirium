# -*- coding: utf-8 -*-
"""
Created on Sat Jul 30 10:41:52 2022

Search chartevents, outputevents, procedureevents,and datetimeevents for 
dialysis information in the specified time frame. 

Runtime: 1 minute per loop

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

#%% Get relevant data from the four tables. 
# Just get our patients, and dialysis info. 

items = pd.read_csv(mimic_path.joinpath('icu','d_items.csv.gz'),
                    usecols=['itemid','label','linksto']).dropna()
rel_items = items[items['label'].str.contains('dialysis',case=False)][['itemid']]

#Get Chart Events data. 
chart_all = pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'),
                       nrows=0,
                       usecols=['stay_id','itemid', 'charttime', 'valuenum'],
                       parse_dates=['charttime'])
for chunk in pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'),
                         chunksize=1000000,
                         usecols=['stay_id', 'itemid', 'charttime', 'valuenum'],
                         parse_dates=['charttime']):
    temp_rows = chunk.merge(rel_items, on='itemid', how='inner')
    chart_all = pd.concat([chart_all,temp_rows])
chart_all.loc[:, 'charttime'] = pd.to_datetime(chart_all['charttime'], 
                                               errors = 'coerce')
    
# #Get outputevents and remove erroneous rows. 
# output_all = pd.read_csv(mimic_path.joinpath('icu', "outputevents.csv.gz"),
#                          usecols=['stay_id', 'charttime', 'itemid', 'value'],
#                          parse_dates=['charttime'])
# output_all = output_all.merge(rel_items, on='itemid', how='inner')

#Get procedure events data and remove erroneous rows. 
proc_all = pd.read_csv(mimic_path.joinpath('icu','procedureevents.csv.gz'),
                       usecols=['stay_id', 'starttime', 'endtime', 
                                'itemid','value'],
                       parse_dates=['starttime','endtime'])
proc_all = proc_all.merge(rel_items, on='itemid', how='inner')


#Get datetimeevents data and remove erroneous rows.
datetime_all = pd.read_csv(mimic_path.joinpath('icu', 'datetimeevents.csv.gz'),
                           usecols=['stay_id', 'itemid', 'charttime', 'value'],
                           parse_dates=['charttime'])
datetime_all = datetime_all.merge(rel_items, on='itemid', how='inner')


#%% Load in data.
for lead_hours in [0,1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        print(lead_hours, ',', obs_hours)
        comp = pd.read_csv(
            dataset_path.joinpath('MIMICIV_relative_'+ str(lead_hours) + 
                                  'hr_lead_' + str(obs_hours) + 
                                  'hr_obs_data_set.csv'),
            usecols=['stay_id','intime','start','end'],
            parse_dates=['intime'])
        
        #%% Pre-processing

        # Filter to just the relevant patients and tack on times. 
        chart = chart_all.merge(comp, on='stay_id', how='inner')
        proc = proc_all.merge(comp, on='stay_id', how='inner')
        datetime = datetime_all.merge(comp, on='stay_id', how='inner')

        # Get offsets.

        chart['offset'] = (chart['charttime'] - chart['intime']
                           ).dt.total_seconds()/60

        proc['startoffset'] = (proc['starttime'] - proc['intime']
                               ).dt.total_seconds()/60
        proc['endoffset'] = (proc['endtime'] - proc['intime']
                             ).dt.total_seconds()/60

        datetime['offset'] = (datetime['charttime'] - datetime['intime']
                              ).dt.total_seconds()/60
        
        #%% Get the feature.
        #Drop data after the observation window for each patient. 
        chart = chart[chart['offset'] <= chart['end']]
        proc = proc[proc['startoffset'] <= proc['end']]
        proc = proc[proc['endoffset'] >= proc['start']]
        datetime = datetime[datetime['offset'] <= datetime['end']]
        
        #CHARTEVENTS has rows labeled 'Dialysis patient' which must be 1 to count.
        chart = chart[(chart['itemid']!=225126) | (chart['valuenum']==1)]
        
        # #Attach label for readability.
        # chart = chart.merge(rel_items,on='itemid',how='left')
        # proc = proc.merge(rel_items,on='itemid',how='left')
        # datetime = datetime.merge(rel_items,on='itemid',how='left')
        
        #Get label counts and run them by Dr. Stevens. 
        # chart_counts = chart['label'].value_counts()
        # proc_counts = proc['label'].value_counts()
        # datetime_counts = datetime['label'].value_counts()
        
        #Get the stays that had dialysis in the first 24 hours. 
        dialysis = chart['stay_id']
        dialysis = pd.concat([dialysis, datetime['stay_id'], proc['stay_id']])
        dialysis.drop_duplicates(inplace=True)
        
        comp['dialysis'] = comp['stay_id'].isin(dialysis).astype(int)
        comp = comp[['stay_id', 'dialysis']]
        comp.to_csv('MIMICIV_relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) 
                     + 'hr_obs_dialysis.csv', index=False)
        
        bal = comp['dialysis'].value_counts()
        
calc = time() - start