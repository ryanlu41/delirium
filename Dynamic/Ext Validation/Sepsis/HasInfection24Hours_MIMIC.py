# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 11:17:43 2020

This code is meant to pull data on whether patients had infections diagnosed
in their MIMIC data, based on diagnosis information, looking both at diagnosis
strings and the ICD9 codes if listed. 

In eICU, this code only counts it if the diagnosis
was added in the first 24 hours of the ICU stay or earlier, but MIMIC doesn't
have time stamps on diagnoses and so all are included.

Check MICROBIOLOGYEVENTS.

Run time: 1 minute.

@author: Kirby
"""
#%% Setup
import pandas as pd
import numpy as np
from time import time
from pathlib import Path

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iii-1.4')

start = time()

#%% Load in data and find vent data.

#Load in data set.
comp = pd.read_csv(dataset_path.joinpath("MIMIC_complete_dataset.csv"),
                   parse_dates=['del_onset_time','INTIME','OUTTIME'])

#Load in items.
items = pd.read_csv(mimic_path.joinpath("D_ITEMS.csv"),
                    usecols=['ITEMID','LABEL','LINKSTO'])
items = items.applymap(lambda s:s.lower() if type(s) == str else s)
rel_items = items[items['LINKSTO'].str.contains('microbiologyevents',na=False)]
#Only include organisms.
rel_items = rel_items[rel_items['ITEMID']>=80002]
rel_items = rel_items[rel_items['ITEMID']<=80312]

#Get organism names that should be discarded.
bad_desc = ['CANCELLED','NO GROWTH']

#Check MICROBIOLOGYEVENTS
micro = pd.read_csv(mimic_path.joinpath("MICROBIOLOGYEVENTS.csv"),
                   usecols=['SUBJECT_ID', 'HADM_ID', 'CHARTDATE',
                            'CHARTTIME','ORG_ITEMID','ORG_NAME'],
                  parse_dates=['CHARTDATE','CHARTTIME'])



#%% Get rows where an organism grew.

#Filter to just our patients.
micro = micro[micro['HADM_ID'].isin(comp['HADM_ID'])]

#Drop rows where both ORG_ITEMID and ORG_NAME are nan.
micro = micro[~(pd.isnull(micro['ORG_ITEMID']) & 
              pd.isnull(micro['ORG_NAME']))]

#Get one time with date time. 
def get_offset(time,date):
    if pd.isnull(time)==True:
        return date
    else:
        return time
micro['CHARTDATETIME'] = micro.apply(lambda row: 
                              get_offset(row['CHARTTIME'],row['CHARTDATE']),
                              axis=1)

#Get ICUSTAY_ID for each lab based on HADM_ID and time stamps. 
def get_ICU_stay(hadm,charttime):
    #Just get the ICU stays in the relevant HADM.
    temp = comp[comp['HADM_ID']==hadm][['ICUSTAY_ID','INTIME','OUTTIME']]
    #Check if the lab took place during any of the stays.
    temp['during_stay?'] = (charttime > temp['INTIME']) & \
        (charttime < temp['OUTTIME'])
    temp = temp[temp['during_stay?']==True]
    #If not, return nan.
    if temp.shape[0] == 0:
        return np.nan
    #If so, return the ICU stay ID.
    else:
        return temp.iloc[0,0]

micro['ICUSTAY_ID'] = micro.apply(lambda row: get_ICU_stay(row['HADM_ID'],
                                                       row['CHARTDATETIME']),
                              axis=1)

#Drop rows missing ICUSTAY_ID, while attaching delirium onset times.
micro = micro[~pd.isnull(micro['ICUSTAY_ID'])]

#Get offsets.
intimes = comp[['ICUSTAY_ID','INTIME']]
micro = micro.merge(intimes,on='ICUSTAY_ID',how='left')
micro['offset'] = (micro['CHARTDATETIME'] - micro['INTIME'])\
        .dt.total_seconds()/60

#Only keep microbiology from first day of ICU stays. 
micro = micro[micro['offset']>=0]
micro = micro[micro['offset']<=1440]
    
#Get rows from items or descriptions.
from_item = micro[micro['ORG_ITEMID'].isin(rel_items['ITEMID'])]
from_desc = micro[~micro['ORG_NAME'].isin(bad_desc)]

#Get patientunitstayids
from_item = from_item[['ICUSTAY_ID']].drop_duplicates()
from_desc = from_desc[['ICUSTAY_ID']].drop_duplicates()

#Combine info
all_ids = from_item.merge(from_desc,how='outer',on='ICUSTAY_ID')

#Save off results. 
comp['Infection'] = comp['ICUSTAY_ID'].isin(all_ids['ICUSTAY_ID'])
comp = comp[['ICUSTAY_ID','Infection']]
comp.to_csv('Infection_24hr_MIMIC.csv',index=False)

#Find class balance info.
bal = comp['Infection'].value_counts()

calc_time = time() - start