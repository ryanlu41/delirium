# -*- coding: utf-8 -*-
"""
Created on Sun Jul 24 20:24:32 2022

This code is meant to pull data on whether patients had infections diagnosed
in their MIMIC-IV data, based on microbiology information.

This info is then combined with SOFA scoring to determine if patients had 
sepsis.

Run time: 15 seconds.

@author: Kirby
"""
#%% Setup
import pandas as pd
import numpy as np
from time import time
from pathlib import Path

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

start = time()

#%% Load in data and find infection data.

#Load in data set.
comp = pd.read_csv(dataset_path.joinpath("MIMICIV_complete_dataset.csv"),
                   usecols = ['stay_id', 'hadm_id', 'intime', 'outtime'],
                   parse_dates = ['intime', 'outtime'])


#Check microbiologyevents
micro = pd.read_csv(mimic_path.joinpath('hosp', "microbiologyevents.csv.gz"),
                   usecols=['hadm_id', 'chartdate', 'charttime','org_itemid',
                            'org_name'],
                  parse_dates=['chartdate','charttime'])

#Get SOFA scoring info. 
sofa = pd.read_csv('MIMICIV_24hr_SOFA_features.csv')

#%% Get rows where an organism grew.

#Filter to just our patients.
micro = micro[micro['hadm_id'].isin(comp['hadm_id'])]

# org_itemid is null if no organism grew. 
#Drop rows where both org_itemid and org_name are nan.
micro = micro[~(pd.isnull(micro['org_itemid']) & 
              pd.isnull(micro['org_name']))]

#Get one time with date time. 
def get_offset(time,date):
    if pd.isnull(time)==True:
        return date
    else:
        return time
micro['chartdatetime'] = micro.apply(lambda row: 
                              get_offset(row['charttime'],row['chartdate']),
                              axis=1)

#%% Get stay_id for each microbiology based on hadm_id and time stamps. 

micro = micro.merge(comp, on = 'hadm_id', how = 'inner')
micro = micro.query('chartdatetime >= intime')
micro = micro.query('chartdatetime <= outtime')

#%% Get offsets.
micro['offset'] = (micro['chartdatetime'] - micro['intime'])\
        .dt.total_seconds()/60

#Only keep microbiology from first day of ICU stays. 
micro = micro[micro['offset'] >= 0]
micro = micro[micro['offset'] <= 1440]

#Save off results. 
comp['infection'] = comp['stay_id'].isin(micro['stay_id']).astype(int)
comp = comp[['stay_id','infection']]

#Put it together with SOFA scores. 
combined = comp.merge(sofa, how='left', on='stay_id')

#Check infection column for True, and suspected sepsis column for 1 to get
#sepsis feature. Can also be used with septic shock.
def get_sepsis(infection,suspected_sepsis):
    if (infection == 1) & (suspected_sepsis == 1): 
        return 1
    else:
        return 0
    
combined['24hr_sepsis'] = combined.apply(lambda row: get_sepsis(
    row['infection'],row['suspected_sepsis']), axis=1)
combined['24hr_septic_shock'] = combined.apply(lambda row: get_sepsis(
    row['infection'],row['suspected_septic_shock']), axis=1)

feature = combined[['stay_id','infection','suspected_sepsis',
                    '24hr_sepsis','suspected_septic_shock','24hr_septic_shock']]
feature.to_csv('infection_and_sepsis_24hr_MIMICIV.csv',index=False)

#Find class balance info.
bal = comp['infection'].value_counts()

calc_time = time() - start