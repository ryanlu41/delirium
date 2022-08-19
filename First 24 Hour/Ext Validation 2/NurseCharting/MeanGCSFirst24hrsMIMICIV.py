# -*- coding: utf-8 -*-
"""
Created on Fri Jul 22 10:15:12 2022

#This pulls the mean GCS value for each patient over the first 24 hours of their
ICU stay, from MIMIC-IV.

Takes about 2 minutes to run.

@author: Kirby
"""

#%% Import packages.
import numpy as np
import pandas as pd
import os
#import multiprocessing as mp
from time import time
from pathlib import Path

#Performance testing. 
start_time = time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

#%% Load in and prepare relevant data. ~1 minute runtime.
comp = pd.read_csv(dataset_path.joinpath("MIMICIV_complete_dataset.csv"),
                   usecols=['stay_id', 'intime', 'del_onset'],
                   parse_dates=['intime'])

#%% Pull the relevant itemids/labels.
items = pd.read_csv(mimic_path.joinpath('icu', "d_items.csv.gz"),
                    usecols=['itemid','label','linksto'])

#Finding relevant labels
#Make it all lower case.
items = items.applymap(lambda s:s.lower() if type(s) == str else s)
relevant_items = items[items['label'].str.contains('gcs',na=False)]

# They're all in chartevents. 
relevant_items.drop(columns = ['linksto'], inplace = True)

#%% Just get the GCS data from chartevents. 
gcs_data = pd.read_csv(mimic_path.joinpath('icu', "chartevents.csv.gz"),
                       nrows=0,usecols=['stay_id', 'itemid', 'charttime', 
                                        'valuenum'],
                       parse_dates = ['charttime'])
for chunk in pd.read_csv(mimic_path.joinpath('icu', "chartevents.csv.gz"),
                         chunksize=1000000,
                         usecols=['stay_id', 'itemid', 'charttime', 
                                          'valuenum'],
                         parse_dates = ['charttime']):
    temp_rows = chunk.merge(relevant_items, on = 'itemid', how = 'inner')
    gcs_data = pd.concat([gcs_data,temp_rows])    

#%% Only keep data relevant to our ICU Stays, while attaching intime and such.
gcs_data = gcs_data.merge(comp, on=['stay_id'], how='inner')

#Calculate offset from ICU admission.
gcs_data['charttime'] = pd.to_datetime(gcs_data['charttime'], errors='coerce')
gcs_data['offset'] = (gcs_data['charttime'] - gcs_data['intime']
                      ).dt.total_seconds()/60

#Drop data that wasn't in the first 24 hours of ICU stay. 
gcs_data = gcs_data[gcs_data['offset'] >= 0]
gcs_data = gcs_data[gcs_data['offset'] <= 1440]
    
#%% Separate out verbal, motor, eye, total values. 

eyes_data = gcs_data[gcs_data['itemid'].isin([220739, 226756, 227011])]
verbal_data = gcs_data[gcs_data['itemid'].isin([223900,226758,227014,228112])]
motor_data = gcs_data[gcs_data['itemid'].isin([223901,226757,227012])]
total_data = gcs_data[gcs_data['itemid'].isin([198,226755,227013])]

#For some reason, there's no data with the total_data itemids left.
#We'll get those by adding up the other 3 scores later. 

#%% Get mean GCS for each part of the score, and each patient stay.

#Generate column of mean GCS for each ID and offset
mean_motor = motor_data[['stay_id','valuenum']].groupby('stay_id')\
    .mean().reset_index(drop=False)
mean_motor.rename(columns={'valuenum':'24hrMeanMotor'},inplace=True)
comp = comp.merge(mean_motor,how='left',on='stay_id')

mean_verbal = verbal_data[['stay_id','valuenum']].groupby('stay_id')\
    .mean().reset_index(drop=False)
mean_verbal.rename(columns={'valuenum':'24hrMeanVerbal'},inplace=True)
comp = comp.merge(mean_verbal,how='left',on='stay_id')

mean_eyes = eyes_data[['stay_id','valuenum']].groupby('stay_id')\
    .mean().reset_index(drop=False)
mean_eyes.rename(columns={'valuenum':'24hrMeanEyes'},inplace=True)
comp = comp.merge(mean_eyes,how='left',on='stay_id')

#Add up the other 3 to get total feature.
comp['24hrMeanTotal'] = comp['24hrMeanMotor'] + comp['24hrMeanVerbal'] + \
    comp['24hrMeanEyes']

#Save off results.
comp = comp[['stay_id','24hrMeanMotor','24hrMeanVerbal','24hrMeanEyes',
             '24hrMeanTotal']]
comp.to_csv('first_24hr_GCS_feature_MIMICIV.csv',index=False)

#performance testing.
calc_time = time() - start_time