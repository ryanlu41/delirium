# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 10:53:37 2020

#This pulls the mean GCS value for each patient over the first 24 hours of their
ICU stay, from MIMIC-III.

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
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iii-1.4')

#%% Load in and prepare relevant data. ~1 minute runtime.
comp = pd.read_csv(dataset_path.joinpath("MIMIC_complete_dataset.csv"),
                   usecols=['ICUSTAY_ID','SUBJECT_ID', 'HADM_ID',
                            'delirium_positive','INTIME','del_onset'],
                   parse_dates=['INTIME'])

#Pull the relevant ITEMIDs/labels.
items = pd.read_csv(mimic_path.joinpath("D_ITEMS.csv"),
                    usecols=['ITEMID','LABEL','LINKSTO'])

#Finding relevant labels
#Make it all lower case.
items = items.applymap(lambda s:s.lower() if type(s) == str else s)
relevant_items = items[items['LABEL'].str.contains('gcs',na=False)]

gcs_data = pd.read_csv(mimic_path.joinpath("CHARTEVENTS_delirium.csv"),
                       nrows=0,usecols=['SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID',
                                        'ITEMID', 'CHARTTIME', 'VALUE', 
                                        'VALUENUM', 'VALUEUOM', 'WARNING', 
                                        'ERROR'])
for chunk in pd.read_csv(mimic_path.joinpath("CHARTEVENTS_delirium.csv"),
                         chunksize=1000000,
                         usecols=['SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID', 
                                  'ITEMID', 'CHARTTIME', 'VALUE', 'VALUENUM', 
                                  'VALUEUOM', 'WARNING', 'ERROR']):
    temp_rows = chunk[chunk['ITEMID'].isin(relevant_items['ITEMID'])]
    gcs_data = pd.concat([gcs_data,temp_rows])    

#Only keep data relevant to our ICU Stays, while attaching INTIME and such.
gcs_data = gcs_data.merge(comp,on=['SUBJECT_ID','HADM_ID','ICUSTAY_ID'],
                          how='inner')

#Only keep data that wasn't erroneous. 
gcs_data = gcs_data[gcs_data['ERROR'] == 0]

# Attach item labels. 
gcs_data = gcs_data.merge(relevant_items,how='left',on='ITEMID')

#Calculate offset from ICU admission.
gcs_data['CHARTTIME'] = pd.to_datetime(gcs_data['CHARTTIME'],errors='coerce')
gcs_data['offset'] = (gcs_data['CHARTTIME'] - gcs_data['INTIME']
                      ).dt.total_seconds()/60

#Drop data that wasn't in the first 24 hours of ICU stay. 
gcs_data = gcs_data[gcs_data['offset']>=0]
gcs_data = gcs_data[gcs_data['offset']<=1440]
    
#%% Separate out verbal, motor, eye, total values. 

eyes_data = gcs_data[gcs_data['ITEMID'].isin([227011,220739,226756])]
verbal_data = gcs_data[gcs_data['ITEMID'].isin([223900,226758,227014,228112])]
motor_data = gcs_data[gcs_data['ITEMID'].isin([223901,226757,227012])]
total_data = gcs_data[gcs_data['ITEMID'].isin([198,226755,227013])]

#For some reason, there's no data with the total_data ITEMIDs left.
#We'll get those by adding up the other 3 scores later. 

#%% Get mean GCS for each part of the score, and each patient stay.

#Generate column of mean GCS for each ID and offset
mean_motor = motor_data[['ICUSTAY_ID','VALUENUM']].groupby('ICUSTAY_ID')\
    .mean().reset_index(drop=False)
mean_motor.rename(columns={'VALUENUM':'24hrMeanMotor'},inplace=True)
comp = comp.merge(mean_motor,how='left',on='ICUSTAY_ID')

mean_verbal = verbal_data[['ICUSTAY_ID','VALUENUM']].groupby('ICUSTAY_ID')\
    .mean().reset_index(drop=False)
mean_verbal.rename(columns={'VALUENUM':'24hrMeanVerbal'},inplace=True)
comp = comp.merge(mean_verbal,how='left',on='ICUSTAY_ID')

mean_eyes = eyes_data[['ICUSTAY_ID','VALUENUM']].groupby('ICUSTAY_ID')\
    .mean().reset_index(drop=False)
mean_eyes.rename(columns={'VALUENUM':'24hrMeanEyes'},inplace=True)
comp = comp.merge(mean_eyes,how='left',on='ICUSTAY_ID')

#Add up the other 3 to get total feature.
comp['24hrMeanTotal'] = comp['24hrMeanMotor'] + comp['24hrMeanVerbal'] + \
    comp['24hrMeanEyes']

#Save off results.
comp = comp[['SUBJECT_ID','HADM_ID','ICUSTAY_ID','24hrMeanMotor',
             '24hrMeanVerbal','24hrMeanEyes','24hrMeanTotal']]
comp.to_csv('first_24hr_GCS_feature_MIMIC.csv',index=False)

#performance testing.
calc_time = time() - start_time