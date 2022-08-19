# -*- coding: utf-8 -*-
"""
Created on Sat 30 Jul 11:49:40 2022

#This pulls the last GCS value for each patient at the end of the observation
 window, from MIMIC-IV.

Runtime: 10 min

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

#%% Get all the GCS data. 

#Pull the relevant itemids/labels.
items = pd.read_csv(mimic_path.joinpath('icu', "d_items.csv.gz"),
                    usecols=['itemid','label','linksto'])

#Finding relevant labels
#Make it all lower case.
items = items.applymap(lambda s:s.lower() if type(s) == str else s)
relevant_items = items[items['label'].str.contains('gcs',na=False)]
relevant_items = relevant_items[['itemid']]

gcs_data_all = pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'),
                       nrows=0,
                       usecols=['stay_id','itemid', 'charttime', 
                                'valuenum'],
                       parse_dates=['charttime'])
for chunk in pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'),
                        chunksize=1000000,
                        usecols=['stay_id', 'itemid', 'charttime', 
                                 'valuenum'],
                        parse_dates=['charttime']):
    temp_rows = chunk.merge(relevant_items, on = 'itemid', how = 'inner')
    gcs_data_all = pd.concat([gcs_data_all,temp_rows])    


#%% Looping through lead times and observation windows.
for lead_hours in [0,1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        print(lead_hours, ',', obs_hours)
        #%% Load in and prepare relevant data. 
        pat_stays = pd.read_csv(
            dataset_path.joinpath('MIMICIV_relative_'+ str(lead_hours) + 
                                  'hr_lead_' + str(obs_hours) + 
                                  'hr_obs_data_set.csv'),
            usecols=['stay_id', 'intime', 'start', 'end'],
            parse_dates=['intime'])
                
        # Only keep data relevant to our ICU Stays, while attaching intime and such.
        gcs_data = gcs_data_all.merge(pat_stays, on='stay_id', how='inner')
        
        # Calculate offset from ICU admission.
        gcs_data.loc[:, 'charttime'] = pd.to_datetime(gcs_data['charttime'], errors='coerce')
        gcs_data['offset'] = (gcs_data['charttime'] - gcs_data['intime']
                              ).dt.total_seconds()/60
        
        # Drop data after the observation window for each patient. 
        gcs_data = gcs_data[(gcs_data['offset'] <= gcs_data['end'])]
            
        #%% Separate out verbal, motor, eye, total values. 
        
        eyes_data = gcs_data[gcs_data['itemid'].isin([227011,220739,226756])]
        verbal_data = gcs_data[gcs_data['itemid'].isin([223900,226758,227014,228112])]
        motor_data = gcs_data[gcs_data['itemid'].isin([223901,226757,227012])]
        total_data = gcs_data[gcs_data['itemid'].isin([198,226755,227013])]
        
        #For some reason, there's no data with the total_data itemids left.
        #We'll get those by adding up the other 3 scores later. 
        
        #%% Get last GCS for each part of the score, and each patient stay.
        
        #Generate column of last GCS for each ID and offset
        last_motor = motor_data[['stay_id','valuenum']].groupby(
            'stay_id').last().reset_index(drop=False)
        last_motor.rename(columns={'valuenum':'last_motor_GCS'},inplace=True)
        pat_stays = pat_stays.merge(last_motor,how='left',on='stay_id')
        
        last_verbal = verbal_data[['stay_id','valuenum']].groupby(
            'stay_id').last().reset_index(drop=False)
        last_verbal.rename(columns={'valuenum':'last_verbal_GCS'},inplace=True)
        pat_stays = pat_stays.merge(last_verbal,how='left',on='stay_id')
        
        last_eyes = eyes_data[['stay_id','valuenum']].groupby(
            'stay_id').last().reset_index(drop=False)
        last_eyes.rename(columns={'valuenum':'last_eyes_GCS'},inplace=True)
        pat_stays = pat_stays.merge(last_eyes,how='left',on='stay_id')
        
        #Add up the other 3 to get total feature.
        pat_stays['last_total_GCS'] = pat_stays['last_motor_GCS'] + \
            pat_stays['last_verbal_GCS'] + pat_stays['last_eyes_GCS']
        
        #Save off results.
        pat_stays = pat_stays[['stay_id','last_motor_GCS','last_verbal_GCS',
                               'last_eyes_GCS','last_total_GCS']]
        pat_stays.to_csv('MIMICIV_relative_'+ str(lead_hours) + 'hr_lead_' + 
                         str(obs_hours) + '_GCS_feature.csv',index=False)

#performance testing.
calc_time = time() - start_time