# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 10:53:37 2020

#This pulls the last GCS value for each patient at the end of the observation
 window, from MIMIC-III.

Runtime: 15 min

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
#%% Looping through lead times and observation windows.
for lead_hours in [1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        #%% Load in and prepare relevant data. 
        pat_stays = pd.read_csv(
            dataset_path.joinpath('MIMIC_relative_'+ str(lead_hours) + 
                                  'hr_lead_' + str(obs_hours) + 
                                  'hr_obs_data_set.csv'),
            usecols=['ICUSTAY_ID','delirium_positive','INTIME',
                     'del_onset','start','end'],
            parse_dates=['INTIME'])

        #Pull the relevant ITEMIDs/labels.
        items = pd.read_csv(mimic_path.joinpath("D_ITEMS.csv"),
                            usecols=['ITEMID','LABEL','LINKSTO'])
        
        #Finding relevant labels
        #Make it all lower case.
        items = items.applymap(lambda s:s.lower() if type(s) == str else s)
        relevant_items = items[items['LABEL'].str.contains('gcs',na=False)]
        
        gcs_data = pd.read_csv(mimic_path.joinpath("CHARTEVENTS_delirium.csv"),
                               nrows=0,
                               usecols=['ICUSTAY_ID','ITEMID', 'CHARTTIME', 
                                        'VALUE', 'VALUENUM', 'VALUEUOM', 
                                        'ERROR'],
                               parse_dates=['CHARTTIME'])
        for chunk in pd.read_csv(mimic_path.joinpath("CHARTEVENTS_delirium.csv"),
                                chunksize=1000000,
                                usecols=['ICUSTAY_ID', 'ITEMID', 'CHARTTIME', 
                                         'VALUE', 'VALUENUM', 'VALUEUOM', 
                                         'ERROR'],
                                parse_dates=['CHARTTIME']):
            temp_rows = chunk[chunk['ITEMID'].isin(relevant_items['ITEMID'])]
            gcs_data = pd.concat([gcs_data,temp_rows])    
        
        #Only keep data relevant to our ICU Stays, while attaching INTIME and such.
        gcs_data = gcs_data.merge(pat_stays,on=['ICUSTAY_ID'], how='inner')
        
        #Only keep data that wasn't erroneous. 
        gcs_data = gcs_data[gcs_data['ERROR'] == 0]
        
        # Attach item labels. 
        gcs_data = gcs_data.merge(relevant_items,how='left',on='ITEMID')
        
        #Calculate offset from ICU admission.
        gcs_data['offset'] = (gcs_data['CHARTTIME'] - gcs_data['INTIME']
                              ).dt.total_seconds()/60
        
        #Drop data after the observation window for each patient. 
        lookup = pat_stays.set_index('ICUSTAY_ID')
        def keep_row(current_ID,offset):
            #Get window time stamps.
            window_start = lookup.loc[current_ID,'start']
            window_end = lookup.loc[current_ID,'end']
            #If the GCS score took place before/in window, keep it. 
            if (offset <= window_end):
                return 1
            else:
                return 0
            
        gcs_data['keep'] = gcs_data.apply(lambda row: keep_row(
            row['ICUSTAY_ID'],row['offset']),axis=1)
        gcs_data = gcs_data[gcs_data['keep']==1]
            
        #%% Separate out verbal, motor, eye, total values. 
        
        eyes_data = gcs_data[gcs_data['ITEMID'].isin([227011,220739,226756])]
        verbal_data = gcs_data[gcs_data['ITEMID'].isin([223900,226758,227014,228112])]
        motor_data = gcs_data[gcs_data['ITEMID'].isin([223901,226757,227012])]
        total_data = gcs_data[gcs_data['ITEMID'].isin([198,226755,227013])]
        
        #For some reason, there's no data with the total_data ITEMIDs left.
        #We'll get those by adding up the other 3 scores later. 
        
        #%% Get last GCS for each part of the score, and each patient stay.
        
        #Generate column of last GCS for each ID and offset
        last_motor = motor_data[['ICUSTAY_ID','VALUENUM']].groupby(
            'ICUSTAY_ID').last().reset_index(drop=False)
        last_motor.rename(columns={'VALUENUM':'last_motor_GCS'},inplace=True)
        pat_stays = pat_stays.merge(last_motor,how='left',on='ICUSTAY_ID')
        
        last_verbal = verbal_data[['ICUSTAY_ID','VALUENUM']].groupby(
            'ICUSTAY_ID').last().reset_index(drop=False)
        last_verbal.rename(columns={'VALUENUM':'last_verbal_GCS'},inplace=True)
        pat_stays = pat_stays.merge(last_verbal,how='left',on='ICUSTAY_ID')
        
        last_eyes = eyes_data[['ICUSTAY_ID','VALUENUM']].groupby(
            'ICUSTAY_ID').last().reset_index(drop=False)
        last_eyes.rename(columns={'VALUENUM':'last_eyes_GCS'},inplace=True)
        pat_stays = pat_stays.merge(last_eyes,how='left',on='ICUSTAY_ID')
        
        #Add up the other 3 to get total feature.
        pat_stays['last_total_GCS'] = pat_stays['last_motor_GCS'] + \
            pat_stays['last_verbal_GCS'] + pat_stays['last_eyes_GCS']
        
        #Save off results.
        pat_stays = pat_stays[['ICUSTAY_ID','last_motor_GCS','last_verbal_GCS',
                               'last_eyes_GCS','last_total_GCS']]
        pat_stays.to_csv('MIMIC_relative_'+ str(lead_hours) + 'hr_lead_' + 
                         str(obs_hours) + '_GCS_feature_MIMIC.csv',index=False)

#performance testing.
calc_time = time() - start_time