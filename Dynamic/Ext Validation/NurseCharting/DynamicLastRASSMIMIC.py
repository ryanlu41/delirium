# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 19:57:28 2020

#This pulls the RASS values for each patient over the first 24 hours of their
ICU stay, and checks if they had -4/-5 at any point to indicate Coma per the
PRE-DELIRIC definition.

Also creates mean, min, and max RASS in the first 24 hours as features. 

Runtime: 11 min

@author: Kirby
"""
#%% Import packages.
import numpy as np
import pandas as pd
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
        #relevant_items = items[items['LABEL'].str.contains('sedation score',na=False)]
        #relevant_items = items[items['LABEL'].str.contains('richmond',na=False)]
        relevant_items = items[items['LABEL']=='richmond-ras scale']
        #Found nothing searching rass.
        #Sedation score labeled data isn't RASS. 
        
        #Just get RASS data. 
        #Takes 5 minutes for full CHARTEVENTS, or <1 min for filered CHARTEVENTS.
        #start = time()
        rass_data = pd.read_csv(mimic_path.joinpath("CHARTEVENTS_delirium.csv"),
                                nrows=0,
                                usecols=['SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID', 
                                          'ITEMID', 'CHARTTIME', 'VALUE', 
                                          'VALUENUM', 'VALUEUOM', 'WARNING', 
                                          'ERROR'],
                                parse_dates=['CHARTTIME'])
        for chunk in pd.read_csv(mimic_path.joinpath("CHARTEVENTS_delirium.csv"),
                                 chunksize=1000000,
                                 usecols=['SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID', 
                                          'ITEMID', 'CHARTTIME', 'VALUE', 
                                          'VALUENUM', 'VALUEUOM', 'WARNING', 
                                          'ERROR'],
                                parse_dates=['CHARTTIME']):
            temp_rows = chunk[chunk['ITEMID'].isin(relevant_items['ITEMID'])]
            rass_data = pd.concat([rass_data,temp_rows])
        #loop_time = time() - start
        
        #%%Process data.
        
        #Only keep rass data for patients we care about. 
        rass_data = rass_data[rass_data['ICUSTAY_ID'].isin(
            pat_stays['ICUSTAY_ID'])]
        
        #Only keep data that wasn't erroneous. 
        rass_data = rass_data[rass_data['ERROR'] == 0]
        
        #Get offset time. 
        intimes = pat_stays[['ICUSTAY_ID','INTIME']]
        rass_data = rass_data.merge(intimes,on='ICUSTAY_ID',how='left')
        rass_data['offset'] = (rass_data['CHARTTIME'] - rass_data['INTIME']
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
            
        rass_data['keep'] = rass_data.apply(lambda row: keep_row(
            row['ICUSTAY_ID'],row['offset']),axis=1)
        rass_data = rass_data[rass_data['keep']==1]
        
        #Sort into order of scores per stay.
        rass_data = rass_data.sort_values(['ICUSTAY_ID','offset'])
        
        #Drop offset.
        rass_data = rass_data[['ICUSTAY_ID','VALUENUM']]
            
        #%% Get last RASS and if each patient had coma (RASS of -4/-5)
        
        last_rass = rass_data.groupby('ICUSTAY_ID').last().reset_index()
        last_rass.rename(columns={'VALUENUM':'last_rass'},inplace=True)
        pat_stays = pat_stays.merge(last_rass,on='ICUSTAY_ID',how='left')
        
        #Any patients that had RASS data, start off marked as no coma.
        had_rass = rass_data['ICUSTAY_ID'].drop_duplicates()
        pat_stays['had_rass'] = pat_stays['ICUSTAY_ID'].isin(had_rass)
        
        coma = rass_data[rass_data['VALUENUM']<=-4]
        coma = coma['ICUSTAY_ID'].drop_duplicates()
        pat_stays['had_coma'] = pat_stays['ICUSTAY_ID'].isin(coma)
        
        def coma_feature(has_rass,has_coma):
            if has_rass == False:
                return np.nan
            elif has_coma == True:
                return 1
            else:
                return 0
            
        pat_stays['coma'] = pat_stays.apply(
            lambda row: coma_feature(row['had_rass'],row['had_coma']),axis=1)
        
        pat_stays = pat_stays[['ICUSTAY_ID','last_rass','coma']]
        
        #Save off results.
        pat_stays.to_csv('MIMIC_relative_'+ str(lead_hours) + 'hr_lead_' + 
                         str(obs_hours) + '_RASS_coma_feature_MIMIC.csv',
                         index=False)

calc_time = time() - start_time