# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 10:53:37 2020

This pulls the last temperature value for each patient from before their 
observation windows of their ICU stay, from MIMIC-III.

Takes about 2 minutes to run.

@author: Kirby
"""

#%% Import packages.
import pandas as pd
#import multiprocessing as mp
import time
from pathlib import Path

#performance testing.
start_time = time.time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
ext_valid_path = file_path.parent.parent.parent.joinpath('Ext Validation')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iii-1.4')

#%% Looping through lead times and observation windows.
for lead_hours in [1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        
        #%% Load in relevant data. ~1 minute runtime.
        pat_stays = pd.read_csv(
            dataset_path.joinpath('MIMIC_relative_'+ str(lead_hours) + 
                                  'hr_lead_' + str(obs_hours) + 
                                  'hr_obs_data_set.csv'),
            usecols=['ICUSTAY_ID','delirium_positive','INTIME',
                     'del_onset','start','end'],
            parse_dates=['INTIME'])
        
        #Pull the relevant ITEMIDs/labels.
        items = pd.read_csv(mimic_path.joinpath("D_ITEMS.csv")
                            ,usecols=['ITEMID','LABEL'])
        
        #Finding relevant labels
        #Make it all lower case.
        items = items.applymap(lambda s:s.lower() if type(s) == str else s)
        relevant_items = items[items['LABEL'].str.contains('temperature',
                                                           na=False)]
        celsius_items = [676,677,223762]
        fahren_items = [678,679,223761]
        
        temper_data = pd.read_csv(
            mimic_path.joinpath("CHARTEVENTS_delirium.csv"),nrows=0,
            usecols=['ICUSTAY_ID', 'ITEMID', 'CHARTTIME','VALUE', 'VALUENUM', 
                     'VALUEUOM', 'WARNING', 'ERROR'],
            parse_dates=['CHARTTIME'])
        for chunk in pd.read_csv(
                mimic_path.joinpath("CHARTEVENTS_delirium.csv"),
                chunksize=1000000,
                usecols=['ICUSTAY_ID', 'ITEMID', 'CHARTTIME', 'VALUE', 
                         'VALUENUM', 'VALUEUOM', 'WARNING', 'ERROR'],
                parse_dates=['CHARTTIME']):
            temp_rows = chunk[chunk['ITEMID'].isin(relevant_items['ITEMID'])]
            temper_data = pd.concat([temper_data,temp_rows])    
        
        #%% Pre-process data.
        
        #Only keep data relevant to our ICU Stays, while attaching INTIME and such.
        temper_data = temper_data.merge(pat_stays,on=['ICUSTAY_ID'],
                                        how='inner')
        
        #Only keep data that wasn't erroneous. 
        temper_data = temper_data[temper_data['ERROR'] == 0]
        
        # Attach item labels. 
        temper_data = temper_data.merge(relevant_items,how='left',on='ITEMID')
        
        #Calculate offset from ICU admission.
        temper_data['offset'] = (temper_data['CHARTTIME'] - 
                                 temper_data['INTIME']).dt.total_seconds()/60
        
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
            
        temper_data['keep'] = temper_data.apply(lambda row: keep_row(
            row['ICUSTAY_ID'],row['offset']),axis=1)
        temper_data = temper_data[temper_data['keep']==1]
            
        #Split out into C and F.
        cels_data = temper_data[temper_data['ITEMID'].isin(celsius_items)]
        fahr_data = temper_data[temper_data['ITEMID'].isin(fahren_items)]
        
        #Convert F data to C, and combine it.
        fahr_data['VALUENUM'] = (fahr_data['VALUENUM']-32)*5/9
        cels_data = pd.concat([cels_data,fahr_data])
        
        #Remove erroneous data.
        cels_data = cels_data[cels_data['VALUENUM']>30]
        cels_data = cels_data[cels_data['VALUENUM']<42]
        
        #Just get columns we care about. 
        cels_data = cels_data[['ICUSTAY_ID','VALUENUM','offset']]
        cels_data.sort_values(['ICUSTAY_ID','offset'],inplace=True)
        
        #%% Get last temperature for each ICU stay.
        last_temper = cels_data.groupby('ICUSTAY_ID').last().reset_index()
        last_temper.rename(columns={'VALUENUM':'last_temp'},inplace=True)
        pat_stays = pat_stays.merge(last_temper,on='ICUSTAY_ID',how='left')
        
        #Save off results.
        pat_stays = pat_stays[['ICUSTAY_ID','last_temp']]
        pat_stays.to_csv('MIMIC_dynamic_'+ str(lead_hours) + 'hr_lead_' + 
                         str(obs_hours) + '_temp_feature_MIMIC.csv',
                         index=False)

#performance testing.
calc_time = time.time() - start_time