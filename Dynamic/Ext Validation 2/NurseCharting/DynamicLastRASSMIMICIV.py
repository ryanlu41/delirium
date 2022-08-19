# -*- coding: utf-8 -*-
"""
Created on Sat 30 Jul 18:00:00 2022

Pulls the last RASS in the in the observation window as a feature, 
and checks if they had -4/-5 at any point to indicate Coma per the
PRE-DELIRIC definition.. 

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
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

#%% Get all the RASS data.

#Pull the relevant itemids/labels.
items = pd.read_csv(mimic_path.joinpath('icu', "d_items.csv.gz"),
                    usecols=['itemid','label','linksto'])

#Finding relevant labels
#Make it all lower case.
items = items.applymap(lambda s:s.lower() if type(s) == str else s)
#relevant_items = items[items['label'].str.contains('sedation score',na=False)]
#relevant_items = items[items['label'].str.contains('richmond',na=False)]
relevant_items = items[items['label']=='richmond-ras scale']
relevant_items = relevant_items[['itemid']]
#Found nothing searching rass.
#Sedation score labeled data isn't RASS. 

#Just get RASS data. 
#Takes 5 minutes for full CHARTEVENTS, or <1 min for filered CHARTEVENTS.
#start = time()
all_rass_data = pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'),
                        nrows=0,
                        usecols=['stay_id', 'itemid', 'charttime',  
                                  'valuenum'],
                        parse_dates=['charttime'])
for chunk in pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'),
                         chunksize=1000000,
                         usecols=['stay_id', 'itemid', 'charttime',  
                                   'valuenum'],
                        parse_dates=['charttime']):
    temp_rows = chunk.merge(relevant_items, on = 'itemid', how= 'inner')
    all_rass_data = pd.concat([all_rass_data,temp_rows])
#loop_time = time() - start


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
        
        #%%Process data.
        
        #Get offset time. 
        rass_data = all_rass_data.merge(pat_stays,on='stay_id',how='inner')
        rass_data.loc[:, 'charttime'] = pd.to_datetime(rass_data['charttime'], errors='coerce')
        rass_data['offset'] = (rass_data['charttime'] - rass_data['intime']
                              ).dt.total_seconds()/60
        
        #Drop data after the observation window for each patient. 
        rass_data = rass_data[(rass_data['offset'] <= rass_data['end'])]
        
        #Sort into order of scores per stay.
        rass_data = rass_data.sort_values(['stay_id','offset'])
        
        #Drop offset.
        rass_data = rass_data[['stay_id','valuenum']]
            
        #%% Get last RASS and if each patient had coma (RASS of -4/-5)
        
        last_rass = rass_data.groupby('stay_id').last().reset_index()
        last_rass.rename(columns={'valuenum':'last_rass'},inplace=True)
        pat_stays = pat_stays.merge(last_rass,on='stay_id',how='left')
        
        #Any patients that had RASS data, start off marked as no coma.
        had_rass = rass_data['stay_id'].drop_duplicates()
        pat_stays['had_rass'] = pat_stays['stay_id'].isin(had_rass)
        
        coma = rass_data[rass_data['valuenum'] <= -4]
        coma = coma['stay_id'].drop_duplicates()
        pat_stays['had_coma'] = pat_stays['stay_id'].isin(coma)
        
        def coma_feature(has_rass,has_coma):
            if has_rass == False:
                return np.nan
            elif has_coma == True:
                return 1
            else:
                return 0
            
        pat_stays['coma'] = pat_stays.apply(
            lambda row: coma_feature(row['had_rass'],row['had_coma']),axis=1)
        
        pat_stays = pat_stays[['stay_id','last_rass','coma']]
        
        #Save off results.
        pat_stays.to_csv('MIMICIV_relative_'+ str(lead_hours) + 'hr_lead_' + 
                         str(obs_hours) + '_RASS_coma_feature.csv',
                         index=False)

calc_time = time() - start_time