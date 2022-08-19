# -*- coding: utf-8 -*-
"""
Created on Sat Jul 23 08:56:23 2022

#This pulls the RASS values for each patient over the first 24 hours of their
ICU stay, and checks if they had -4/-5 at any point to indicate Coma per the
PRE-DELIRIC definition.

Also creates mean, min, and max RASS in the first 24 hours as features. 

Runtime: a minute

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

#%% Load in and prepare relevant data.
comp = pd.read_csv(dataset_path.joinpath("MIMICIV_complete_dataset.csv"),
                        parse_dates=['intime'])

#Pull the relevant itemids/labels.
items = pd.read_csv(mimic_path.joinpath('icu', "d_items.csv.gz"),
                    usecols=['itemid','label','linksto'])

#Finding relevant labels
#Make it all lower case.
items = items.applymap(lambda s:s.lower() if type(s) == str else s)
#relevant_items = items[items['label'].str.contains('sedation score',na=False)]
#relevant_items = items[items['label'].str.contains('richmond',na=False)]
relevant_items = items[items['label']=='richmond-ras scale']
#Found nothing searching rass.
#Sedation score labeled data isn't RASS. 

#%% Just get RASS data. 
rass_data = pd.read_csv(mimic_path.joinpath('icu', "chartevents.csv.gz"),
                        nrows=0,
                        usecols=['stay_id', 'itemid', 'charttime', 'valuenum'],
                        parse_dates=['charttime'])
for chunk in pd.read_csv(mimic_path.joinpath('icu', "chartevents.csv.gz"),
                         chunksize=1000000,
                         usecols=['stay_id', 'itemid', 'charttime', 'valuenum'],
                        parse_dates=['charttime']):
    temp_rows = chunk.merge(relevant_items, on = 'itemid', how = 'inner')
    rass_data = pd.concat([rass_data,temp_rows])
#loop_time = time() - start

#%%Process data.

#Only keep rass data for patients we care about, and tack on in time.
rass_data = rass_data.merge(comp, on = 'stay_id',  how = 'inner')

#Get offset time. 
rass_data.loc[:, 'charttime'] = pd.to_datetime(rass_data['charttime'], errors = 'coerce')
rass_data['offset'] = (rass_data['charttime'] - rass_data['intime']
                      ).dt.total_seconds()/60

#Drop data outside the first 24 hour of ICU stay for each patient. 
rass_data = rass_data[rass_data['offset'] >= 0]
rass_data = rass_data[rass_data['offset'] <= 1440]

#Drop offset.
rass_data = rass_data[['stay_id', 'valuenum']]
    
#%% Get if each patient had coma (RASS of -4/-5)

#Any patients that had RASS data start off marked as no coma.
had_rass = rass_data['stay_id'].drop_duplicates()
comp['had_rass'] = comp['stay_id'].isin(had_rass)

coma = rass_data[rass_data['valuenum'] <= -4]
coma = coma['stay_id'].drop_duplicates()
comp['had_coma'] = comp['stay_id'].isin(coma)

def coma_feature(has_rass,has_coma):
    if has_rass == False:
        return np.nan
    elif has_coma == True:
        return 1
    else:
        return 0
    
comp['First24hrComa'] = comp.apply(lambda row: coma_feature(row['had_rass'],
                                                            row['had_coma']),
                                   axis=1)

comp = comp[['stay_id','First24hrComa']]

#%% Get each patient's min/mean/max RASS score in the first 24 hours.

min_rass = rass_data.groupby('stay_id').min().reset_index()\
    .rename(columns={'valuenum':'First24hrMinRASS'})
mean_rass = rass_data.groupby('stay_id').mean().reset_index()\
    .rename(columns={'valuenum':'First24hrMeanRASS'})
max_rass = rass_data.groupby('stay_id').max().reset_index()\
    .rename(columns={'valuenum':'First24hrMaxRASS'})

comp = comp.merge(min_rass,on='stay_id',how='left')
comp = comp.merge(mean_rass,on='stay_id',how='left')
comp = comp.merge(max_rass,on='stay_id',how='left')


#%% Save off results.
comp.to_csv('first_24hr_rass_and_coma_feature_MIMICIV.csv',index=False)

calc_time = time() - start_time