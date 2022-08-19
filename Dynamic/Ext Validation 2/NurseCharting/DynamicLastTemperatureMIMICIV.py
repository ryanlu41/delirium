# -*- coding: utf-8 -*-
"""
Created on Sat 30 July 6:28:30 2022

This pulls the last temperature value for each patient from before their 
observation windows of their ICU stay, from MIMIC-IV.

Takes about 7 minutes to run.

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
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

#%% Get all temperature data.

#Pull the relevant itemids/labels.
items = pd.read_csv(mimic_path.joinpath('icu', "d_items.csv.gz")
                    ,usecols=['itemid','label'])

#Finding relevant labels
#Make it all lower case.
items = items.applymap(lambda s:s.lower() if type(s) == str else s)
relevant_items = items[items['label'].str.contains('temperature',
                                                   na=False)]
relevant_items = relevant_items.query('(itemid == 223761) | (itemid == 223762)')
relevant_items = relevant_items[['itemid']]
celsius_items = [223762]
fahren_items = [223761]

all_temper_data = pd.read_csv(
    mimic_path.joinpath('icu', 'chartevents.csv.gz'),nrows=0,
    usecols=['stay_id', 'itemid', 'charttime','valuenum'],
    parse_dates=['charttime'])
for chunk in pd.read_csv(
        mimic_path.joinpath('icu', 'chartevents.csv.gz'),
        chunksize=1000000,
        usecols=['stay_id', 'itemid', 'charttime','valuenum'],
        parse_dates=['charttime']):
    temp_rows = chunk[chunk['itemid'].isin(relevant_items['itemid'])]
    all_temper_data = pd.concat([all_temper_data,temp_rows])    

#%% Looping through lead times and observation windows.
for lead_hours in [0,1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        print(lead_hours, ',', obs_hours)
        #%% Load in relevant data. ~1 minute runtime.
        pat_stays = pd.read_csv(
            dataset_path.joinpath('MIMICIV_relative_'+ str(lead_hours) + 
                                  'hr_lead_' + str(obs_hours) + 
                                  'hr_obs_data_set.csv'),
            usecols=['stay_id','intime','start','end'],
            parse_dates=['intime'])
        
        #%% Pre-process data.
        
        #Only keep data relevant to our ICU Stays, while attaching intime and such.
        temper_data = all_temper_data.merge(pat_stays,on=['stay_id'],
                                        how='inner')
        
        #Calculate offset from ICU admission.
        temper_data['charttime'] = pd.to_datetime(temper_data['charttime'],
                                                  errors='coerce')
        temper_data['offset'] = (temper_data['charttime'] - 
                                 temper_data['intime']).dt.total_seconds()/60
        
        #Drop data after the observation window for each patient. 
        temper_data = temper_data[(temper_data['offset'] <= temper_data['end'])]
            
        #Split out into C and F.
        cels_data = temper_data[temper_data['itemid'].isin(celsius_items)]
        fahr_data = temper_data[temper_data['itemid'].isin(fahren_items)].copy()
        
        #Convert F data to C, and combine it.
        fahr_data.loc[:,'valuenum'] = (fahr_data['valuenum']-32)*5/9
        cels_data = pd.concat([cels_data,fahr_data])
        
        #Remove erroneous data.
        cels_data = cels_data[cels_data['valuenum']>=30]
        cels_data = cels_data[cels_data['valuenum']<=42]
        
        #Just get columns we care about. 
        cels_data = cels_data[['stay_id','valuenum','offset']]
        cels_data.sort_values(['stay_id','offset'],inplace=True)
        
        #%% Get last temperature for each ICU stay.
        last_temper = cels_data.groupby('stay_id').last().reset_index()
        last_temper.rename(columns={'valuenum':'last_temp'},inplace=True)
        pat_stays = pat_stays.merge(last_temper,on='stay_id',how='left')
        
        #Save off results.
        pat_stays = pat_stays[['stay_id','last_temp']]
        pat_stays.to_csv('MIMICIV_dynamic_'+ str(lead_hours) + 'hr_lead_' + 
                         str(obs_hours) + '_temp_feature.csv',
                         index=False)

#performance testing.
calc_time = time.time() - start_time