# -*- coding: utf-8 -*-
"""
Created on Sat Jul 23 09:07:32 2022

#This pulls the min/max/mean temperature value for each patient over the first 24 
hours of their ICU stay, from MIMIC-IV.

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
ext_valid_path = file_path.parent.parent.parent.joinpath('Ext Validation 2')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

#%% Load in relevant data. ~1 minute runtime.
comp = pd.read_csv(dataset_path.joinpath("MIMICIV_complete_dataset.csv"),
                   usecols=['stay_id', 'intime'],
                   parse_dates=['intime'])

#Pull the relevant itemids/labels.
items = pd.read_csv(mimic_path.joinpath('icu', "d_items.csv.gz")
                    ,usecols=['itemid','label'])

#Finding relevant labels
#Make it all lower case.
items = items.applymap(lambda s:s.lower() if type(s) == str else s)
relevant_items = items[items['label'].str.contains('temperature',na=False)]
relevant_items = relevant_items.query('(itemid == 223761) | (itemid == 223762)')
celsius_items = [223762]
fahren_items = [223761]

#%% Get relevant data. 
temper_data = pd.read_csv(mimic_path.joinpath('icu', "chartevents.csv.gz"),
                          nrows=0,usecols=['stay_id', 'itemid', 'charttime',
                                           'valuenum'])
for chunk in pd.read_csv(mimic_path.joinpath('icu', "chartevents.csv.gz"),
                         chunksize=1000000,usecols=['stay_id', 'itemid', 
                                                    'charttime', 'valuenum']):
    temp_rows = chunk.merge(relevant_items, on = 'itemid', how = 'inner')
    temper_data = pd.concat([temper_data,temp_rows])    

#%% Pre-process data.

#Only keep data relevant to our ICU Stays, while attaching intime and such.
temper_data = temper_data.merge(comp, on=['stay_id'], how='inner')


#Calculate offset from ICU admission.
temper_data['charttime'] = pd.to_datetime(temper_data['charttime'],
                                          errors='coerce')
temper_data['offset'] = (temper_data['charttime'] - temper_data['intime']
                         ).dt.total_seconds()/60

#Drop data that wasn't in the first 24 hours of ICU stay. 
temper_data = temper_data[temper_data['offset']>=0]
temper_data = temper_data[temper_data['offset']<=1440]
    
#Split out into C and F.
cels_data = temper_data[temper_data['itemid'].isin(celsius_items)]
fahr_data = temper_data[temper_data['itemid'].isin(fahren_items)]

#Convert F data to C, and combine it.
fahr_data['valuenum'] = (fahr_data['valuenum']-32)*5/9
cels_data = pd.concat([cels_data,fahr_data])

#Remove erroneous data.
cels_data = cels_data[cels_data['valuenum']>30]
cels_data = cels_data[cels_data['valuenum']<42]

#Just get columns we care about. 
cels_data = cels_data[['stay_id','valuenum']]

#%% Get min/mean/max temperature for each ICU stay.

mean_temper = cels_data.groupby('stay_id').mean()
mean_temper.rename(columns={'valuenum':'24hrMeanTemp'},inplace=True)
comp = comp.merge(mean_temper,on='stay_id',how='left')

min_temper = cels_data.groupby('stay_id').min()
min_temper.rename(columns={'valuenum':'24hrMinTemp'},inplace=True)
comp = comp.merge(min_temper,on='stay_id',how='left')

max_temper = cels_data.groupby('stay_id').max()
max_temper.rename(columns={'valuenum':'24hrMaxTemp'},inplace=True)
comp = comp.merge(max_temper,on='stay_id',how='left')

#Save off results.
comp = comp[['stay_id', '24hrMeanTemp', '24hrMinTemp', '24hrMaxTemp']]
comp.to_csv('first_24hr_temper_feature_MIMICIV.csv',index=False)

#performance testing.
calc_time = time.time() - start_time