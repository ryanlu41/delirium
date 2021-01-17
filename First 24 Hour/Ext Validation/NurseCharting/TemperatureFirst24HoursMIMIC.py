# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 10:53:37 2020

#This pulls the min/max/mean temperature value for each patient over the first 24 
hours of their ICU stay, from MIMIC-III.

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

#%% Load in relevant data. ~1 minute runtime.
comp = pd.read_csv(dataset_path.joinpath("MIMIC_complete_dataset.csv"),
                   usecols=['ICUSTAY_ID','SUBJECT_ID', 'HADM_ID',
                            'delirium_positive','INTIME','del_onset'],
                   parse_dates=['INTIME'])

#Pull the relevant ITEMIDs/labels.
items = pd.read_csv(mimic_path.joinpath("D_ITEMS.csv")
                    ,usecols=['ITEMID','LABEL'])

#Finding relevant labels
#Make it all lower case.
items = items.applymap(lambda s:s.lower() if type(s) == str else s)
relevant_items = items[items['LABEL'].str.contains('temperature',na=False)]
celsius_items = [676,677,223762]
fahren_items = [678,679,223761]

temper_data = pd.read_csv(mimic_path.joinpath("CHARTEVENTS_delirium.csv"),
                          nrows=0,usecols=['SUBJECT_ID', 'HADM_ID', 
                                           'ICUSTAY_ID', 'ITEMID', 'CHARTTIME',
                                           'VALUE', 'VALUENUM', 'VALUEUOM', 
                                           'WARNING', 'ERROR'])
for chunk in pd.read_csv(mimic_path.joinpath("CHARTEVENTS_delirium.csv"),
                         chunksize=1000000,usecols=['SUBJECT_ID', 'HADM_ID', 
                                                    'ICUSTAY_ID', 'ITEMID', 
                                                    'CHARTTIME', 'VALUE', 
                                                    'VALUENUM', 'VALUEUOM', 
                                                    'WARNING', 'ERROR']):
    temp_rows = chunk[chunk['ITEMID'].isin(relevant_items['ITEMID'])]
    temper_data = pd.concat([temper_data,temp_rows])    

#%% Pre-process data.

#Only keep data relevant to our ICU Stays, while attaching INTIME and such.
temper_data = temper_data.merge(comp,on=['SUBJECT_ID','HADM_ID','ICUSTAY_ID'],
                                how='inner')

#Only keep data that wasn't erroneous. 
temper_data = temper_data[temper_data['ERROR'] == 0]

# Attach item labels. 
temper_data = temper_data.merge(relevant_items,how='left',on='ITEMID')

#Calculate offset from ICU admission.
temper_data['CHARTTIME'] = pd.to_datetime(temper_data['CHARTTIME'],
                                          errors='coerce')
temper_data['offset'] = (temper_data['CHARTTIME'] - temper_data['INTIME']
                         ).dt.total_seconds()/60

#Drop data that wasn't in the first 24 hours of ICU stay. 
temper_data = temper_data[temper_data['offset']>=0]
temper_data = temper_data[temper_data['offset']<=1440]
    
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
cels_data = cels_data[['ICUSTAY_ID','VALUENUM']]

#%% Get min/mean/max temperature for each ICU stay.

mean_temper = cels_data.groupby('ICUSTAY_ID').mean()
mean_temper.rename(columns={'VALUENUM':'24hrMeanTemp'},inplace=True)
comp = comp.merge(mean_temper,on='ICUSTAY_ID',how='left')

min_temper = cels_data.groupby('ICUSTAY_ID').min()
min_temper.rename(columns={'VALUENUM':'24hrMinTemp'},inplace=True)
comp = comp.merge(min_temper,on='ICUSTAY_ID',how='left')

max_temper = cels_data.groupby('ICUSTAY_ID').max()
max_temper.rename(columns={'VALUENUM':'24hrMaxTemp'},inplace=True)
comp = comp.merge(max_temper,on='ICUSTAY_ID',how='left')

#Save off results.
comp = comp[['SUBJECT_ID','HADM_ID','ICUSTAY_ID','24hrMeanTemp','24hrMinTemp',
             '24hrMaxTemp']]
comp.to_csv('first_24hr_temper_feature_MIMIC.csv',index=False)

#performance testing.
calc_time = time.time() - start_time