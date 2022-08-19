# -*- coding: utf-8 -*-
"""
Created on Sat Jul 24 19:08:12 2022

Pulls Urine info from the first day from outputevents.
Based on the urine_output_first_day.sql from the MIMIC-III
code base on Github. 

Runtime: 10 seconds.

@author: Kirby
"""

#%% Import packages.
import numpy as np
import pandas as pd
import os
#import multiprocessing as mp
from time import time
from pathlib import Path
import matplotlib.pyplot as plt

#Performance testing. 
start = time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

#%% Load in and prepare relevant data. ~1 minute runtime.
comp = pd.read_csv(dataset_path.joinpath("MIMICIV_complete_dataset.csv"),
                   usecols=['stay_id','intime'],
                   parse_dates=['intime'])

#Pull the relevant itemids/labels.
items = pd.read_csv(mimic_path.joinpath('icu', "d_items.csv.gz"),
                    usecols=['itemid','label','linksto'])

#Finding relevant items.
rel_items = pd.read_csv('UrineITEMIDs.csv').rename(columns = {'ITEMID': 'itemid'})

output = pd.read_csv(mimic_path.joinpath('icu', "outputevents.csv.gz"),
                     usecols=['stay_id', 'charttime', 'itemid',
                              'value', 'valueuom'],
                     parse_dates=['charttime'])

#Only keep data relevant to our ICU Stays, while attaching intime and such.
output = output.merge(comp,on=['stay_id'],how='inner')

#Only keep urine data. 
output = output[output['itemid'].isin(rel_items['itemid'])]

#Attach labels.
output = output.merge(rel_items,on='itemid',how='left')

#Calculate offset from ICU admission.
output['offset'] = (output['charttime'] - output['intime']
                      ).dt.total_seconds()/60

#Drop data that wasn't in the first 24 hours of ICU stay. 
output = output[(output['offset']>=0) & (output['offset']<=1440)]

#Input of GU irrigant is negative volume.
def mod_vol(itemid,val):
        if itemid != 227488:
            return val
        else:
            return val * -1
        
output['volume'] = output.apply(lambda row: mod_vol(row['itemid'],row['value']),
                                                    axis=1)
feature = output.groupby('stay_id').sum()['volume'].reset_index()

#Remove negative values. 
feature = feature[feature['volume']>=0]

comp = comp.merge(feature,on='stay_id',how='left')
comp.rename(columns={'volume':'first_24hr_urine'},inplace=True)

#Save off results.
comp = comp[['stay_id','first_24hr_urine']]
comp.to_csv('first_24hr_urine_feature_MIMICIV.csv',index=False)

plt.figure()
comp['first_24hr_urine'].hist()
plt.figure()
comp[comp['first_24hr_urine']<500]['first_24hr_urine'].hist()

#performance testing.
calc_time = time() - start