# -*- coding: utf-8 -*-
"""
Created on Thu Dec 10 12:14:53 2020

Pulls Urine info from the first day from OUTPUTEVENTS.
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

#Performance testing. 
start = time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iii-1.4')

#%% Load in and prepare relevant data. ~1 minute runtime.
comp = pd.read_csv(dataset_path.joinpath("MIMIC_complete_dataset.csv"),
                   usecols=['ICUSTAY_ID','delirium_positive','INTIME',
                            'del_onset'],
                   parse_dates=['INTIME'])

#Pull the relevant ITEMIDs/labels.
items = pd.read_csv(mimic_path.joinpath("D_ITEMS.csv"),
                    usecols=['ITEMID','LABEL','LINKSTO'])

#Finding relevant items.
rel_items = pd.read_csv('UrineITEMIDs.csv')

output = pd.read_csv(mimic_path.joinpath("OUTPUTEVENTS.csv"),
                     usecols=['ICUSTAY_ID', 'CHARTTIME', 'ITEMID',
                              'VALUE', 'VALUEUOM','ISERROR'],
                     parse_dates=['CHARTTIME'])

#Only keep data relevant to our ICU Stays, while attaching INTIME and such.
output = output.merge(comp,on=['ICUSTAY_ID'],how='inner')

#Only keep urine data. 
output = output[output['ITEMID'].isin(rel_items['ITEMID'])]

#Only keep data that wasn't erroneous. 
output = output[pd.isnull(output['ISERROR'])]

#Attach labels.
output = output.merge(rel_items,on='ITEMID',how='left')

#Calculate offset from ICU admission.
output['offset'] = (output['CHARTTIME'] - output['INTIME']
                      ).dt.total_seconds()/60

#Drop data that wasn't in the first 24 hours of ICU stay. 
output = output[(output['offset']>=0) & (output['offset']<=1440)]

#Input of GU irrigant is negative volume.
def mod_vol(itemid,val):
        if itemid != 227488:
            return val
        else:
            return val * -1
        
output['volume'] = output.apply(lambda row: mod_vol(row['ITEMID'],row['VALUE']),
                                                    axis=1)
feature = output.groupby('ICUSTAY_ID').sum()['volume'].reset_index()
comp = comp.merge(feature,on='ICUSTAY_ID',how='left')
comp.rename(columns={'volume':'first_24hr_urine'},inplace=True)

#Save off results.
comp = comp[['ICUSTAY_ID','first_24hr_urine']]
comp.to_csv('first_24hr_urine_feature_MIMIC.csv',index=False)

#performance testing.
calc_time = time() - start