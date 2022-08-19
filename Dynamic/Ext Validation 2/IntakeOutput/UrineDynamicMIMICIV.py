# -*- coding: utf-8 -*-
"""
Created on Sat 30 Jul 20:32:29 2022

Pulls Urine info from the first day from OUTPUTEVENTS.
Based on the urine_output_first_day.sql from the MIMIC-III
code base on Github. 

Runtime: 10 seconds per loop, 2 min total for 20 loops.

@author: Kirby
"""

#%% Import packages.
import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
#import multiprocessing as mp
from time import time
from pathlib import Path

#Performance testing. 
start = time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

#%% Pull in all urine data.

#Pull the relevant itemids/labels.
items = pd.read_csv(mimic_path.joinpath('icu', "d_items.csv.gz"),
                    usecols=['itemid','label','linksto'])

#Finding relevant items.
rel_items = pd.read_csv('UrineITEMIDs.csv')[['ITEMID']]
rel_items.rename(columns = {'ITEMID': 'itemid'},inplace = True)

output_all = pd.read_csv(mimic_path.joinpath('icu', "outputevents.csv.gz"),
                     usecols=['stay_id', 'charttime', 'itemid',
                              'value', 'valueuom'],
                     parse_dates=['charttime'])
#Only keep urine data.
output_all = output_all.merge(rel_items, on = 'itemid', how = 'inner')


#%% Load in and prepare relevant data. ~1 minute runtime.
for lead_hours in [0,1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        comp = pd.read_csv(
            dataset_path.joinpath('MIMICIV_relative_'+ str(lead_hours) + 
                                  'hr_lead_' + str(obs_hours) + 
                                  'hr_obs_data_set.csv'),
            usecols=['stay_id','intime','start','end'],
            parse_dates=['intime'])

        #Only keep data relevant to our ICU Stays, while attaching intime and such.
        output = output_all.merge(comp,on=['stay_id'],how='inner')
        
        
        #Calculate offset from ICU admission.
        output['offset'] = (output['charttime'] - output['intime']
                              ).dt.total_seconds()/60
        
        #Drop data that wasn't in the last 24 hours before end of observation window.
        output = output[(output['offset'] >= (output['end']-1440)) & 
                        (output['offset'] <= output['end'])]
        
        #Input of GU irrigant is negative volume.
        def mod_vol(itemid,val):
                if itemid != 227488:
                    return val
                else:
                    return val * -1
                
        output['volume'] = output.apply(
            lambda row: mod_vol(row['itemid'],row['value']),axis=1)
        feature = output.groupby('stay_id').sum()['volume'].reset_index()
        
        #Remove negative values. 
        feature = feature[feature['volume'] >= 0]
        
        comp = comp.merge(feature,on='stay_id',how='left')
        comp.rename(columns={'volume':'last_24hr_urine'},inplace=True)
        
        #Save off results.
        comp = comp[['stay_id','last_24hr_urine']]
        comp.to_csv('MIMICIV_relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) 
                     + 'hr_obs_last_24hr_urine.csv',index=False)
        
        plt.figure()
        comp['last_24hr_urine'].hist()
        plt.figure()
        comp[comp['last_24hr_urine']<500]['last_24hr_urine'].hist()

#performance testing.
calc_time = time() - start