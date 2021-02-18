# -*- coding: utf-8 -*-
"""
Created on Thu Feb 11 12:12:41 2021

Determine which patients had AKI in the first 24 hours of their ICU stay, 
based on KDIGO criteria (creatinine value of 1.5 times baseline, or increase 
 of 0.3 in 24 hours.)

Must be run after the labs features, to have pre-processed creatinine data 
available. 

runtime: 30 seconds.

@author: Kirby
"""
#%% Package and path setup.
import numpy as np
import pandas as pd
from pathlib import Path
from time import time

start = time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iii-1.4')
labs_path = file_path.parent.parent.parent.joinpath(
    'Ext Validation','Labs','AllLabsBeforeDeliriumMIMIC','creatinine.csv')

#%% Looping through lead times and observation windows.
for lead_hours in [1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        
        #%%Load in data. 
        
        #Load the IDs of the patients we care about.
        pat_stays = pd.read_csv(
                    dataset_path.joinpath('MIMIC_relative_'+ str(lead_hours) + 
                                          'hr_lead_' + str(obs_hours) + 
                                          'hr_obs_data_set.csv'),
                    usecols=['ICUSTAY_ID','start','end'])
        
        #Get creatinine information.
        creat = pd.read_csv(labs_path).rename(
            columns={'patientunitstayid':'ICUSTAY_ID'})
        #%% Get feature.
        #Only keep the stays that had delirium testing.
        creat = creat[creat['ICUSTAY_ID'].isin(pat_stays['ICUSTAY_ID'])]
        
        #Drop data after the observation window for each patient. 
        lookup = pat_stays.set_index('ICUSTAY_ID')
        def keep_row(current_ID,offset):
            #Get window time stamps.
            window_start = lookup.loc[current_ID,'start']
            window_end = lookup.loc[current_ID,'end']
            #If the creatinine took place before/in window, keep it. 
            if (offset <= window_end):
                return 1
            else:
                return 0
            
        creat['keep'] = creat.apply(lambda row: keep_row(
            row['ICUSTAY_ID'],row['labresultoffset']),axis=1)
        creat = creat[creat['keep']==1]
        
        #Get initial creatinine values as baseline. 
        baseline = creat.groupby('ICUSTAY_ID').first().reset_index().rename(
            columns={'labresult':'baseline'})[['ICUSTAY_ID','baseline']]
        
        #Attach baseline data to all creatinine vaules.
        creat = creat.merge(baseline,on='ICUSTAY_ID',how='left')
        
        #Determine if that row constitutes AKI.
        def is_AKI(labresult,baseline):
            if labresult >= (1.5*baseline):
                return 1
            elif (labresult-baseline) >= 0.3:
                return 1
            else: 
                return 0
        
        creat['AKI'] = creat.apply(lambda row: is_AKI(
            row['labresult'],row['baseline']),axis=1)
        
        feature = creat.groupby('ICUSTAY_ID').max().reset_index()[
            ['ICUSTAY_ID','AKI']]
        
        #Count up AKI prevalence. 
        counts = feature['AKI'].value_counts()
        
        pat_stays = pat_stays.merge(feature,on='ICUSTAY_ID',how='left')
        #Save it off.
        pat_stays = pat_stays[['ICUSTAY_ID','AKI']]
        pat_stays.to_csv('MIMIC_dynamic_'+ str(lead_hours) + 'hr_lead_' + 
                         str(obs_hours) + 'hr_obs_AKI_feature_MIMIC.csv',
                         index=False)

calc = time() - start