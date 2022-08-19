# -*- coding: utf-8 -*-
"""
Created on Sat Jul 30 20:57:15 2022

Determine which patients had AKI in the any time in the observation window
in their ICU stay, 
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
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')
labs_path = file_path.parent.parent.parent.joinpath(
    'Ext Validation 2','Labs','AllLabsBeforeDeliriumMIMICIV','creatinine.csv')

#%% Get all creatinine data.
creat_all = pd.read_csv(labs_path).rename(
    columns={'patientunitstayid':'stay_id'})

#%% Looping through lead times and observation windows.
for lead_hours in [0,1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        print(lead_hours, ',', obs_hours)
        #%%Load in data. 
        
        #Load the IDs of the patients we care about.
        pat_stays = pd.read_csv(
                    dataset_path.joinpath('MIMICIV_relative_'+ str(lead_hours) + 
                                          'hr_lead_' + str(obs_hours) + 
                                          'hr_obs_data_set.csv'),
                    usecols=['stay_id','start','end'])
        
        
        #%% Get feature.
        #Only keep the stays that had delirium testing while tacking on info.
        creat = creat_all.merge(pat_stays, on = 'stay_id', how = 'inner')
        
        #Drop data after the observation window for each patient. 
        creat = creat[creat['labresultoffset'] <= creat['end']]
        
        #Get initial creatinine values as baseline. 
        baseline = creat.groupby('stay_id').first().reset_index().rename(
            columns={'labresult':'baseline'})[['stay_id','baseline']]
        
        #Attach baseline data to all creatinine vaules.
        creat = creat.merge(baseline,on='stay_id',how='left')
        
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
        
        feature = creat.groupby('stay_id').max().reset_index()[
            ['stay_id','AKI']]
        
        #Count up AKI prevalence. 
        counts = feature['AKI'].value_counts()
        
        pat_stays = pat_stays.merge(feature,on='stay_id',how='left')
        #Save it off.
        pat_stays = pat_stays[['stay_id','AKI']]
        pat_stays.to_csv('MIMICIV_dynamic_'+ str(lead_hours) + 'hr_lead_' + 
                         str(obs_hours) + 'hr_obs_AKI_feature.csv',
                         index=False)
        
        bal = pat_stays['AKI'].value_counts()

calc = time() - start