# -*- coding: utf-8 -*-
"""
Created on Tue Jul 12 12:12:41 2022

Determine which patients had AKI in the first 24 hours of their ICU stay, 
based on KDIGO criteria (creatinine value of 1.5 times baseline, or increase 
 of 0.3 in 24 hours.)

Must be run after the labs features, to have pre-processed creatinine data 
available. 

@author: Kirby
"""
#%% Package and path setup.
import numpy as np
import pandas as pd
from pathlib import Path


file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')
labs_path = file_path.parent.parent.parent.joinpath(
    'Ext Validation 2','Labs','AllLabsBeforeDeliriumMIMICIV','creatinine.csv')

#%%Load in data. 

#Load the IDs of the patients we care about.
comp = pd.read_csv(dataset_path.joinpath('MIMICIV_complete_dataset.csv'),
                   usecols=['stay_id'])

#Get creatinine information.
creat = pd.read_csv(labs_path).rename(
    columns={'patientunitstayid':'stay_id'})
#%% Get feature.
#Only keep the stays that had delirium testing.
creat = creat[creat['stay_id'].isin(comp['stay_id'])]

#Only keep data from first 24 hours. 
creat = creat[creat['labresultoffset'] >= 0]
creat = creat[creat['labresultoffset'] <= 1440]

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

creat['AKI'] = creat.apply(lambda row: is_AKI(row['labresult'],row['baseline'])
                           ,axis=1)

feature = creat.groupby('stay_id').max().reset_index()[['stay_id','AKI']]

#Count up AKI prevalence. 
counts = feature['AKI'].value_counts()

comp = comp.merge(feature,on='stay_id',how='left')
#Save it off.
comp.to_csv('AKI_24hours_MIMICIV.csv',index=False)
