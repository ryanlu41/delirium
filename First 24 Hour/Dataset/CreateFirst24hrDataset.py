# -*- coding: utf-8 -*-
"""
Created on Wed Oct  7 10:17:08 2020

Re-creating old SQL code in Python to create the first 24 hour prediction data
set, which contains patients that had CAM-ICUs/ICDSCs, an ICU stay of at least 24 hrs, 
and only developed delirium after 24 hours. 

@author: Kirby
"""

#%% Import packages and load tables.
import numpy as np
import pandas as pd
import os
#import multiprocessing as mp
import time

pat = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\patient.csv", usecols=['patientunitstayid', 'unitdischargeoffset'])


#%% Find all CAM-ICUs and delirium diagnoses, remove NaNs

#Just get CAM-ICU/ICDSC data.
nurse_data = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\nurseCharting.csv",nrows=0)
for chunk in pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\nurseCharting.csv", chunksize=500000):
    temp_rows = chunk[chunk['nursingchartcelltypevalname']=='Delirium Score']
    nurse_data = pd.concat([nurse_data,temp_rows])
    
nurse_data = nurse_data[['patientunitstayid', 'nursingchartoffset','nursingchartvalue']].dropna()

#%% Convert the values to 1s or 0s (1 for positive for delirium, 0 for negative).
def get_delirium_testing(value):
    #Check if str
    if value.lower() == 'yes':
        return 1
    elif value.lower() == 'no': 
        return 0
    #Check if num
    elif int(value) >= 4:
        return 1
    else:
        return 0
    
    
nurse_data['del_positive'] = nurse_data.apply(lambda row: get_delirium_testing(row['nursingchartvalue']),axis=1)

#%% Get patientstay ID list with onsets and labels. 
del_onset = nurse_data[['patientunitstayid','nursingchartoffset']].groupby('patientunitstayid').min().reset_index()
labels = nurse_data[['patientunitstayid','del_positive']].groupby('patientunitstayid').max().reset_index()
dataset = del_onset.merge(labels,on='patientunitstayid',how='inner')

#27,250 pat stays
#%% Further filtering.

#Remove those with ICU stays less than 24 hours.
dataset = dataset.merge(pat,how='left',on='patientunitstayid')
dataset = dataset[dataset['unitdischargeoffset']>=1440]
#21,339 pat stays

#Remove those with delirium onset before 24 hours. 
dataset = dataset[dataset['nursingchartoffset']<1440]
#17,598 pat stays

#%% Save off stuff. 
dataset.rename(columns={'nursingchartoffset':'del_onset','unitdischargeoffset':'LOS'},inplace=True)
dataset.to_csv('first_24hr_prediction_dataset.csv',index=False)