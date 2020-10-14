# -*- coding: utf-8 -*-
"""
Created on Wed Oct  7 10:17:08 2020

Re-creating old SQL code in Python to create the first 24 hour prediction data
set, which contains patients that had CAM-ICUs/ICDSCs, an ICU stay of at least 24 hrs, 
and only developed delirium after 24 hours. 

Diagnoses were used to remove early-onset patients, but were not considered
reliable enough time stamps to use for delirium onset time since it's just when 
the delirium was entered. 

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

#Get delirium diagnosis data.
diagnosis_data = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\diagnosis.csv")
diagnosis_data = diagnosis_data[diagnosis_data["diagnosisstring"]=='neurologic|altered mental status / pain|delirium']

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

#Get only positive delirium scores. 
del_onset = nurse_data[['patientunitstayid','nursingchartoffset','del_positive']]
del_onset = del_onset[del_onset['del_positive']==1]
#Find the earliest positive delirium score. 
del_onset = del_onset[['patientunitstayid','nursingchartoffset']].groupby('patientunitstayid').min().reset_index()
#Find all patients that had any positive delirium scoring. 
labels = nurse_data[['patientunitstayid','del_positive']].groupby('patientunitstayid').max().reset_index()
#Combine the info.
dataset = del_onset.merge(labels,on='patientunitstayid',how='right')

#27,250 pat stays
#%% Further filtering.

#Remove those with ICU stays less than 24 hours.
dataset = dataset.merge(pat,how='left',on='patientunitstayid')
dataset = dataset[dataset['unitdischargeoffset']>=1440]
#21,339 pat stays

#Remove those with delirium onset before 24 hours. 
early_onset = nurse_data[nurse_data['nursingchartoffset']<1440]
early_onset = early_onset[early_onset['del_positive']==1]
early_onset = early_onset['patientunitstayid'].drop_duplicates()
dataset = dataset[~dataset['patientunitstayid'].isin(early_onset)]
#18,443 pat stays

#Get early delirium diagnoses. 
early_diag = diagnosis_data[diagnosis_data['diagnosisoffset']<1440]
early_diag = early_diag['patientunitstayid'].drop_duplicates()
dataset = dataset[~dataset['patientunitstayid'].isin(early_diag)]
#18,346 pat stays.

#%% Save off stuff. 
dataset.rename(columns={'nursingchartoffset':'del_onset','unitdischargeoffset':'LOS'},inplace=True)
dataset.to_csv('first_24hr_prediction_dataset.csv',index=False)

