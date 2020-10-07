# -*- coding: utf-8 -*-
"""
Created on Wed Oct  7 10:17:08 2020

Re-creating old SQL code in Python to create the complete_patientstayid_list.csv,
which contains any patients with CAM-ICUs, ICDSCs or delirium diagnoses that 
had ICU stays of at least 12 hours. This represents all the patients we might
ever use, for prediction or clustering. 

@author: Kirby
"""

#%% Import packages and load tables.
import numpy as np
import pandas as pd
import os
#import multiprocessing as mp
import time

pat = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\patient.csv", usecols=['patientunitstayid', 'unitdischargeoffset'])


#%% Find all CAM-ICUs and delirium diagnoses.

#Just get CAM-ICU/ICDSC data.
nurse_data = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\nurseCharting.csv",nrows=0)
for chunk in pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\nurseCharting.csv", chunksize=500000):
    temp_rows = chunk[chunk['nursingchartcelltypevalname']=='Delirium Score']
    nurse_data = pd.concat([nurse_data,temp_rows])
    
#Get delirium diagnosis data.
diagnosis_data = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\diagnosis.csv")
diagnosis_data = diagnosis_data[diagnosis_data["diagnosisstring"]=='neurologic|altered mental status / pain|delirium']

#%% Get patientstay ID list.
nurse_ids = nurse_data[['patientunitstayid']].drop_duplicates().sort_values('patientunitstayid')
diag_ids = diagnosis_data[['patientunitstayid']].drop_duplicates().sort_values('patientunitstayid')
all_ids = nurse_ids.merge(diag_ids,how='outer',on='patientunitstayid')

#%% Remove those with short ICU stays.
all_ids = all_ids.merge(pat,how='left',on='patientunitstayid')
all_ids = all_ids[all_ids['unitdischargeoffset']>720]

#all_ids.to_csv('complete_patientstayid_list.csv',index=False)

#27,939 patient unit stays.
