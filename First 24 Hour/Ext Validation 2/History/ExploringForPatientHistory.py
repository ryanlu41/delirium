# -*- coding: utf-8 -*-
"""
Created on Thu Jul 14 13:36:00 2022

Use this code to search for patient history info in MIMIC IV.

Looking in chartevents.

Let's also check noteevents. Unfortunately not publicly available. 

run time: 6 min

@author: Kirby
"""
#%% Setup
import pandas as pd
import numpy as np
from pathlib import Path
import time

start = time.time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
ext_valid_path = file_path.parent.parent.parent.joinpath('Ext Validation 2')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

#%% Analyze the icd codes. 

# look at icd codes in database. 
# Note that icd codes have periods removed. 
diag_items = pd.read_csv(mimic_path.joinpath('hosp', 'd_icd_diagnoses.csv.gz'))

diag_items.loc[:, 'long_title'] = diag_items['long_title'].str.lower()
pers_hist = diag_items[diag_items['long_title'].str.contains('personal history')]

diag = pd.read_csv(mimic_path.joinpath('hosp', 'diagnoses_icd.csv.gz'))
diag = diag.merge(pers_hist, on = ['icd_code', 'icd_version'], how = 'inner')

unique_history = diag[['icd_code', 'icd_version', 'long_title']].drop_duplicates().sort_values(['long_title'])
unique_history.to_csv('unique_history_icd.csv', index = False)


#%% omr not relevant, looks like measurements. 
# omr = pd.read_csv(mimic_path.joinpath('hosp', 'omr.csv.gz'), nrows = 10000)

#%% Analyze the items. 
items = pd.read_csv(mimic_path.joinpath('icu', 'd_items.csv.gz'),
                    usecols=['itemid', 'label', 'linksto'])

items.loc[:, 'label'] = items['label'].str.lower()

#Trying terms. "history" all link to chartevents.
history = items[items['label'].str.contains('history', na=False)]

#Pulling chart events with the relevant IDs found above. 
history_rows = pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'),
                           nrows=0)
for chunk in pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'), 
                         chunksize=1000000):
    temp_rows = chunk.merge(history, on ='itemid', how = 'inner')
    history_rows = pd.concat([history_rows, temp_rows])
    
unique_history = history_rows[['itemid', 'label', 'value']].drop_duplicates().sort_values(['label'])

unique_history.to_csv('unique_history_items.csv', index = False)

calc_time = time.time() - start
