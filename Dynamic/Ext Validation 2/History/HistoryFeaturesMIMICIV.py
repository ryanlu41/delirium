# -*- coding: utf-8 -*-
"""
Created on Thu Jul 14 13:36:00 2022

Data exploration to determine where history data is done in ExploringForPatientHistory.py

Use this code to search for patient history info from the chartevents and 
ICD based diagnosis data. 

Run time: 10 seconds

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

#%% Load in data and get the relevant rows.

comp = pd.read_csv(dataset_path.joinpath("MIMICIV_complete_dataset.csv"),
                   usecols = ['stay_id', 'hadm_id'])

# Get history converter, which was manually created to pull out history data
# From either chart events or from ICD diagnosis info. 
hist_conv = pd.read_csv('history_eICU_to_MIMICIV.csv')
eicu_cols = hist_conv['eICU'].drop_duplicates()
hist_conv_chart = hist_conv[['eICU', 'value']].dropna()
hist_conv_diag = hist_conv[['eICU', 'icd_code', 'icd_version']].dropna()

# Relevant items determined manually. 
rel_items = [225811, 225059]
chart_cols = ['stay_id', 'charttime', 'itemid', 'value']
chart_hist = pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'),
                           nrows=0, usecols = chart_cols)
for chunk in pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'), 
                         chunksize=1000000, usecols = chart_cols):
    temp_rows = chunk[chunk['itemid'].isin(rel_items)]
    temp_rows = temp_rows.merge(hist_conv_chart, on = 'value', how = 'inner')
    chart_hist = pd.concat([chart_hist, temp_rows])

# Pull in diagnosis data, only keep relevant rows. 
diag = pd.read_csv(mimic_path.joinpath('hosp', 'diagnoses_icd.csv.gz'),
                   usecols = ['hadm_id', 'icd_code', 'icd_version'])
# Attach stay id. 
diag = diag.merge(comp[['hadm_id', 'stay_id']], on = 'hadm_id', how = 'inner')
diag = diag.merge(hist_conv_diag, on = ['icd_code', 'icd_version'], how = 'inner')


#%% Pull binary history features. 

# Put together the two sources of information.
diag = diag[['stay_id', 'eICU']]
chart_hist = chart_hist[['stay_id', 'eICU']]
both = pd.concat([diag, chart_hist])

# Loop through all the different history columns and generate the features.
for col_name in eicu_cols:
    feat = both.copy()
    feat.loc[:, col_name] = (feat['eICU'] == col_name).astype(int)
    feat.drop(columns = ['eICU'], inplace = True)
    feat = feat.groupby('stay_id').max()
    comp = comp.merge(feat, on = 'stay_id', how = 'left')
        
# Fill in all missing values with 0s. 
comp = comp.fillna(0)

#%% Save off results.
comp.to_csv('history_features_MIMICIV.csv',index=False)

#Performance testing.
calc_time = time.time() - start

#Get proportions of history for sanity checking.
prop = pd.DataFrame(columns=['proportion'])
for col_name in eicu_cols:
    if comp[col_name].value_counts().shape[0] == 1:
        prop.loc[col_name,'proportion'] = 0
        continue
    prop.loc[col_name,'proportion'] = \
        round(comp[col_name].value_counts().loc[1]/comp.shape[0],3)

prop.to_csv('history_feature_proportions.csv',index=True)