# -*- coding: utf-8 -*-
"""
Created on Wed Aug 26 16:46:23 2020

This code pulls static info from MIMIC for validation. Looks at the ADMISSIONS,
PATIENTS, CHARTEVENTS ,TRANSFERS tables, 
and uses .csv.

Covers:
Age
Gender
AdmitWeight
Ethnicity
AdmitTime24
TeachingStatus
Unit Type
Unit Stay Type  - needs conversion

TODO:
admission category (maybe later, needs to look at adm Dxs), 
Can't find unit admit source anywhere.
UrgentAdmission - needs conversion

Run time: 30 seconds.

@author: Kirby
"""
#%% package setup
import pandas as pd
import numpy as np
import time
import datetime
from pathlib import Path

start = time.time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
ext_valid_path = file_path.parent.parent.parent.joinpath('Ext Validation')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iii-1.4')

#%% Load in the info
comp = pd.read_csv(dataset_path.joinpath("MIMIC_complete_dataset.csv"),
                   usecols=['ICUSTAY_ID','SUBJECT_ID', 'HADM_ID',
                            'delirium_positive','INTIME','del_onset'],
                   parse_dates=['INTIME'])
adm = pd.read_csv(mimic_path.joinpath("ADMISSIONS.csv"),
                  usecols=['SUBJECT_ID', 'HADM_ID', 'ADMITTIME',
                           'ADMISSION_TYPE', 'ETHNICITY'],
                  parse_dates=['ADMITTIME'])
pat = pd.read_csv(mimic_path.joinpath("PATIENTS.csv"),
                  usecols=['SUBJECT_ID', 'GENDER', 'DOB'],
                  parse_dates=['DOB'])

tran = pd.read_csv(mimic_path.joinpath("TRANSFERS.csv"),
                  usecols=['HADM_ID','ICUSTAY_ID','PREV_CAREUNIT',
                           'CURR_CAREUNIT', 'EVENTTYPE','PREV_WARDID',
                           'CURR_WARDID','INTIME','OUTTIME'],
                  parse_dates=['INTIME','OUTTIME'])

weight_items = [226512,226531]
weight = pd.read_csv(mimic_path.joinpath("CHARTEVENTS_delirium.csv"),
                     nrows=0,
                     usecols=['ICUSTAY_ID', 'ITEMID','CHARTTIME', 'VALUE', 
                              'VALUENUM','WARNING', 'ERROR'])
for chunk in pd.read_csv(mimic_path.joinpath("CHARTEVENTS_delirium.csv"),
                         chunksize=1000000,
                         usecols=['ICUSTAY_ID','ITEMID', 'CHARTTIME', 
                                  'VALUE', 'VALUENUM','WARNING', 'ERROR']):
    temp_rows = chunk[chunk['ITEMID'].isin(weight_items)]
    weight = pd.concat([weight,temp_rows])  
    
#%% Drop stuff we don't care about.

#Drop irrelevant rows.
adm = adm[adm['HADM_ID'].isin(comp['HADM_ID'])]
pat = pat[pat['SUBJECT_ID'].isin(comp['SUBJECT_ID'])]
tran = tran[tran['ICUSTAY_ID'].isin(comp['ICUSTAY_ID'])]
weight = weight[weight['ICUSTAY_ID'].isin(comp['ICUSTAY_ID'])]
weight = weight[weight['WARNING']==0]
weight = weight[weight['ERROR']==0]

#%% Get weight.
#Convert all weight to kg, mean if multiple values.
kg_weight = weight[weight['ITEMID']==226512]
lb_weight = weight[weight['ITEMID']==226531]

kg_weight = kg_weight[['ICUSTAY_ID','VALUENUM']]

lb_weight = lb_weight[['ICUSTAY_ID','VALUENUM']]
lb_weight['VALUENUM'] = lb_weight['VALUENUM']/2.2

all_weight = pd.concat([kg_weight,lb_weight])
all_weight = all_weight.groupby('ICUSTAY_ID').mean()
all_weight.reset_index(inplace=True)

#%% Get unit type, 
tran = tran[tran['HADM_ID'].isin(comp['HADM_ID'])]
tran = tran.sort_values(['HADM_ID','INTIME'],ascending=True)

# Set all unit type features to 0, based on clinician feedback.
comp['UnitType_MedSurg'] = 0
comp['UnitType_Neuro'] = 0

#%% and unit stay type. Currently not in pruned feature list.
# #Find transfers.
# #Check if in each ICU stay

# #Find Readmits.
# #Check if there were subsequent ICUSTAY_IDs in teh same HADM_ID

# #No SDU stays as far as I can tell.
# comp['UnitStayType_Stepdown'] = 0

#%% Merge stuff together.
comp = comp.merge(adm,how='left',on=['SUBJECT_ID','HADM_ID'])
comp = comp.merge(pat,how='left',on='SUBJECT_ID')
comp = comp.merge(all_weight,how='left',on='ICUSTAY_ID')

comp.rename(columns={'VALUENUM':'AdmitWeight',
                     'ADMISSION_TYPE':'UrgentAdmission',
                     'ADMITTIME':'AdmitTime24',
                     'ETHNICITY':'Ethnicity',
                     'GENDER':'Gender'
                     },inplace=True)

#%% Tack on teaching status column.
comp['TeachingStatus'] = 1

#%% AdmitTime format
comp['AdmitTime24'] = comp['AdmitTime24'].dt.time

#%% Format ethnicity to match eICU.
def format_ethnic(ethnic):
    if ethnic.count('WHITE')>0:
        return 'Caucasion'
    elif ethnic.count('BLACK')>0:
        return 'African American'
    elif ethnic.count('ASIAN')>0:
        return 'Asian'
    elif ethnic.count('HISPANIC')>0:
        return 'Hispanic'
    elif ethnic.count('AMERICAN INDIAN')>0:
        return 'Native American'
    else:
        return 'Other/Unknown'
    
comp['Ethnicity'] = comp['Ethnicity'].apply(format_ethnic)

#%% Format gender.
def format_gender(gender):
    if gender == 'F':
        return 'Female'
    else:
        return 'Male'
    
comp['Gender'] = comp['Gender'].apply(format_gender)

#%%Calculate age from DOB and ADMITTIME

#Extract year and day of year to avoid overflow issues.
comp['DOB_day'] = comp['DOB'].dt.dayofyear
comp['DOB_year'] = comp['DOB'].dt.year + comp['DOB_day']/365

comp['INTIME_day'] = comp['INTIME'].dt.dayofyear
comp['INTIME_year'] = comp['INTIME'].dt.year + comp['INTIME_day']/365

comp['Age'] = comp['INTIME_year'] - comp['DOB_year']

#Handle >90 year old patients, and convert to integers. 
def format_age(age):
    if age >= 100:
        return 90
    else: 
        return age
    
comp['Age'] = comp['Age'].apply(format_age)


#%% Find if it's a readmit. 

comp.to_csv('static_features_MIMIC.csv',index=False)
calc_time = time.time() - start