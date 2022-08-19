# -*- coding: utf-8 -*-
"""
Created on Tue Jul 26 19:04:23 2022

This code pulls static info from MIMIC IV for validation. Looks at the ADMISSIONS,
PATIENTS, CHARTEVENTS ,TRANSFERS, and services tables.

Covers:
Age
Gender
AdmitWeight
Ethnicity
HospitalAdmitTime24
UnitAdmitTime24
NumBeds
TeachingStatus
Unit Type
Trauma admisssion
time of day at time of prediction


TODO:
admission category (maybe later, needs to look at adm Dxs), 
Can't find unit admit source anywhere.
UrgentAdmission - needs conversion

Run time: 15 min

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
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

#%% Finding relevant items. 

items = pd.read_csv(mimic_path.joinpath('icu', 'd_items.csv.gz'))
items.loc[:, 'label'] = items['label'].str.lower()
items.loc[:, 'abbreviation'] = items['abbreviation'].str.lower()

# TODO: Add omr table for weights? 
weight_items = items[items['label'].str.contains('weight')]
# Curated manually.
weight_items = [224639, 226512, 226531]
weight_items = items[items['itemid'].isin(weight_items)][['itemid']]

height_items = items[items['label'].str.contains('height')]
# 226707 is inches, 226730 is cm. Convert it all to cm.
height_items = height_items[['itemid']]

#%% Load in the info
comp = pd.read_csv(dataset_path.joinpath("MIMICIV_complete_dataset.csv"),
                   parse_dates=['intime'])
adm = pd.read_csv(mimic_path.joinpath('hosp',"admissions.csv.gz"),
                  usecols = ['subject_id', 'hadm_id', 'admittime', 
                             'admission_type', 'race'],
                  parse_dates = ['admittime'])
pat = pd.read_csv(mimic_path.joinpath('hosp',"patients.csv.gz"),
                  usecols = ['subject_id', 'gender', 'anchor_age'])

tran = pd.read_csv(mimic_path.joinpath('hosp', "transfers.csv.gz"),
                  parse_dates=['intime','outtime'])

icu = pd.read_csv(mimic_path.joinpath('icu', "icustays.csv.gz"))

serv = pd.read_csv(mimic_path.joinpath('hosp', 'services.csv.gz'),
                   usecols=['hadm_id','curr_service'])

weight = pd.read_csv(mimic_path.joinpath('icu', "chartevents.csv.gz"),
                     nrows = 0)
height = pd.read_csv(mimic_path.joinpath('icu', "chartevents.csv.gz"),
                     nrows = 0)
for chunk in pd.read_csv(mimic_path.joinpath('icu', "chartevents.csv.gz"),
                         chunksize=1000000):
    temp_rows = chunk.merge(weight_items, how = 'inner', on = 'itemid')
    weight = pd.concat([weight,temp_rows])  
    temp_rows = chunk.merge(height_items, how = 'inner', on = 'itemid')
    height = pd.concat([height,temp_rows])  
    
#%% Drop stuff we don't care about.

#Drop irrelevant rows.
adm = adm[adm['hadm_id'].isin(comp['hadm_id'])]
pat = pat[pat['subject_id'].isin(comp['subject_id'])]
tran = tran[tran['hadm_id'].isin(comp['hadm_id'])]
icu = icu[icu['stay_id'].isin(comp['stay_id'])]
serv = serv[serv['hadm_id'].isin(comp['hadm_id'])]
weight = weight[weight['stay_id'].isin(comp['stay_id'])]
weight = weight[weight['warning'] == 0]
height = height[height['stay_id'].isin(comp['stay_id'])]
height = height[height['warning'] == 0]

#%% Get weight.
#Convert all weight to kg, mean if multiple values.
weight.loc[:, 'valuenum'] = weight['valuenum'].astype(float)
kg_weight = weight[weight['valueuom'] == 'kg']
lb_weight = weight[weight['valueuom'] != 'kg']

kg_weight = kg_weight[['stay_id', 'charttime', 'valuenum']]

lb_weight = lb_weight[['stay_id', 'charttime', 'valuenum']]
lb_weight.loc[:, 'valuenum'] = lb_weight['valuenum']/2.2

all_weight = pd.concat([kg_weight,lb_weight])
# Get rid of impossible or irrelevant weight measurements (less than 25 kg)
all_weight = all_weight[all_weight['valuenum'] >= 25]
# Take the first measurement per ICU stay as admission weight. 
all_weight.sort_values(['stay_id', 'charttime'], inplace = True)
all_weight = all_weight.groupby('stay_id').first()
all_weight.reset_index(inplace=True)
all_weight.drop(columns = 'charttime', inplace = True)
all_weight.rename(columns = {'valuenum':'AdmissionWeight'}, inplace = True)

#%% Get height. 
#Convert all height to cm, mean if multiple values.
height.loc[:, 'valuenum'] = height['valuenum'].astype(float)
cm_height = height[height['valueuom'] == 'cm']
in_height = height[height['valueuom'] != 'cm']

cm_height = height[['stay_id', 'charttime', 'valuenum']]

in_height = in_height[['stay_id', 'charttime', 'valuenum']]
in_height.loc[:, 'valuenum'] = in_height['valuenum']*2.54

all_height = pd.concat([cm_height,in_height])
# Get rid of impossible or irrelevant height measurements 
all_height = all_height[all_height['valuenum'] >= 120]
all_height = all_height[all_height['valuenum'] <= 270]

# Take the first measurement per ICU stay as admission height. 
all_height.sort_values(['stay_id', 'charttime'], inplace = True)
all_height = all_height.groupby('stay_id').first()
all_height.reset_index(inplace=True)
all_height.drop(columns = 'charttime', inplace = True)
all_height.rename(columns = {'valuenum':'AdmissionHeight'}, inplace = True)

#%% Get unit type. UnitType_MedSurg and UnitType_Neuro are two features in model.
unit_type = icu[['stay_id', 'first_careunit']].copy()
unit_type.loc[:, 'UnitType_MedSurg'] = unit_type['first_careunit'] == 'Medical/Surgical Intensive Care Unit (MICU/SICU)'
unit_type.loc[:, 'UnitType_Neuro'] = unit_type['first_careunit'] == 'Neuro Surgical Intensive Care Unit (Neuro SICU)'
for col_name in ['UnitType_MedSurg', 'UnitType_Neuro']:
    unit_type.loc[:, col_name] = unit_type[col_name].astype(int)
    
#%% Get if trauma or not. 
serv.loc[:, 'Trauma'] = (serv['curr_service'] == 'TRAUM').astype(int).copy()
# Get if neurology/neurosurgery admission.
serv.loc[:, 'Neurology/Neurosurgery'] = ((serv['curr_service'] == 'EYE') |
                                         (serv['curr_service'] == 'NMED') |
                                         (serv['curr_service'] == 'NSURG')).astype(int).copy()

serv.drop(columns = 'curr_service', inplace = True)


#%% and unit stay type. Currently not in pruned feature list.
# #Find transfers.
# #Check if in each ICU stay

# #Find Readmits.
# #Check if there were subsequent stay_ids in teh same hadm_id

# #No SDU stays as far as I can tell.
# comp['UnitStayType_Stepdown'] = 0

#%% Merge stuff together.
comp = comp.merge(adm, how = 'left', on = ['subject_id', 'hadm_id'])
comp = comp.merge(serv, how = 'left', on = 'hadm_id')
comp = comp.merge(pat, how = 'left', on = 'subject_id')
comp = comp.merge(unit_type, how = 'left', on = 'stay_id')
comp = comp.merge(all_weight, how = 'left', on = 'stay_id')
comp = comp.merge(all_height, how = 'left', on = 'stay_id')


comp.rename(columns={'admission_type':'UrgentAdmission',
                     'admittime':'hospitalAdmitTime24',
                     'race':'Ethnicity',
                     'gender':'Gender',
                     'anchor_age': 'Age'
                     },inplace=True)

#%% Get urgent admissions.
comp['UrgentAdmission'] = (comp['UrgentAdmission'] == 'URGENT').astype(int)

#%% Tack on teaching status and numbeds column.
comp['TeachingStatus'] = 1
comp['NumBeds'] = 4 #>=500 beds.

#%% AdmitTime format
comp['hospitalAdmitTime24'] = comp['hospitalAdmitTime24'].dt.time
comp['UnitAdmitTime24'] = comp['intime'].dt.time

#%% Format ethnicity to match eICU.
def format_ethnic(ethnic):
    if ethnic.count('WHITE')>0:
        return 'Caucasian'
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

#%%Get LOS at end of observation window. 
comp['LOS'] = comp['end']

#%% Save off data.


for lead_hours in [0,1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        comp.to_csv('MIMICIV_relative_'+ str(lead_hours) + 'hr_lead_' + 
                    str(obs_hours) + '_static_features.csv',index=False)
        
calc_time = time.time() - start