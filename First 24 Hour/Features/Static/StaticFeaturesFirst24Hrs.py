# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 15:09:19 2020

Converting old SQL code to Python.

Pull static features from eICU, that are used in the first 24 hour models. 
Will pull from the Patient, Hospital, and ApachePatientResult, 

@author: Kirby
"""
#%% Packages
import numpy as np
import pandas as pd
from time import time
from pathlib import Path

start = time()
file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
eicu_path = file_path.parent.parent.parent.parent.joinpath('eicu')

#%% Load in needed data.

comp = pd.read_csv(dataset_path.joinpath("complete_patientstayid_list.csv"))
comp.rename(columns={'PatientStayID':'patientunitstayid'},inplace=True)
pat = pd.read_csv(eicu_path.joinpath("patient.csv")
                  ,usecols=['patientunitstayid','age','gender','ethnicity',
                            'apacheadmissiondx','admissionheight',
                            'hospitaladmittime24','hospitaladmitoffset',
                            'hospitaladmitsource','unittype','unitadmittime24',
                            'unitadmitsource','unitvisitnumber','unitstaytype',
                            'admissionweight','hospitalid'])
hosp = pd.read_csv(eicu_path.joinpath("hospital.csv"))
apache = pd.read_csv(eicu_path.joinpath("apachepatientresult.csv"),
                     usecols=['patientunitstayid','apachescore',
                               'apacheversion'])

#%% Get apache scores.
apache = apache[apache['apacheversion']=='IVa']
comp = comp.merge(apache,how='left',on='patientunitstayid')
comp.drop(columns='apacheversion',inplace=True)

#%% Get patient info.
comp = comp.merge(pat,how='left',on='patientunitstayid')

#Convert age to numeric.
def age_to_nums(age):
    if age == '> 89':
        return 90
    else:
        return age

comp['age'] = comp['age'].apply(age_to_nums)

#%% Get hospital traits.
comp = comp.merge(hosp,how='left',on='hospitalid')

#Convert hospital bed size to numerical categories.
def beds_to_nums(numbedscategory):
    if numbedscategory == '<100':
        return 1
    elif numbedscategory == '100 - 249':
        return 2
    elif numbedscategory == '250 - 499':
        return 3
    elif numbedscategory == '>= 500':
        return 4
    else:
        return np.nan

comp['numbedscategory'] = comp['numbedscategory'].apply(beds_to_nums)

#Convert teaching status to numerical categories.
def teach_to_nums(teachingstatus):
    if teachingstatus == 't':
        return 1
    elif teachingstatus == 'f':
        return 0
    else:
        return np.nan
    
comp['teachingstatus'] = comp['teachingstatus'].apply(teach_to_nums)

#Convert region to numerical categories.
def region_to_nums(region):
    if region == 'West':
        return 1
    elif region == 'Midwest':
        return 2
    elif region == 'Northeast':
        return 3
    elif region == 'South':
        return 4
    else:
        return np.nan
    
comp['region'] = comp['region'].apply(region_to_nums)

#%% Save off results.
comp.to_csv('static_features_first_24hrs.csv',index=False)