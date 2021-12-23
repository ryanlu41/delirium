# -*- coding: utf-8 -*-
"""
Created on Tue Mar  2 23:03:29 2021

Checks apachepredvar, treatment, and admissiondx to determine if patients were
surgical patients, or had surgery in their first 24 hours of ICU stay. 

runtime: 10 seconds.

@author: Kirby
"""

#%% Import packages.
import numpy as np
import pandas as pd
import os
#import multiprocessing as mp
from time import time
from pathlib import Path

start = time()
filepath = Path(__file__)
eicu_path = filepath.parent.parent.parent.parent.joinpath('eicu')
dataset_path = filepath.parent.parent.parent.joinpath('Dataset')

#%% Load in data. 
comp = pd.read_csv(dataset_path.joinpath('complete_patientstayid_list.csv'))
comp.rename(columns={'PatientStayID':'patientunitstayid'},inplace=True)

adm_dx = pd.read_csv(eicu_path.joinpath("admissionDx.csv"),
                     usecols=['patientunitstayid', 'admitdxenteredoffset',
                              'admitdxpath','admitdxname'])
apache = pd.read_csv(eicu_path.joinpath("apachepredvar.csv"),
                     usecols=['patientunitstayid', 'admitdiagnosis', 
                              'electivesurgery', 'admitsource'])
treat = pd.read_csv(eicu_path.joinpath("treatment.csv"),
                    usecols=['patientunitstayid', 'treatmentoffset',
                             'treatmentstring'])

#%% Remove irrelevant patients, and treatments after 24 hrs. 
for data in [adm_dx,apache,treat]:
    drop_index = data[~data['patientunitstayid'].isin(
        comp['patientunitstayid'])].index
    data.drop(drop_index, inplace=True)
    
treat = treat[treat['treatmentoffset']<=1440]

#%% Identify surgical patients in AdmissionDx/ApachePredVar

all_dxs = adm_dx[['admitdxpath']].drop_duplicates().sort_values('admitdxpath')
#Keyword searching through all paths. 
operative = all_dxs[all_dxs['admitdxpath'].str.contains("\\|Operative")]
oper_room = all_dxs[all_dxs['admitdxpath'].str.contains("O\.R\.")]
elective = all_dxs[all_dxs['admitdxpath'].str.contains("Elective")]

#Find all patients that had operative dxs.
op_dx_pats = adm_dx[adm_dx['admitdxpath'].isin(operative['admitdxpath'])]
op_dx_pats = op_dx_pats[['patientunitstayid']].drop_duplicates()

#Get all rows that had operation diagnoses of some sort.
op_dx_pats_info = adm_dx[adm_dx['patientunitstayid'].isin(
    op_dx_pats['patientunitstayid'])]

#Only keep patients that had S- prefixes in admit diagnosis. 
s_pats_info = apache[apache['admitdiagnosis'].str.contains('S-',na=False)]
s_pats = s_pats_info[['patientunitstayid']]

#Combine the info together.
admdx_apache = op_dx_pats.merge(s_pats,on='patientunitstayid',how='outer')

#%% Identify surgical patients from treatment.

#Using Youn-hoa's manual review document to filter out non-surgical stuff.
review = pd.read_excel('relevant_treatment_descriptions_yhj.xls',
                       usecols=['treatmentstring','Surgery?'])
relevant_treat_str = review[review['Surgery?']==1]['treatmentstring']
treat_pats = treat[treat['treatmentstring'].isin(relevant_treat_str)]
treat_pats = treat_pats[['patientunitstayid']]

#Combine with admdx and apache info.
all_three = admdx_apache.merge(treat_pats,on='patientunitstayid',how='outer')
all_three = all_three['patientunitstayid'].drop_duplicates()

comp['first_24hr_surgical'] = comp['patientunitstayid'].isin(all_three).astype(int)
comp.to_csv('first_24hr_surgical_patients.csv',index=False)

calc = time() - start




