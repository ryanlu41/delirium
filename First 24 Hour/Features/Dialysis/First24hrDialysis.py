# -*- coding: utf-8 -*-
"""
Created on Tue Mar  2 10:41:00 2021

Search careplangeneral,apacheapsvar, and treatment.

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

cpl = pd.read_csv(eicu_path.joinpath('CarePlanGeneral.csv'),
                  usecols=['patientunitstayid','cplitemoffset','cplitemvalue'])

apache = pd.read_csv(eicu_path.joinpath('ApacheApsVar.csv'),
                     usecols=['patientunitstayid','dialysis'])

treat = pd.read_csv(eicu_path.joinpath('Treatment.csv'),
                    usecols=['patientunitstayid', 'treatmentoffset',
                             'treatmentstring'])

#%% Filter out irrelevant rows.

#Just get data for our patient stays.
for data in [cpl,apache,treat]:
    drop_index = data[~data['patientunitstayid'].isin(
        comp['patientunitstayid'])].index
    data.drop(drop_index, inplace=True)

cpl = cpl[(cpl['cplitemoffset']<=1440) & (cpl['cplitemoffset']>=0)]
cpl = cpl[cpl['cplitemvalue']=='Dialysis']
dialysis = cpl['patientunitstayid']

apache = apache[apache['dialysis']==1]
dialysis = dialysis.append(apache['patientunitstayid'],ignore_index=True)

treat = treat[(treat['treatmentoffset']<=1440) & (treat['treatmentoffset']>=0)]
treat = treat[treat['treatmentstring'].str.contains('dialysis')]
dialysis = dialysis.append(treat['patientunitstayid'],ignore_index=True)

dialysis.drop_duplicates(inplace=True)

comp['first_24hr_dialysis'] = comp['patientunitstayid'].isin(dialysis).astype(int)
comp.to_csv('first_24hr_dialysis_feature.csv',index=False)




