# -*- coding: utf-8 -*-
"""
Created on Thu May  6 17:53:43 2021

Count up how many hospitals are represented in our patients.

@author: Kirby
"""

#%% Package setup
import pandas as pd
#import multiprocessing as mp
from time import time
from pathlib import Path

start = time()

file_path = Path(__file__)
parent = file_path.parent
eicu_path = file_path.parent.parent.parent.joinpath('eicu')

#%% Load in data.

comp = pd.read_csv('relative_1hr_lead_1hr_obs_data_set.csv')
comp.rename(columns={'PatientStayID':'patientunitstayid'},inplace=True)

pat = pd.read_csv(eicu_path.joinpath('patient.csv'),
                  usecols=['patientunitstayid','hospitalid'])

comp = comp.merge(pat,on='patientunitstayid',how='left')

count = comp['hospitalid'].drop_duplicates()