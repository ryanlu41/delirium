# -*- coding: utf-8 -*-
"""
Created on Mon Jul 11 19:15:45 2022

Analyze if there are multiple units for each lab type in MIMIC-IV.

Also pulls unit types from labs in eICU to compare. 

Lab equivalence between MIMIC-IV and eICU was determined manually.

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
eicu_path = file_path.parent.parent.parent.parent.joinpath('eicu')

#%% Load in data.

#Pulls all lab info and drop columns we don't need.
# labs_list = pd.read_excel("MIMICIV_to_eICU_Labs_List.xlsx")
labs_MIMIC = items = pd.read_csv(mimic_path.joinpath('hosp', 'labevents.csv.gz'), 
                                 usecols = ['itemid', 'valueuom'])
labs_eICU = pd.read_csv(eicu_path.joinpath("lab_delirium.csv"),
                        usecols=['labname','labmeasurenamesystem'])
items = pd.read_csv(mimic_path.joinpath('hosp', 'd_labitems.csv.gz'))

#%% Pull unique combinations of units and labs. 
labs_MIMIC.drop_duplicates(inplace = True)
labs_eICU.drop_duplicates(inplace = True)
labs_MIMIC = labs_MIMIC.merge(items, how = 'inner', on = 'itemid')
labs_MIMIC = labs_MIMIC[labs_MIMIC['fluid'] == 'Blood']

# Use this to manually look over different labs. 
print(labs_MIMIC[labs_MIMIC['label'].str.contains('Lactate')])

