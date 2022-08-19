# -*- coding: utf-8 -*-
"""
Created on Fri Feb 26 20:15:15 2021

Pulls a number of binary features regarding intake output, 
and blood product transfusions for the first 24 hours. Checks inputevents.

Runtime: 45 seconds.

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
mimic_path = filepath.parent.parent.parent.parent.joinpath('mimic-iv-2.0')
dataset_path = filepath.parent.parent.parent.joinpath('Dataset')


#%% Load data 
comp = pd.read_csv(dataset_path.joinpath("MIMICIV_complete_dataset.csv"),
                   usecols=['stay_id','intime'],
                   parse_dates=['intime'])
#Pull the relevant itemids/labels.
items = pd.read_csv(mimic_path.joinpath('icu', "d_items.csv.gz"),
                    usecols=['itemid','label','linksto'])

#GET INPUTEVENTS_MV data.
inp = pd.read_csv(mimic_path.joinpath('icu', "inputevents.csv.gz"),
                 usecols=['stay_id', 'starttime', 'endtime', 'itemid'],
                 parse_dates=['starttime', 'endtime'])

#%% Filter data.
inp = inp.merge(comp,on='stay_id',how='inner')

#Convert chart times to offsets. 
inp['startoffset'] = (inp['starttime'] - inp['intime']).dt.total_seconds()/60
inp['endoffset'] = (inp['endtime'] - inp['intime']).dt.total_seconds()/60

#First 24 hours only.
inp = inp[(inp['startoffset']<=1440) & (inp['endoffset']>=0)]

#%% Blood loss and transfusions.
path_files = ['RBCitems.csv','plasmaitems.csv',
              'plateletitems.csv']
col_names = ['first_24hr_rbc','first_24hr_plasma',
             'first_24hr_platelet_transf']
for i in range(0,3):
    temp_paths = pd.read_csv(path_files[i]).rename(columns = {'ITEMID':'itemid'})
    data = inp.merge(temp_paths,on='itemid',how='inner')
    data = data['stay_id'].drop_duplicates()
    comp[col_names[i]] = comp['stay_id'].isin(data).astype(int)

#Save off features.
comp.drop(columns=['intime'],inplace=True)
comp.to_csv('first_24hr_io_features_MIMICIV.csv',index=False)

#Look at prevalences. 
prev = dict()
for col in col_names:
    prev.update({col:comp[col].value_counts()})

calc = time() - start