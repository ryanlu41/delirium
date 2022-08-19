# -*- coding: utf-8 -*-
"""
Created on Sat 30 Jul 20:47:58 2022

Pulls a number of binary features regarding intake output, 
and blood product transfusions for the first 24 hours. Checks CHARTEVENTS,
INPUTEVENTS_CV, INPUTEVENTS_MV.

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

#Pull the relevant itemids/labels.
items = pd.read_csv(mimic_path.joinpath('icu', "d_items.csv.gz"),
                    usecols=['itemid','label','linksto'])

#GET INPUTEVENTS_MV data.
inp_all = pd.read_csv(mimic_path.joinpath('icu', "inputevents.csv.gz"),
                 usecols=['stay_id', 'starttime', 'endtime', 'itemid'],
                 parse_dates=['starttime', 'endtime'])

#%% Load data 
for lead_hours in [0,1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        print(lead_hours, ',', obs_hours)
        comp = pd.read_csv(
            dataset_path.joinpath('MIMICIV_relative_'+ str(lead_hours) + 
                                  'hr_lead_' + str(obs_hours) + 
                                  'hr_obs_data_set.csv'),
            usecols=['stay_id','intime','start','end'],
            parse_dates=['intime'])
        
        #%% Filter data.
        inp = inp_all.merge(comp,on='stay_id',how='inner')
        
        #Convert chart times to offsets. 
        inp['startoffset'] = (inp['starttime'] - inp['intime']).dt.total_seconds()/60
        inp['endoffset'] = (inp['endtime'] - inp['intime']).dt.total_seconds()/60
        
        #Only data during/before observation window.
        inp = inp[(inp['startoffset'] <= inp['end'])]
        
        #%% Blood loss and transfusions.
        path_files = ['RBCitems.csv','plasmaitems.csv',
                      'plateletitems.csv']
        col_names = ['tranfuse_rbc','tranfuse_plasma',
                     'tranfuse_platelet']
        for i in range(0,3):
            temp_paths = pd.read_csv(path_files[i]).rename(columns = {'ITEMID':'itemid'})
            data = inp.merge(temp_paths,on='itemid',how='inner')
            data = data['stay_id'].drop_duplicates()
            comp[col_names[i]] = comp['stay_id'].isin(data).astype(int)
        
        #Save off features.
        comp.drop(columns=['intime','start','end'],inplace=True)
        comp.to_csv('MIMICIV_relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) 
                     + 'hr_obs_transfuse.csv',index=False)
        
        #Look at prevalences. 
        prev = dict()
        for col in col_names:
            prev.update({col:comp[col].value_counts()})

calc = time() - start