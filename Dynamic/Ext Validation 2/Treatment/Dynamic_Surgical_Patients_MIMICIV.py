# -*- coding: utf-8 -*-
"""
Created on Sat 30 Jul 20:19:10 2022

Checks services to determine if patients were surgical patients, having had
surgery before the end of the observation window, up to 24 hours before 
icu admission.

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
file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

#%% Load in data. 

serv_all = pd.read_csv(mimic_path.joinpath('hosp', "services.csv.gz"),
                   usecols=['hadm_id','transfertime','curr_service'],
                   parse_dates=['transfertime'])

#Get which hospital stays had surgery, and when.
serv_all = serv_all[serv_all['curr_service'].str.contains(
    'SURG|ORTHO|NSURG|CSURG|VSURG|TSURG|PSURG')]

#%%
for lead_hours in [0,1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        print(lead_hours, ',', obs_hours)
        comp = pd.read_csv(
            dataset_path.joinpath('MIMICIV_relative_'+ str(lead_hours) + 
                                  'hr_lead_' + str(obs_hours) + 
                                  'hr_obs_data_set.csv'),
            usecols=['stay_id','hadm_id','intime','start','end'],
            parse_dates=['intime'])

        #%% Only keep those where surgery was before a day of ICU LOS and before
        # end of observation window.
        serv = serv_all.merge(comp,on='hadm_id',how='inner')
        #Get relative time of surgery to ICU admission, in minutes.
        serv['surgoffset'] = (serv['transfertime'] - serv['intime']
                              ).dt.total_seconds()/60
        serv = serv[(serv['surgoffset'] <= serv['end']) & 
                    (serv['surgoffset'] >= -1440)]
        
        comp['surgical'] = comp['stay_id'].isin(serv['stay_id']
                                                              ).astype(int)
        comp = comp[['stay_id','surgical']]
        comp.to_csv('MIMICIV_relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) 
                     + 'hr_obs_surgical.csv',index=False)
        
        bal = comp['surgical'].value_counts()
        
calc = time() - start
        
        
        
