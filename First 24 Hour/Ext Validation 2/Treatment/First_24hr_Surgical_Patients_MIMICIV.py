# -*- coding: utf-8 -*-
"""
Created on Sun Jul 24 19:07:34 2022

Checks services to determine if patients were surgical patients, having had
surgery before or within 24 hours of icu admission.

runtime: 2 seconds.

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
comp = pd.read_csv(dataset_path.joinpath("MIMICIV_complete_dataset.csv"),
                   usecols=['stay_id','hadm_id','intime'],
                   parse_dates=['intime'])

serv = pd.read_csv(mimic_path.joinpath('hosp', "services.csv.gz"),
                   usecols=['hadm_id','transfertime','curr_service'],
                   parse_dates=['transfertime'])

#%% Find whch patients had surgery before/within 24 hours of their ICU Stay. 
#Get which hospital stays had surgery, and when.
serv = serv[serv['curr_service'].str.contains('SURG|ORTHO|NSURG|CSURG|VSURG|TSURG|PSURG')]
serv = serv.merge(comp,on='hadm_id',how='inner')
#Get relative time of surgery to ICU admission, in minutes.
serv['surgoffset'] = (serv['transfertime'] - serv['intime']
                      ).dt.total_seconds()/60
#Only keep those where surgery was before a day of ICU LOS.
serv = serv[(serv['surgoffset'] <= 1440) & (serv['surgoffset'] >= -1440)]

comp['first_24hr_surgical'] = comp['stay_id'].isin(serv['stay_id']
                                                      ).astype(int)
comp = comp[['stay_id','first_24hr_surgical']]
comp.to_csv('first_24hr_surgical_patients_MIMICIV.csv',index=False)

bal = comp['first_24hr_surgical'].value_counts()

calc = time() - start



