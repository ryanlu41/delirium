# -*- coding: utf-8 -*-
"""
Created on Fri 29 Jul 00:48:36 2022

This file uses a file path to lists of drug names to search for, to pull 
medication features from MIMIC-IV. It examines the data during and before
the observation window.

Runtime: ~4 hrs.

@author: Kirby
"""

import pandas as pd
import ValidationTwoDynamicDrugFunction as df
import os.path
import glob
from time import time
from pathlib import Path

start = time()
file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')

# for lead_hours in [0,1,3,6,12]:
for lead_hours in [6,12]:
    # for obs_hours in [1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        comp = pd.read_csv(
            dataset_path.joinpath('MIMICIV_relative_'+ str(lead_hours) + 'hr_lead_' + 
                                  str(obs_hours) + 'hr_obs_data_set.csv'))
        
        #define all the paths. 
        drugPathList = glob.glob('DrugNameLists\*')
        
        #error checking
        
        for path in drugPathList:
            if os.path.isfile(path) == False:
                raise NameError(path+" is not a valid file path")
                
        #Loop through each medication class, attach results into one dataframe.
        for i in range(1,len(drugPathList)):
            comp = pd.concat(
                objs=[comp,df.DynamicDrugFeature(drugPathList[i],
                                                 lead_hours,
                                                 obs_hours)],
                axis=1)
        
        #Save off results.    
        comp.drop(columns = ['hadm_id', 'subject_id', 'intime', 'outtime', 
                             'del_onset', 'del_onset_time', 'delirium_pos',
                             'start', 'end'],
                  inplace = True)
        comp.to_csv('MIMICIV_relative_'+ str(lead_hours) + 'hr_lead_' + 
                    str(obs_hours) + 'AllDrugFeatures.csv',index=False)

calc = time() - start