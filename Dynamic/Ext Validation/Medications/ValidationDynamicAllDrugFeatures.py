# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 12:00:15 2020

This file uses a file path to lists of drug names to search for, to pull 
medication features from MIMIC-III. It examines the data during and before
the observation window.

Runtime: ~4 hrs.

@author: Kirby
"""

import pandas as pd
import ValidationDynamicDrugFunction as df
import os.path
import glob
from time import time
from pathlib import Path

start = time()
file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')

for lead_hours in [1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        comp = pd.read_csv(
            dataset_path.joinpath('MIMIC_relative_'+ str(lead_hours) + 'hr_lead_' + 
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
        comp.to_csv('MIMIC_relative_'+ str(lead_hours) + 'hr_lead_' + 
                    str(obs_hours) + 'AllDrugFeatures.csv',index=False)

calc = time() - start