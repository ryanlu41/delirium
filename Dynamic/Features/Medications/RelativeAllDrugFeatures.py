# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 11:45:34 2020

# This file uses a list of paths to csvs, with a list of drug names to search for. 
# Then spits out relative medication features for each one.
Can modify hours to get different time amounts before delirium onset.

Runtime: ~30 min

@author: Kirby
"""

import pandas as pd
import RelativeDrugFeaturesFunction as df
import os.path
import glob
import time as time
from pathlib import Path

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
eicu_path = file_path.parent.parent.parent.parent.joinpath('eicu')
    
start = time.time()

for lead_hours in [1,3,6,12]:
    for obs_hours in [1,3,6]:
        print(str(lead_hours) + ', ' + str(obs_hours))
        comp = pd.read_csv(
            dataset_path.joinpath('relative_'+ str(lead_hours) + 'hr_lead_' + 
                                  str(obs_hours) + 'hr_obs_data_set.csv'))
        
        #define all the paths. 
        drugPathList = glob.glob('DrugNameLists\*')
        
        treatmentPathList = glob.glob('TreatmentStrings\\*')
        
        #error checking
        if len(drugPathList)!=len(treatmentPathList):
            raise NameError('you goofin fam, the lists of paths are different lengths')
        
        for path in drugPathList:
            if os.path.isfile(path) == False:
                raise NameError(path+" is not a valid file path")
                
        for path in treatmentPathList:
            if os.path.isfile(path) == False:
                raise NameError(path+" is not a valid file path")
        
        for i in range(1,len(drugPathList)):
            comp = pd.concat(
                objs=[comp,df.RelativeDrugFeature(drugPathList[i],treatmentPathList[i],
                                                  lead_hours,obs_hours)],axis = 1)
            
        comp.to_csv('relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) +        \
                    'hr_obs_AllDrugFeatures.csv')

calc_time = time.time() - start