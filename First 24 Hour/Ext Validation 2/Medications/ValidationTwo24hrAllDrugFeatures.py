# -*- coding: utf-8 -*-
"""
Created on Tue Jul 12 12:29:25 2022

This file uses a file path to lists of drug names to search for, to pull 
medication features from MIMIC-III. It examines the first 24 hours of data.

Runtime: ~30 min.

@author: Kirby
"""

import pandas as pd
import ValidationTwoTwentyFourHrDrugFunction as df
import os.path
import glob
from time import time
from pathlib import Path

start = time()
file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')


comp = pd.read_csv(dataset_path.joinpath('MIMICIV_complete_dataset.csv'))

#define all the paths. 
drugPathList = glob.glob('DrugNameLists\*')

#error checking

for path in drugPathList:
    if os.path.isfile(path) == False:
        raise NameError(path+" is not a valid file path")
        
#Loop through each medication class, attach results into one dataframe.
for i in range(1,len(drugPathList)):
    print(drugPathList[i])
    comp = pd.concat(objs=[comp,df.TwentyFourHourDrugFeature(drugPathList[i])],axis = 1)

#Save off results.
comp.drop(columns = ['hadm_id', 'subject_id', 'intime', 'outtime', 'del_onset',
                     'del_onset_time', 'delirium_pos'], inplace = True)
comp.to_csv('MIMICIV_24hrs_all_drug_features.csv',index=False)

calc = time() - start