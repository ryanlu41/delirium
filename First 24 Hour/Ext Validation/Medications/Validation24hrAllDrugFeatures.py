# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 12:00:15 2020

This file uses a file path to lists of drug names to search for, to pull 
medication features from MIMIC-III. It examines the first 24 hours of data.

Runtime: ~30 min.

@author: Kirby
"""

import pandas as pd
import ValidationTwentyFourHrDrugFunction as df
import os.path
import glob

comp = pd.read_csv('MIMIC_complete_dataset.csv')

#define all the paths. 
drugPathList = glob.glob(r'C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\MedicationFeatures\DrugNameLists\*')

#error checking

for path in drugPathList:
    if os.path.isfile(path) == False:
        raise NameError(path+" is not a valid file path")
        
#Loop through each medication class, attach results into one dataframe.
for i in range(1,len(drugPathList)):
    comp = pd.concat(objs=[comp,df.TwentyFourHourDrugFeature(drugPathList[i])],axis = 1)

#Save off results.    
comp.to_csv('Validation24hrsAllDrugFeatures.csv',index=False)