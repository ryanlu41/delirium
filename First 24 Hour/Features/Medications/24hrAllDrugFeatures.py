# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 11:45:34 2020

This file uses a list of paths to csvs, with a list of drug names to search for. 
Then spits out first 24 hour of ICU data features for each one

@author: Kirby
"""


import numpy as np
import pandas as pd
import TwentyFourHrDrugFeaturesFunction as df
import os.path
import glob



comp = pd.read_csv('complete_patientstayid_list.csv')

#define all the paths. 
drugPathList = glob.glob(r'C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\Medications\DrugNameLists\*')

treatmentPathList = glob.glob(r'C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\Medications\TreatmentStrings\*')

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
    comp = pd.concat(objs=[comp,df.TwentyFourHourDrugFeature(drugPathList[i],treatmentPathList[i])],axis = 1)
    
comp.to_csv(r'C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\Medications\24hrsAllDrugFeatures.csv')