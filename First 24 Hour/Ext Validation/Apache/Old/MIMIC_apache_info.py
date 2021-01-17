# -*- coding: utf-8 -*-
"""
Created on Wed Oct 28 15:07:02 2020

Pull ApacheIV data from MIMIC-III. 

There's no apache data at all seemingly in CHARTEVENTS when filtered by our 
ICUSTAY_IDs.

It's not in CHARTEVENTS? Even though it should be? 

Seems there's no each way to get APACHE. Should we drop it? 

@author: Kirby
"""
#%% Package setup
import pandas as pd
import numpy as np

#%% Load in data.
comp = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\Validation\MIMIC_complete_dataset.csv",
                   usecols=['ICUSTAY_ID','SUBJECT_ID', 'HADM_ID',
                            'delirium_positive','INTIME','del_onset']
                   ,parse_dates=['INTIME'])
items = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\D_ITEMS.csv",
                    usecols=['ITEMID','LABEL'])

#Finding relevant labels
#Make it all lower case.
items = items.applymap(lambda s:s.lower() if type(s) == str else s)
#relevant_items = [226993,226994,226995] #identified in ExploringItemsTable.py.
relevant_items = items[items['LABEL'].str.contains('apache',na=False)]

apache = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\CHARTEVENTS_delirium.csv",
                     nrows=0,
                     usecols=['SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID', 'ITEMID',
                              'CHARTTIME', 'VALUE', 'VALUENUM', 'VALUEUOM', 
                              'WARNING', 'ERROR'])
for chunk in pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\CHARTEVENTS_delirium.csv",
                         chunksize=1000000,
                         usecols=['SUBJECT_ID', 'HADM_ID', 'ICUSTAY_ID', 
                                  'ITEMID', 'CHARTTIME', 'VALUE', 'VALUENUM', 
                                  'VALUEUOM', 'WARNING', 'ERROR']):
    temp_rows = chunk[chunk['ITEMID'].isin(relevant_items['ITEMID'])]
    apache = pd.concat([apache,temp_rows])    

#There's no data for apacheiv_los/mortality/natural antilog in our data set. 
#%%

#Filter it to only our patients. 

#Process it