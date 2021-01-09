# -*- coding: utf-8 -*-
"""
Created on Mon Aug 31 13:51:36 2020

Use this code to search for patient history info in CHARTEVENTS.

Let's also check NOTEEVENTS.


@author: Kirby
"""
#%% Setup
import pandas as pd
import numpy as np
import time


#%% 
items = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\D_ITEMS.csv",usecols=['ITEMID','LABEL'])

#Trying terms
history = items[items['LABEL'].str.contains('history',na=False)]

#Pulling chart events with the relevant IDs found above. 
history_rows = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\CHARTEVENTS_delirium.csv",nrows=0)
for chunk in pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\CHARTEVENTS_delirium.csv",chunksize=1000000):
    #start = time.time()
    temp_rows = chunk[chunk['ITEMID'].isin(history['ITEMID'])]
    history_rows = pd.concat([history_rows,temp_rows])
    #calc_time = time.time() - start
    
unique_history = history_rows['VALUE'].drop_duplicates().sort_values()
