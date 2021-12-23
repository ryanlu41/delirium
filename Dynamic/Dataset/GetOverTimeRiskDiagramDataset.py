# -*- coding: utf-8 -*-
"""
Created on Sat Apr 24 12:36:27 2021

Pick some positive and negative patients, and then get time windows every hour
to create diagrams of risk scoring over time. 

End result has patientunitstayid, start, end, delirium?, del_start, total_LOS

Runtime:

@author: Kirby
"""

#%% Package Setup
import numpy as np
import pandas as pd
from pathlib import Path
from time import time
import matplotlib.pyplot as plt

file_path = Path(__file__)
eicu_path = file_path.parent.parent.parent.joinpath('eicu')

start = time()

#%% Get dataset. 
data = pd.read_csv('relative_1hr_lead_12hr_obs_data_set.csv',
                   usecols=['patientunitstayid','del_start','delirium?'])

pat = pd.read_csv(eicu_path.joinpath('patient.csv'),
                  usecols=['patientunitstayid','unitdischargeoffset'])

#Tack on LOS info. 
data = data.merge(pat,on='patientunitstayid',how='left')

#Get random 50 positiven 50 negative patients. 
pos = data[data['delirium?'] == 1].sample(50,random_state=1)
neg = data[data['delirium?'] == 0].sample(50,random_state=1)

#%% Generate 1 hour windows every 1 hour until delirium onset. 
final = pd.DataFrame(columns=['patientunitstayid','start','end','delirium?',
                              'del_start','total_LOS'])

for row in pos.itertuples(index=False):
    stay_id = row[0]
    del_start = row[1]
    label = row[2]
    total_los = row[3]
    #Get number of rows to generate. 
    if label == 1:
        num_rows = np.floor(del_start/60)
    else: 
        num_rows = np.floor(total_los/60)
    num_rows = int(num_rows)
    
    #Make lists for each column.
    id_list = [stay_id] * num_rows
    label_list = [label] * num_rows
    start_list = list(range(0,num_rows))
    start_list = [element * 60 for element in start_list]
    end_list = list(range(1,num_rows + 1))
    end_list = [element * 60 for element in end_list]
    del_start_list = [del_start] * num_rows
    total_los_list = [total_los] * num_rows
    
    #Make dataframe out of list and append it. 
    temp = pd.DataFrame(list(zip(id_list,label_list,start_list,end_list,
                                 del_start_list,total_los_list)),
                        columns=['patientunitstayid','delirium?','start','end',
                                 'del_start','total_LOS'])
    final = pd.concat([final,temp])

final.to_csv('over_time_risk_diagram_dataset.csv',index=False)