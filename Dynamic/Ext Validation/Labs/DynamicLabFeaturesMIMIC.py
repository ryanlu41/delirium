# -*- coding: utf-8 -*-
"""
Created on Tue Nov 3 19:57:28 2020

This pulls the last value of each lab for each patient in the dynamic
 predictive model. Takes in lead time and observation window length in hours.
 Only considers a lab valid for 24 hours. 

Run time: 10 min.
@author: Kirby
"""

#%% Package setup
import numpy as np
import pandas as pd
import os
#import multiprocessing as mp
import statistics as stat
from pathlib import Path
from time import time

#%%Inputs.
file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')

#performance testing.
start = time()

#Get list of lab names
lab_list = pd.read_csv("LabsListMIMIC.csv")
#Uncomment if only running part of the labs.
# lab_list = pd.read_csv("TempLabsList.csv")

lab_list = lab_list.transpose()
lab_list = lab_list.values.tolist()[0]

final = pd.read_csv(dataset_path.joinpath("MIMIC_complete_dataset.csv"))
final.rename(columns={'ICUSTAY_ID':'patientunitstayid'},inplace=True)

#%% Generate feature.
for lead_hours in [1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        final = pd.read_csv(
            dataset_path.joinpath('MIMIC_relative_' + str(lead_hours) + 
                                  'hr_lead_' + str(obs_hours) + 
                                  'hr_obs_data_set.csv'))
        final.rename(columns={'ICUSTAY_ID':'patientunitstayid'},inplace=True)
        #Receive a lab name to look for.
        for lab_name in lab_list:
            
            #Progress report.
            print(lab_name)
            
            #Pulls list of Stay IDs and offsets we care about.
            ids = pd.read_csv(
                dataset_path.joinpath('MIMIC_relative_' + str(lead_hours) + 
                                      'hr_lead_' + str(obs_hours) + 
                                      'hr_obs_data_set.csv'),
                               usecols=['ICUSTAY_ID','start','end'])
            ids['start'] = ids['start'] - 1440 #Labs are valid for 24 hours. 
            ids.rename(columns={'ICUSTAY_ID':'patientunitstayid'},inplace=True)
            
            #Load the list of the relevant labs
            folder = "AllLabsBeforeDeliriumMIMIC"
            full_file_path = os.path.join(folder,lab_name+".csv")
            labs = pd.read_csv(full_file_path,
                               usecols=['patientunitstayid','labresultoffset',
                                        'labresult'])
            
            #Drop data after the observation window for each patient. 
            lookup = ids.set_index('patientunitstayid')
            def keep_row(current_ID,offset):
                #Get window time stamps.
                window_start = lookup.loc[current_ID,'start']
                window_end = lookup.loc[current_ID,'end']
                #If the lab took place before/in window, keep it. 
                if (offset <= window_end):
                    return 1
                else:
                    return 0
            
            labs['keep'] = labs.apply(
                lambda row: keep_row(row['patientunitstayid'],
                                     row['labresultoffset']),
                axis=1)
            labs = labs[labs['keep']==1]
            
            #Make sure all the data's in order by patientstayid and offset.
            labs.sort_values(['patientunitstayid','labresultoffset'],
                             inplace=True)
            
            #Get the last lab for each patientstayid. 
            last_labs = labs.groupby('patientunitstayid').last().reset_index(
                drop=False)
            last_labs.rename(columns={'labresult':'last_'+lab_name},
                             inplace=True)
            ids = ids.merge(last_labs,how='left',on='patientunitstayid')
            
            #Get difference of last and second to last lab. 
            labs['diff'] = labs.groupby(
                'patientunitstayid').diff()['labresult']
            diff_labs = labs.groupby(
                'patientunitstayid').last().reset_index(drop=False)
            diff_labs.rename(columns={'diff':'diff_'+lab_name},inplace=True)
            ids = ids.merge(diff_labs,how='left',on='patientunitstayid')
            
            #Only keep the columns I care about for the model.
            ids = ids[['patientunitstayid','last_'+lab_name,'diff_'+lab_name]]
            
            #Save off results.
            final = final.merge(ids,on='patientunitstayid',how='left')
    
        #Save off results.
        final.to_csv('MIMIC_dynamic_' + str(lead_hours) + 'hr_lead_' + str(obs_hours) 
                     + 'hr_obs_lab_features.csv',index=False)

calc_time = time() - start