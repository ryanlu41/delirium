# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 00:47:18 2022

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
lab_list = pd.read_excel("MIMICIV_to_eICU_Labs_List.xlsx")
lab_list.dropna(inplace = True)
lab_list = lab_list['labname'].drop_duplicates()
#Uncomment if only running part of the labs.
# lab_list = pd.read_csv("TempLabsList.csv")

final = pd.read_csv(dataset_path.joinpath("MIMICIV_complete_dataset.csv"))
final.rename(columns={'stay_id':'patientunitstayid'},inplace=True)

#%% Generate feature.
for lead_hours in [0,1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        #Progress report.
        print(str(lead_hours) + ', ' + str(obs_hours))
        
        final = pd.read_csv(
            dataset_path.joinpath('MIMICIV_relative_' + str(lead_hours) + 
                                  'hr_lead_' + str(obs_hours) + 
                                  'hr_obs_data_set.csv'),
            usecols=['stay_id','start','end'])
        final.rename(columns={'stay_id':'patientunitstayid'},inplace=True)
        #Receive a lab name to look for.
        for lab_name in lab_list:
            
            #Pulls list of Stay IDs and offsets we care about.
            ids = pd.read_csv(
                dataset_path.joinpath('MIMICIV_relative_' + str(lead_hours) + 
                                      'hr_lead_' + str(obs_hours) + 
                                      'hr_obs_data_set.csv'),
                               usecols=['stay_id','start','end'])
            ids['start'] = ids['start'] - 1440 #Labs are valid for 24 hours. 
            ids.rename(columns={'stay_id':'patientunitstayid'},inplace=True)
            
            
            #Load the list of the relevant labs
            folder = "AllLabsBeforeDeliriumMIMICIV"
            full_file_path = os.path.join(folder,lab_name+".csv")
            labs = pd.read_csv(full_file_path,
                               usecols=['patientunitstayid','labresultoffset',
                                        'labresult'])
            
            #Filter out labs from patients I don't care about.
            labs = labs.merge(ids, on = 'patientunitstayid', how = 'inner')
            
            #Drop data after the observation window for each patient. 
            labs = labs[labs['labresultoffset'] <= labs['end']]
            
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
        ids.rename(columns = {'patientunitstayid':'stay_id'}, inplace = True)
        final.to_csv('MIMICIV_dynamic_' + str(lead_hours) + 'hr_lead_' + str(obs_hours) 
                     + 'hr_obs_lab_features.csv', index=False)

calc_time = time() - start