# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 19:57:28 2020

#This pulls the value of each lab for each patient in the relative
 predictive model. Takes in lead time and observation window length in hours.
 Only considers a lab valid for 24 hours. 
 

Run time:~6 minutes for ~40 labs
@author: Kirby
"""


import numpy as np
import pandas as pd
import os
#import multiprocessing as mp
import time
import statistics as stat

#Get list of lab names
lab_list = pd.read_csv("LabsList.csv")
#Uncommet if only running part of the labs.
# lab_list = pd.read_csv("TempLabsList.csv")

lab_list = lab_list.transpose()
lab_list = lab_list.values.tolist()[0]

#Hours before delirium that we're looking at.
lead_hours = 1
obs_hours = 6

#Receive a lab name to look for.
for lab_name in lab_list:
    
    #Pulls list of Stay IDs and offsets we care about.
    ids = pd.read_csv('relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) + 'hr_obs_data_set.csv')
    
    #Load the list of the relevant labs
    script_dir = os.path.dirname(__file__)
    folder = "AllLabsBeforeDelirium"
    full_file_path = os.path.join(script_dir,folder,lab_name+".csv")
    labs = pd.read_csv(full_file_path,usecols=['patientunitstayid','labresultoffset','labresult'])
    
    #Filter out labs from patients I don't care about.
    labs = labs[labs['patientunitstayid'].isin(ids['patientunitstayid'])]
    
    #start_timer = time.time()
    
    #Drop data after the observation window for each patient. 
    lookup = ids.set_index('patientunitstayid')
    def keep_row(current_ID,offset):
        #Get window time stamps.
        window_start = lookup.loc[current_ID,'start'] - 1440 #Labs are valid for 24 hours. 
        window_end = lookup.loc[current_ID,'end']
        #If the lab took place before/in window, keep it. 
        if (offset <= window_end):
            return 1
        else:
            return 0
    
    labs['keep'] = labs.apply(lambda row: keep_row(row['patientunitstayid'],row['labresultoffset']),axis=1)
    labs = labs[labs['keep']==1]
    
    #For performance testing.    
    #dict_timer = time.time() - start_timer
    
    #Make sure all the data's in order by patientstayid and offset.
    labs.sort_values(['patientunitstayid','labresultoffset'],inplace=True)
    
    #Get the last lab for each patientstayid. 
    last_labs = labs.groupby('patientunitstayid').last().reset_index(drop=False)
    last_labs.rename(columns={'labresult':'last_'+lab_name},inplace=True)
    ids = ids.merge(last_labs,how='left',on='patientunitstayid')
    
    #Only keep the columns I care about for the model.
    ids = ids[['patientunitstayid','last_'+lab_name]]
    
    #Save off results.
    script_dir = os.path.dirname(__file__)
    folder = 'relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) + 'hr_obs_feature_data'
    full_file_path = os.path.join(script_dir,folder,lab_name+".csv")
    ids.to_csv(full_file_path,index=False)
