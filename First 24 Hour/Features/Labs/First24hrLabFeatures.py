# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 19:57:28 2020

#This pulls the mean, min and max of each lab for each patient in the first
24 hr predictive model. 

Run time:
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
#Uncomment if only running part of the labs.
# lab_list = pd.read_csv("TempLabsList.csv")

lab_list = lab_list.transpose()
lab_list = lab_list.values.tolist()[0]

#Receive a lab name to look for.
for lab_name in lab_list:
    
    #Pulls list of Stay IDs and offsets we care about.
    ids = pd.read_csv("complete_patientstayid_list.csv")
    ids.rename(columns={'PatientStayID':'patientunitstayid'},inplace=True)
    ids['start'] = 0
    ids['end'] = 1440
    
    #Load the list of the relevant labs
    script_dir = os.path.dirname(__file__)
    folder = "AllLabsBeforeDelirium"
    full_file_path = os.path.join(script_dir,folder,lab_name+".csv")
    labs = pd.read_csv(full_file_path,usecols=['patientunitstayid','labresultoffset','labresult'])
    
    start_timer = time.time()
    #Generates a dictionary of patientunitstayid -> dataframe of labs relevant to that ID.
    labs_dict = {}
    #Vectorize this? 
    for row in ids.itertuples(index=False):
        current_ID = row[0]
        #Get the labs with the right patient stay ID.
        rel_labs = labs[labs['patientunitstayid']==current_ID][['labresultoffset','labresult']]
        rel_labs = rel_labs.set_index(['labresultoffset']).to_dict().get('labresult')
        labs_dict.update({current_ID:rel_labs})
    
    #For performance testing.    
    dict_timer = time.time() - start_timer
    
    #Vectorized version. 
    start_timer = time.time()
    
    #Gets an array of relevant labs, using ID, start times, and end times.
    #Returns a list of those lab values.
    def get_rel_labs(current_ID,start,end):
        #Get the labs with the right patient stay ID.
        relevant_labs = labs_dict.get(current_ID)
        #Among those, get the labs that happened in the desired time frame.
        relevant_labs_copy = relevant_labs.copy()
        for k in (relevant_labs.keys()):
            if (k > end) | (k < start):
                del relevant_labs_copy[k]   
        
        if relevant_labs_copy == {}:
            return np.nan
        else:
            return list(relevant_labs_copy.values())
    
    #Generate column of list of values for each ID.
    ids['relevant_'+lab_name] = ids.apply(lambda row:get_rel_labs(row['patientunitstayid'],row['start'],row['end']),axis=1)
    
    #Generate columns of mean, min, and max for each ID. 
    def get_mean(labs):
        if np.isnan(labs).any():
            return np.nan
        else:
            return stat.mean(labs)
    
    def get_min(labs):
        if np.isnan(labs).any():
            return np.nan
        else:
            return min(labs)
    
    def get_max(labs):
        if np.isnan(labs).any():
            return np.nan
        else:
            return max(labs)
    
    ids['mean_'+lab_name] = ids.apply(lambda row:get_mean(row['relevant_'+lab_name]),axis=1)
    ids['min_'+lab_name] = ids.apply(lambda row:get_min(row['relevant_'+lab_name]),axis=1)
    ids['max_'+lab_name] = ids.apply(lambda row:get_max(row['relevant_'+lab_name]),axis=1)
    
    #For performance testing. 
    calculation_timer = time.time()-start_timer
    
    #Drop the unneed columns. 
    ids = ids[['patientunitstayid','mean_'+lab_name, 'min_'+lab_name, 'max_'+lab_name]]
    
    #Save off results.
    script_dir = os.path.dirname(__file__)
    folder = "First24hrsFeatureData"
    full_file_path = os.path.join(script_dir,folder,lab_name+".csv")
    ids.to_csv(full_file_path,index=False)
