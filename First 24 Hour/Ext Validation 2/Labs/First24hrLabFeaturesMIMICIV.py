# -*- coding: utf-8 -*-
"""
Created on Tue Jul 12 2022

#This pulls the mean, min and max of each lab for each patient in the first
24 hr predictive model. 

Run time: 1 min.
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

#Receive a lab name to look for.
for lab_name in lab_list:
    
    #Progress report.
    print(lab_name)
    
    #Pulls list of Stay IDs and offsets we care about.
    ids = pd.read_csv(dataset_path.joinpath("MIMICIV_complete_dataset.csv"),
                       usecols=['stay_id'])
    ids.rename(columns={'stay_id':'patientunitstayid'},inplace=True)
    ids['start'] = 0
    ids['end'] = 1440
    
    #Load the list of the relevant labs
    folder = "AllLabsBeforeDeliriumMIMICIV"
    full_file_path = os.path.join(folder,lab_name + ".csv")
    labs = pd.read_csv(full_file_path,usecols=['patientunitstayid',
                                               'labresultoffset','labresult'])
    
    #Make sure all the data's in order by patientstayid and offset.
    labs.sort_values(['patientunitstayid','labresultoffset'],inplace=True)
    
    #Filter out labs from patients I don't care about.
    labs = labs.merge(ids, on = 'patientunitstayid', how = 'inner')
    
    #Drop data after the observation window for each patient. 
    labs = labs[labs['labresultoffset'] <= labs['end']]
    
    #Get mean, min, max, count.
    mean_labs = labs.groupby('patientunitstayid').mean().reset_index(drop=False)
    mean_labs.rename(columns={'labresult':'mean_' + lab_name},inplace=True)
    mean_labs = mean_labs[['patientunitstayid', 'mean_' + lab_name]]
    ids = ids.merge(mean_labs, how='left', on='patientunitstayid')
    
    min_labs = labs.groupby('patientunitstayid').min().reset_index(drop=False)
    min_labs.rename(columns={'labresult':'min_' + lab_name},inplace=True)
    min_labs = min_labs[['patientunitstayid', 'min_' + lab_name]]
    ids = ids.merge(min_labs, how='left', on='patientunitstayid')
    
    max_labs = labs.groupby('patientunitstayid').max().reset_index(drop=False)
    max_labs.rename(columns={'labresult':'max_' + lab_name},inplace=True)
    max_labs = max_labs[['patientunitstayid', 'max_' + lab_name]]
    ids = ids.merge(max_labs, how='left', on='patientunitstayid')
    
    count_labs = labs.groupby('patientunitstayid').count().reset_index(drop=False)
    count_labs.rename(columns={'labresult':'count_' + lab_name},inplace=True)
    count_labs = count_labs[['patientunitstayid', 'count_' + lab_name]]
    ids = ids.merge(count_labs, how='left', on='patientunitstayid')
    
    #Get difference of last and second to last lab. 
    labs['diff'] = labs.groupby('patientunitstayid').diff()['labresult']
    diff_labs = labs.groupby('patientunitstayid').last().reset_index(drop=False)
    diff_labs.rename(columns={'diff':'diff_' + lab_name},inplace=True)
    diff_labs = diff_labs[['patientunitstayid', 'diff_' + lab_name]]
    ids = ids.merge(diff_labs,how='left',on='patientunitstayid')
    
    #Drop the unneed columns. 
    ids = ids[['patientunitstayid','mean_' + lab_name,'min_' + lab_name,
               'max_' + lab_name,'count_' + lab_name,'diff_' + lab_name]]
    
    #Save off results.
    final = final.merge(ids, on='patientunitstayid', how='left')
    
#Save off results.
final.drop(columns = ['hadm_id', 'subject_id', 'intime', 'outtime', 
                      'del_onset', 'del_onset_time', 'delirium_pos'],
           inplace = True)
final.to_csv('first_24_hour_lab_features_MIMICIV.csv',index=False)

calc_time = time() - start