# -*- coding: utf-8 -*-
"""
Created on Wed May 27 12:50:59 2020

This code is meant take delirium testing data output from 
FindingChartEventDeliriumInfo.py then convert it a clean binary label of
 positive or negative delirium at specific time frames. 

runtime: 2 minutes

@author: Kirby
"""

#Looking in CHARTEVENTS
import pandas as pd
import numpy as np
from time import time
import datetime
from pathlib import Path

start = time()

file_path = Path(__file__)
mimic_path = file_path.parent.parent.parent.joinpath('mimic-iii-1.4')

#Load in the info
chart_events = pd.read_csv('all_delirium_chart_events.csv')

#only keep some columns.
ids_and_results = chart_events[['SUBJECT_ID','HADM_ID','ICUSTAY_ID',
                                'CHARTTIME','LABEL','VALUE']]

#Get list of all values and labels possible. 
all_vals_and_labels = ids_and_results[['LABEL','VALUE']]
all_vals_and_labels.drop_duplicates(inplace=True)

#Get unique subjects/times to look through. 
subjects_and_times = ids_and_results[['SUBJECT_ID','HADM_ID','ICUSTAY_ID',
                                      'CHARTTIME']]
subjects_and_times.drop_duplicates(inplace=True)
subjects_and_times.sort_values(['SUBJECT_ID','CHARTTIME'],inplace=True)

def get_delirium_testing(subject_id,chart_time,ids_and_results):
    #Get the testing done at this subject and time. 
    temp_rows = ids_and_results[ids_and_results['SUBJECT_ID'] == subject_id]
    temp_rows = temp_rows[temp_rows['CHARTTIME'] == chart_time]
    #Check if delirium assessment is present. 
    del_assess = temp_rows[temp_rows['LABEL']=='delirium assessment']
    if del_assess.shape[0] > 0:
        if del_assess['VALUE'].iloc[0] == 'Negative':
            return 0
        elif del_assess['VALUE'].iloc[0] == 'Positive':
            return 1
        elif del_assess['VALUE'].iloc[0] == 'UTA':
            return np.nan
    #If there are no delirium assessments then look at the CAM-ICU parts. 
    #Pull all parts. 
    ms_change_rows = temp_rows[temp_rows['LABEL']=='cam-icu ms change']
    if ms_change_rows.shape[0] > 0:
        ms_change = ms_change_rows['VALUE'].iloc[0]
    else:
        ms_change = 'Unable to Assess'
        
    inattention_rows = temp_rows[temp_rows['LABEL']=='cam-icu inattention']
    if inattention_rows.shape[0] > 0:
        inattention = inattention_rows['VALUE'].iloc[0]
    else:
        inattention = 'Unable to Assess'
        
    rass_loc_rows = temp_rows[temp_rows['LABEL']=='cam-icu rass loc']
    if rass_loc_rows.shape[0] > 0:
        rass_loc = rass_loc_rows['VALUE'].iloc[0]
    else:
        rass_loc = 'Unable to Assess'
        
    altered_loc_rows = temp_rows[temp_rows['LABEL']=='cam-icu altered loc']
    if altered_loc_rows.shape[0] > 0:
        altered_loc = altered_loc_rows['VALUE'].iloc[0]
    else:
        altered_loc = 'Unable to Assess'
        
    disorganized_rows = temp_rows[temp_rows['LABEL']==
                                  'cam-icu disorganized thinking']
    if disorganized_rows.shape[0] > 0:
        disorganized = disorganized_rows['VALUE'].iloc[0]
    else:
        disorganized = 'Unable to Assess'
    
    #Determine if delirium positive or negative.
    #Check for mental status change
    if 'No' in ms_change:
        return 0
    elif 'Unable to Assess' in ms_change:
        return np.nan
    elif 'No' in inattention:
        return 0
    elif 'Unable to Assess' in inattention:
        return np.nan
    elif (rass_loc=='Yes') | (altered_loc=='Yes'):
        return 1
    elif disorganized == 'No':
        return 0
    elif disorganized == 'Unable to Assess':
        return np.nan
    elif disorganized == 'Yes':
        return 1

#Loop through all the subjects and times
subjects_and_times['delirium_positive'] = subjects_and_times.apply(
    lambda row: get_delirium_testing(
        row['SUBJECT_ID'],row['CHARTTIME'],ids_and_results),axis=1)

#Save off results
subjects_and_times.to_csv('MIMIC_chart_events_delirium_labels.csv',index=False)

calc_time = time() - start