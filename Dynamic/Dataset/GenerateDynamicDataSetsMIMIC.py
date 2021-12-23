# -*- coding: utf-8 -*-
"""
Created on Sun July 19 13:14:56 2020

This code is meant to generate observation window time stamps for the dynamic
model, based on each patient's delirium onset (first positive CAM-ICU/ICSDSC), 
and the desired lead time and observation window length. For negative patients,
 it looks at the first CAM-ICU/ICDSC test after the summed lengths of the
 observation window and the lead time. 

Also removes stays with conflicting info (delirium listed in problem list, 
but no positive CAM-ICU/ICDSDCs)

Run time: 10 sec per leadtime/obs window combination.

@author: Kirby
"""
#%% Package Setup
import numpy as np
import pandas as pd
from pathlib import Path
from time import time
import matplotlib.pyplot as plt

file_path = Path(__file__)
mimic_path = file_path.parent.parent.parent.joinpath('mimic-iii-1.4')

start = time()

#%% Inputs
#Set the lead times
for lead_hours in [0,1,3,6,12]:
    lead_minutes = lead_hours*60
    
    #Set the observation window lengths
    for obs_hours in [1,3,6,12]:
        obs_minutes = obs_hours*60
        
        #Pulls list of Stay IDs, and delirium testing. 
        comp = pd.read_csv("MIMIC_complete_dataset.csv",
                           parse_dates=['INTIME'])
        
        tests = pd.read_csv('MIMIC_chart_events_delirium_labels.csv',
                            parse_dates=['CHARTTIME'])
        
        #%% Keep patient stays where delirium started after 
        #(observation window + lead time), and after 12 hours in ICU.
        
        positive_del = comp[comp['del_onset'] >= (lead_minutes + obs_minutes)] 
        positive_del = positive_del[positive_del['del_onset']>=720]
        
        positive_del.loc[:,'delirium_positive'] = 1
        positive_del.loc[:,'start'] = (positive_del['del_onset'] - 
                                       (lead_minutes + obs_minutes) )
        positive_del.loc[:,'end'] = positive_del['del_onset'] - lead_minutes
        
        #%% Find windows for patient stays that never had delirium.
        negative_del = comp[pd.isnull(comp['del_onset'])] 
        #Only look at tests of the negative patients.
        tests = tests[tests['ICUSTAY_ID'].isin(negative_del['ICUSTAY_ID'])]
        #Get offset from admission of each test. 
        tests = tests.merge(comp[['ICUSTAY_ID','INTIME']],on='ICUSTAY_ID',
                            how='left')
        tests['offset'] = (tests['CHARTTIME'] - tests['INTIME']
                           ).dt.total_seconds()/60
        #Only look at delirium tests that are after (observation window + lead time) of ICU stay.
        tests = tests[tests['offset'] >= (lead_minutes + obs_minutes)]
        #Get the time stamp used for creating an observation window.
        tests = tests[['ICUSTAY_ID','offset']]
        #Get the first test per patient stay ID.
        # tests = tests.groupby('patientunitstayid').min()
        #Randomly sample a test per patient stay id. 
        tests = tests.groupby('ICUSTAY_ID').apply(
            lambda x: x.sample(1)).reset_index(drop=True)
        
        negative_del = tests.copy()
        negative_del.reset_index(inplace=True,drop=False)
        negative_del.loc[:,'delirium_positive'] = 0
        negative_del.loc[:,'start'] = negative_del['offset'] - (lead_minutes + obs_minutes)
        negative_del.loc[:,'end'] = negative_del['offset'] - lead_minutes
        
        negative_del.drop(columns=['offset'],inplace=True)
        
        #Add on other useful info to negatiev patients.
        negative_del = negative_del.merge(comp,
                                          on=['ICUSTAY_ID','delirium_positive']
                                          ,how='left')
        
        #Put together the negative patients and positive patients.
        final = pd.concat([positive_del,negative_del])
        
        #Drop unneeded columns.
        final.drop(columns='index',inplace=True)
        
        #%%Save off results
        final.to_csv('MIMIC_relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) + 'hr_obs_data_set.csv',index=False)
calc_time = time() - start

#%% Get distributions of start windows in positive and negative cohorts.
#Positive cohort.
plt.figure()
plt.title('Positive Window Dist (<10000)')
final[(final['delirium_positive']==1) & (final['start']<=10000)]['start'].hist()
plt.figure()
plt.title('Positive Window Dist (All)')
final[final['delirium_positive']==1]['start'].hist()
#Negative cohort.
plt.figure()
plt.title('Negative Window Dist (<10000)')
final[(final['delirium_positive']==0) & (final['start']<=10000)]['start'].hist()
plt.figure()
plt.title('Negative Window Dist (All)')
final[final['delirium_positive']==0]['start'].hist()