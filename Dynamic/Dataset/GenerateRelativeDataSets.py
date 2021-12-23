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

Run time: 10 sec per lead time/obs window length combination.

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

#%% Inputs
#Set the lead time.
for lead_hours in [0,1,3,6,12]:
    lead_minutes = lead_hours*60
    
    #Set the observation window length.
    for obs_hours in [1,3,6,12]:
        obs_minutes = obs_hours*60
        
        #Pulls list of Stay IDs, and delirium testing. 
        comp = pd.read_csv("complete_patientstayid_list.csv")
        comp.rename(columns={'PatientStayID':'patientunitstayid'},inplace=True)
        
        tests = pd.read_csv('AllDeliriumTests.csv')
        
        #Pulls diagnosis data to remove stays with conflicting info.
        diag = pd.read_csv(eicu_path.joinpath("diagnosis.csv"))
        diag = diag[diag["diagnosisstring"]==
                    'neurologic|altered mental status / pain|delirium']
        
        #%% Find delirium onset times. 
        #Only look at positive CAM-ICU/ICSDSCs.
        del_onset = tests[tests['delirium']==True]
        #Get the earliest one for each stay. 
        del_onset = del_onset.groupby('patientunitstayid').min().reset_index()
        comp = comp.merge(del_onset,on='patientunitstayid',how='left')
        comp.rename(columns={'offset':'del_start'},inplace=True)
        
        #%% Keep patient stays where delirium started after 
        #(observation window + lead time), and after 12 hours in ICU.
        
        positive_del = comp[comp['del_start'] >= (lead_minutes + obs_minutes)] 
        positive_del = positive_del[positive_del['del_start']>=720]
        
        positive_del.loc[:,'delirium?'] = 1
        positive_del.loc[:,'start'] = (positive_del['del_start'] - 
                                       (lead_minutes + obs_minutes) )
        positive_del.loc[:,'end'] = positive_del['del_start'] - lead_minutes
        positive_del.drop(columns=['delirium'],inplace=True)
        
        #%% Find windows for patient stays that never had delirium.
        negative_del = comp[pd.isnull(comp['del_start'])] 
        #Only look at tests of the negative patients.
        tests = tests[tests['patientunitstayid'].isin(
            negative_del['patientunitstayid'])]
        #Only look at delirium tests that are after (observation window + lead time) of ICU stay.
        tests = tests[tests['offset'] >= (lead_minutes + obs_minutes)]
        #Get the time stamp used for creating an observation window.
        tests = tests[['patientunitstayid','offset']]
        #Get the first test per patient stay ID.
        # tests = tests.groupby('patientunitstayid').min()
        #Randomly sample a test per patient stay id. 
        tests = tests.groupby('patientunitstayid').apply(
            lambda x: x.sample(1, random_state = 1)).reset_index(drop=True)
        
        negative_del = tests.copy()
        negative_del.reset_index(inplace=True,drop=False)
        negative_del.loc[:,'delirium?'] = 0
        negative_del.loc[:,'start'] = negative_del['offset'] - (lead_minutes + 
                                                                obs_minutes)
        negative_del.loc[:,'end'] = negative_del['offset'] - lead_minutes
        
        negative_del.drop(columns=['offset'],inplace=True)
        
        #Put together the negative patients and positive patients.
        final = pd.concat([positive_del,negative_del])
        
        #%% Remove the patients that had delirium diagnoses but no positive tests.
        diag = diag[['patientunitstayid']].drop_duplicates()
        no_pos_test = final[final['delirium?']==0]
        no_pos_test = no_pos_test[['patientunitstayid']].drop_duplicates()
        diag_but_no_pos = diag.merge(no_pos_test,how='inner',
                                     on='patientunitstayid')
        final = final[~final['patientunitstayid'].isin(
            diag_but_no_pos['patientunitstayid'])]
        
        #Save off results
        final.to_csv('relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) + 'hr_obs_data_set.csv',
                     index=False)
calc_time = time() - start

#%% Get distributions of start windows in positive and negative cohorts.
#Positive cohort.
plt.figure()
plt.title('Positive Window Dist (<10000)')
final[(final['delirium?']==1) & (final['start']<=10000)]['start'].hist()
plt.figure()
plt.title('Positive Window Dist (All)')
final[final['delirium?']==1]['start'].hist()
#Negative cohort.
plt.figure()
plt.title('Negative Window Dist (<10000)')
final[(final['delirium?']==0) & (final['start']<=10000)]['start'].hist()
plt.figure()
plt.title('Negative Window Dist (All)')
final[final['delirium?']==0]['start'].hist()