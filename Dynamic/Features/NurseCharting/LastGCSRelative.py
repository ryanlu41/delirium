# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 19:57:28 2020

#This pulls the last GCS value for each patient at the end of the observation
 window.

Takes about 5 minutes to run.

#TODO: Add in Score (Glasgow Coma Scale) data.
@author: Kirby
"""
#%% Import packages.
import numpy as np
import pandas as pd
import os
#import multiprocessing as mp
import time

start_timer = time.time()

#%% Load in and prepare relevant data.
#Pulls list of Stay IDs and offsets we care about.
#Hours before delirium that we're looking at.
lead_hours = 1
obs_hours = 6
pat_stays = pd.read_csv('relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) + 'hr_obs_data_set.csv')

#Just get GCS data.
GCS_data = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\nurseCharting_delirium.csv",
                       nrows=0,
                       usecols=['patientunitstayid','nursingchartoffset',
                                'nursingchartcelltypevallabel',
                                'nursingchartcelltypevalname',
                                'nursingchartvalue'])
for chunk in pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\nurseCharting_delirium.csv", 
                         chunksize=500000,
                         usecols=['patientunitstayid','nursingchartoffset',
                                  'nursingchartcelltypevallabel',
                                  'nursingchartcelltypevalname',
                                  'nursingchartvalue']):
    temp_rows = chunk[
        chunk['nursingchartcelltypevallabel']=='Glasgow coma score']
    GCS_data = pd.concat([GCS_data,temp_rows])

#Only keep GCS data for patients we care about. 
GCS_data = GCS_data[GCS_data['patientunitstayid'].isin(
    pat_stays['patientunitstayid'])]

#Drop data after the observation window for each patient. 
lookup = pat_stays.set_index('patientunitstayid')
def keep_row(current_ID,offset):
    #Get window time stamps.
    window_start = lookup.loc[current_ID,'start']
    window_end = lookup.loc[current_ID,'end']
    #If the GCS score took place before/in window, keep it. 
    if (offset <= window_end):
        return 1
    else:
        return 0

GCS_data['keep'] = GCS_data.apply(lambda row: keep_row(
    row['patientunitstayid'],row['nursingchartoffset']),axis=1)
GCS_data = GCS_data[GCS_data['keep']==1]

#Make the data all numeric.
GCS_data['patientunitstayid'] = pd.to_numeric(
    GCS_data['patientunitstayid'],errors='coerce')
GCS_data['nursingchartvalue'] = pd.to_numeric(
    GCS_data['nursingchartvalue'],errors='coerce')

#Split out data for the different parts.
motor_data = GCS_data[GCS_data['nursingchartcelltypevalname']=='Motor']
verbal_data = GCS_data[GCS_data['nursingchartcelltypevalname']=='Verbal']
eyes_data = GCS_data[GCS_data['nursingchartcelltypevalname']=='Eyes']
total_data = GCS_data[GCS_data['nursingchartcelltypevalname']=='GCS Total']

#Only keep columns we care about.
motor_data = motor_data[['patientunitstayid','nursingchartoffset',
                         'nursingchartvalue']]
verbal_data = verbal_data[['patientunitstayid','nursingchartoffset',
                           'nursingchartvalue']]
eyes_data = eyes_data[['patientunitstayid','nursingchartoffset',
                       'nursingchartvalue']]
total_data = total_data[['patientunitstayid','nursingchartoffset',
                         'nursingchartvalue']]

    
#%% Get last GCS for each part of the score, and each patient stay.

#Make sure all the data's in order by patientstayid and offset.
motor_data.sort_values(['patientunitstayid','nursingchartoffset'],
                       inplace=True)
verbal_data.sort_values(['patientunitstayid','nursingchartoffset'],
                        inplace=True)
eyes_data.sort_values(['patientunitstayid','nursingchartoffset'],
                      inplace=True)
total_data.sort_values(['patientunitstayid','nursingchartoffset'],
                       inplace=True)

#Generate column of last GCS for each ID and offset
last_motor = motor_data.groupby('patientunitstayid').last().reset_index(drop=False)
last_motor.rename(columns={'nursingchartvalue':'last_motor_GCS'},inplace=True)
pat_stays = pat_stays.merge(last_motor,how='left',on='patientunitstayid')

last_verbal = verbal_data.groupby('patientunitstayid').last().reset_index(drop=False)
last_verbal.rename(columns={'nursingchartvalue':'last_verbal_GCS'},inplace=True)
pat_stays = pat_stays.merge(last_verbal,how='left',on='patientunitstayid')

last_eyes = eyes_data.groupby('patientunitstayid').last().reset_index(drop=False)
last_eyes.rename(columns={'nursingchartvalue':'last_eyes_GCS'},inplace=True)
pat_stays = pat_stays.merge(last_eyes,how='left',on='patientunitstayid')

last_total = total_data.groupby('patientunitstayid').last().reset_index(drop=False)
last_total.rename(columns={'nursingchartvalue':'last_total_GCS'},inplace=True)
pat_stays = pat_stays.merge(last_total,how='left',on='patientunitstayid')


#For performance testing. 
calculation_timer = time.time()-start_timer

#Save off results.
pat_stays = pat_stays[['patientunitstayid','last_motor_GCS','last_verbal_GCS','last_eyes_GCS','last_total_GCS']]
pat_stays.to_csv('relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) + 'hr_obs_GCS_feature.csv',index=False)
