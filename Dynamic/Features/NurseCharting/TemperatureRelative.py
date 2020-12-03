# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 17:38:30 2020

Get last temperature data before/in the observation window, taken from the
nursecharting table.

Runtime: ~3 min

@author: Kirby
"""

#%% Package setup
import pandas as pd
import numpy as np
import time
import datetime

start_timer = time.time()

#%% Load in data. 

#Pulls list of Stay IDs and offsets we care about.
#Hours before delirium that we're looking at.
lead_hours = 1
obs_hours = 6
pat_stays = pd.read_csv('relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) + 'hr_obs_data_set.csv')

#Just get temp data. Only keeping Celsius, we've verified F and C are mostly identical data.
temperature_data = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\nurseCharting_delirium.csv",
                               nrows=0,
                               usecols=['patientunitstayid',
                                        'nursingchartoffset',
                                        'nursingchartcelltypevallabel',
                                        'nursingchartcelltypevalname',
                                        'nursingchartvalue'])
for chunk in pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\nurseCharting_delirium.csv", 
                         chunksize=1000000,
                         usecols=['patientunitstayid','nursingchartoffset',
                                  'nursingchartcelltypevallabel',
                                  'nursingchartcelltypevalname',
                                  'nursingchartvalue']):
    temp_rows = chunk[chunk['nursingchartcelltypevalname']=='Temperature (C)']
    temperature_data = pd.concat([temperature_data,temp_rows])

#%% Process the data.
#Just get data on patients we care about. 
temperature_data = temperature_data[
    temperature_data['patientunitstayid'].isin(pat_stays['patientunitstayid'])]

#Drop data after the observation window for each patient. 
lookup = pat_stays.set_index('patientunitstayid')
def keep_row(current_ID,offset):
    #Get window time stamps.
    window_start = lookup.loc[current_ID,'start']
    window_end = lookup.loc[current_ID,'end']
    #If the temp score took place before/in window, keep it. 
    if (offset <= window_end):
        return 1
    else:
        return 0

temperature_data['keep'] = temperature_data.apply(
    lambda row: keep_row(
        row['patientunitstayid'],row['nursingchartoffset']),axis=1)
temperature_data = temperature_data[temperature_data['keep']==1]

#Make the data all numeric.
temperature_data['nursingchartoffset'] = pd.to_numeric(
    temperature_data['nursingchartoffset'],errors='coerce')

#Discard columns I don't care about.
temperature_data = temperature_data[[
    'patientunitstayid','nursingchartoffset','nursingchartvalue']]

#%% Get the last temperature value for each patient in the observation window. 

#Make sure all the data's in order by patientstayid and offset.
temperature_data.sort_values(['patientunitstayid','nursingchartoffset'],
                             inplace=True)

#Generate column of last temperature for each ID.
last_temp = temperature_data.groupby('patientunitstayid').last().reset_index(
    drop=False)
last_temp.rename(columns={'nursingchartvalue':'last_temp'},inplace=True)
pat_stays = pat_stays.merge(last_temp,how='left',on='patientunitstayid')

#Save off results.
pat_stays = pat_stays[['patientunitstayid','last_temp']]
pat_stays.to_csv('relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) + 'hr_obs_temp_feature.csv',index=False)

#For performance testing. 
calculation_timer = time.time()-start_timer