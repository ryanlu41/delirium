# -*- coding: utf-8 -*-
"""
Created on Mon Jun  1 17:38:30 2020

Get mean, min, max, from first 24 hours of temperature data, taken from the
nursecharting table.

@author: Kirby
"""
#%% Package setup.
import pandas as pd
import numpy as np
import time
import datetime
from time import time
from pathlib import Path

start = time()
filepath = Path(__file__)
eicu_path = filepath.parent.parent.parent.parent.joinpath('eicu')
dataset_path = filepath.parent.parent.parent.joinpath('Dataset')


#%%Get temp data.
temper_data = pd.read_csv(eicu_path.joinpath("nurseCharting.csv"),nrows=0)
for chunk in pd.read_csv(eicu_path.joinpath("nurseCharting.csv"), 
                         chunksize=1000000):
    temp_rows = chunk[chunk['nursingchartcelltypevallabel']=='Temperature']
    temper_data = pd.concat([temper_data,temp_rows])
    
#Just get data on patients we care about. 
comp = pd.read_csv('complete_patientstayid_list.csv')
comp.rename(columns={'PatientStayID':'patientunitstayid'},inplace=True)

temper_data = temper_data[temper_data['patientunitstayid'].isin(comp['patientunitstayid'])]

#Get first 24 hour data only.
temper_data = temper_data[temper_data['nursingchartoffset'] <= 1440]
temper_data = temper_data[temper_data['nursingchartoffset'] >= 0]

#Only keep celsius data, discading location and F temperature data.
temper_data = temper_data[temper_data['nursingchartcelltypevalname']=='Temperature (C)']

#%%Figure out how much data is in C or F
# celsius = temper_data[temper_data['nursingchartcelltypevalname']=='Temperature (C)']
# fahrenheit = temper_data[temper_data['nursingchartcelltypevalname']=='Temperature (F)']
# celsius = celsius[['patientunitstayid','nursingchartoffset']]
# fahrenheit = fahrenheit[['patientunitstayid','nursingchartoffset']]
# merged = celsius.merge(fahrenheit,how='outer',on=['patientunitstayid','nursingchartoffset'],indicator=True)
# overlap = merged[merged['_merge']=='both']
#Most (99.9394%) of the C/F data is overlapping 

#%% Discard columns I don't care about.
temper_data = temper_data[['patientunitstayid','nursingchartvalue']]
#Convert nursingchartvalue data to numbers.
temper_data['nursingchartvalue'] = temper_data['nursingchartvalue'].astype(float)

#%%Put it in order. Get the min, max, and mean. 
minimum = temper_data.groupby(by=['patientunitstayid']).min()
maximum = temper_data.groupby(by=['patientunitstayid']).max()
mean = temper_data.groupby(by=['patientunitstayid']).mean()

#Put all the data together, and save it off.
comp = comp.merge(minimum,how='left',on=['patientunitstayid'])
comp.rename(columns={'nursingchartvalue':'24hr_minimum_temp'},inplace=True)
comp = comp.merge(maximum,how='left',on=['patientunitstayid'])
comp.rename(columns={'nursingchartvalue':'24hr_maximum_temp'},inplace=True)
comp = comp.merge(mean,how='left',on=['patientunitstayid'])
comp.rename(columns={'nursingchartvalue':'24hr_mean_temp'},inplace=True)

comp.to_csv('24hr_temper_data.csv',index=False)