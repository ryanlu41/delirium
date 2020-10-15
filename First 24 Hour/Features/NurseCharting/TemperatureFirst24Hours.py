# -*- coding: utf-8 -*-
"""
Created on Mon Jun  1 17:38:30 2020

Get mean, min, max, from first 24 hours of temperature data, taken from the
nursecharting table.

@author: Kirby
"""

import pandas as pd
import numpy as np
import time
import datetime

#Just get temp data.
temperature_data = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\nurseCharting.csv",nrows=0)
for chunk in pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\nurseCharting.csv", chunksize=1000000):
    temp_rows = chunk[chunk['nursingchartcelltypevallabel']=='Temperature']
    temperature_data = pd.concat([temperature_data,temp_rows])
    
#Just get data on patients we care about. 
comp = pd.read_csv('complete_patientstayid_list.csv')

temperature_data = temperature_data[temperature_data['patientunitstayid'].isin(comp['PatientStayID'])]

#Get first 24 hour data only.
temperature_data = temperature_data[temperature_data['nursingchartoffset'] <= 1440]
temperature_data = temperature_data[temperature_data['nursingchartoffset'] >= 0]

#Only keep celsius data, discading location and F temperature data.
temperature_data = temperature_data[temperature_data['nursingchartcelltypevalname']=='Temperature (C)']

#Figure out how much data is in C or F
# celsius = temperature_data[temperature_data['nursingchartcelltypevalname']=='Temperature (C)']
# fahrenheit = temperature_data[temperature_data['nursingchartcelltypevalname']=='Temperature (F)']
# celsius = celsius[['patientunitstayid','nursingchartoffset']]
# fahrenheit = fahrenheit[['patientunitstayid','nursingchartoffset']]
# merged = celsius.merge(fahrenheit,how='outer',on=['patientunitstayid','nursingchartoffset'],indicator=True)
# overlap = merged[merged['_merge']=='both']
#Most (99.9394%) of the C/F data is overlapping 

#Discard columns I don't care about.
temperature_data = temperature_data[['patientunitstayid','nursingchartvalue']]
#Convert nursingchartvalue data to numbers.
temperature_data['nursingchartvalue'] = temperature_data['nursingchartvalue'].astype(float)

#Put it in order. Get the min, max, and mean. 
minimum = temperature_data.groupby(by=['patientunitstayid']).min()
maximum = temperature_data.groupby(by=['patientunitstayid']).max()
mean = temperature_data.groupby(by=['patientunitstayid']).mean()

#Put all the data together, and save it off.
comp.rename(columns={'PatientStayID':'patientunitstayid'},inplace=True)
comp = comp.merge(minimum,how='left',on=['patientunitstayid'])
comp.rename(columns={'nursingchartvalue':'24hr_minimum_temp'},inplace=True)
comp = comp.merge(maximum,how='left',on=['patientunitstayid'])
comp.rename(columns={'nursingchartvalue':'24hr_maximum_temp'},inplace=True)
comp = comp.merge(mean,how='left',on=['patientunitstayid'])
comp.rename(columns={'nursingchartvalue':'24hr_mean_temp'},inplace=True)

comp.to_csv('24hr_temperature_data.csv',index=False)