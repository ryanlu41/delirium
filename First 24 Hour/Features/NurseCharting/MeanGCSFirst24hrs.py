# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 19:57:28 2020

#This pulls the mean GCS value for each patient over the first 24 hours of their
ICU stay. 

Takes about 15 minutes to run.
@author: Kirby
"""
#%% Import packages.
import numpy as np
import pandas as pd
import os
#import multiprocessing as mp
import time

#%% Load in and prepare relevant data.
comp = pd.read_csv('complete_patientstayid_list.csv')
comp.rename(columns={'PatientStayID':'patientunitstayid'},inplace=True)

#Just get GCS data.
GCS_data = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\nurseCharting.csv",nrows=0,usecols=['patientunitstayid','nursingchartoffset','nursingchartcelltypevallabel','nursingchartcelltypevalname','nursingchartvalue'])
keep_list = ['Glasgow coma score','Score (Glasgow Coma Scale)']
for chunk in pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\nurseCharting.csv",chunksize=500000,usecols=['patientunitstayid','nursingchartoffset','nursingchartcelltypevallabel','nursingchartcelltypevalname','nursingchartvalue']):
    temp_rows = chunk[chunk['nursingchartcelltypevallabel'].isin(keep_list)]
    GCS_data = pd.concat([GCS_data,temp_rows])

#%%Process data.
#Only keep GCS data for patients we care about. 
GCS_data = GCS_data[GCS_data['patientunitstayid'].isin(comp['patientunitstayid'])]

#Drop data outside the first 24 hour of ICU stay for each patient. 
GCS_data = GCS_data[GCS_data['nursingchartoffset']>=0]
GCS_data = GCS_data[GCS_data['nursingchartoffset']<=1440]

#Make the data all numeric.
GCS_data['patientunitstayid'] = pd.to_numeric(GCS_data['patientunitstayid'],errors='coerce')
GCS_data['nursingchartvalue'] = pd.to_numeric(GCS_data['nursingchartvalue'],errors='coerce')

#Split out data for the different parts.
motor_data = GCS_data[GCS_data['nursingchartcelltypevalname']=='Motor']
verbal_data = GCS_data[GCS_data['nursingchartcelltypevalname']=='Verbal']
eyes_data = GCS_data[GCS_data['nursingchartcelltypevalname']=='Eyes']
total_list = ['GCS Total','Value']
total_data = GCS_data[GCS_data['nursingchartcelltypevalname'].isin(total_list)]

#Only keep columns we care about.
motor_data = motor_data[['patientunitstayid','nursingchartoffset','nursingchartvalue']]
verbal_data = verbal_data[['patientunitstayid','nursingchartoffset','nursingchartvalue']]
eyes_data = eyes_data[['patientunitstayid','nursingchartoffset','nursingchartvalue']]
total_data = total_data[['patientunitstayid','nursingchartoffset','nursingchartvalue']]

    
#%% Get mean GCS for each part of the score, and each patient stay.

#Generate column of mean GCS for each ID and offset
mean_motor = motor_data.groupby('patientunitstayid').mean().reset_index(drop=False)
mean_motor.rename(columns={'nursingchartvalue':'24hrMeanMotor'},inplace=True)
comp = comp.merge(mean_motor,how='left',on='patientunitstayid')

mean_verbal = verbal_data.groupby('patientunitstayid').mean().reset_index(drop=False)
mean_verbal.rename(columns={'nursingchartvalue':'24hrMeanVerbal'},inplace=True)
comp = comp.merge(mean_verbal,how='left',on='patientunitstayid')

mean_eyes = eyes_data.groupby('patientunitstayid').mean().reset_index(drop=False)
mean_eyes.rename(columns={'nursingchartvalue':'24hrMeanEyes'},inplace=True)
comp = comp.merge(mean_eyes,how='left',on='patientunitstayid')

mean_total = total_data.groupby('patientunitstayid').mean().reset_index(drop=False)
mean_total.rename(columns={'nursingchartvalue':'24hrMeanTotal'},inplace=True)
comp = comp.merge(mean_total,how='left',on='patientunitstayid')


#Save off results.
comp = comp[['patientunitstayid','24hrMeanMotor','24hrMeanVerbal','24hrMeanEyes','24hrMeanTotal']]
comp.to_csv('first_24hr_GCS_feature.csv',index=False)
