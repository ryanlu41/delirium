# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 19:57:28 2020

#This pulls the RASS values for each patient over the first 24 hours of their
ICU stay, and checks if they had -4/-5 at any point to indicate Coma per the
PRE-DELIRIC definition.

Also creates mean, min, and max RASS in the first 24 hours as features. 

Runtime: a few minutes.

@author: Kirby
"""
#%% Import packages.
import numpy as np
import pandas as pd
#import multiprocessing as mp
import time

#Performance testing. 
start_time = time.time()

#%% Load in and prepare relevant data.
comp = pd.read_csv(r'C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\complete_patientstayid_list.csv')
comp.rename(columns={'PatientStayID':'patientunitstayid'},inplace=True)

#Just get RASS data.
rass_data = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\nurseCharting_delirium.csv",nrows=0,usecols=['patientunitstayid','nursingchartoffset','nursingchartcelltypevallabel','nursingchartcelltypevalname','nursingchartvalue'])
keep_list = ['RASS','SEDATION SCORE','Sedation Scale/Score/Goal']
for chunk in pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\nurseCharting_delirium.csv",chunksize=1000000,usecols=['patientunitstayid','nursingchartoffset','nursingchartcelltypevallabel','nursingchartcelltypevalname','nursingchartvalue']):
    temp_rows = chunk[chunk['nursingchartcelltypevallabel'].isin(keep_list)]
    rass_data = pd.concat([rass_data,temp_rows])

#%% Explore data to figure out if RASS, SEDATION SCORE, and Sedation Scale/Score/Goal are equivalent.
# #Explore RASS
# rass = rass_data[rass_data['nursingchartcelltypevallabel']=='RASS']
# rass['nursingchartvalue'] = pd.to_numeric(rass['nursingchartvalue'])
# rass_counts = rass['nursingchartvalue'].value_counts().sort_index()

# #Explore SEDATION SCORE
# caps_sedat = rass_data[rass_data['nursingchartcelltypevallabel']=='SEDATION SCORE']
# caps_sedat['nursingchartvalue'] = pd.to_numeric(caps_sedat['nursingchartvalue'])
# caps_sedat_counts = caps_sedat['nursingchartvalue'].value_counts().sort_index()

# #Explore Sedation Scale/Score/Goal
# low_sedat = rass_data[rass_data['nursingchartcelltypevallabel']=='Sedation Scale/Score/Goal']
# low_sedat_counts = low_sedat['nursingchartvalue'].value_counts().sort_index()

# low_scale = low_sedat[low_sedat['nursingchartcelltypevalname']=='Sedation Scale']
# low_scale_counts = low_scale['nursingchartvalue'].value_counts().sort_index()

# low_score = low_sedat[low_sedat['nursingchartcelltypevalname']=='Sedation Score']
# low_score_counts = low_score['nursingchartvalue'].value_counts().sort_index()

# low_goal = low_sedat[low_sedat['nursingchartcelltypevalname']=='Sedation Goal']
# low_goal_counts = low_goal['nursingchartvalue'].value_counts().sort_index()

# #Goals can be tossed.
# #Merge the sedation score and scale data together to see what was worth keeping. 

#%% Clean up and combine the RASS data. 
#Get the 'RASS' data.
rass = rass_data[rass_data['nursingchartcelltypevallabel']=='RASS']
#Get the 'SEDATION SCORE' data.
caps_score = rass_data[rass_data['nursingchartcelltypevallabel']==
                       'SEDATION SCORE']
#Get the other data. 
scale = rass_data[rass_data['nursingchartcelltypevallabel']==
                        'Sedation Scale/Score/Goal']
scale = scale[scale['nursingchartcelltypevalname']=='Sedation Scale']
#Drop the data that isn't RASS.
scale = scale[scale['nursingchartvalue']=='RASS']
scale = scale[['patientunitstayid','nursingchartoffset']]
#Get the scores.
score = rass_data[rass_data['nursingchartcelltypevallabel']==
                        'Sedation Scale/Score/Goal']
score = score[score['nursingchartcelltypevalname']=='Sedation Score']
#Only keep the scores that were RASS.
score = score.merge(scale,on=['patientunitstayid','nursingchartoffset'],
                    how='inner')

#Combine all 3 sources of RASS data.
rass_data = pd.concat([rass,caps_score,score])
rass_data = rass_data[['patientunitstayid', 'nursingchartoffset',
       'nursingchartvalue']]

#%%Process data.
#Only keep rass data for patients we care about. 
rass_data = rass_data[rass_data['patientunitstayid'].isin(
    comp['patientunitstayid'])]

#Drop data outside the first 24 hour of ICU stay for each patient. 
rass_data = rass_data[rass_data['nursingchartoffset']>=0]
rass_data = rass_data[rass_data['nursingchartoffset']<=1440]

#Make the data all numeric.
rass_data['patientunitstayid'] = pd.to_numeric(rass_data['patientunitstayid'],
                                               errors='coerce')
rass_data['nursingchartvalue'] = pd.to_numeric(rass_data['nursingchartvalue'],
                                               errors='coerce')

#Drop offset.
rass_data = rass_data[['patientunitstayid','nursingchartvalue']]
    
#%% Get if each patient had coma (RASS of -4/-5)

#Any patients that had RASS data start off marked as no coma.
had_rass = rass_data['patientunitstayid'].drop_duplicates()
comp['had_rass'] = comp['patientunitstayid'].isin(had_rass)

coma = rass_data[rass_data['nursingchartvalue']<=-4]
coma = coma['patientunitstayid'].drop_duplicates()
comp['had_coma'] = comp['patientunitstayid'].isin(coma)

def coma_feature(has_rass,has_coma):
    if has_rass == False:
        return np.nan
    elif has_coma == True:
        return 1
    else:
        return 0
    
comp['First24hrComa'] = comp.apply(lambda row: coma_feature(row['had_rass'],
                                                            row['had_coma']),
                                   axis=1)

comp = comp[['patientunitstayid','First24hrComa']]

#%% Get each patient's min/mean/max RASS score in the first 24 hours.

min_rass = rass_data.groupby('patientunitstayid').min().reset_index()\
    .rename(columns={'nursingchartvalue':'First24hrMinRASS'})
mean_rass = rass_data.groupby('patientunitstayid').mean().reset_index()\
    .rename(columns={'nursingchartvalue':'First24hrMeanRASS'})
max_rass = rass_data.groupby('patientunitstayid').max().reset_index()\
    .rename(columns={'nursingchartvalue':'First24hrMaxRASS'})

comp = comp.merge(min_rass,on='patientunitstayid',how='left')
comp = comp.merge(mean_rass,on='patientunitstayid',how='left')
comp = comp.merge(max_rass,on='patientunitstayid',how='left')


#%% Save off results.
comp.to_csv('first_24hr_rass_and_coma_feature.csv',index=False)
