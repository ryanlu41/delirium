# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 01:34:06 2020

This code pulls whether or not patients were documented as receiving 
ventilation during their ICU stay any time before the observation window. 

Checked the PhysicalExam, or Treatment tables. 

CarePlanGeneral, and respiratory care are less reliable, so not using them.
Not using apacheApsVar because it doesn't indicate exactly when it happens.

Run time: 2 minutes

@author: Kirby
"""
#%% Package setup
import pandas as pd
import numpy as np
import time
import datetime
from pathlib import Path

start_timer = time.time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
eicu_path = file_path.parent.parent.parent.parent.joinpath('eicu')

#%% Load in data.

#Pulls list of Stay IDs and offsets we care about.
#Hours before delirium that we're looking at.
for lead_hours in [1,3,6,12]:
    for obs_hours in [1,3,6]:
        pat_stays = pd.read_csv(dataset_path.joinpath(
            'relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) + 
            'hr_obs_data_set.csv'))
        
        #Get physicalexam data.
        phys = pd.read_csv(eicu_path.joinpath("physicalexam.csv"),
                           usecols=['patientunitstayid','physicalexamoffset',
                                    'physicalexamtext'])
        #Get treatment
        treat = pd.read_csv(eicu_path.joinpath("treatment.csv"),
                            usecols=['patientunitstayid','treatmentoffset',
                                    'treatmentstring'])
        #%% Get feature and save it off.
        #Only keep the stays we care about.
        phys = phys[phys['patientunitstayid'].isin(pat_stays['patientunitstayid'])]
        treat = treat[treat['patientunitstayid'].isin(pat_stays['patientunitstayid'])]
        
        #Drop data after the observation window for each patient. 
        lookup = pat_stays.set_index('patientunitstayid')
        def keep_row(current_ID,offset):
            #Get window time stamps.
            window_start = lookup.loc[current_ID,'start']
            window_end = lookup.loc[current_ID,'end']
            #If the ventilation took place before/in window, keep it. 
            if (offset <= window_end):
                return 1
            else:
                return 0
        
        phys['keep'] = phys.apply(lambda row: keep_row(
                row['patientunitstayid'],row['physicalexamoffset']),axis=1)
        treat['keep'] = treat.apply(lambda row: keep_row(
                row['patientunitstayid'],row['treatmentoffset']),axis=1)
        
        phys = phys[phys['keep']==1]
        treat = treat[treat['keep']==1]
        
        #Get ventilation data.
        phys = phys[phys['physicalexamtext']=='ventilated']
        mech = treat[treat['treatmentstring'].str.contains('mechanical ventilation')]
        noninv = treat[treat['treatmentstring'].str.contains(
            'non-invasive ventilation')]
        
        #Just get patientunitstayids that had ventilation in first 24 hours. 
        vent_ids = pd.concat([phys[['patientunitstayid']],mech[['patientunitstayid']],
                              noninv[['patientunitstayid']]])
        vent_ids.drop_duplicates(inplace=True)
        
        pat_stays['vented'] = pat_stays['patientunitstayid'].isin(vent_ids['patientunitstayid'])
        
        #Save off results. 
        pat_stays.to_csv('relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) \
                         + 'hr_obs_vent_feature.csv',index=False)
        
        #For performance testing. 
        calculation_timer = time.time()-start_timer