# -*- coding: utf-8 -*-
"""
Created on Thu Dec 17 15:25:10 2020

This code pulls for the prediction/clustering patients whether or not they were 
documented as receiving ventilation during their ICU stay before the onset 
of delirium. 

Checked CHARTEVENTS and PROCEDUREEVENTSMV, based on methodology from MIMIC-III github. 
(ventilation_classification.sql)

CPTEVENTS and ICD PROCEDURES not reliable enough (time info)

Run time: 45 seconds

@author: Kirby
"""
#%% Package setup.
import numpy as np
import pandas as pd
from pathlib import Path
from time import time
start = time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iii-1.4')

for lead_hours in [1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        #%% Load in data.
        #Load the IDs of the patients we care about.
        comp = pd.read_csv(
            dataset_path.joinpath(
                'MIMIC_relative_'+ str(lead_hours) + 'hr_lead_' + 
                str(obs_hours) + 'hr_obs_data_set.csv'),
            parse_dates=['INTIME'])
        
        #Import relevant items. Obtained from MIMIC github 
        rel_items = pd.read_csv('CHARTEVENTS_vent_items.csv')['ITEMID']
        oxy_vals = pd.read_csv('oxygen_therapy_values.csv')['VALUE']
        
        #Get all relevant CHARTEVENTS
        chart = pd.read_csv(mimic_path.joinpath("CHARTEVENTS_delirium.csv"),
                             usecols=['ICUSTAY_ID','CHARTTIME','VALUE','ITEMID'],
                             parse_dates=['CHARTTIME'])
        chart = chart[chart['ICUSTAY_ID'].isin(comp['ICUSTAY_ID'])]
        mech = chart[chart['ITEMID'].isin(rel_items)]
        end_mech = chart[chart['ITEMID'].isin([226732,467,640])]
        
        #Get relevant PROCEDUREEVENTS_MV extubation events.
        proc = pd.read_csv(mimic_path.joinpath("PROCEDUREEVENTS_MV.csv"),
                           usecols=['ICUSTAY_ID','STARTTIME',
                                    'ITEMID'],
                           parse_dates=['STARTTIME'])
        proc = proc[proc['ITEMID'].isin([227194,225468,225477])]
        proc = proc[proc['ICUSTAY_ID'].isin(comp['ICUSTAY_ID'])]
        
        #%% Get instances of mechanical ventilation.
        
        #Drop stuff that's not clearly mechanical ventilation.
        mech = mech[~((mech['ITEMID']==223848) & (mech['VALUE']=='Other'))]
        
        #Just get unique times.
        mech = mech[['ICUSTAY_ID','CHARTTIME']]
        mech['mech'] = 1
        mech.drop_duplicates(inplace=True)
        
        #%% Get instaces of oxygen therapy/extubation (ending mech vent)
        
        #Drop stuff that's not clearly oxygen therapy/extubation.
        end_mech = end_mech[((end_mech['ITEMID']==226732) & 
                              (end_mech['VALUE'].isin(oxy_vals)))]
        
        #Reformat and add info from PROCEDUREEVENTS_MV
        proc.rename(columns={'STARTTIME':'CHARTTIME'},inplace=True)
        end_mech = pd.concat([end_mech,proc])
        
        #Just get unique times.
        end_mech = end_mech[['ICUSTAY_ID','CHARTTIME']]
        end_mech['mech'] = 0
        end_mech.drop_duplicates(inplace=True)
        
        #%% Combine all ventilation info, and generate feature. 
        all_mech = pd.concat([mech,end_mech])
        all_mech.sort_values(['ICUSTAY_ID','CHARTTIME'],ascending=True,
                             inplace=True)
        
        #Remove info during ventilation, just find changes. 
        
        #Keep the first row of each stay, while removing info between changes.
        all_mech['last_mech'] = all_mech['mech'].shift(periods=1)
        all_mech['last_id'] = all_mech['ICUSTAY_ID'].shift(periods=1)
        all_mech['keep'] = ((all_mech['last_id'] != all_mech['ICUSTAY_ID']) |
                            (all_mech['last_mech'] != all_mech['mech']))
        all_mech = all_mech[all_mech['keep']==True]
        
        #Keep the rows that were the end of an instance of ventilation.
        all_mech['mech_start'] = all_mech['CHARTTIME'].shift(periods=1)
        all_mech['keep'] = ((all_mech['last_mech']==1) & (all_mech['mech']==0) & 
                            (all_mech['last_id']==all_mech['ICUSTAY_ID']))
        all_mech = all_mech[all_mech['keep']]
        all_mech.rename(columns={'CHARTTIME':'mech_end'},inplace=True)
        
        #Tack on intime, convert to offsets. 
        intimes = comp[['ICUSTAY_ID','INTIME']]
        all_mech = all_mech.merge(intimes,on='ICUSTAY_ID',how='left')
        all_mech['start_offset'] = (all_mech['mech_start'] - 
                                    all_mech['INTIME']).dt.total_seconds()/60
        all_mech['end_offset'] = (all_mech['mech_end'] - 
                                    all_mech['INTIME']).dt.total_seconds()/60
        
        #Only keep if ventilation started before or during observation window. 
        lookup = comp.set_index('ICUSTAY_ID')
        def keep_row(current_ID,offset):
            #Get window time stamps.
            window_start = lookup.loc[current_ID,'start']
            window_end = lookup.loc[current_ID,'end']
            #If the ventilation took place before/in window, keep it. 
            if (offset <= window_end):
                return 1
            else:
                return 0
        
        all_mech['keep'] = all_mech.apply(lambda row: keep_row(
            row['ICUSTAY_ID'],row['start_offset']),
            axis=1)
        all_mech = all_mech[all_mech['keep'] == 1]
        
        comp['vented'] = comp['ICUSTAY_ID'].isin(
            all_mech['ICUSTAY_ID']).astype(int)
        
        comp = comp[['ICUSTAY_ID','vented']]
        
        comp.to_csv('relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) 
                         + 'hr_obs_vent_feature_MIMIC.csv',index=False)
        
        calc_time = time() - start