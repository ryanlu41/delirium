# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 11:17:43 2020

This code is meant to pull data on whether patients had infections diagnosed
in their MIMIC IV data, based on microbiology data. 

This info is then combined with SOFA scoring to determine if patients had 
sepsis.

Run time: 10 minutes.

@author: Kirby
"""
#%% Setup
import pandas as pd
import numpy as np
from time import time
from pathlib import Path

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

start = time()

#%% Load in data and find infection data.

#Pulls list of Stay IDs and offsets we care about.
#Hours before delirium that we're looking at.
for lead_hours in [0,1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        print(lead_hours, ',', obs_hours)
        #%% Load in relevant data.
        comp = pd.read_csv(
            dataset_path.joinpath('MIMICIV_relative_' + str(lead_hours) + 
                                  'hr_lead_' + str(obs_hours) + 
                                  'hr_obs_data_set.csv'),
            usecols = ['subject_id', 'stay_id', 'intime', 'outtime',
                       'start', 'end'],
            parse_dates=['intime','outtime'])
        
        sofa = pd.read_csv('MIMICIV_relative_' + str(lead_hours) + 'hr_lead_' + 
                           str(obs_hours) + '_SOFA_features.csv')
        
        # #Load in items.
        # items = pd.read_csv(mimic_path.joinpath('icu', "d_items.csv.gz"),
        #                     usecols=['itemid','label','linksto'])
        # items = items.applymap(lambda s:s.lower() if type(s) == str else s)
        # rel_items = items[items['linksto'].str.contains('microbiologyevents',
        #                                                 na=False)]
        # #Only include organisms.
        # rel_items = rel_items[rel_items['itemid'] >= 80002]
        # rel_items = rel_items[rel_items['itemid'] <= 80312]
        
        #Get organism names that should be discarded.
        # bad_desc = ['CANCELLED','NO GROWTH']
        
        #Check MICROBIOLOGYEVENTS
        micro = pd.read_csv(mimic_path.joinpath('hosp', "microbiologyevents.csv.gz"),
                           usecols=['subject_id', 
                                    'charttime','org_itemid','org_name'],
                          parse_dates=['charttime'])
        
        #%% Get rows where an organism grew.
        
        # Filter to just our patients.
        micro = micro.merge(comp, on = 'subject_id', how = 'inner')
        
        # Drop rows where both org_itemid and org_name are nan.
        micro = micro[~(pd.isnull(micro['org_itemid']) & 
                      pd.isnull(micro['org_name']))]
        
        # Drop rows missing stay_id.
        micro = micro[~pd.isnull(micro['stay_id'])]
        
        # Get offsets.
        micro['offset'] = (micro['charttime'] - micro['intime'])\
                .dt.total_seconds()/60
        micro = micro[micro['offset'] < micro['end']]
        micro = micro['stay_id'].drop_duplicates()
        
        #Save off results. 
        comp['infection'] = comp['stay_id'].isin(micro).astype(int)
        comp = comp[['stay_id','infection']]
        
        #%% Find if they had sepsis.
        comp = comp.merge(sofa,how='left',on=['stay_id'])
        #Check infection column for True, and suspected sepsis column for 1 to get
        #sepsis feature. Can also be used with septic shock.
        def get_sepsis(infection,suspected_sepsis):
            if (infection == 1) & (suspected_sepsis) == 1: 
                return 1
            else:
                return 0
            
        comp['sepsis'] = comp.apply(lambda row: get_sepsis(
                row['infection'],row['suspected_sepsis']), axis=1)
        comp['septic_shock'] = comp.apply(lambda row: get_sepsis(
            row['infection'],row['suspected_septic_shock']), 
            axis=1)
        
        comp = comp[['stay_id','infection',
                               'suspected_sepsis','sepsis',
                               'suspected_septic_shock','septic_shock']]
        
        comp.to_csv('relative_'+ str(lead_hours) +'hr_lead_' + 
                     str(obs_hours) + 'hr_obs_inf_sepsis_feature_MIMICIV.csv',
                     index=False)

#Find class balance info.
bal = comp['infection'].value_counts()

calc_time = time() - start