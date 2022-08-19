# -*- coding: utf-8 -*-
"""
Created on Sat Jul 23 23:28:15 2022

This code pulls for the prediction/clustering patients whether or not they were 
documented as receiving ventilation during their ICU stay in the 1st 24 hours. 

Checked chartevents and procedureevents, based on methodology from MIMIC-III github. 
(ventilation_classification.sql)

CPTEVENTS and ICD PROCEDURES not reliable enough (time info)

Run time: 9 min 

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
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

#%% Load in data.
#Load the IDs of the patients we care about.
comp = pd.read_csv(dataset_path.joinpath('MIMICIV_complete_dataset.csv'),
                   usecols = ['stay_id', 'intime'],
                   parse_dates=['intime'])

#Import relevant items. Obtained from MIMIC github 
rel_items = pd.read_csv('CHARTEVENTS_vent_items.csv')['ITEMID'].rename({'ITEMID': 'itemid'})
oxy_vals = pd.read_csv('oxygen_therapy_values.csv')['VALUE'].rename({'VALUE': 'value'})

#Get all relevant CHARTEVENTS
chart = pd.read_csv(mimic_path.joinpath('icu', "chartevents.csv.gz"),
                     usecols = ['stay_id','charttime','value','itemid'],
                     parse_dates = ['charttime'])
chart = chart[chart['stay_id'].isin(comp['stay_id'])]
mech = chart[chart['itemid'].isin(rel_items)]
end_mech = chart[chart['itemid'].isin([226732,467,640])]

#Get relevant PROCEDUREEVENTS_MV extubation events.
proc = pd.read_csv(mimic_path.joinpath('icu', "procedureevents.csv.gz"),
                   usecols=['stay_id', 'starttime', 'itemid'],
                   parse_dates=['starttime'])
proc = proc[proc['itemid'].isin([227194, 225468, 225477])]
proc = proc[proc['stay_id'].isin(comp['stay_id'])]

#%% Get instances of mechanical ventilation.

#Drop stuff that's not clearly mechanical ventilation.
mech = mech[~((mech['itemid']==223848) & (mech['value']=='Other'))]

#Just get unique times.
mech = mech[['stay_id','charttime']]
mech['mech'] = 1
mech.drop_duplicates(inplace=True)

#%% Get instaces of oxygen therapy/extubation (ending mech vent)

#Drop stuff that's not clearly oxygen therapy/extubation.
end_mech = end_mech[((end_mech['itemid']==226732) & 
                      (end_mech['value'].isin(oxy_vals)))]

#Reformat and add info from PROCEDUREEVENTS_MV
proc.rename(columns={'starttime':'charttime'},inplace=True)
end_mech = pd.concat([end_mech,proc])

#Just get unique times.
end_mech = end_mech[['stay_id','charttime']]
end_mech['mech'] = 0
end_mech.drop_duplicates(inplace=True)

#%% Combine all ventilation info, and generate feature. 
all_mech = pd.concat([mech,end_mech])
all_mech.sort_values(['stay_id','charttime'],ascending=True,inplace=True)

#Remove info during ventilation, just find changes. 

#Keep the first row of each stay, while removing info between changes.
all_mech['last_mech'] = all_mech['mech'].shift(periods=1)
all_mech['last_id'] = all_mech['stay_id'].shift(periods=1)
all_mech['keep'] = ((all_mech['last_id'] != all_mech['stay_id']) |
                    (all_mech['last_mech'] != all_mech['mech']))
all_mech = all_mech[all_mech['keep']==True]

#Keep the rows that were the end of an instance of ventilation.
all_mech['mech_start'] = all_mech['charttime'].shift(periods=1)
all_mech['keep'] = ((all_mech['last_mech']==1) & (all_mech['mech']==0) & 
                    (all_mech['last_id']==all_mech['stay_id']))
all_mech = all_mech[all_mech['keep']]
all_mech.rename(columns={'charttime':'mech_end'},inplace=True)

#Tack on intime, convert to offsets. 
intimes = comp[['stay_id','intime']]
all_mech = all_mech.merge(intimes,on='stay_id',how='left')
all_mech['start_offset'] = (all_mech['mech_start'] - 
                            all_mech['intime']).dt.total_seconds()/60
all_mech['end_offset'] = (all_mech['mech_end'] - 
                            all_mech['intime']).dt.total_seconds()/60

#Get stays where mechanical ventilation happened in first 24 hours. 
all_mech['keep'] = ((all_mech['start_offset'] <= 1440) & 
                    (all_mech['end_offset'] >= 0))
all_mech = all_mech[all_mech['keep']==True]

comp['first24hrs_vented'] = comp['stay_id'].isin(
    all_mech['stay_id']).astype(int)
comp = comp[['stay_id', 'first24hrs_vented']]

comp.to_csv('First24HoursVented_MIMICIV.csv',index=False)

calc_time = time() - start