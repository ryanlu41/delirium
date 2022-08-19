# -*- coding: utf-8 -*-
"""
Created on Thu Dec 17 15:25:10 2020

This code pulls for the prediction/clustering patients whether or not they were 
documented as receiving ventilation during their ICU stay before the onset 
of delirium. 

Checked chartevents and procedureevents, based on methodology from MIMIC-III github. 
(ventilation_classification.sql)

CPTEVENTS and ICD PROCEDURES not reliable enough (time info)

Run time: 10 min

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

#%% Get all relevant data.

#Import relevant items. Obtained from MIMIC github 
rel_items = pd.read_csv('CHARTEVENTS_vent_items.csv')[['ITEMID']]
rel_items.rename(columns = {'ITEMID': 'itemid'}, inplace = True)
oxy_vals = pd.read_csv('oxygen_therapy_values.csv')['VALUE']
oxy_vals.rename({'VALUE': 'value'}, inplace = True)

#Get all relevant CHARTEVENTS
chart_all = pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'),
                       nrows=0,
                       usecols=['stay_id','itemid', 'charttime', 
                                'value'],
                       parse_dates=['charttime'])
for chunk in pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'),
                        chunksize=1000000,
                        usecols=['stay_id', 'itemid', 'charttime', 
                                 'value'],
                        parse_dates=['charttime']):
    temp_rows = chunk.merge(rel_items, on = 'itemid', how = 'inner')
    chart_all = pd.concat([chart_all,temp_rows])    

mech_all = chart_all.merge(rel_items, on = 'itemid', how = 'inner')
end_mech_all = chart_all[chart_all['itemid'].isin([226732,467,640])]

#Get relevant procedureevents extubation events.
proc_all = pd.read_csv(mimic_path.joinpath('icu', "procedureevents.csv.gz"),
                   usecols=['stay_id', 'starttime', 'itemid'],
                   parse_dates=['starttime'])
proc_all = proc_all[proc_all['itemid'].isin([227194, 225468, 225477])]
proc_all.rename(columns={'starttime':'charttime'},inplace=True)

#%%
for lead_hours in [0,1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        print(lead_hours, ',', obs_hours)
        
        #%% Load in data.
        #Load the IDs of the patients we care about.
        comp = pd.read_csv(
            dataset_path.joinpath(
                'MIMICIV_relative_'+ str(lead_hours) + 'hr_lead_' + 
                str(obs_hours) + 'hr_obs_data_set.csv'),
            usecols = ['stay_id', 'intime', 'start', 'end'],
            parse_dates=['intime'])
        
        #%% Get instances of mechanical ventilation.
        
        #Drop stuff that's not clearly mechanical ventilation.
        mech = mech_all[~((mech_all['itemid']==223848) & (mech_all['value']=='Other'))]
        
        #Just get unique times.
        mech = mech[['stay_id','charttime']]
        mech['mech'] = 1
        mech.drop_duplicates(inplace=True)
        
        #%% Get instaces of oxygen therapy/extubation (ending mech vent)
        
        #Drop stuff that's not clearly oxygen therapy/extubation.
        end_mech = end_mech_all[((end_mech_all['itemid']==226732) & 
                              (end_mech_all['value'].isin(oxy_vals)))]
        
        #Reformat and add info from PROCEDUREEVENTS_MV
        end_mech = pd.concat([end_mech,proc_all])
        
        #Just get unique times.
        end_mech = end_mech[['stay_id','charttime']]
        end_mech['mech'] = 0
        end_mech.drop_duplicates(inplace=True)
        
        #%% Combine all ventilation info, and generate feature. 
        all_mech = pd.concat([mech,end_mech])
        all_mech.sort_values(['stay_id','charttime'],ascending=True,
                             inplace=True)
        all_mech.loc[:, 'charttime'] = pd.to_datetime(all_mech['charttime'], 
                                                      errors='coerce')
        
        # Remove info during ventilation, just find changes. 
        
        # Keep the first row of each stay, while removing info between changes.
        all_mech['last_mech'] = all_mech['mech'].shift(periods=1)
        all_mech['last_id'] = all_mech['stay_id'].shift(periods=1)
        all_mech['keep'] = ((all_mech['last_id'] != all_mech['stay_id']) |
                            (all_mech['last_mech'] != all_mech['mech']))
        all_mech = all_mech[all_mech['keep']==True]
        
        # Keep the rows that were the end of an instance of ventilation.
        all_mech['mech_start'] = all_mech['charttime'].shift(periods=1)
        all_mech['keep'] = ((all_mech['last_mech']==1) & (all_mech['mech']==0) & 
                            (all_mech['last_id']==all_mech['stay_id']))
        all_mech = all_mech[all_mech['keep']]
        all_mech.rename(columns={'charttime':'mech_end'},inplace=True)
        
        # Tack on intime, convert to offsets.  
        all_mech = all_mech.merge(comp,on='stay_id',how='inner')
        all_mech['start_offset'] = (all_mech['mech_start'] - 
                                    all_mech['intime']).dt.total_seconds()/60
        all_mech['end_offset'] = (all_mech['mech_end'] - 
                                    all_mech['intime']).dt.total_seconds()/60
        
        # Only keep if ventilation started before or during observation window. 
        all_mech = all_mech[all_mech['start_offset'] <= all_mech['end']]
        
        comp['vented'] = comp['stay_id'].isin(all_mech['stay_id']).astype(int)
        
        comp = comp[['stay_id','vented']]
        
        comp.to_csv('MIMICIV_relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) 
                         + 'hr_obs_vent_feature.csv',index=False)
        
        print(comp['vented'].value_counts())
        
        calc_time = time() - start