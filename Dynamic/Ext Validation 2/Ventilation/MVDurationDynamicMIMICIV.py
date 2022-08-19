# -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 15:25:10 2021

This code pulls for the prediction/clustering patients the duration of MV 
during their ICU stay thus far at the prediction time.

Checked CHARTEVENTS and PROCEDUREEVENTS, based on methodology from MIMIC-III github. 
(ventilation_classification.sql)

CPTEVENTS and ICD PROCEDURES not reliable enough (time info)

Run time: 17 min for 20 loops.

@author: Kirby
"""
#%% Package setup.
import numpy as np
import pandas as pd
from pathlib import Path
from time import time
import matplotlib.pyplot as plt
start = time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

#%% Get all relevant data
 
#Set up list of counts of patients that had no MV duration.
no_mv_counts = list()

#Import all relevant items. Obtained from MIMIC github.
rel_items = pd.read_csv('CHARTEVENTS_duration_items.csv')[['ITEMID']]
rel_items.rename(columns = {'ITEMID': 'itemid'}, inplace = True)
#Items that alone indicate MV.
mv_items = pd.read_csv('CHARTEVENTS_mv_items.csv')['ITEMID']

#Strings with certain items that indicate supplemental oxygen.
oxy_vals = pd.read_csv('oxygen_therapy_values.csv')['VALUE']

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
    

#Get relevant PROCEDUREEVENTS extubation events.
proc_all = pd.read_csv(mimic_path.joinpath('icu', "procedureevents.csv.gz"),
                   usecols=['stay_id','starttime',
                            'itemid'],
                   parse_dates=['starttime'])
proc_all = proc_all[proc_all['itemid'].isin([227194,225468,225477])]


#%%       
for lead_hours in [0,1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        print('lead hrs: '+ str(lead_hours) + ', obs hrs:' + str(obs_hours))
        #%% Load in data.
        #Load the IDs of the patients we care about.
        comp = pd.read_csv(
            dataset_path.joinpath(
                'MIMICIV_relative_'+ str(lead_hours) + 'hr_lead_' + 
                str(obs_hours) + 'hr_obs_data_set.csv'),
            usecols=['stay_id','intime','start', 'end'],
            parse_dates=['intime'])
        
        #%% Combine cohort data with documentation.
        chart = chart_all.merge(comp,on='stay_id',how='left')
        chart.loc[:, 'charttime'] = pd.to_datetime(chart['charttime'], 
                                                   errors='coerce')
        proc = proc_all.merge(comp,on='stay_id',how='left')
        
        #%% Remove data before ICU stay and after prediction time. 
        #Get minutes from ICU admission time. 
        chart['offset'] = \
            (chart['charttime'] - chart['intime']).dt.total_seconds()/60
        proc['offset'] = \
            (proc['starttime'] - proc['intime']).dt.total_seconds()/60
        #Keep data after ICU admission. 
        chart = chart[chart['offset'] >= 0]
        proc = proc[proc['offset'] >= 0]
        #Keep data before prediction time.
        chart = chart[chart['offset'] <= chart['end']]
        proc = proc[proc['offset'] <= proc['end']]
        
        #%% Get instances of mechanical ventilation.
        
        #Drop stuff that's not clearly mechanical ventilation.
        mech1 = chart[(chart['itemid'] == 720) & 
                      (chart['value'] != 'Other/Remarks')]
        mech2 = chart[(chart['itemid'] == 223848) & 
                      (chart['value'] !='Other')]
        mech3 = chart[chart['itemid'] == 223849]
        mech4 = chart[(chart['itemid'] == 467) & 
                      (chart['value'] ==' Ventilator')]
        mech5 = chart[chart['itemid'].isin(mv_items)]
                      
        mech = pd.concat([mech1,mech2,mech3,mech4,mech5]).drop_duplicates()
        #Just get unique times.
        mech = mech[['stay_id','offset']]
        mech['MV'] = 1
        mech.drop_duplicates(inplace=True)
        
        #%% Get instances of oxygen therapy/extubation (ending mech vent)
        
        #Just get instances of oxygen therapy/extubation.
        end_mech1 = chart[(chart['itemid'] == 226732) & 
                          (chart['value'].str.contains('|'.join(oxy_vals),
                                                       case=False))]
        end_mech2 = chart[(chart['itemid'] == 467) & 
                          (chart['value'].str.contains('|'.join(oxy_vals),
                                                       case=False,
                                                       na=True))]
        end_mech3 = chart[(chart['itemid'] == 640) & 
                          (chart['value'].isin(['Extubated',
                                                'Self Extubation']))]
        #Add a stop at the end time of the observation window. 
        ends = comp[['stay_id','end']].copy()
        ends.rename(columns={'end':'offset'},inplace=True)
        
        #Put it all together.
        end_mech = pd.concat([end_mech1,end_mech2,end_mech3,proc,ends])
        end_mech = end_mech[['stay_id','offset']]
        end_mech['MV'] = 0
        end_mech.drop_duplicates(inplace=True)
        
        #%% Combine all ventilation info, and generate feature. 
        all_mech = pd.concat([mech,end_mech])
        all_mech.sort_values(['stay_id','offset'],ascending=True,
                             inplace=True)
        #Remove rows where MV status doesn't change.
        #Get next row's stay ID and MV status.
        all_mech['last_MV'] = all_mech['MV'].shift(periods=1)
        all_mech['last_stay'] = all_mech['stay_id'].shift(periods=1)
        #Check if next row is same ICU stay and MV status. 
        all_mech['last_MV_same?'] = all_mech['MV'] == all_mech['last_MV']
        all_mech['last_stay_same?'] = \
            all_mech['stay_id'] == all_mech['last_stay']        
        all_mech = all_mech[(all_mech['last_MV_same?'] == False) | 
                            (all_mech['last_stay_same?'] == False)]
        #Calculate durations of instances of MV. 
        all_mech['last_offset'] = all_mech['offset'].shift(periods=1)
        def calc_dur(curr_mv,last_mv,curr_offset,last_offset,last_stay_same):
            if (curr_mv == 0) & (last_mv == 1) & (last_stay_same == True):
                return curr_offset-last_offset
            else:
                return 0
        all_mech['mv_duration'] = all_mech.apply(
            lambda row: calc_dur(row['MV'],row['last_MV'],row['offset'],
                                 row['last_offset'],row['last_stay_same?']),
            axis=1)
        #Sum it up per patient stay. 
        feat = all_mech.groupby('stay_id').sum()['mv_duration']
        feat = feat.reset_index()

        
        feat.to_csv('relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) 
                         + 'hr_obs_MV_duration_MIMICIV.csv',index=False)
        
        #%%Generate histograms and count patients w/out MV.
        plt.figure()
        plt.title('All MV Duration (hours)')
        (feat['mv_duration']/60).hist()
        plt.figure()
        plt.title('All MV Duration (hours, excluding 0 hrs)')
        ((feat[feat['mv_duration'] > 0])['mv_duration']/60).hist()
        plt.figure()
        plt.title('All MV Duration (hours, more than 0, less than 24h)')
        ((feat[(feat['mv_duration'] > 0) & 
               (feat['mv_duration'] <= 1440)])['mv_duration']/60).hist()
        no_mv_counts.append(feat[feat['mv_duration'] == 0]
                            ['mv_duration'].count()) #~30% have MV durations.
        
calc_time = time() - start