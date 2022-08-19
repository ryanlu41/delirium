# -*- coding: utf-8 -*-
"""
Created on Sun Jul 31 12:49:23 2022

This code calculate the SOFA score and qSOFA score in MIMIC IV in the 
observation window. This info is then combined with SOFA scoring to 
determine if patients had sepsis.

Run time: 17 minutes.

@author: Kirby
"""

#%% Import packages
import sys
import numpy as np
import pandas as pd
import datetime
#from pandarallel import pandarallel
from time import time
from pathlib import Path

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
ext_path = file_path.parent.parent.parent.joinpath('Ext Validation 2')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

start = time()


#%% Find SBP, DBP, MBP, HR, RR, CVP, PaO2, PaCO2, FiO2 in chartevents and get means. 

#%% Just get the relevant chartevents data.
items = pd.read_csv(mimic_path.joinpath('icu', 'd_items.csv.gz'),
                    usecols = ['itemid', 'label'])
items.loc[:, 'label'] = items['label'].str.lower()
sbp_items = items[items['label'].str.contains('systolic')]
dbp_items = items[items['label'].str.contains('diastolic')]
mbp_items = items[items['label'].str.contains('blood pressure mean')]
hr_items = items[items['label'] == 'heart rate']
rr_items = items[items['label'].str.contains('respiratory rate')]
cvp_items = items[items['label'] == 'central venous pressure']
pao2_items = items[items['label'].str.contains('arterial o2 pressure')]
paco2_items = items[items['label'].str.contains('arterial co2 pressure')]
fio2_items = items[items['label'].str.contains('inspired o2 fraction')]
items_list = [sbp_items, dbp_items, mbp_items, hr_items, rr_items, cvp_items,
              pao2_items, paco2_items, fio2_items]
all_items = pd.concat(items_list)

# Just get relevant data from chartevents.
all_data_pre = pd.read_csv(mimic_path.joinpath('icu', "chartevents.csv.gz"),
                           nrows=0,usecols=['stay_id', 'itemid', 'charttime', 
                                            'valuenum'],
                           parse_dates = ['charttime'])
for chunk in pd.read_csv(mimic_path.joinpath('icu', "chartevents.csv.gz"),
                         chunksize=1000000,
                         usecols=['stay_id', 'itemid', 'charttime', 
                                          'valuenum'],
                         parse_dates = ['charttime']):
    temp_rows = chunk.merge(all_items, on = 'itemid', how = 'inner')
    all_data_pre = pd.concat([all_data_pre,temp_rows])    
   


# %% Generate SOFA score and component features
def SOFA_score(row):
    
    # Resp: (PaO2/FiO2, ventilation) into "sofa_resp" 
    sofa_resp = 0
    if ((not np.isnan(row['pO2'])) and (not np.isnan(row['fiO2']))):
        try:
            paO2_fiO2_ratio = row['pO2']/row['fiO2']
        except ZeroDivisionError:
            paO2_fiO2_ratio = 1000
        if (paO2_fiO2_ratio < 100 and row['ventilator']):
            sofa_resp = 4
        elif (paO2_fiO2_ratio < 200 and row['ventilator']):
            sofa_resp = 3
        elif (paO2_fiO2_ratio < 300):
            sofa_resp = 2
        elif (paO2_fiO2_ratio < 400):
            sofa_resp = 1
            
    # Nervous: (GCS) into "sofa_nervous"
    sofa_nervous = 0
    if (not np.isnan(row['gcs'])):
        if (row['gcs'] < 6):
            sofa_nervous = 4
        elif (row['gcs'] < 10 and row['gcs'] >= 6):
            sofa_nervous = 3
        elif (row['gcs'] < 13 and row['gcs'] >= 10):
            sofa_nervous = 2
        elif (row['gcs'] < 15 and row['gcs'] >= 13):
            sofa_nervous = 1
            
    # Cardio: (MBP, vasopressors) into "sofa_cardio"
    sofa_cardio = 0
    if ((not np.isnan(row['mbp'])) and (row['mbp'] < 70)):
        sofa_cardio = 1
    if (row['vasopressors']):
        sofa_cardio = 2
    
    # Liver: (bilirubin) into "sofa_liver"
    sofa_liver = 0
    if (not np.isnan(row['bilirubin'])):
        if (row['bilirubin'] >= 12):
            sofa_liver = 4
        elif (row['bilirubin'] >= 6 and row['bilirubin'] < 12):
            sofa_liver = 3
        elif (row['bilirubin'] >= 2 and row['bilirubin'] < 6):
            sofa_liver = 2
        elif (row['bilirubin'] >= 1.2 and row['bilirubin'] < 2):
            sofa_liver = 1
    
    # Coag: (platelets) into "sofa_coag"
    sofa_coag = 0
    if (not np.isnan(row['platelets'])):
        if (row['platelets'] < 20):
            sofa_coag = 4
        elif (row['platelets'] < 50):
            sofa_coag = 3
        elif (row['platelets'] < 100):
            sofa_coag = 2
        elif (row['platelets'] < 150):
            sofa_coag = 1
    
    # Kidneys: (creatinine and urine output) into "sofa_kidney"
    sofa_kidney = 0
    if (not np.isnan(row['creatinine'])):
        if (row['creatinine'] >= 5):
            sofa_kidney = 4
        elif (row['creatinine'] >= 3.4 and row['creatinine'] < 5):
            sofa_kidney = 3
        elif (row['creatinine'] >= 2 and row['creatinine'] < 3.4):
            sofa_kidney = 2
        elif (row['creatinine'] >= 1.2 and row['creatinine'] < 2):
            sofa_kidney = 1
    elif (not np.isnan(row['urine'])):
        if (row['urine'] <= 200):
            sofa_kidney = 4
        elif (row['urine'] <= 500):
            sofa_kidney = 3
            
    temp_sofa_score = sofa_resp + sofa_nervous + sofa_cardio + sofa_liver + sofa_coag + sofa_kidney
    
    return sofa_resp, sofa_nervous, sofa_cardio, sofa_liver, sofa_coag, sofa_kidney, temp_sofa_score

# Generate qSOFA score and component features
def qSOFA_score(row):
    
    # GCS
    if row['sofa_nervous'] == 0:
        altered_mental_state = False
    else:
        altered_mental_state = True
        
    # Resp
    resp_rate = False
    if ((not np.isnan(row['resp'])) and (row['resp'] >= 22)):
        resp_rate = True
     
    # Systolic
    sys_bp = False
    if ((not np.isnan(row['sbp'])) and (row['sbp'] <= 100)):
        sys_bp = True
     
    qSOFA = altered_mental_state + resp_rate + sys_bp
    
    return altered_mental_state, resp_rate, sys_bp, qSOFA

# Generate sepsis and component features
def sepsis(row):
    
    # Sepsis suspected
    if (row['sofa_score'] >= 2 and row['qsofa_score'] >= 2):
        suspected_sepsis = True
    else:
        suspected_sepsis = False
        
    # Lactate
    sepsis_lactate = False
    if ((not np.isnan(row['lactate'])) and (row['lactate'] > 2)):
        sepsis_lactate = True

    # MBP
    sepsis_map = False
    if ((not np.isnan(row['mbp'])) and (row['mbp'] >= 65)):
        sepsis_map = True
        
    # Septic shock suspected
    suspected_septic_shock = (suspected_sepsis and sepsis_lactate and sepsis_map)
  
    return suspected_sepsis, sepsis_lactate, sepsis_map, suspected_septic_shock
#%% 
for lead_hours in [0,1,3,6,12]:
    for obs_hours in [1,3,6,12]:
        print(lead_hours, ',', obs_hours)
        
        #%% Get patients and time frames to pull from. 

        comp = pd.read_csv(
            dataset_path.joinpath('MIMICIV_relative_'+ str(lead_hours) + 
                                  'hr_lead_' + str(obs_hours) + 
                                  'hr_obs_data_set.csv'),
            usecols=['stay_id', 'intime', 'start', 'end'],
            parse_dates=['intime'])
        #%% Tack on admission times and only keep patients we care about.
        all_data = all_data_pre.merge(comp, on = 'stay_id', how = 'inner')

        #Calculate offset from ICU admission.
        all_data['charttime'] = pd.to_datetime(all_data['charttime'], errors='coerce')
        all_data['offset'] = (all_data['charttime'] - all_data['intime']
                              ).dt.total_seconds()/60

        # Drop data after the observation window for each patient. 
        all_data = all_data[(all_data['offset'] >= all_data['start']) & 
                            (all_data['offset'] <= all_data['end'])]

        # Sort it, then drop timing info. 
        all_data.sort_values(['stay_id', 'offset'], inplace = True)
        all_data = all_data[['stay_id', 'valuenum', 'itemid']]

        #%% Get means of each kind of data.  
        names_list = ['sbp', 'dbp', 'mbp', 'hr', 'resp', 'cvp', 'pO2', 'paCO2', 'fiO2']
        for i in range(0, len(names_list)):
            temp_items = items_list[i]
            name = names_list[i]
            temp_data = all_data.merge(temp_items, on = 'itemid', how = 'inner')
            temp_data = temp_data.groupby('stay_id').last().reset_index()
            temp_data.rename(columns = {'valuenum': name}, inplace = True)
            temp_data = temp_data[['stay_id', name]]
            comp = comp.merge(temp_data, on = 'stay_id', how = 'left')

        # Remove where fiO2 is erroneously 0.
        comp.loc[:, 'fiO2'] = comp['fiO2'].replace(to_replace = 0, value = np.nan)

        #%% Pull labs Bilirubin, Platelets, Creatinine, Lactate, BUN, 
        # Arterial pH, WBC, Hemoglobin, Hematocrit, Potassium
        labs = pd.read_csv(ext_path.joinpath(
            'Labs', 'MIMICIV_dynamic_' + str(lead_hours) + 
            'hr_lead_' + str(obs_hours) + 'hr_obs_lab_features.csv'),
                           usecols = ['patientunitstayid', 'last_total bilirubin', 
                                      'last_platelets x 1000', 
                                      'last_creatinine', 'last_lactate', 'last_BUN',
                                      'last_pH', 'last_WBC x 1000', 'last_Hgb', 
                                      'last_Hct', 'last_potassium'])
        labs.rename(columns = {'patientunitstayid': 'stay_id', 
                               'last_total bilirubin': 'bilirubin', 
                               'last_platelets x 1000': 'platelets', 
                               'last_creatinine': 'creatinine', 
                               'last_lactate': 'lactate', 
                               'last_BUN': 'bun',
                               'last_pH': 'partial_pH', 
                               'last_WBC x 1000': 'wbc', 
                               'last_Hgb': 'hemoglobin', 
                               'last_Hct': 'hematocrit', 
                               'last_potassium': 'potassium'}, inplace = True)
        comp = comp.merge(labs, on = 'stay_id', how = 'left')

        #%% Pull urine output
        urine = pd.read_csv(ext_path.joinpath(
            'IntakeOutput', 'MIMICIV_relative_' + str(lead_hours) + 
            'hr_lead_' + str(obs_hours) + 'hr_obs_last_24hr_urine.csv'))
        comp = comp.merge(urine, on = 'stay_id', how = 'left')

        #%% Vasopressor and ventilator use.
        vaso = pd.read_csv(ext_path.joinpath(
            'Medications', 'MIMICIV_relative_' + str(lead_hours) + 
            'hr_lead_' + str(obs_hours) + 'AllDrugFeatures.csv'),
                          usecols = ['stay_id', 'relative_' + str(lead_hours) + 
                          'hr_lead_' + str(obs_hours) + 'hr_obsVasopressors'])
        comp = comp.merge(vaso, on = 'stay_id', how = 'left')

        vent = pd.read_csv(ext_path.joinpath(
            'Ventilation', 'MIMICIV_relative_' + str(lead_hours) + 
            'hr_lead_' + str(obs_hours) + 'hr_obs_vent_feature.csv'),
                          usecols = ['stay_id', 'vented'])
        comp = comp.merge(vent, on = 'stay_id', how = 'left')

        #%% Pull Temp, GCS from features. 
        
        temper = pd.read_csv(ext_path.joinpath(
            'NurseCharting', 'MIMICIV_dynamic_' + str(lead_hours) + 
            'hr_lead_' + str(obs_hours) + '_temp_feature.csv'),
                                 usecols = ['stay_id', 'last_temp'])
        
        gcs = pd.read_csv(ext_path.joinpath(
            'NurseCharting', 'MIMICIV_relative_' + str(lead_hours) + 
            'hr_lead_' + str(obs_hours) + '_GCS_feature.csv'),
                              usecols = ['stay_id', 'last_total_GCS'])
        
        comp = comp.merge(gcs, on = 'stay_id', how = 'left')
        
        comp = comp.merge(temper, on = 'stay_id', how = 'left')

        #%% Rename columns to make it work with original calculation code.
        comp.rename(columns = {'vented': 'ventilator',
                               'relative_' + str(lead_hours) + 
                               'hr_lead_' + str(obs_hours) + 'hr_obsVasopressors': 'vasopressors',
                               'last_total_GCS': 'gcs', 
                               'last_temp': 'temperature',
                               'last_24hr_urine': 'urine'
                               }, inplace = True)
        #%% Actually calculate scores. 

        comp['sofa_resp'], comp['sofa_nervous'], comp['sofa_cardio'], comp['sofa_liver'], comp['sofa_coag'], comp['sofa_kidney'], comp['sofa_score'] = zip(*comp.apply(lambda row: SOFA_score(row), axis=1))
        comp['qsofa_altered_mental'], comp['qsofa_resp_rate'], comp['qsofa_sys_bp'], comp['qsofa_score'] = zip(*comp.apply(lambda row: qSOFA_score(row), axis=1))
        comp['suspected_sepsis'], comp['sepsis_lactate'], comp['sepsis_map'], comp['suspected_septic_shock'] = zip(*comp.apply(lambda row: sepsis(row), axis=1))

        # Export to csv.
        comp = comp[['stay_id', 'sofa_resp', 'sofa_nervous', 'sofa_cardio', 
                     'sofa_liver', 'sofa_coag', 'sofa_kidney', 'sofa_score', 
                     'qsofa_altered_mental', 'qsofa_resp_rate', 'qsofa_sys_bp',
                     'qsofa_score', 'suspected_sepsis', 'sepsis_lactate', 
                     'sepsis_map', 'suspected_septic_shock']]
        comp.to_csv('MIMICIV_relative_'+ str(lead_hours) + 'hr_lead_' + 
                     str(obs_hours) + '_SOFA_features.csv',index=False)

        calc_time = time() - start