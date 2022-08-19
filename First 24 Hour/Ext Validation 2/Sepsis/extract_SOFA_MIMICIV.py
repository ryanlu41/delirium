# -*- coding: utf-8 -*-
"""
Created on Sun Jul 24 20:38:32 2022

This code calculate the SOFA score and qSOFA score in MIMIC IV for the 1st 
24 hrs. This info is then combined with SOFA scoring to determine if 
patients had sepsis.

Run time: 17 minutes.

@author: Kirby
"""
#%% Setup
import pandas as pd
import numpy as np
from time import time
from pathlib import Path

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
ext_path = file_path.parent.parent.parent.joinpath('Ext Validation 2')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')

start = time()

#%% Get patients and time frames to pull from. 

comp = pd.read_csv(dataset_path.joinpath('MIMICIV_complete_dataset.csv'),
                   usecols = ['stay_id', 'intime'],
                   parse_dates = ['intime'])

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
all_data = pd.read_csv(mimic_path.joinpath('icu', "chartevents.csv.gz"),
                       nrows=0,usecols=['stay_id', 'itemid', 'charttime', 
                                        'valuenum'],
                       parse_dates = ['charttime'])
for chunk in pd.read_csv(mimic_path.joinpath('icu', "chartevents.csv.gz"),
                         chunksize=1000000,
                         usecols=['stay_id', 'itemid', 'charttime', 
                                          'valuenum'],
                         parse_dates = ['charttime']):
    temp_rows = chunk.merge(all_items, on = 'itemid', how = 'inner')
    all_data = pd.concat([all_data,temp_rows])    
   
# Tack on admission times and only keep patients we care about.
all_data = all_data.merge(comp, on = 'stay_id', how = 'inner')

#Calculate offset from ICU admission.
all_data['charttime'] = pd.to_datetime(all_data['charttime'], errors='coerce')
all_data['offset'] = (all_data['charttime'] - all_data['intime']
                      ).dt.total_seconds()/60

#Drop data that wasn't in the first 24 hours of ICU stay. 
all_data = all_data[all_data['offset'] >= 0]
all_data = all_data[all_data['offset'] <= 1440]

all_data = all_data[['stay_id', 'valuenum', 'itemid']]

#%% Get means of each kind of data.  
names_list = ['sbp', 'dbp', 'mbp', 'hr', 'resp', 'cvp', 'paO2', 'paCO2', 'fiO2']
for i in range(0, len(names_list)):
    temp_items = items_list[i]
    name = names_list[i]
    temp_data = all_data.merge(temp_items, on = 'itemid', how = 'inner')
    temp_data = temp_data.groupby('stay_id').mean().reset_index()
    temp_data.rename(columns = {'valuenum': name}, inplace = True)
    temp_data = temp_data[['stay_id', name]]
    comp = comp.merge(temp_data, on = 'stay_id', how = 'left')

# Remove where fiO2 is erroneously 0.
comp.loc[:, 'fiO2'] = comp['fiO2'].replace(to_replace = 0, value = np.nan)

#%% Pull labs Bilirubin, Platelets, Creatinine, Lactate, BUN, 
# Arterial pH, WBC, Hemoglobin, Hematocrit, Potassium
labs = pd.read_csv(ext_path.joinpath('Labs', 'first_24_hour_lab_features_MIMICIV.csv'),
                   usecols = ['patientunitstayid', 'mean_total bilirubin', 
                              'mean_platelets x 1000', 
                              'mean_creatinine', 'mean_lactate', 'mean_BUN',
                              'mean_pH', 'mean_WBC x 1000', 'mean_Hgb', 
                              'mean_Hct', 'mean_potassium'])
labs.rename(columns = {'patientunitstayid': 'stay_id', 
                       'mean_total bilirubin': 'bilirubin', 
                       'mean_platelets x 1000': 'platelets', 
                       'mean_creatinine': 'creatinine', 
                       'mean_lactate': 'lactate', 
                       'mean_BUN': 'bun',
                       'mean_pH': 'partial_pH', 
                       'mean_WBC x 1000': 'wbc', 
                       'mean_Hgb': 'hemoglobin', 
                       'mean_Hct': 'hematocrit', 
                       'mean_potassium': 'potassium'}, inplace = True)
comp = comp.merge(labs, on = 'stay_id', how = 'left')

#%% Pull urine output
urine = pd.read_csv(ext_path.joinpath('IntakeOutput', 'first_24hr_urine_feature_MIMICIV.csv'))
comp = comp.merge(urine, on = 'stay_id', how = 'left')

#%% Vasopressor and ventilator use.
vaso = pd.read_csv(ext_path.joinpath('Medications', 'MIMICIV_24hrs_all_drug_features.csv'),
                  usecols = ['stay_id', 'FirstDayVasopressors'])
comp = comp.merge(vaso, on = 'stay_id', how = 'left')

vent = pd.read_csv(ext_path.joinpath('Ventilation', 'First24HoursVented_MIMICIV.csv'),
                  usecols = ['stay_id', 'first24hrs_vented'])
comp = comp.merge(vent, on = 'stay_id', how = 'left')

#%% Pull Temp, GCS from features. 
gcs = pd.read_csv(ext_path.joinpath('NurseCharting', 'first_24hr_GCS_feature_MIMICIV.csv'),
                  usecols = ['stay_id', '24hrMeanTotal'])
comp = comp.merge(gcs, on = 'stay_id', how = 'left')

temper = pd.read_csv(ext_path.joinpath('NurseCharting', 'first_24hr_temper_feature_MIMICIV.csv'),
                     usecols = ['stay_id', '24hrMeanTemp'])
comp = comp.merge(temper, on = 'stay_id', how = 'left')

#%% Rename columns to make it work with original calculation code.
comp.rename(columns = {'first24hrs_vented': 'ventilator',
                       'FirstDayVasopressors': 'vasopressors',
                       '24hrMeanTotal': 'gcs', 
                       '24hrMeanTemp': 'temperature',
                       'first_24hr_urine': 'urine'
                       }, inplace = True)

#%% Calculate relevant scores. 

# Generate SOFA score and component features
def SOFA_score(row):
    
    # Resp: (PaO2/FiO2, ventilation) into "sofa_resp" 
    sofa_resp = 0
    if ((not np.isnan(row['paO2'])) and (not np.isnan(row['fiO2']))):
        paO2_fiO2_ratio = row['paO2']/row['fiO2']
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
            sofa_nervous = 4
        elif (row['bilirubin'] >= 6 and row['bilirubin'] < 12):
            sofa_nervous = 3
        elif (row['bilirubin'] >= 2 and row['bilirubin'] < 6):
            sofa_nervous = 2
        elif (row['bilirubin'] >= 1.2 and row['bilirubin'] < 2):
            sofa_nervous = 1
    
    # Coag: (platelets) into "sofa_coag"
    sofa_coag = 0
    if (not np.isnan(row['platelets'])):
        if (row['platelets'] < 20):
            sofa_nervous = 4
        elif (row['platelets'] < 50):
            sofa_nervous = 3
        elif (row['platelets'] < 100):
            sofa_nervous = 2
        elif (row['platelets'] < 150):
            sofa_nervous = 1
    
    # Kidneys: (creatinine and urine output) into "sofa_kidney"
    sofa_kidney = 0
    if (not np.isnan(row['creatinine'])):
        if (row['creatinine'] >= 5):
            sofa_nervous = 4
        elif (row['creatinine'] >= 3.4 and row['creatinine'] < 5):
            sofa_nervous = 3
        elif (row['creatinine'] >= 2 and row['creatinine'] < 3.4):
            sofa_nervous = 2
        elif (row['creatinine'] >= 1.2 and row['creatinine'] < 2):
            sofa_nervous = 1
    elif (not np.isnan(row['urine'])):
        if (row['urine'] <= 200):
            sofa_nervous = 3
        elif (row['creatinine'] <= 500):
            sofa_nervous = 4
            
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


#%% Actually calculate scores. 

comp['sofa_resp'], comp['sofa_nervous'], comp['sofa_cardio'], comp['sofa_liver'], comp['sofa_coag'], comp['sofa_kidney'], comp['sofa_score'] = zip(*comp.apply(lambda row: SOFA_score(row), axis=1))
comp['qsofa_altered_mental'], comp['qsofa_resp_rate'], comp['qsofa_sys_bp'], comp['qsofa_score'] = zip(*comp.apply(lambda row: qSOFA_score(row), axis=1))
comp['suspected_sepsis'], comp['sepsis_lactate'], comp['sepsis_map'], comp['suspected_septic_shock'] = zip(*comp.apply(lambda row: sepsis(row), axis=1))

# Export to csv.
comp = comp[['stay_id', 'sofa_resp', 'sofa_nervous', 'sofa_cardio', 'sofa_liver', 'sofa_coag',
         'sofa_kidney', 'sofa_score', 'qsofa_altered_mental', 
         'qsofa_resp_rate', 'qsofa_sys_bp', 'qsofa_score', 'suspected_sepsis',
         'sepsis_lactate', 'sepsis_map', 'suspected_septic_shock']]
comp.to_csv("MIMICIV_24hr_SOFA_features.csv", index = False)

calc_time = time() - start

#%% OLD CODE
# # Add nurseCharting data to features
# def combine_nurseCharting(feature_df, search_string):
#     feature_df = feature_df[feature_df['patientunitstayid'].isin(pids['patientunitstayid'])].copy()
#     feature_df.dropna(axis = 0, how = 'any', inplace = True)
    
#     df = nurseCharting.copy()
#     df = df[df['nursingchartcelltypevalname'].str.contains(search_string, case = False)]
#     df = df[~df['patientunitstayid'].isin(feature_df['patientunitstayid'].unique())]
#     df = df[['patientunitstayid', 'nursingchartoffset', 'nursingchartvalue']]
#     df.columns = ["patientunitstayid", "offset", "value"]
#     df['offset'] = pd.to_numeric(df['offset'],errors='coerce')
#     df['value'] = pd.to_numeric(df['value'],errors='coerce')
    
#     feature = pd.concat([feature_df, df]) 
#     return feature

# # Generate the mean/result of variables found in nurseCharting
# # Used for SBP, DBP, MBP, RESP
# def result_nurseCharting(row, df):
#     temp = df[df['patientunitstayid'] == row['patientunitstayid']]
#     temp = temp[(temp['offset'] >= row['start_offset']) & (temp['offset'] <= row['end_offset'])]
#     return np.mean(temp.dropna(subset = ['value'])['value'])

# # Generate the mean/result of variables found in nurseCharting
# # Used for Temperature, CVP, GCS
# def result_nurseCharting2(row, df):
#     temp = df[df['patientunitstayid'] == row['patientunitstayid']]
#     temp = temp[(temp['nursingchartoffset'] >= row['start_offset']) & (temp['nursingchartoffset'] <= row['end_offset'])]
#     return np.mean(temp.dropna(subset = ['nursingchartvalue'])['nursingchartvalue'])

# # Generate the mean/result of variables found in lab
# # Used for PaO2, FiO2, Bilirubin, Platelets, Creatinine, Lactate, BUN, Arterial pH, 
# # WBC, PaCO2, Hemoglobin, Hematocrit, Potassium 
# def result_lab(row, df):
#     temp = df[df['patientunitstayid'] == row['patientunitstayid']]
#     temp = temp[(temp['labresultrevisedoffset'] >= row['start_offset']) & (temp['labresultrevisedoffset'] <= row['end_offset'])]
#     return np.mean(temp.dropna(subset = ['labresult'])['labresult'])

# # Generate the mean/result of variables found in intakeOutput
# # Used for Urine
# def result_intakeOutput(row, df):
#     temp = df[df['patientunitstayid'] == row['patientunitstayid']]
#     temp = temp[(temp['intakeoutputoffset'] >= row['start_offset']) & (temp['intakeoutputoffset'] <= row['end_offset'])]
#     return np.sum(temp.dropna(subset = ['cellvaluenumeric'])['cellvaluenumeric'])

# # Generate the mean/result of vasopressors
# def result_vasopressors(row):
#     vs1 = vasopressors1[vasopressors1['patientunitstayid'] == row['patientunitstayid']]
#     vs1 = vs1[(vs1['infusionoffset'] >= row['start_offset']) & (vs1['infusionoffset'] <= row['end_offset'])]
#     vs2 = vasopressors2[vasopressors2['patientunitstayid'] == row['patientunitstayid']]
#     vs2 = vs2[(vs2['drugstartoffset'] >= row['start_offset']) & (vs2['drugstartoffset'] <= row['end_offset'])]
#     vs3 = vasopressors3[vasopressors3['patientunitstayid'] == row['patientunitstayid']]
#     vs3 = vs3[(vs3['treatmentoffset'] >= row['start_offset']) & (vs3['treatmentoffset'] <= row['end_offset'])]
#     return ((len(vs1)+len(vs2)+len(vs3)) > 0)

# # Generate the mean/result of ventilator
# def result_ventilator(row):
#     temp = ventilator[ventilator['patientunitstayid'] == row['patientunitstayid']]
#     temp = temp[(temp['hrs'] >= row['start_offset']) & (temp['hrs'] <= row['end_offset'])]
#     return (len(temp) > 0)

# # Initialization
# # pandarallel.initialize(progress_bar = False)

# # Read in patient IDs and windows
# pids = pd.read_csv("Sepsis_Windows.csv")

# # Load Nurse Charting to "nurseCharting"
# # Used for SBP, DBP, MBP, RESP, Temperature, CVP, GCS
# nurseCharting = pd.read_csv("Delirium_eICU/delirium_nurseCharting.csv")
# nurseCharting = nurseCharting[nurseCharting['patientunitstayid'].isin(pids['patientunitstayid'])]
# nurseCharting['nursingchartoffset'] = pd.to_numeric(nurseCharting['nursingchartoffset'],errors='coerce')
# nurseCharting['nursingchartvalue'] = pd.to_numeric(nurseCharting['nursingchartvalue'],errors='coerce')

# # 1. HR (Heart Rate) to "hr" 
# hr = pd.read_csv("Delirium_eICU/delirium_hr.csv")
# hr = combine_nurseCharting(hr, 'heart rate')
# pids['hr'] = pids.apply(lambda row: result_nurseCharting(row, hr), axis=1)
# del hr

# pids.to_csv("delirium_SOFA_features_hr.csv")

# # 2. SBP (Systolic Blood Pressure) to "sbp"
# sbp = pd.read_csv("Delirium_eICU/delirium_isys.csv")
# sbp = combine_nurseCharting(sbp, 'bp systolic')
# pids['sbp'] = pids.apply(lambda row: result_nurseCharting(row, sbp), axis=1)
# del sbp

# # 3. DBP (Diastolic Blood Pressure) to "dbp"
# dbp = pd.read_csv("Delirium_eICU/delirium_idias.csv")
# dbp = combine_nurseCharting(dbp, 'bp diastolic')
# pids['dbp'] = pids.apply(lambda row: result_nurseCharting(row, dbp), axis=1)
# del dbp

# # 4. MBP (Mean Blood Pressure) to "mbp"
# mbp = pd.read_csv("Delirium_eICU/delirium_imean.csv")
# mbp = combine_nurseCharting(mbp, 'bp mean')
# pids['mbp'] = pids.apply(lambda row: result_nurseCharting(row, mbp), axis=1)
# del mbp

# # 5. RESP (Respiratory Rate) to "resp"
# resp = pd.read_csv("Delirium_eICU/delirium_resp.csv")
# resp = combine_nurseCharting(resp, 'respiratory rate')
# pids['resp'] = pids.apply(lambda row: result_nurseCharting(row, resp), axis=1)
# del resp

# # 6. Temperature (C) to "temperature"
# temperature = nurseCharting[nurseCharting['nursingchartcelltypevalname'] == 'Temperature (C)']
# pids['temperature'] = pids.apply(lambda row: result_nurseCharting2(row, temperature), axis=1)
# del temperature

# # 7. CVP (Central Venous Pressure) to "cvp"
# cvp = nurseCharting[nurseCharting['nursingchartcelltypevallabel'].str.contains("CVP", case = False)]
# pids['cvp'] = pids.apply(lambda row: result_nurseCharting2(row, cvp), axis=1)
# del cvp

# # 8. GCS (Glasgow Coma Score) into "gcs"
# gcs = nurseCharting[nurseCharting['nursingchartcelltypevallabel'] == 'Glasgow coma score']
# gcs = gcs[gcs['nursingchartcelltypevalname'] == 'GCS Total']
# pids['gcs'] = pids.apply(lambda row: result_nurseCharting2(row, gcs), axis=1)
# del gcs

# del nurseCharting

# # Load Lab to "lab"
# # Used for PaO2, FiO2, Bilirubin, Platelets, Creatinine, Lactate, BUN, Arterial pH, WBC, PaCO2, Hemoglobin, Hematocrit, Potassium 
# lab = pd.read_csv("Delirium_eICU/delirium_lab.csv")
# lab = lab[lab['patientunitstayid'].isin(pids['patientunitstayid'])]

# # 9. PaO2 (Partial Pressure of Oxygen) to "paO2"
# paO2 = lab[lab['labname'] == "paO2"]
# pids['paO2'] = pids.apply(lambda row: result_lab(row, paO2), axis=1)
# del paO2

# # 10. FiO2 (Fraction of Inspired Oxygen) to "fiO2"
# fiO2 = lab[lab['labname'] == "FiO2"]
# pids['fiO2'] = pids.apply(lambda row: result_lab(row, fiO2), axis=1)
# del fiO2

# # 11. Bilirubin into "bilirubin"
# bilirubin = lab[lab['labname'] == "direct bilirubin"]
# pids['bilirubin'] = pids.apply(lambda row: result_lab(row, bilirubin), axis=1)
# del bilirubin

# # 12. Platelets into "platelets"
# platelets = lab[lab['labname'] == "platelets x 1000"]
# pids['platelets'] = pids.apply(lambda row: result_lab(row, platelets), axis=1)
# del platelets

# # 13. Creatinine into "creatinine"
# creatinine = lab[lab['labname'] == "creatinine"]
# pids['creatinine'] = pids.apply(lambda row: result_lab(row, creatinine), axis=1)
# del creatinine

# # 14. Lactate into "lactate"
# lactate = lab[lab['labname'] == "lactate"]
# pids['lactate'] = pids.apply(lambda row: result_lab(row, lactate), axis=1)
# del lactate

# # 15. BUN (Blood Urea Nitrogen) into "bun"
# bun = lab[lab['labname'] == "BUN"]
# pids['bun'] = pids.apply(lambda row: result_lab(row, bun), axis=1)
# del bun

# # 16. Arterial pH into "arterial_pH"
# arterial_pH = lab[lab['labname'] == "pH"]
# pids['arterial_pH'] = pids.apply(lambda row: result_lab(row, arterial_pH), axis=1)
# del arterial_pH

# # 17. WBC (White Blood Count) into "wbc"
# wbc = lab[lab['labname'] == "WBC x 1000"]
# pids['wbc'] = pids.apply(lambda row: result_lab(row, wbc), axis=1)
# del wbc

# # 18. PaCO2 (Partial Pressure of Carbon Dioxide) into "paCO2"
# paCO2 = lab[lab['labname'] == "paCO2"]
# pids['paCO2'] = pids.apply(lambda row: result_lab(row, paCO2), axis=1)
# del paCO2

# # 19. Hemoglobin into "hemoglobin"
# hemoglobin = lab[lab['labname'] == "Hgb"]
# pids['hemoglobin'] = pids.apply(lambda row: result_lab(row, hemoglobin), axis=1)
# del hemoglobin

# # 20. Hematocrit into "hematocrit"
# hematocrit = lab[lab['labname'] == "Hct"]
# pids['hematocrit'] = pids.apply(lambda row: result_lab(row, hematocrit), axis=1)
# del hematocrit

# # 21. Potassium into "potassium"
# potassium = lab[lab['labname'] == "potassium"]
# pids['potassium'] = pids.apply(lambda row: result_lab(row, potassium), axis=1)
# del potassium

# del lab

# # Load intakeoutput to "intakeOutput"
# # Used for Urine
# intakeOutput = pd.read_csv("Delirium_eICU/delirium_intakeOutput.csv")
# intakeOutput = intakeOutput[intakeOutput['patientunitstayid'].isin(pids['patientunitstayid'])]

# # 22. Urine into "urine"
# urine = intakeOutput[intakeOutput['celllabel'] == "Urine"]
# pids['urine'] = pids.apply(lambda row: result_intakeOutput(row, urine), axis=1)
# del intakeOutput, urine

# # Load infusiondrug into "infusionDrug"
# # Used for Vasopressors (1)
# infusionDrug = pd.read_csv("Delirium_eICU/delirium_infusionDrug.csv")
# infusionDrug = infusionDrug[infusionDrug['patientunitstayid'].isin(pids['patientunitstayid'])]
# infusionDrug.dropna(subset = ['drugname'], inplace = True)
# infusionDrug['infusionoffset'] = pd.to_numeric(infusionDrug['infusionoffset'],errors='coerce')

# # Load medication into "medication"
# # Used for Vasopressors (2)
# medication = pd.read_csv("Delirium_eICU/delirium_medication.csv")
# medication = medication[medication['patientunitstayid'].isin(pids['patientunitstayid'])]
# medication.dropna(subset = ['drugname'], inplace = True)
# medication['drugstartoffset'] = pd.to_numeric(medication['drugstartoffset'],errors='coerce')

# # Load treatment into "treatment"
# # Used for Vasopressors (3)
# treatment = pd.read_csv("Delirium_eICU/delirium_treatment.csv")
# treatment = treatment[treatment['patientunitstayid'].isin(pids['patientunitstayid'])]
# treatment.dropna(subset = ['treatmentstring'], inplace = True)
# treatment['treatmentoffset'] = pd.to_numeric(treatment['treatmentoffset'],errors='coerce')

# # 23. Vasopressors into ""vasopressors1", "vasopressors2", and "vasopressors3"
# vasopressors1 = infusionDrug[infusionDrug['drugname'].str.contains('dopamine|dobutamine|epinephrine|norepinephrine', case=False)]
# vasopressors2 = medication[medication['drugname'].str.contains('dopamine|dobutamine|epinephrine|norepinephrine', case=False)]
# vasopressors3 = treatment[treatment['treatmentstring'].str.contains('dopamine|dobutamine|epinephrine|norepinephrine', case=False)]

# del infusionDrug, medication, treatment

# pids['vasopressors'] = pids.apply(lambda row: result_vasopressors(row), axis=1)

# del vasopressors1, vasopressors2, vasopressors3

# # 24. df_vent_event into "ventilator"
# ventilator = pd.read_csv("Delirium_eICU/df_vent_event.csv")
# ventilator = ventilator[ventilator['patientunitstayid'].isin(pids['patientunitstayid'])]
# ventilator = ventilator[ventilator['event'].str.contains('mechvent', case=False)]
# ventilator['hrs'] = ventilator['hrs']*60
# pids['ventilator'] = pids.apply(lambda row: result_ventilator(row), axis=1)
# del ventilator

