"""
Pulls data to calculate SOFA score, qSOFA score, and suspected sepsis
"""

# Import packages
import numpy as np
import pandas as pd
from pandarallel import pandarallel

# Set file paths
pid_file = "first_24hr_prediction_dataset.csv"
nurseCharting_file = "Delirium_eICU/delirium_nurseCharting.csv"
sbp_file = "Delirium_eICU/delirium_isys.csv"
mbp_file = "Delirium_eICU/delirium_imean.csv"
resp_file = "Delirium_eICU/delirium_resp.csv"
lab_file = "Delirium_eICU/delirium_lab.csv"
intakeOutput_file = "Delirium_eICU/delirium_intakeOutput.csv"
infusionDrug_file = "Delirium_eICU/delirium_infusionDrug.csv"
medication_file = "Delirium_eICU/delirium_medication.csv"
treatment_file = "Delirium_eICU/delirium_treatment.csv"
ventilator_file = "Delirium_eICU/df_vent_event.csv"
output_file = "delirium_SOFA_features_24hr.csv"

## Define relevant functions
# Add nurseCharting data to features
def combine_nurseCharting(feature_df, search_string):
    feature_df = feature_df[feature_df['patientunitstayid'].isin(pids['patientunitstayid'])].copy()
    feature_df.dropna(axis = 0, how = 'any', inplace = True)
    
    df = nurseCharting.copy()
    df = df[df['nursingchartcelltypevalname'].str.contains(search_string, case = False)]
    df = df[~df['patientunitstayid'].isin(feature_df['patientunitstayid'].unique())]
    df = df[['patientunitstayid', 'nursingchartoffset', 'nursingchartvalue']]
    df.columns = ["patientunitstayid", "offset", "value"]
    df['offset'] = pd.to_numeric(df['offset'],errors='coerce')
    df['value'] = pd.to_numeric(df['value'],errors='coerce')
    
    feature = pd.concat([feature_df, df]) 
    return feature

# Generate the result of variables found in nurseCharting
# Used for SBP, MBP, RESP
def result_nurseCharting(row, df):
    temp = df[df['patientunitstayid'] == row['patientunitstayid']]
    temp = temp[(temp['offset'] >= 0) & (temp['offset'] <= 1440)]
    return np.amin(temp.dropna(subset = ['value'])['value'])

# Generate the result of variables found in nurseCharting
# Used for GCS
def result_nurseCharting2(row, df):
    temp = df[df['patientunitstayid'] == row['patientunitstayid']]
    temp = temp[(temp['nursingchartoffset'] >= 0) & (temp['nursingchartoffset'] <= 1440)]
    return np.amin(temp.dropna(subset = ['nursingchartvalue'])['nursingchartvalue'])

# Generate the result of variables found in lab
# Used for PaO2, FiO2, Bilirubin, Platelets, Creatinine, Lactate
def result_lab(row, df):
    temp = df[df['patientunitstayid'] == row['patientunitstayid']]
    temp = temp[(temp['labresultrevisedoffset'] >= 0) & (temp['labresultrevisedoffset'] <= 1440)]
    return np.amin(temp.dropna(subset = ['labresult'])['labresult'])

# Generate the result of variables found in intakeOutput
# Used for Urine
def result_intakeOutput(row, df):
    temp = df[df['patientunitstayid'] == row['patientunitstayid']]
    temp = temp[(temp['intakeoutputoffset'] >= 0) & (temp['intakeoutputoffset'] <= 1440)]
    return np.sum(temp.dropna(subset = ['cellvaluenumeric'])['cellvaluenumeric'])

# Generate the result of vasopressors
def result_vasopressors(row):
    vs1 = vasopressors1[vasopressors1['patientunitstayid'] == row['patientunitstayid']]
    vs1 = vs1[(vs1['infusionoffset'] >= 0) & (vs1['infusionoffset'] <= 1440)]
    vs2 = vasopressors2[vasopressors2['patientunitstayid'] == row['patientunitstayid']]
    vs2 = vs2[(vs2['drugstartoffset'] >= 0) & (vs2['drugstartoffset'] <= 1440)]
    vs3 = vasopressors3[vasopressors3['patientunitstayid'] == row['patientunitstayid']]
    vs3 = vs3[(vs3['treatmentoffset'] >= 0) & (vs3['treatmentoffset'] <= 1440)]
    return ((len(vs1)+len(vs2)+len(vs3)) > 0)

# Generate the result of ventilator
def result_ventilator(row):
    temp = ventilator[ventilator['patientunitstayid'] == row['patientunitstayid']]
    temp = temp[(temp['hrs'] >= 0) & (temp['hrs'] <= 1440)]
    return (len(temp) > 0)

# Generate SOFA score and component features
def SOFA_score(row):
    
    # Resp: (PaO2/FiO2, ventilation) into "sofa_resp" 
    sofa_resp = 0
    if ((not np.isnan(row['paO2'])) and (not np.isnan(row['fiO2'])) and (row['fiO2'] > 0)):
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

# Initialization
pandarallel.initialize(progress_bar = False)

# Read in patient IDs and windows
pids = pd.read_csv(pid_file)
pids = pids[['patientunitstayid']]

# Load Nurse Charting to "nurseCharting"
# Used for SBP, MBP, RESP, GCS
nurseCharting = pd.read_csv(nurseCharting_file)
nurseCharting = nurseCharting[nurseCharting['patientunitstayid'].isin(pids['patientunitstayid'])]
nurseCharting['nursingchartoffset'] = pd.to_numeric(nurseCharting['nursingchartoffset'],errors='coerce')
nurseCharting['nursingchartvalue'] = pd.to_numeric(nurseCharting['nursingchartvalue'],errors='coerce')

# SBP (Systolic Blood Pressure) to "sbp"
sbp = pd.read_csv(sbp_file)
sbp = combine_nurseCharting(sbp, 'bp systolic')
pids['sbp'] = pids.parallel_apply(lambda row: result_nurseCharting(row, sbp), axis=1)
del sbp

# MBP (Mean Blood Pressure) to "mbp"
mbp = pd.read_csv(mbp_file)
mbp = combine_nurseCharting(mbp, 'bp mean')
pids['mbp'] = pids.parallel_apply(lambda row: result_nurseCharting(row, mbp), axis=1)
del mbp

# RESP (Respiratory Rate) to "resp"
resp = pd.read_csv(resp_file)
resp = combine_nurseCharting(resp, 'respiratory rate')
pids['resp'] = pids.parallel_apply(lambda row: result_nurseCharting(row, resp), axis=1)
del resp

# GCS (Glasgow Coma Score) into "gcs"
gcs = nurseCharting[nurseCharting['nursingchartcelltypevallabel'] == 'Glasgow coma score']
gcs = gcs[gcs['nursingchartcelltypevalname'] == 'GCS Total']
pids['gcs'] = pids.parallel_apply(lambda row: result_nurseCharting2(row, gcs), axis=1)
del gcs

del nurseCharting

# Load Lab to "lab"
# Used for PaO2, FiO2, Bilirubin, Platelets, Creatinine, Lactate
lab = pd.read_csv(lab_file)
lab = lab[lab['patientunitstayid'].isin(pids['patientunitstayid'])]

# PaO2 (Partial Pressure of Oxygen) to "paO2"
paO2 = lab[lab['labname'] == "paO2"]
pids['paO2'] = pids.parallel_apply(lambda row: result_lab(row, paO2), axis=1)
del paO2

# FiO2 (Fraction of Inspired Oxygen) to "fiO2"
fiO2 = lab[lab['labname'] == "FiO2"]
pids['fiO2'] = pids.parallel_apply(lambda row: result_lab(row, fiO2), axis=1)
del fiO2

# Bilirubin into "bilirubin"
bilirubin = lab[lab['labname'] == "direct bilirubin"]
pids['bilirubin'] = pids.parallel_apply(lambda row: result_lab(row, bilirubin), axis=1)
del bilirubin

# Platelets into "platelets"
platelets = lab[lab['labname'] == "platelets x 1000"]
pids['platelets'] = pids.parallel_apply(lambda row: result_lab(row, platelets), axis=1)
del platelets

# Creatinine into "creatinine"
creatinine = lab[lab['labname'] == "creatinine"]
pids['creatinine'] = pids.parallel_apply(lambda row: result_lab(row, creatinine), axis=1)
del creatinine

# Lactate into "lactate"
lactate = lab[lab['labname'] == "lactate"]
pids['lactate'] = pids.parallel_apply(lambda row: result_lab(row, lactate), axis=1)
del lactate

del lab

# Load intakeoutput to "intakeOutput"
# Used for Urine
intakeOutput = pd.read_csv(intakeOutput_file)
intakeOutput = intakeOutput[intakeOutput['patientunitstayid'].isin(pids['patientunitstayid'])]

# Urine into "urine"
urine = intakeOutput[intakeOutput['celllabel'] == "Urine"]
pids['urine'] = pids.apply(lambda row: result_intakeOutput(row, urine), axis=1)
del intakeOutput, urine

# Load infusiondrug into "infusionDrug"
# Used for Vasopressors (1)
infusionDrug = pd.read_csv(infusionDrug_file)
infusionDrug = infusionDrug[infusionDrug['patientunitstayid'].isin(pids['patientunitstayid'])]
infusionDrug.dropna(subset = ['drugname'], inplace = True)
infusionDrug['infusionoffset'] = pd.to_numeric(infusionDrug['infusionoffset'],errors='coerce')

# Load medication into "medication"
# Used for Vasopressors (2)
medication = pd.read_csv(medication_file)
medication = medication[medication['patientunitstayid'].isin(pids['patientunitstayid'])]
medication.dropna(subset = ['drugname'], inplace = True)
medication['drugstartoffset'] = pd.to_numeric(medication['drugstartoffset'],errors='coerce')

# Load treatment into "treatment"
# Used for Vasopressors (3)
treatment = pd.read_csv(treatment_file)
treatment = treatment[treatment['patientunitstayid'].isin(pids['patientunitstayid'])]
treatment.dropna(subset = ['treatmentstring'], inplace = True)
treatment['treatmentoffset'] = pd.to_numeric(treatment['treatmentoffset'],errors='coerce')

# Vasopressors into ""vasopressors1", "vasopressors2", and "vasopressors3"
vasopressors1 = infusionDrug[infusionDrug['drugname'].str.contains('dopamine|dobutamine|epinephrine|norepinephrine', case=False)]
vasopressors2 = medication[medication['drugname'].str.contains('dopamine|dobutamine|epinephrine|norepinephrine', case=False)]
vasopressors3 = treatment[treatment['treatmentstring'].str.contains('dopamine|dobutamine|epinephrine|norepinephrine', case=False)]

del infusionDrug, medication, treatment

pids['vasopressors'] = pids.parallel_apply(lambda row: result_vasopressors(row), axis=1)

del vasopressors1, vasopressors2, vasopressors3

# df_vent_event into "ventilator"
ventilator = pd.read_csv(ventilator_file)
ventilator = ventilator[ventilator['patientunitstayid'].isin(pids['patientunitstayid'])]
ventilator = ventilator[ventilator['event'].str.contains('mechvent', case=False)]
ventilator['hrs'] = ventilator['hrs']*60
pids['ventilator'] = pids.parallel_apply(lambda row: result_ventilator(row), axis=1)
del ventilator

pids['sofa_resp'], pids['sofa_nervous'], pids['sofa_cardio'], pids['sofa_liver'], pids['sofa_coag'], pids['sofa_kidney'], pids['sofa_score'] = zip(*pids.parallel_apply(lambda row: SOFA_score(row), axis=1))
pids['qsofa_altered_mental'], pids['qsofa_resp_rate'], pids['qsofa_sys_bp'], pids['qsofa_score'] = zip(*pids.parallel_apply(lambda row: qSOFA_score(row), axis=1))
pids['suspected_sepsis'], pids['sepsis_lactate'], pids['sepsis_map'], pids['suspected_septic_shock'] = zip(*pids.parallel_apply(lambda row: sepsis(row), axis=1))

# Export to csv
pids.to_csv(output_file)
