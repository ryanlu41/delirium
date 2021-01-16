import sys
import numpy as np
import pandas as pd
from pandarallel import pandarallel

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

# Generate the mean/result of variables found in nurseCharting
# Used for SBP, DBP, MBP, RESP
def result_nurseCharting(row, df):
    temp = df[df['patientunitstayid'] == row['patientunitstayid']]
    temp = temp[(temp['offset'] >= row['start']) & (temp['offset'] <= row['end'])]
    return np.mean(temp.dropna(subset = ['value'])['value'])

# Generate the mean/result of variables found in nurseCharting
# Used for Temperature, CVP, GCS
def result_nurseCharting2(row, df):
    temp = df[df['patientunitstayid'] == row['patientunitstayid']]
    temp = temp[(temp['nursingchartoffset'] >= row['start']) & (temp['nursingchartoffset'] <= row['end'])]
    return np.mean(temp.dropna(subset = ['nursingchartvalue'])['nursingchartvalue'])

# Generate the mean/result of variables found in lab
# Used for PaO2, FiO2, Bilirubin, Platelets, Creatinine, Lactate, BUN, Arterial pH, 
# WBC, PaCO2, Hemoglobin, Hematocrit, Potassium 
def result_lab(row, df):
    temp = df[df['patientunitstayid'] == row['patientunitstayid']]
    temp = temp[(temp['labresultrevisedoffset'] >= row['start']) & (temp['labresultrevisedoffset'] <= row['end'])]
    return np.mean(temp.dropna(subset = ['labresult'])['labresult'])

# Generate the mean/result of variables found in intakeOutput
# Used for Urine
def result_intakeOutput(row, df):
    temp = df[df['patientunitstayid'] == row['patientunitstayid']]
    temp = temp[(temp['intakeoutputoffset'] >= row['start']) & (temp['intakeoutputoffset'] <= row['end'])]
    return np.sum(temp.dropna(subset = ['cellvaluenumeric'])['cellvaluenumeric'])

# Generate the mean/result of vasopressors
def result_vasopressors(row):
    vs1 = vasopressors1[vasopressors1['patientunitstayid'] == row['patientunitstayid']]
    vs1 = vs1[(vs1['infusionoffset'] >= row['start']) & (vs1['infusionoffset'] <= row['end'])]
    vs2 = vasopressors2[vasopressors2['patientunitstayid'] == row['patientunitstayid']]
    vs2 = vs2[(vs2['drugstartoffset'] >= row['start']) & (vs2['drugstartoffset'] <= row['end'])]
    vs3 = vasopressors3[vasopressors3['patientunitstayid'] == row['patientunitstayid']]
    vs3 = vs3[(vs3['treatmentoffset'] >= row['start']) & (vs3['treatmentoffset'] <= row['end'])]
    return ((len(vs1)+len(vs2)+len(vs3)) > 0)

# Generate the mean/result of ventilator
def result_ventilator(row):
    temp = ventilator[ventilator['patientunitstayid'] == row['patientunitstayid']]
    temp = temp[(temp['hrs'] >= row['start']) & (temp['hrs'] <= row['end'])]
    return (len(temp) > 0)

# Generate SOFA score and component features
def SOFA_score(row):
    
    # Resp: (PaO2/FiO2, ventilation) into "sofa_resp" 
    sofa_resp = 0
    if ((not np.isnan(row['paO2'])) and (not np.isnan(row['fiO2']))):
        try:
            paO2_fiO2_ratio = row['paO2']/row['fiO2']
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
            sofa_kidney = 3
        elif (row['creatinine'] <= 500):
            sofa_kidney = 4
            
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

# Pick file
inp = sys.argv[1]
inp_filename = f"../relative_1hr_lead_{inp}hr_obs_data_set.csv"
out_filename = f"relative_1hr_lead_{inp}hr_obs_SOFA_features.csv"

# Read in patient IDs and windows
pids = pd.read_csv(inp_filename)

# Load Nurse Charting to "nurseCharting"
# Used for SBP, DBP, MBP, RESP, Temperature, CVP, GCS
nurseCharting = pd.read_csv("Delirium_eICU/delirium_nurseCharting.csv")
nurseCharting = nurseCharting[nurseCharting['patientunitstayid'].isin(pids['patientunitstayid'])]
nurseCharting['nursingchartoffset'] = pd.to_numeric(nurseCharting['nursingchartoffset'],errors='coerce')
nurseCharting['nursingchartvalue'] = pd.to_numeric(nurseCharting['nursingchartvalue'],errors='coerce')

# 1. HR (Heart Rate) to "hr" 
hr = pd.read_csv("Delirium_eICU/delirium_hr.csv")
hr = combine_nurseCharting(hr, 'heart rate')
pids['hr'] = pids.parallel_apply(lambda row: result_nurseCharting(row, hr), axis=1)
del hr

# 2. SBP (Systolic Blood Pressure) to "sbp"
sbp = pd.read_csv("Delirium_eICU/delirium_isys.csv")
sbp = combine_nurseCharting(sbp, 'bp systolic')
pids['sbp'] = pids.parallel_apply(lambda row: result_nurseCharting(row, sbp), axis=1)
del sbp

# 3. DBP (Diastolic Blood Pressure) to "dbp"
dbp = pd.read_csv("Delirium_eICU/delirium_idias.csv")
dbp = combine_nurseCharting(dbp, 'bp diastolic')
pids['dbp'] = pids.parallel_apply(lambda row: result_nurseCharting(row, dbp), axis=1)
del dbp

# 4. MBP (Mean Blood Pressure) to "mbp"
mbp = pd.read_csv("Delirium_eICU/delirium_imean.csv")
mbp = combine_nurseCharting(mbp, 'bp mean')
pids['mbp'] = pids.parallel_apply(lambda row: result_nurseCharting(row, mbp), axis=1)
del mbp

# 5. RESP (Respiratory Rate) to "resp"
resp = pd.read_csv("Delirium_eICU/delirium_resp.csv")
resp = combine_nurseCharting(resp, 'respiratory rate')
pids['resp'] = pids.parallel_apply(lambda row: result_nurseCharting(row, resp), axis=1)
del resp

# 6. Temperature (C) to "temperature"
temperature = nurseCharting[nurseCharting['nursingchartcelltypevalname'] == 'Temperature (C)']
pids['temperature'] = pids.parallel_apply(lambda row: result_nurseCharting2(row, temperature), axis=1)
del temperature

# 7. CVP (Central Venous Pressure) to "cvp"
cvp = nurseCharting[nurseCharting['nursingchartcelltypevallabel'].str.contains("CVP", case = False)]
pids['cvp'] = pids.parallel_apply(lambda row: result_nurseCharting2(row, cvp), axis=1)
del cvp

# 8. GCS (Glasgow Coma Score) into "gcs"
gcs = nurseCharting[nurseCharting['nursingchartcelltypevallabel'] == 'Glasgow coma score']
gcs = gcs[gcs['nursingchartcelltypevalname'] == 'GCS Total']
pids['gcs'] = pids.parallel_apply(lambda row: result_nurseCharting2(row, gcs), axis=1)
del gcs

del nurseCharting

# Load Lab to "lab"
# Used for PaO2, FiO2, Bilirubin, Platelets, Creatinine, Lactate, BUN, Arterial pH, WBC, PaCO2, Hemoglobin, Hematocrit, Potassium 
lab = pd.read_csv("Delirium_eICU/delirium_lab.csv")
lab = lab[lab['patientunitstayid'].isin(pids['patientunitstayid'])]

# 9. PaO2 (Partial Pressure of Oxygen) to "paO2"
paO2 = lab[lab['labname'] == "paO2"]
pids['paO2'] = pids.parallel_apply(lambda row: result_lab(row, paO2), axis=1)
del paO2

# 10. FiO2 (Fraction of Inspired Oxygen) to "fiO2"
fiO2 = lab[lab['labname'] == "FiO2"]
pids['fiO2'] = pids.parallel_apply(lambda row: result_lab(row, fiO2), axis=1)
del fiO2

# 11. Bilirubin into "bilirubin"
bilirubin = lab[lab['labname'] == "direct bilirubin"]
pids['bilirubin'] = pids.parallel_apply(lambda row: result_lab(row, bilirubin), axis=1)
del bilirubin

# 12. Platelets into "platelets"
platelets = lab[lab['labname'] == "platelets x 1000"]
pids['platelets'] = pids.parallel_apply(lambda row: result_lab(row, platelets), axis=1)
del platelets

# 13. Creatinine into "creatinine"
creatinine = lab[lab['labname'] == "creatinine"]
pids['creatinine'] = pids.parallel_apply(lambda row: result_lab(row, creatinine), axis=1)
del creatinine

# 14. Lactate into "lactate"
lactate = lab[lab['labname'] == "lactate"]
pids['lactate'] = pids.parallel_apply(lambda row: result_lab(row, lactate), axis=1)
del lactate

# 15. BUN (Blood Urea Nitrogen) into "bun"
bun = lab[lab['labname'] == "BUN"]
pids['bun'] = pids.parallel_apply(lambda row: result_lab(row, bun), axis=1)
del bun

# 16. Arterial pH into "arterial_pH"
arterial_pH = lab[lab['labname'] == "pH"]
pids['arterial_pH'] = pids.parallel_apply(lambda row: result_lab(row, arterial_pH), axis=1)
del arterial_pH

# 17. WBC (White Blood Count) into "wbc"
wbc = lab[lab['labname'] == "WBC x 1000"]
pids['wbc'] = pids.parallel_apply(lambda row: result_lab(row, wbc), axis=1)
del wbc

# 18. PaCO2 (Partial Pressure of Carbon Dioxide) into "paCO2"
paCO2 = lab[lab['labname'] == "paCO2"]
pids['paCO2'] = pids.parallel_apply(lambda row: result_lab(row, paCO2), axis=1)
del paCO2

# 19. Hemoglobin into "hemoglobin"
hemoglobin = lab[lab['labname'] == "Hgb"]
pids['hemoglobin'] = pids.parallel_apply(lambda row: result_lab(row, hemoglobin), axis=1)
del hemoglobin

# 20. Hematocrit into "hematocrit"
hematocrit = lab[lab['labname'] == "Hct"]
pids['hematocrit'] = pids.parallel_apply(lambda row: result_lab(row, hematocrit), axis=1)
del hematocrit

# 21. Potassium into "potassium"
potassium = lab[lab['labname'] == "potassium"]
pids['potassium'] = pids.parallel_apply(lambda row: result_lab(row, potassium), axis=1)
del potassium

del lab

# Load intakeoutput to "intakeOutput"
# Used for Urine
intakeOutput = pd.read_csv("Delirium_eICU/delirium_intakeOutput.csv")
intakeOutput = intakeOutput[intakeOutput['patientunitstayid'].isin(pids['patientunitstayid'])]

# 22. Urine into "urine"
urine = intakeOutput[intakeOutput['celllabel'] == "Urine"]
pids['urine'] = pids.apply(lambda row: result_intakeOutput(row, urine), axis=1)
del intakeOutput, urine

# Load infusiondrug into "infusionDrug"
# Used for Vasopressors (1)
infusionDrug = pd.read_csv("Delirium_eICU/delirium_infusionDrug.csv")
infusionDrug = infusionDrug[infusionDrug['patientunitstayid'].isin(pids['patientunitstayid'])]
infusionDrug.dropna(subset = ['drugname'], inplace = True)
infusionDrug['infusionoffset'] = pd.to_numeric(infusionDrug['infusionoffset'],errors='coerce')

# Load medication into "medication"
# Used for Vasopressors (2)
medication = pd.read_csv("Delirium_eICU/delirium_medication.csv")
medication = medication[medication['patientunitstayid'].isin(pids['patientunitstayid'])]
medication.dropna(subset = ['drugname'], inplace = True)
medication['drugstartoffset'] = pd.to_numeric(medication['drugstartoffset'],errors='coerce')

# Load treatment into "treatment"
# Used for Vasopressors (3)
treatment = pd.read_csv("Delirium_eICU/delirium_treatment.csv")
treatment = treatment[treatment['patientunitstayid'].isin(pids['patientunitstayid'])]
treatment.dropna(subset = ['treatmentstring'], inplace = True)
treatment['treatmentoffset'] = pd.to_numeric(treatment['treatmentoffset'],errors='coerce')

# 23. Vasopressors into ""vasopressors1", "vasopressors2", and "vasopressors3"
vasopressors1 = infusionDrug[infusionDrug['drugname'].str.contains('dopamine|dobutamine|epinephrine|norepinephrine', case=False)]
vasopressors2 = medication[medication['drugname'].str.contains('dopamine|dobutamine|epinephrine|norepinephrine', case=False)]
vasopressors3 = treatment[treatment['treatmentstring'].str.contains('dopamine|dobutamine|epinephrine|norepinephrine', case=False)]

del infusionDrug, medication, treatment

pids['vasopressors'] = pids.parallel_apply(lambda row: result_vasopressors(row), axis=1)

del vasopressors1, vasopressors2, vasopressors3

# 24. df_vent_event into "ventilator"
ventilator = pd.read_csv("Delirium_eICU/df_vent_event.csv")
ventilator = ventilator[ventilator['patientunitstayid'].isin(pids['patientunitstayid'])]
ventilator = ventilator[ventilator['event'].str.contains('mechvent', case=False)]
ventilator['hrs'] = ventilator['hrs']*60
pids['ventilator'] = pids.parallel_apply(lambda row: result_ventilator(row), axis=1)
del ventilator

pids['sofa_resp'], pids['sofa_nervous'], pids['sofa_cardio'], pids['sofa_liver'], pids['sofa_coag'], pids['sofa_kidney'], pids['sofa_score'] = zip(*pids.parallel_apply(lambda row: SOFA_score(row), axis=1))
pids['qsofa_altered_mental'], pids['qsofa_resp_rate'], pids['qsofa_sys_bp'], pids['qsofa_score'] = zip(*pids.parallel_apply(lambda row: qSOFA_score(row), axis=1))
pids['suspected_sepsis'], pids['sepsis_lactate'], pids['sepsis_map'], pids['suspected_septic_shock'] = zip(*pids.parallel_apply(lambda row: sepsis(row), axis=1))

# Export to csv
pids.to_csv(out_filename)
