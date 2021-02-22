# Import packages
import sys
import numpy as np
import pandas as pd
import datetime
from pandarallel import pandarallel

# Declare filepaths
lead_time = sys.argv[1]
obs_time = sys.argv[2]
pids_file = f'Dynamic Data/Input/MIMIC_relative_{lead_time}hr_lead_{obs_time}hr_obs_data_set.csv'
lab_events_file = 'MIMIC_Data/LABEVENTS.csv'
input_events_cv_file = 'MIMIC_Data/INPUTEVENTS_CV.csv'
input_events_mv_file = 'MIMIC_Data/INPUTEVENTS_MV.csv'
output_events_file = 'MIMIC_Data/OUTPUTEVENTS.csv'
chart_events_file = 'MIMIC_Data/CHARTEVENTS.csv'
proc_events_file = 'MIMIC_Data/PROCEDUREEVENTS_MV.csv'
out_file = f'Dynamic Data/Output/MIMIC_relative_{lead_time}hr_lead_{obs_time}hr_obs_SOFA.csv'

# Read in patient IDs
pids = pd.read_csv(pids_file, parse_dates=['INTIME'])

# Only keep HADM_ID, ICUSTAY_ID, and time entered
pids_and_starts = pids[['HADM_ID', 'ICUSTAY_ID', 'INTIME']]

# Declare relevant ITEMIDS
lab_ids = [50885,50912,50813,51265,50821,50816]
input_cv_ids = [30047,30120,30044,30119,30309,30127,30128,30051,30043,30307,30125,30046]
input_mv_ids = [221906,221289,221749,222315,221662,227692]
output_ids = [40055,43175,40069,40094,40715,40473,40085,40057,40056,40405,40428,40086,40096,40651,\
              226559,226560,226561,226584,226563,226564,226565,226567,226557,226558,227489,227488]
ventilation_ids = [720,223849,223848,445,448,449,450,1340,1486,1600,224687,\
                   639,654,681,682,683,684,224685,224684,224686,\
                   218,436,535,444,224697,224695,224696,224746,224747,\
                   221,1,1211,1655,2000,226873,224738,224419,224750,227187,\
                   543,\
                   5865,5866,224707,224709,224705,224706,\
                   60,437,505,506,686,220339,224700,\
                   3459,\
                   501,502,503,224702,\
                   223,667,668,669,670,671,672,\
                   224701,\
                   226732,467,640]
chart_ids = ventilation_ids + [51,442,455,6701,220179,220050,\
                               456,52,6702,443,220052,220181,225312,\
                               615,618,220210,224690,\
                               3420,3422,190,223835,\
                               723,223900,454,223901,184,220739]
proc_ids = [227194,225468,225477]

# Define relevant functions
# Generate the result of variables found in chart_events
# Used for SBP, MBP, RESP, FiO2
def result_chart_events(row, feature):
    df = chart_events
    df = df[df['ICUSTAY_ID'] == row['ICUSTAY_ID']]
    df = df[(df['offset'] >= (row['end']-1440)) & (df['offset'] <= row['end'])]
    
    if feature == "sbp":
        ids = [51,442,455,6701,220179,220050]
        df = df[df['ITEMID'].isin(ids)]
        df = df[(df['VALUENUM'] > 0) & (df['VALUENUM'] < 400)]
    elif feature == "mbp":
        ids = [456,52,6702,443,220052,220181,225312]
        df = df[df['ITEMID'].isin(ids)]
        df = df[(df['VALUENUM'] > 0) & (df['VALUENUM'] < 300)]
    elif feature == "resp":
        ids = [615,618,220210,224690]
        df = df[df['ITEMID'].isin(ids)]
        df = df[(df['VALUENUM'] > 0) & (df['VALUENUM'] < 70)]
    elif feature == "fiO2":
        ids = [3420,3422,190,223835]
        df = df[df['ITEMID'].isin(ids)]
        cond1 = ((df['ITEMID'] == 190) & (df['VALUENUM'] > 0.2) & (df['VALUENUM'] < 1))
        cond2 = ((df['ITEMID'] == 223835) & (df['VALUENUM'] > 0) & (df['VALUENUM'] <= 1))
        cond3 = ((df['ITEMID'] == 223835) & (df['VALUENUM'] > 1) & (df['VALUENUM'] < 21))
        cond4 = ((df['ITEMID'] == 223835) & (df['VALUENUM'] > 100))
        df['VALUENUM'] = np.where(cond1,df['VALUENUM']*100,df['VALUENUM'])
        df['VALUENUM'] = np.where(cond2,df['VALUENUM']*100,df['VALUENUM'])
        df['VALUENUM'] = np.where(cond3,np.nan,df['VALUENUM'])
        df['VALUENUM'] = np.where(cond4,np.nan,df['VALUENUM'])
    else:
        return np.nan

    return np.amin(df.dropna(subset = ['VALUENUM'])['VALUENUM'])
     
# Generate the result of GCS
def result_gcs(row):
    df = chart_events
    df = df[df['ICUSTAY_ID'] == row['ICUSTAY_ID']]
    df = df[(df['offset'] >= (row['end']-1440)) & (df['offset'] <= row['end'])]
    df = df[df['ERROR'] == 0]
    
    verbal = df[df['ITEMID'].isin([723, 223900])]
    motor = df[df['ITEMID'].isin([454, 223901])]
    eyes = df[df['ITEMID'].isin([184, 220739])]
    
    min_verbal = np.amin(verbal.dropna(subset = ['VALUENUM'])['VALUENUM'])
    min_motor = np.amin(motor.dropna(subset = ['VALUENUM'])['VALUENUM'])
    min_eyes = np.amin(eyes.dropna(subset = ['VALUENUM'])['VALUENUM'])
    
    return min_verbal + min_motor + min_eyes
    
# Generate the result of variables found in lab_events
# Used for PO2, FiO2, Bilirubin, Platelets, Creatinine, Lactate
def result_lab_events(row, feature):
    df = lab_events
    df = df[df['ICUSTAY_ID'] == row['ICUSTAY_ID']]
    df = df[(df['offset'] >= (row['end']-1440)) & (df['offset'] <= row['end'])]
    
    if feature == "bilirubin":
        df = df[df['ITEMID'] == 50885]
        df = df[(df['VALUENUM'] > 0) & (df['VALUENUM'] < 150)]
    elif feature == "creatinine":
        df = df[df['ITEMID'] == 50912]
        df = df[(df['VALUENUM'] > 0) & (df['VALUENUM'] <150)]
    elif feature == "lactate":
        df = df[df['ITEMID'] == 50813]
        df = df[(df['VALUENUM'] > 0) & (df['VALUENUM'] < 50)]
    elif feature == "platelets":
        df = df[df['ITEMID'] == 51265]
        df = df[(df['VALUENUM'] > 0) & (df['VALUENUM'] < 10000)]
    elif feature == "pO2":
        df = df[df['ITEMID'] == 50821]
        df = df[(df['VALUENUM'] > 0) & (df['VALUENUM'] < 800)]
    elif feature == "fiO2":
        df = df[df['ITEMID'] == 50816]
        df = df[(df['VALUENUM'] > 20) & (df['VALUENUM'] < 100)]
    
    return np.amin(df.dropna(subset = ['VALUENUM'])['VALUENUM'])

# Generate the result of vasopressors
def result_vasopressors(row):
    df1 = input_events_cv
    df2 = input_events_mv
    
    df1 = df1[df1['ICUSTAY_ID'] == row['ICUSTAY_ID']]
    df1 = df1[(df1['offset'] >= (row['end']-1440)) & (df1['offset'] <= row['end'])]

    df2 = df2[df2['ICUSTAY_ID'] == row['ICUSTAY_ID']]
    df2 = df2[(df2['offset'] >= (row['end']-1440)) & (df2['offset'] <= row['end'])]
    df2 = df2[df2['STATUSDESCRIPTION'] != 'Rewritten']
    
    return ((len(df1)+len(df2)) > 0)

# Generate the result of urine
def result_urine(row):
    df = output_events
    df = df[df['ICUSTAY_ID'] == row['ICUSTAY_ID']]
    df = df[(df['offset'] >= (row['end']-1440)) & (df['offset'] <= row['end'])]
    
    pos_df = df[df['ITEMID'] != 227488]
    neg_df = df[df['ITEMID'] == 227488]
    
    return (pos_df['VALUE'].sum() - neg_df['VALUE'].sum())

# Generate the result of ventilator
def result_ventilator():
    end_mech_ids = [226732,467,640]
    mech_ids = [i for i in ventilation_ids if i not in end_mech_ids]
    
    oxy_vals = ["Nasal cannula","Face tent","Aerosol-cool","Trach mask",\
                "High flow neb","Non-rebreather","Venti mask","Medium conc mask",\
                "T-piece","High flow nasal cannula","Ultrasonic neb","Vapomist"]
    
    mech = ventilation_events[ventilation_events['ITEMID'].isin(mech_ids)]
    end_mech = ventilation_events[ventilation_events['ITEMID'].isin(end_mech_ids)]

    proc = proc_events.copy()
    proc.rename(columns={'STARTTIME':'CHARTTIME'},inplace=True)

    # Get unique ventilation times
    mech = mech[~((mech['ITEMID']==223848) & (mech['VALUE']=='Other'))]
    mech = mech[['ICUSTAY_ID','CHARTTIME']]
    mech['mech'] = 1
    mech.drop_duplicates(inplace=True)

    # Get unique oxygen therapy/extubation times
    end_mech = end_mech[((end_mech['ITEMID']==226732) & (end_mech['VALUE'].isin(oxy_vals)))]
    end_mech = pd.concat([end_mech,proc])
    end_mech = end_mech[['ICUSTAY_ID','CHARTTIME']]
    end_mech['mech'] = 0
    end_mech.drop_duplicates(inplace=True)
    
    # Combine and find changes
    all_mech = pd.concat([mech,end_mech])
    all_mech.sort_values(['ICUSTAY_ID','CHARTTIME'],ascending=True,inplace=True)

    all_mech['last_mech'] = all_mech['mech'].shift(periods=1)
    all_mech['last_id'] = all_mech['ICUSTAY_ID'].shift(periods=1)
    all_mech['keep'] = ((all_mech['last_id'] != all_mech['ICUSTAY_ID']) |
                        (all_mech['last_mech'] != all_mech['mech']))
    all_mech = all_mech[all_mech['keep']==True]

    all_mech['mech_start'] = all_mech['CHARTTIME'].shift(periods=1)
    all_mech['keep'] = ((all_mech['last_mech']==1) & (all_mech['mech']==0) & 
                        (all_mech['last_id']==all_mech['ICUSTAY_ID']))
    all_mech = all_mech[all_mech['keep']]
    all_mech.rename(columns={'CHARTTIME':'mech_end'},inplace=True)

    intimes = pids_and_starts[['ICUSTAY_ID','INTIME']]
    all_mech = all_mech.merge(intimes,on='ICUSTAY_ID',how='left')
    all_mech['start_offset'] = (all_mech['mech_start'] - 
                                all_mech['INTIME']).dt.total_seconds()/60
    all_mech['end_offset'] = (all_mech['mech_end'] - 
                                all_mech['INTIME']).dt.total_seconds()/60

    lookup = pids.set_index('ICUSTAY_ID')
    def keep_row(current_ID,start_offset,end_offset):
        window_start = lookup.loc[current_ID,'start']
        window_end = lookup.loc[current_ID,'end']
        if (start_offset <= window_end):
            if (end_offset <= window_start):
                return False
            else:
                return True
        else:
            return False
    
    all_mech['keep'] = all_mech.apply(lambda row: keep_row(
        row['ICUSTAY_ID'],row['start_offset'],row['end_offset']), axis=1)
    all_mech = all_mech[all_mech['keep']==True]

    pids['ventilator'] = pids['ICUSTAY_ID'].isin(all_mech['ICUSTAY_ID']).astype(int)

# Generate SOFA score and component features
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

# Initialization
pandarallel.initialize(progress_bar = False)

# Read in relevant data files and generate associated features
# Chart events
chart_events = pd.read_csv(chart_events_file, parse_dates=['CHARTTIME'],
                           usecols=['ICUSTAY_ID','ITEMID','CHARTTIME','VALUE','VALUENUM','ERROR'])
chart_events = chart_events.merge(pids_and_starts, on='ICUSTAY_ID')
chart_events = chart_events[chart_events['ITEMID'].isin(chart_ids)]
chart_events['CHARTTIME'] = pd.to_datetime(chart_events['CHARTTIME'])
chart_events['INTIME'] = pd.to_datetime(chart_events['INTIME'])
chart_events['offset'] = (chart_events['CHARTTIME'] - chart_events['INTIME']).dt.total_seconds()/60
ventilation_events = chart_events[chart_events['ITEMID'].isin(ventilation_ids)]

pids['sbp'] = pids.parallel_apply(lambda row: result_chart_events(row, 'sbp'), axis=1)
pids['mbp'] = pids.parallel_apply(lambda row: result_chart_events(row, 'mbp'), axis=1)
pids['resp'] = pids.parallel_apply(lambda row: result_chart_events(row, 'resp'), axis=1)
pids['fiO2_chart'] = pids.parallel_apply(lambda row: result_chart_events(row, 'fiO2'), axis=1)
pids['gcs'] = pids.parallel_apply(lambda row: result_gcs(row), axis=1)
del chart_events

# Lab events
lab_events = pd.read_csv(lab_events_file, parse_dates=['CHARTTIME'], 
                         usecols=['HADM_ID','ITEMID','CHARTTIME','VALUENUM'])
lab_events = lab_events.merge(pids_and_starts, on='HADM_ID')
lab_events = lab_events[lab_events['ITEMID'].isin(lab_ids)]
lab_events['offset'] = (lab_events['CHARTTIME'] - lab_events['INTIME']).dt.total_seconds()/60

pids['bilirubin'] = pids.parallel_apply(lambda row: result_lab_events(row, 'bilirubin'), axis=1)
pids['creatinine'] = pids.parallel_apply(lambda row: result_lab_events(row, 'creatinine'), axis=1)
pids['lactate'] = pids.parallel_apply(lambda row: result_lab_events(row, 'lactate'), axis=1)
pids['platelets'] = pids.parallel_apply(lambda row: result_lab_events(row, 'platelets'), axis=1)
pids['pO2'] = pids.parallel_apply(lambda row: result_lab_events(row, 'pO2'), axis=1)
pids['fiO2_lab'] = pids.parallel_apply(lambda row: result_lab_events(row, 'fiO2'), axis=1)
del lab_events

# Input events CareVue/Metavision
input_events_cv = pd.read_csv(input_events_cv_file, parse_dates=['CHARTTIME'], 
                              usecols=['ICUSTAY_ID','ITEMID','CHARTTIME','RATE'])
input_events_cv = input_events_cv.merge(pids_and_starts, on='ICUSTAY_ID')
input_events_cv = input_events_cv[input_events_cv['ITEMID'].isin(input_cv_ids)]
input_events_cv = input_events_cv[input_events_cv['RATE'] > 0]
input_events_cv['offset'] = (input_events_cv['CHARTTIME'] - input_events_cv['INTIME']).dt.total_seconds()/60

input_events_mv = pd.read_csv(input_events_mv_file, parse_dates=['STARTTIME'], 
                              usecols=['ICUSTAY_ID','ITEMID','STARTTIME','RATE','STATUSDESCRIPTION'])
input_events_mv = input_events_mv.merge(pids_and_starts, on='ICUSTAY_ID')
input_events_mv = input_events_mv[input_events_mv['ITEMID'].isin(input_mv_ids)]
input_events_mv = input_events_mv[input_events_mv['RATE'] > 0]
input_events_mv['offset'] = (input_events_mv['STARTTIME'] - input_events_mv['INTIME']).dt.total_seconds()/60

pids['vasopressors'] = pids.parallel_apply(lambda row: result_vasopressors(row), axis=1)
del input_events_cv, input_events_mv

# Output events
output_events = pd.read_csv(output_events_file, parse_dates=['CHARTTIME'],
                            usecols=['ICUSTAY_ID','ITEMID','CHARTTIME','VALUE'])
output_events = output_events.merge(pids_and_starts, on='ICUSTAY_ID')
output_events['offset'] = (output_events['CHARTTIME'] - output_events['INTIME']).dt.total_seconds()/60

pids['urine'] = pids.parallel_apply(lambda row: result_urine(row), axis=1)
del output_events

# Procedure events Metavision
proc_events = pd.read_csv(proc_events_file, parse_dates=['STARTTIME'],
                          usecols = ['ICUSTAY_ID','STARTTIME','ITEMID'])
proc_events = proc_events.merge(pids_and_starts, on='ICUSTAY_ID')
proc_events = proc_events[proc_events['ITEMID'].isin(proc_ids)]
proc_events['offset'] = (proc_events['STARTTIME'] - proc_events['INTIME']).dt.total_seconds()/60

result_ventilator()
del proc_events, ventilation_events

# Consolidate fiO2
pids['fiO2'] = pids.apply(lambda row: min(row['fiO2_chart'],row['fiO2_lab']), axis=1)
pids.drop(columns=['fiO2_chart', 'fiO2_lab'], inplace = True)

# Calculate scores
pids['sofa_resp'], pids['sofa_nervous'], pids['sofa_cardio'], pids['sofa_liver'], pids['sofa_coag'], pids['sofa_kidney'], pids['sofa_score'] = zip(*pids.parallel_apply(lambda row: SOFA_score(row), axis=1))
pids['qsofa_altered_mental'], pids['qsofa_resp_rate'], pids['qsofa_sys_bp'], pids['qsofa_score'] = zip(*pids.parallel_apply(lambda row: qSOFA_score(row), axis=1))
pids['suspected_sepsis'], pids['sepsis_lactate'], pids['sepsis_map'], pids['suspected_septic_shock'] = zip(*pids.parallel_apply(lambda row: sepsis(row), axis=1))

# Export to csv
pids.to_csv(out_file)