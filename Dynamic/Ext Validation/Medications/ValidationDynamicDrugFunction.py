# -*- coding: utf-8 -*-
"""
Created on Wed Sep  2 13:29:25 2020

Pulling medication data from MIMIC-III. 
Looking in prescriptions, INPUTEVENTS (CV and MV)

#Runtime: ~30 seconds.

@author: Kirby
"""

def DynamicDrugFeature(drugSearchListPath,lead_hours,obs_hours):

    #%% Setup
    import pandas as pd
    import numpy as np
    from pathlib import Path
    
    file_path = Path(__file__)
    dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
    mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iii-1.4')


    #%% Pull relevant info
    items = pd.read_csv(mimic_path.joinpath("D_ITEMS.csv"),
                        usecols=['ITEMID','LABEL'])
    #icu = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\ICUSTAYS.csv",usecols=['SUBJECT_ID','HADM_ID','ICUSTAY_ID','INTIME'],parse_dates=['INTIME'])
    
    #Pull patient cohort IDs. 
    comp = pd.read_csv(
        dataset_path.joinpath('MIMIC_relative_'+ str(lead_hours) +'hr_lead_' + 
                              str(obs_hours) + 'hr_obs_data_set.csv'),
                       usecols=['ICUSTAY_ID','delirium_positive','INTIME',
                                'del_onset','start','end'],
                       parse_dates=['INTIME'])
    
    #Import lists of drug names to search for, and make it all lowercase.
    #Change this part to get different drug features.
    drug=pd.read_csv(drugSearchListPath)
    drug = drug.applymap(lambda s:s.lower() if type(s) == str else s)
    druglist = drug.values.astype(str).tolist()
    druglist = [item.lower() for sublist in druglist for item in sublist]
    
    #Pull prescriptions, input data that has already been filtered by our relevant patients.
    presc = pd.read_csv(mimic_path.joinpath("PRESCRIPTIONS_delirium.csv"),
                        usecols=['ICUSTAY_ID', 'STARTDATE', 'ENDDATE','DRUG'],
                        parse_dates=['STARTDATE', 'ENDDATE'])
    input_cv = pd.read_csv(mimic_path.joinpath("INPUTEVENTS_CV_delirium.csv"),
                           usecols=['ICUSTAY_ID', 'CHARTTIME', 'ITEMID',
                                    'STOPPED'],
                           parse_dates=['CHARTTIME'])
    input_mv = pd.read_csv(mimic_path.joinpath("INPUTEVENTS_MV_delirium.csv"),
                           usecols=['ICUSTAY_ID', 'STARTTIME', 'ENDTIME',
                                    'ITEMID','STATUSDESCRIPTION'],
                           parse_dates=['STARTTIME', 'ENDTIME'])
    
    #%% Exploring PRESCRIPTIONS. 
    #Drug column is always filled out. Other data less consistent, but no need for them.
    
    # unique_presc = presc[['DRUG_TYPE', 'DRUG', 'DRUG_NAME_POE', 'DRUG_NAME_GENERIC',
    #        'FORMULARY_DRUG_CD', 'GSN', 'NDC']]
    # unique_presc.drop_duplicates(inplace=True)
    
    #%% Only keep rows with relevant patients.
    #icu = icu[icu['ICUSTAY_ID'].isin(comp['ICUSTAY_ID'])]
    presc = presc[presc['ICUSTAY_ID'].isin(comp['ICUSTAY_ID'])]
    input_cv = input_cv[input_cv['ICUSTAY_ID'].isin(comp['ICUSTAY_ID'])]
    input_mv = input_mv[input_mv['ICUSTAY_ID'].isin(comp['ICUSTAY_ID'])]
    
    #input_cv ends up with 0 rows, so we can exclude it from future work here.
    
    #%% Only keep rows with relevant drugs.
    
    #Find terms in D_ITEMS to use.
    items = items.applymap(lambda s:s.lower() if type(s) == str else s)
    relevant_items = items[items['LABEL'].isin(druglist)]
    #Use those items to get the relevant rows from the two INPUT tables
    #input_cv = input_cv[input_cv['ITEMID'].isin(relevant_items['ITEMID'])]
    input_mv = input_mv[input_mv['ITEMID'].isin(relevant_items['ITEMID'])]
    
    #Check CHARTEVENTS
    # chartevents_rows = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\CHARTEVENTS.csv",nrows=0)
    # for chunk in pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\CHARTEVENTS.csv",chunksize=1000000):
    #     #start = time.time()
    #     temp_rows = chunk[chunk['ITEMID'].isin(relevant_items['ITEMID'])]
    #     chartevents_rows = pd.concat([chartevents_rows,temp_rows])
    #CHARTEVENTS does have some medication data, but it's sparse and very messy. (1/1000th of the rest of data)
    
    #Check PRESCRIPTIONS
    presc = presc.applymap(lambda s:s.lower() if type(s) == str else s)
    presc = presc[presc['DRUG'].str.contains('|'.join(druglist),na=False)]
    
    #%% Convert chart times to offsets. 
    
    #Merge INTIME onto each medication table.
    input_mv = input_mv.merge(comp,on='ICUSTAY_ID',how='left')
    presc = presc.merge(comp,on='ICUSTAY_ID',how='left')
    
    #Subtract that row's time from admit time, make it minutes. 
    input_mv['startoffset'] = input_mv['STARTTIME'] - input_mv['INTIME']
    input_mv['startoffset'] = input_mv['startoffset'].dt.total_seconds()/60
    input_mv['endoffset'] = input_mv['ENDTIME'] - input_mv['INTIME']
    input_mv['endoffset'] = input_mv['endoffset'].dt.total_seconds()/60
    
    presc['startoffset'] = presc['STARTDATE'] - presc['INTIME']
    presc['startoffset'] = presc['startoffset'].dt.total_seconds()/60
    presc['endoffset'] = presc['ENDDATE'] - presc['INTIME']
    presc['endoffset'] = presc['endoffset'].dt.total_seconds()/60
    
    #Put the tables together.
    input_mv = input_mv[['ICUSTAY_ID','startoffset','endoffset']]
    presc = presc[['ICUSTAY_ID','startoffset','endoffset']]
    comp_data = pd.concat([input_mv,presc])
    
    #%% Generate the feature.
    #Only keep the rows before/during the observation window.
    window_lookup = comp.set_index('ICUSTAY_ID')
    def keep_row(current_ID,drug_start,drug_stop):
        window_start = window_lookup.loc[current_ID,'start']
        window_end = window_lookup.loc[current_ID,'end']
        #if drug was given before window, and didn't end, keep it.
        if (drug_start < window_end) & (np.isnan(drug_stop)):
            return 1 
        #If drug was given before window, and stopped after window, keep it. 
        if (drug_start < window_end) & (drug_stop > window_end):
            return 1
        else:
            return 0
    
    if comp_data.shape[0] != 0:
        comp_data['keep'] = comp_data.apply(lambda row: keep_row(
            row['ICUSTAY_ID'],row['startoffset'],row['endoffset']),
            axis=1)
        comp_data = comp_data[comp_data['keep'] == 1]
    
    #Get the ICUSTAY_IDs that have rows.
    comp_data = comp_data['ICUSTAY_ID'].drop_duplicates()
    
    #Make feature
    new_col_name = 'relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) +   \
    'hr_obs' + drug.columns.values[0]
    comp[new_col_name] = comp['ICUSTAY_ID'].isin(comp_data)
    
    return comp[new_col_name]

#%% Testing
import time
drugSearchListPath='DrugNameLists\\Vasopressors.csv'
lead_hours = 1
obs_hours = 1
start = time.time()
test = DynamicDrugFeature(drugSearchListPath,lead_hours,obs_hours)
calc_time = time.time() - start