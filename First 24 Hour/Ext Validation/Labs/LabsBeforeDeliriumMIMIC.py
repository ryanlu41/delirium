# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 18:25:04 2020

Pulls the labs in MIMIC that occurred before delirium only. 
It uses csvs that were already filtered down to the patients in our
"MIMIC_complete_dataset.csv".
Sorts them by patientstayid and offset. Called by AllLabsBeforeDeliriumMIMIC.py.
Converts MIMIC data to work with our eICU lab code.
ITEMID/LABEL -> eICU lab name conversions created by Kirby, verified by Dr. Stevens.
Should end with 5 columns: patientunitstayid,labresultoffset,labname,labresult,deliriumstart

Runtime for BUN: ~6 minutes.

#TO DO: Do I need to do unit conversions?

@author: Kirby
"""


def labs_before_delirium(lab_name):
    #%% Package setup
    import numpy as np
    import pandas as pd
    from pathlib import Path
    
    file_path = Path(__file__)
    dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
    ext_valid_path = file_path.parent.parent.parent.joinpath('Ext Validation')
    mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iii-1.4')
    
    #%% Load in data.
    #Pulls list of Stay IDs.
    comp = pd.read_csv(dataset_path("MIMIC_complete_dataset.csv"),
                       usecols=['ICUSTAY_ID','SUBJECT_ID', 'HADM_ID',
                                'delirium_positive','INTIME','OUTTIME',
                                'del_onset'],
                       parse_dates=['INTIME','OUTTIME'])
    
    #Pulls all lab info and drop columns we don't need.
    lab = pd.read_csv(mimic_path.joinpath("LABEVENTS_delirium.csv"),
                      usecols=['SUBJECT_ID', 'HADM_ID', 'ITEMID', 'CHARTTIME',
                               'VALUE', 'VALUENUM', 'VALUEUOM'],
                      parse_dates=['CHARTTIME'])
    
    #Pulls lists of MIMIC and eICU labs, and which are equivalent. 
    labs_list = pd.read_excel("MIMIC_to_eICU_Labs_List.xlsx")
    
    #%% Pre-processing.
    
    #Only keep labs from full possible list.
    lab = lab[lab['ITEMID'].isin(labs_list['ITEMID'])]
    
    #Only keeps the hospital stays we want.
    lab = lab[lab['HADM_ID'].isin(comp['HADM_ID'])]

    #Convert to eICU labnames.
    lab_name_dict = labs_list.set_index('ITEMID').to_dict().get('labname')
    lab['labname'] = lab.apply(lambda row: lab_name_dict.get(row['ITEMID'],
                                                             np.nan),
                               axis=1)

    #Only keeps the lab we want.
    lab = lab[lab['labname']==lab_name]
    
    #Get ICUSTAY_ID for each lab based on HADM_ID and time stamps. 
    def get_ICU_stay(hadm,charttime):
        #Just get the ICU stays in the relevant HADM.
        temp = comp[comp['HADM_ID']==hadm][['ICUSTAY_ID','INTIME','OUTTIME']]
        #Check if the lab took place during any of the stays.
        temp['during_stay?'] = (charttime > temp['INTIME']) & \
            (charttime < temp['OUTTIME'])
        temp = temp[temp['during_stay?']==True]
        #If not, return nan.
        if temp.shape[0] == 0:
            return np.nan
        #If so, return the ICU stay ID.
        else:
            return temp.iloc[0,0]
    
    lab['ICUSTAY_ID'] = lab.apply(lambda row: get_ICU_stay(row['HADM_ID'],
                                                           row['CHARTTIME']),
                                  axis=1)
    
    #Drop rows missing ICUSTAY_ID, while attaching delirium onset times.
    lab = lab.merge(comp[['ICUSTAY_ID','INTIME','del_onset']],
                    on='ICUSTAY_ID', how='inner')
    
    #Get labresultoffset.
    lab['labresultoffset'] = (lab['CHARTTIME'] - lab['INTIME'])\
        .dt.total_seconds()/60

    #Convert units if needed.
    
    #Rename and drop columns.
    lab.rename(columns={'ICUSTAY_ID':'patientunitstayid',
                        'del_onset':'deliriumstart',
                        'VALUENUM':'labresult'},inplace=True)
    lab = lab[['patientunitstayid','labresultoffset','labname','labresult',
               'deliriumstart']]
    
    #Drop labs before delirium onset. Keep all of them if no delirium.
    lab = lab[(lab['labresultoffset'] < lab['deliriumstart']) | \
              (lab['deliriumstart'].isna()) ]
    lab.sort_values(by=['patientunitstayid','labresultoffset'],inplace=True)
    
    return lab

#%% For texting purposes, when running with F5.
if __name__ == '__main__':
    #Change out this name for different labs. 
    #String for lab_name must exactly match the labname used in the Lab table of eICU.
    import time as time
    start = time.time()
    lab_name = 'bicarbonate_totalCO2_HCO3'
    test = labs_before_delirium(lab_name)
    test.to_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\LabFeatures\AllLabsBeforeDeliriumMIMIC\\" + lab_name + ".csv",index=False)
    calc_time = time.time() - start