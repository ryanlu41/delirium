# -*- coding: utf-8 -*-
"""
Created on Tue Jul 12 00:25:04 2022

Pulls the labs in MIMIC IV that occurred before delirium only to save 
computation later. 
Sorts them by patientstayid and offset. Called by AllLabsBeforeDeliriumMIMIC.py.
Converts MIMIC data to work with our eICU lab code.
itemid/LABEL -> eICU lab name conversions created by Kirby, verified by Dr. Stevens.
Should end with 5 columns: 
    patientunitstayid,labresultoffset,labname,labresult,deliriumstart

No unit conversions needed.

Runtime for BUN: ~1 minute or less.

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
    mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')
    
    #%% Load in data.
    #Pulls list of Stay IDs.
    comp = pd.read_csv(dataset_path.joinpath("MIMICIV_complete_dataset.csv"),
                       usecols=['stay_id','subject_id', 'hadm_id',
                                'delirium_pos','intime', 'outtime', 
                                'del_onset', 'del_onset_time'],
                       parse_dates=['intime', 'outtime', 'del_onset_time'])
    
    # icu = pd.read_csv(mimic_path.joinpath('icu', 'icustays.csv.gz'),
    #                   usecols = ['hadm_id', 'stay_id', 'intime', 'outtime'],
    #                   parse_dates = ['intime', 'outtime'])
    
    # Pulls all lab info and drop columns/info we don't need.
    # lab = pd.read_csv(mimic_path.joinpath('hosp', "labevents.csv.gz"),
    #                   usecols=['hadm_id', 'itemid', 'charttime', 'valuenum'],
    #                   parse_dates=['charttime'])
    lab = pd.read_feather(mimic_path.joinpath('hosp', "labevents.ftr"))

    
    #Pulls lists of MIMIC and eICU labs, and which are equivalent. 
    labs_list = pd.read_excel("MIMICIV_to_eICU_Labs_List.xlsx")
    
    #Only keep labs from full possible list.
    lab = lab[lab['itemid'].isin(labs_list['itemid'])]
    # lab.reset_index(drop = True).to_feather(mimic_path.joinpath('hosp', 'labevents.ftr'))

    
    #%% Pre-processing.
    
    # Only keeps the hospital stays we want, and attach icustay info.
    lab = lab.merge(comp, on = 'hadm_id', how = 'inner')
    # lab = lab[lab['hadm_id'].isin(comp['hadm_id'])]

    # Only keeps the lab we want.
    labs_list = labs_list[labs_list['labname'] == lab_name]

    # Convert to eICU labnames.
    lab = lab.merge(labs_list, on = 'itemid', how = 'inner')
    
    # Just keep labs that happened in that ICU stay,
    lab = lab[lab['charttime'] >= lab['intime']]
    lab = lab[lab['charttime'] <= lab['outtime']]
    # and before delirium onset if there was delirium in that stay.
    lab = lab[~(lab['charttime'] > lab['del_onset_time'])]
    
    #Get labresultoffset.
    lab['labresultoffset'] = (lab['charttime'] - lab['intime']).dt.total_seconds()/60

    #Convert units if needed. Not needed.
    
    #%% Make it into the same format as eICU.
    
    #Rename and drop columns.
    lab.rename(columns={'stay_id':'patientunitstayid',
                        'del_onset':'deliriumstart',
                        'valuenum':'labresult'}, inplace=True)
    lab = lab[['patientunitstayid','labresultoffset','labname','labresult',
               'deliriumstart']]
    
    lab.sort_values(by=['patientunitstayid','labresultoffset'],inplace=True)
    
    return lab

#%% For testing purposes, when running with F5.
if __name__ == '__main__':
    #Change out this name for different labs. 
    #String for lab_name must exactly match the labname used in the Lab table of eICU.
    import time as time
    start = time.time()
    lab_name = 'bicarbonate_totalCO2_HCO3'
    test = labs_before_delirium(lab_name)
    test.to_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\First 24 Hour\Ext Validation 2\Labs\AllLabsBeforeDeliriumMIMICIV\\" + lab_name + ".csv",index=False)
    calc_time = time.time() - start