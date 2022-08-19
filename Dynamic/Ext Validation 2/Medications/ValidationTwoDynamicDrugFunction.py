# -*- coding: utf-8 -*-
"""
Created on Fri 29 Jul 00:48:36 2022

Pulling medication data from MIMIC-IV. 
Looking in prescriptions, inputevents, pharmacy, and ingredient events.

Feature indicates if the drug was currently being given to the patient at 
time of prediction.

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
    mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')


    #%% Pull relevant info
    items = pd.read_csv(mimic_path.joinpath('icu', "d_items.csv.gz"),
                        usecols=['itemid','label'])
    
    #Pull patient cohort IDs. 
    comp = pd.read_csv(
        dataset_path.joinpath('MIMICIV_relative_'+ str(lead_hours) +'hr_lead_' + 
                              str(obs_hours) + 'hr_obs_data_set.csv'),
                       usecols=['hadm_id', 'stay_id','intime', 'start', 'end'],
                       parse_dates=['intime'])
    
    #Import lists of drug names to search for, and make it all lowercase.
    #Change this part to get different drug features.
    drug = pd.read_csv(drugSearchListPath)
    drug = drug.applymap(lambda s:s.lower() if type(s) == str else s)
    druglist = drug.values.astype(str).tolist()
    druglist = [item.lower() for sublist in druglist for item in sublist]
    
    # Pull prescriptions.
    # All prescriptions have drug name filled out, so using that.
    presc = pd.read_csv(mimic_path.joinpath('hosp', "prescriptions.csv.gz"),
                        usecols = ['hadm_id', 'starttime', 'stoptime', 'drug'],
                        parse_dates = ['starttime', 'stoptime'])
    
    # Pull pharmacy information. 
    # Status is mostly ended, since most pharmacy records were closed.
    pharm = pd.read_csv(mimic_path.joinpath('hosp', 'pharmacy.csv.gz'),
                        usecols = ['hadm_id', 'starttime', 'stoptime', 'medication'],
                        parse_dates = ['starttime', 'stoptime'])
    
    # Pull inputs, requires joining with itemids for drug names.
    inputs = pd.read_csv(mimic_path.joinpath('icu', 'inputevents.csv.gz'),
                         usecols = ['stay_id', 'starttime', 'endtime', 'itemid'],
                         parse_dates = ['starttime', 'endtime'])
    
    # Pull emar information. Ties to pharmacy table with pharmacy_id.
    # It's not useful information, doesn't list given times.
    # emar = pd.read_csv(mimic_path.joinpath('hosp', 'emar.csv.gz'),
    #                    usecols = ['hadm_id', 'pharmacy_id', 'medication', 
    #                               'event_txt'])
    
    # Pull emar information. Ties to pharmacy table with pharmacy_id.
    # It's not useful information, doesn't list given times.
    # emar_det = pd.read_csv(mimic_path.joinpath('hosp', 'emar_detail.csv.gz'),
    #                        usecols = ['subject_id', 'product_code', 
    #                                   'product_description',
    #                                   'product_description_other', 
    #                                   'dose_given'])
    
    # Pull ingredient event information, need to join with itemids to get drug names.
    ingred = pd.read_csv(mimic_path.joinpath('icu', 'ingredientevents.csv.gz'),
                         usecols = ['stay_id', 'starttime', 'endtime', 'itemid'],
                         parse_dates = ['starttime', 'endtime'])
    
    # Don't need poe, it's just physician order entry.
    # poe = pd.read_csv(mimic_path.joinpath('hosp', 'poe.csv.gz'), nrows = 0,
    #                   usecols = ['hadm_id', 'order_time'])
    
    #%% Only keep rows with relevant patients.
    presc = presc[presc['hadm_id'].isin(comp['hadm_id'])]
    pharm = pharm[pharm['hadm_id'].isin(comp['hadm_id'])]
    inputs = inputs[inputs['stay_id'].isin(comp['stay_id'])]
    ingred = ingred[ingred['stay_id'].isin(comp['stay_id'])]
    
    #%% Only keep rows with relevant drugs.
    
    # Find terms in D_ITEMS to use.
    items = items.applymap(lambda s:s.lower() if type(s) == str else s)
    relevant_items = items[items['label'].isin(druglist)]
    
    # Use those items to get the relevant rows from the two input and ingredient event tables
    inputs = inputs[inputs['itemid'].isin(relevant_items['itemid'])]
    ingred = ingred[ingred['itemid'].isin(relevant_items['itemid'])]
        
    #Check prescriptions and pharmacy tables.
    presc = presc.applymap(lambda s:s.lower() if type(s) == str else s)
    presc = presc[presc['drug'].str.contains('|'.join(druglist), na=False)]
    pharm = pharm.applymap(lambda s:s.lower() if type(s) == str else s)
    pharm = pharm[pharm['medication'].str.contains('|'.join(druglist), na=False)]
    
    #%% Convert chart times to offsets. 
    
    # Merge intime onto each medication table.
    intimes = comp[['stay_id', 'intime']]
    inputs = inputs.merge(intimes, on = 'stay_id', how = 'left')
    ingred = ingred.merge(intimes, on = 'stay_id', how = 'left')
    
    intimes = comp[['hadm_id', 'stay_id', 'intime']]
    presc = presc.merge(intimes, on = 'hadm_id', how = 'left')
    pharm = pharm.merge(intimes, on = 'hadm_id', how = 'left')
    
    # starttimes cannot be handled by read_csv's date parser for pharm.
    # Coerce it to datetimes. 
    pharm['starttime'] = pd.to_datetime(pharm['starttime'], errors = 'coerce')
    
    # Subtract that row's time from admit time, make it minutes. 
    def make_offsets(df, start_col, end_col):
        df['startoffset'] = df[start_col] - df['intime']
        df['startoffset'] = df['startoffset'].dt.total_seconds()/60
        df['endoffset'] = df[end_col] - df['intime']
        df['endoffset'] = df['endoffset'].dt.total_seconds()/60
        return df
    
    inputs = make_offsets(inputs, 'starttime', 'endtime')
    ingred = make_offsets(ingred, 'starttime', 'endtime')
    presc = make_offsets(presc, 'starttime', 'stoptime')
    pharm = make_offsets(pharm, 'starttime', 'stoptime')

    #%% Put the tables together.
    
    # Just get relevant columns.
    def rel_columns(df):
        return df[['stay_id','startoffset','endoffset']]
    
    inputs = rel_columns(inputs)
    ingred = rel_columns(ingred)
    pharm = rel_columns(pharm)
    presc = rel_columns(presc)
    
    comp_data = pd.concat([inputs, ingred, pharm, presc])
    
    #%% Generate the feature.
    
    # Tack on relevant time frames.
    windows = comp[['stay_id', 'start', 'end']]
    comp_data = comp_data.merge(windows, on = 'stay_id', how = 'inner')
    
    # Only keep the rows that happened in the relevant time frame.
    # Drug must have been given before time of 
    comp_data = comp_data[((comp_data['startoffset'] <= comp_data['end']) & 
                          (comp_data['endoffset'].isna())) |
                          ((comp_data['startoffset'] <= comp_data['end']) & 
                          (comp_data['endoffset'] >= comp_data['end']))]
    
    #Get the stay_ids that have rows.
    comp_data = comp_data['stay_id'].drop_duplicates()

    #Make feature
    new_col_name = 'relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) +   \
    'hr_obs' + drug.columns.values[0]
    comp[new_col_name] = comp['stay_id'].isin(comp_data)
    
    return comp[new_col_name].astype(int)

#%% Testing
import time
drugSearchListPath='DrugNameLists\\Vasopressors.csv'
lead_hours = 1
obs_hours = 1
start = time.time()
test = DynamicDrugFeature(drugSearchListPath,lead_hours,obs_hours)
calc_time = time.time() - start