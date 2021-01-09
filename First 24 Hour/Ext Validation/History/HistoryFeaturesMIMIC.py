# -*- coding: utf-8 -*-
"""
Created on Mon Aug 31 13:51:36 2020

Use this code to search for patient history info from the NOTEEVENTS, 
looking at Discharge Summaries. 

Run time: 10 seconds

@author: Kirby
"""
#%% Setup
import pandas as pd
import numpy as np
from pathlib import Path
import time

start = time.time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
ext_valid_path = file_path.parent.parent.parent.joinpath('Ext Validation')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iii-1.4')

#%% Load in data.

comp = pd.read_csv(dataset_path.joinpath("MIMIC_complete_dataset.csv"))

# items = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\D_ITEMS.csv",usecols=['ITEMID','LABEL'])

# #Trying terms
# history = items[items['LABEL'].str.contains('history',na=False)]

# #Pulling chart events with the relevant IDs found above. 
# history_rows = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\CHARTEVENTS_delirium.csv",nrows=0)
# for chunk in pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\CHARTEVENTS_delirium.csv",chunksize=1000000):
#     #start = time.time()
#     temp_rows = chunk[chunk['ITEMID'].isin(history['ITEMID'])]
#     history_rows = pd.concat([history_rows,temp_rows])
#     #calc_time = time.time() - start
    
# unique_history = history_rows['VALUE'].drop_duplicates().sort_values()

#Pulling note info.
notes = pd.read_csv(mimic_path.joinpath("NOTEEVENTS_delirium.csv"),
                    usecols=['HADM_ID','CHARTTIME','CATEGORY','DESCRIPTION',
                             'ISERROR','TEXT'])

notes = notes[~(notes['ISERROR']==1)]

#Pulling strings to search for.
search_strs = pd.read_excel('HistoryKeywordsMIMIC.xlsx')

#%% Get history section of discharge summary notes.

#Just discharge summary notes.
disch = notes[notes['CATEGORY']=='Discharge summary']

#Just notes from patients we care about.
disch = disch[disch['HADM_ID'].isin(comp['HADM_ID'])]

#Get just the Past Medical History section of the notes.
hist = disch[['HADM_ID','TEXT']]
hist_temp = hist['TEXT'].str.split(pat='Past Medical History:',expand=True)
hist.loc[:,'TEXT'] = hist_temp[1]
hist_temp = hist['TEXT'].str.split(pat='Social History:',expand=True)
hist.loc[:,'TEXT'] = hist_temp[0]

#Make it all lowercase.
hist = hist.applymap(lambda s:s.lower() if type(s) == str else s)

hist.dropna(inplace=True)


#%% Search history sections of notes for relevant keywords. 

#Get lists of keywords and associated history feature. 
col_names = list(search_strs['Column Name'])
keywords_list = list(search_strs['Keywords/Phrases'])

def find_history(note_text,keywords):
    #Look through the text for each keyword.
    for keyword in keywords:
        if note_text.count(keyword)>0:
            return 1
        else:
            continue
    #If you don't find it, return 0.
    return 0

#For each history feature, search text for relevant keywords.
for num in range(0,len(keywords_list)):
    col_name = col_names[num]
    keywords = keywords_list[num]
    keywords = keywords.lower()
    keywords = keywords.split(',')
    
    #Search for them for the note for each patient.
    hist.loc[:,col_name] = hist.apply(
        lambda row: find_history(row['TEXT'],keywords),axis=1)
    
#Handle HADM_IDs with multiple discharge summary notes.
hist.drop(columns='TEXT',inplace=True)
#Assume if they had the history in one note, they had it for the whole stay.
hist = hist.groupby('HADM_ID').max().reset_index()

#%% Save off results.
comp = comp.merge(hist,on='HADM_ID',how='left')

comp.to_csv('history_features_MIMIC.csv',index=False)

#Performance testing.
calc_time = time.time() - start

#Get proportions of history for sanity checking.
prop = pd.DataFrame(columns=['proportion'])
for col_name in col_names:
    if col_name == 'HistHeartTransp':
        prop.loc[col_name,'proportion'] = 0
        continue
    prop.loc[col_name,'proportion'] = \
        round(hist[col_name].value_counts().loc[1]/hist.shape[0],3)

prop.to_csv('history_feature_proportions.csv',index=True)