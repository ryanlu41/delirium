# -*- coding: utf-8 -*-
"""
Created on Wed May 27 12:50:59 2020

This code is meant to explore and find any information about delirium testing/
diagnoses that is present in MIMIC III. Saves off any rows with delirium 
testing information from CHARTEVENTS into a csv.

@author: Kirby
"""

#Looking in CHARTEVENTS
import pandas as pd
import numpy as np
import time

#Explore different chart event item IDs
items = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\D_ITEMS.csv",usecols=['ITEMID','LABEL'])

#Finding relevant labels
#Make it all lower case.
items = items.applymap(lambda s:s.lower() if type(s) == str else s)
#Find different terms.
cam_ids = items[items['LABEL'].str.contains('cam-icu',na=False)]
delirium_ids = items[items['LABEL'].str.contains('delirium',na=False)]
#icdsc_ids = items[items['LABEL'].str.contains('icdsc',na=False)]
#delirious_ids = items[items['LABEL'].str.contains('delirious',na=False)]
#confusion_ids = items[items['LABEL'].str.contains('confusion',na=False)]
#mental_ids = items[items['LABEL'].str.contains('mental status',na=False)]
relevant_ids = pd.concat([cam_ids,delirium_ids])

#Pulling chart events with the relevant IDs found above. 
delirium_testing_rows = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\CHARTEVENTS.csv",
                                    nrows=0)
for chunk in pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\CHARTEVENTS.csv",
                         chunksize=1000000):
    #start = time.time()
    temp_rows = chunk[chunk['ITEMID'].isin(relevant_ids['ITEMID'])]
    delirium_testing_rows = pd.concat([delirium_testing_rows,temp_rows])
    #calc_time = time.time() - start
    
#should get about ~119k rows. Take a few minutes.

#Figuring out if mental status is actually relevant. 
#It's not. only values are Oriented to own ability and Forgets limitations
# all_ms_rows = delirium_testing_rows[delirium_testing_rows['ITEMID']==227346]
# mental_status_values = all_ms_rows[['VALUE']]
# mental_status_values.drop_duplicates(inplace=True)

#Figure out all the values for the different delirium testing rows.
# del_values = delirium_testing_rows[['ITEMID','VALUE']]
# del_values = del_values.merge(relevant_ids,how='left',on='ITEMID')
# del_values.drop_duplicates(inplace=True)

#Figure out if I need the data on parts of CAM-ICU, or if each patient has a 
#row for delirium assessment as well. 
del_values = delirium_testing_rows[['SUBJECT_ID','CHARTTIME','LABEL','VALUE']]
del_values.sort_values(['SUBJECT_ID','CHARTTIME'],inplace=True)
#get unique subjects/time stamps for delirium assessments
assessments = del_values[del_values['LABEL']=='delirium assessment']
assessment_times = assessments[['SUBJECT_ID','CHARTTIME']]
assessment_times.drop_duplicates(inplace=True)
#get unique subjects/time stamps for parts
parts = del_values[del_values['LABEL']!='delirium assessment']
parts_times = parts[['SUBJECT_ID','CHARTTIME']]
parts_times.drop_duplicates(inplace=True)
#Find where they don't over lap.
non_overlap = parts_times.merge(assessment_times,how='outer',
                                on=['SUBJECT_ID','CHARTTIME'],indicator=True)
non_overlap = non_overlap[non_overlap['_merge']!='both']
#There's pretty significant amount of non overlap. 

#I'll need to code to examine delirium assessments and the parts of CAMICU separately.
#Going to do that in a new file. 
#See GetChartEventsDeliriumLabels.py

#Add labels and save off info.
delirium_testing_rows = delirium_testing_rows.merge(relevant_ids,how='left',
                                                    on='ITEMID')
delirium_testing_rows.to_csv('all_delirium_chart_events.csv', index=False)

delirium_IDs = delirium_testing_rows[['SUBJECT_ID','HADM_ID','ICUSTAY_ID']]
delirium_IDs.drop_duplicates(inplace=True)
delirium_IDs.to_csv('MIMIC_delirium_chart_event_IDs.csv',index=False)

