# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 18:30:57 2020

This code generates the AKI feature for the 24 hour prediction model, by 
looking at AKI diagnoses in the first 24 hours. Strings to search for were 
determined with physician input from Dr. Stevens.

@author: Kirby
"""

import numpy as np
import pandas as pd

#Load the IDs of the patients we care about.
comp = pd.read_csv('complete_patientstayid_list.csv')
comp.rename(columns={'PatientStayID':'patientunitstayid'},inplace=True)

#Get diagnosis information.
diag = pd.read_csv('diagnosis.csv',usecols=['patientunitstayid','diagnosisoffset','diagnosisstring','icd9code'])
#Only keep the stays that had delirium testing.
diag = diag[diag['patientunitstayid'].isin(comp['patientunitstayid'])]

#Only keep diagnoses done in the first 24 hours of the ICU stay or earlier.
diag = diag[diag['diagnosisoffset']<=1440]

#Make it all lowercase
diag = diag.applymap(lambda s:s.lower() if type(s) == str else s)

#Only keep AKI related diagnoses
# strings = diag[['diagnosisstring']]
# strings.drop_duplicates(inplace=True)
search_terms_list = ['acute renal failure','traumatic renal injury'] #Tried aki, got nothing.
diag = diag[diag['diagnosisstring'].str.contains('|'.join(search_terms_list),na=False)]

#Just get the stay IDs
diag = diag[['patientunitstayid']]
diag.drop_duplicates(inplace=True)

#Create a column for the infection feature

comp['24hr_AKI'] = comp['patientunitstayid'].isin(diag['patientunitstayid'])
comp.to_csv('AKI_24hours.csv',index=False)