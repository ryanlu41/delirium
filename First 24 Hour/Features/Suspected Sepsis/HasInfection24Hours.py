# -*- coding: utf-8 -*-
"""
Created on Tue Apr 28 22:40:18 2020

This code is meant to pull data on whether patients had infections diagnosed
in their eICU data, based on diagnosis information, looking both at diagnosis
strings and the ICD9 codes if listed. This code only counts it if the diagnosis
was added in the first 24 hours of the ICU stay or earlier. 

@author: Kirby
"""

import numpy as np
import pandas as pd

#Load the IDs of the patients we care about.
comp = pd.read_csv('complete_patientstayid_list.csv')

#Get diagnosis information.
diag = pd.read_csv('diagnosis.csv',usecols=['patientunitstayid',
                                            'diagnosisoffset',
                                            'diagnosisstring','icd9code'])
#Only keep the stays that had delirium testing.
diag = diag[diag['patientunitstayid'].isin(comp['PatientStayID'])]

#Only keep diagnoses done in the first 24 hours of the ICU stay or earlier.
diag = diag[diag['diagnosisoffset']<=1440]

#Make it all lowercase
diag = diag.applymap(lambda s:s.lower() if type(s) == str else s)

#This function takes in the icd9code as a lower case string, harvests the 
#first part if present, removes any entries with letters, and converts it to a float. 
#and returns nan if there isn't any value. 
def shorten_icd9(icd9):
    if type(icd9) == float:
        return np.nan
    else:
        #Get the first part separated by commas
        icd9 = icd9.split(',')[0]
        #Check if it's got letters in it. If so, get rid of it. 
        if icd9.upper() != icd9:
                return np.nan
        else: 
            return float(icd9)

diag['icd9'] = diag.apply(lambda row: shorten_icd9(row['icd9code']),axis=1)

#Load lists of icd9 codes to look for. 
rounded_codes = pd.read_csv('ICD9_codes_rounded.csv',header=None)
rounded_codes = rounded_codes.values.astype(str).tolist()[0]
exact_codes = pd.read_csv('ICD9_codes_exact.csv',header=None)
exact_codes = exact_codes.values.astype(str).tolist()[0]

#Keep the rows that have ICD9 codes related to infections.
rounded_code_stays = diag[np.floor(diag['icd9']).isin(rounded_codes)]
exact_code_stays = diag[diag['icd9'].isin(exact_codes)]

#Keep the rows where the diagnosis string contains these words
search_terms_list = ['infection','infectious']
string_stays = diag[diag['diagnosisstring'].str.contains(
    '|'.join(search_terms_list),na=False)]
#Drop the rows where diagnosis string contains "non-infectious"
string_stays = string_stays[np.logical_not(
    string_stays['diagnosisstring'].str.contains('non-infectious'))]

#Combine all the stays.
all_stays = pd.concat([rounded_code_stays,exact_code_stays,string_stays])
all_stays.drop_duplicates(inplace=True)
all_stays.sort_values(['patientunitstayid','diagnosisoffset'],inplace=True)

#Just get the stay IDs
all_stays = all_stays[['patientunitstayid']]
all_stays.drop_duplicates(inplace=True)

#Create a column for the infection feature
comp['infection'] = comp['PatientStayID'].isin(all_stays['patientunitstayid'])

comp.to_csv('patients_with_infections_24hours.csv',index=False)

# Obsolete code below.

# #Exploring ICD9 code info.
# icds = diag[['diagnosisstring','icd9code']]
# icds.drop_duplicates(inplace=True)
# #Harvest the first part of the ICD9 code

# #Exploring diagnosis strings
# strings = icds.copy()
# #Make it all lowercase
# strings = strings.applymap(lambda s:s.lower() if type(s) == str else s)


