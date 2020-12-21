# CURRENTLY PULLS THIS FOR EVERY SINGLE PATIENT, not sure how complete_patientstayid_list is formatted

#%% Packages
import numpy as np 
import pandas as pd 
import time as time 

#%% Load in needed data.
comp = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\complete_patientstayid_list.csv")
comp.rename(columns={'PatientStayID':'patientunitstayid'},inplace=True)
admissiondx = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\patient.csv")

#%% Sort admission data and removes unnecessary parts of diagnosis path
admissiondx = admissiondx.sort_values(["patientunitstayid", "admissiondxid"])
admissiondx['strippeddxpath'] = admissiondx.apply(lambda row: row.admitdxpath[20 :], axis = 1)

#%% Convert diagnosis string category to one of 5 categories
def determine_class(string):
    # 0 : Unknown
    # 1 : Surgery
    # 2 : Medical
    # 3 : Trauma
    # 4:  Neurology/neurosurgery
    if ('Neurology' in string) or ('Neurologic' in string):
            return 4
    
    elif len(string) > 12 and string[:13] == 'All Diagnosis':
       
        if string[14:24] == 'Operative':
            return 1
        else:
            if ('Trauma' in string):
                return 3
            else:
                return 2
    elif ('Non-operative Organ Systems' in string):
        if ('Trauma' in string):
            return 3
        else:
            return 2
    elif ('Operative Organ Systems' in string):
        return 1
    else:
        return 0

#%% Defines category precedence if patient has multiple issues
def precedence(array):
    if 4 in array:
        return 4
    elif 1 in array:
        return 1
    elif 3 in array:
        return 3
    elif 2 in array:
        return 2
    else:
        return 0

#%% Applies categorization and precedence
admissiondx['diagnosisclass'] = admissiondx.apply(lambda row: determine_class(row.strippeddxpath), axis = 1)

# Creates a temp dict that concatenates all the categories for each patientunitstayid, and then replaces it with most important
temp_dict = {}

for ind in admissiondx.index:
    if (admissiondx['patientunitstayid'][ind] in temp_dict):
        temp_dict[admissiondx['patientunitstayid'][ind]].append(admissiondx['diagnosisclass'][ind])   
    else:
        temp_dict[admissiondx['patientunitstayid'][ind]] = [admissiondx['diagnosisclass'][ind]]

for unitid in temp_dict:
    temp = precedence(temp_dict[unitid])
    temp_dict[unitid] = temp

admit_feature = pd.DataFrame.from_dict(temp_dict, orient='index', columns = ['diagnosis_classes'])
admit_feature['PatientStayID'] = admit_feature.index
admit_feature = admit_feature.reset_index()
# Might have to drop a column here??

admit_feature = pd.to_csv("admission_diagnosis_first_24hrs.csv", index=False)
