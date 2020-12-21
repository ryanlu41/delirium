import numpy as np 
import pandas as pd 
import time as time 

# Determines if an admission is elective or not
def determine_urgency(string):
    if string == 'admission diagnosis|Elective|Yes':
        return 0
    else:
        return 1

# Determines precedence for multiple admission diagnoses.
def precedence(array):
    if 0 in array:
        return 0
    else:
        return 1


comp = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\complete_patientstayid_list.csv")
comp.rename(columns={'PatientStayID':'patientunitstayid'},inplace=True)
admissiondx = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\patient.csv")

admissiondx = admissiondx.sort_values(["patientunitstayid", "admissiondxid"])
admissiondx = admissiondx.loc[:,~admissiondx.columns.duplicated()]

admissiondx['urgent'] = admissiondx.apply(lambda row: determine_urgency(row.admitdxpath), axis = 1)

temp_dict = {}

for ind in admissiondx.index:
    if (admissiondx['patientunitstayid'][ind] in temp_dict):
        temp_dict[admissiondx['patientunitstayid'][ind]].append(admissiondx['urgent'][ind])   
    else:
        temp_dict[admissiondx['patientunitstayid'][ind]] = [admissiondx['urgent'][ind]]

for unitid in temp_dict:
    temp = precedence(temp_dict[unitid])
    temp_dict[unitid] = temp
    
urgent_feature = pd.DataFrame.from_dict(temp_dict, orient='index', columns = ['urgentadmission'])

urgent_feature['patientunitstayid'] = urgent_feature.index
urgent_feature = urgent_feature.reset_index()

urgent_feature = pd.to_csv("urgent_admission_first_24hrs.csv", index=False)