# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 10:25:04 2020

Pulls the labs taht occurred before delirium only. Sorts them by patientstayid
and offset. Called by AllLabsBeforeDelirium.py

@author: Kirby
"""

def labs_before_delirium(lab_name):
       
    import numpy as np
    import pandas as pd
    
    #Pulls list of Stay IDs.
    comp = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\complete_patientstayid_list.csv")
    
    # Pull Delirium Onset Times from csv, which were obtained in SQL. 
    del_start = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\DeliriumStartTimes.csv")
    # make it into a dictionary of ID -> delirium start time.
    del_start.set_index("patientunitstayid", drop=True, inplace=True)
    del_start_dict = del_start.to_dict(orient="dict")
    del_start_dict = del_start_dict.get("deliriumstartoffset")
    
    #Pulls all lab info. 
    lab = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\lab.csv")
    #Drop columns we don't need.
    lab = lab[['patientunitstayid','labresultoffset','labname','labresult']]
    #Only keeps the lab we want.
    lab = lab[lab['labname']==lab_name]
    #Only keeps the patient stays we want.
    lab = lab[lab['patientunitstayid'].isin(comp['PatientStayID'])]
    #Adds delirium start time info to each row.
    lab['deliriumstart'] = lab['patientunitstayid'].apply(del_start_dict.get)
    #If they occurred before the ICU stay remove them.
    lab = lab[lab['labresultoffset'] >= 0]
    #Only keep labs that happened before the delirium start time. 
    #If there was never delirium, keep all the labs.
    lab = lab[(lab['labresultoffset'] < lab['deliriumstart']) | (lab['deliriumstart'].isna()) ]
    lab.sort_values(by=['patientunitstayid','labresultoffset'],inplace=True)
    return lab

if __name__ == '__main__':
    #Change out this name for different labs. 
    #String for lab_name must exactly match the labname used in the Lab table of eICU.
    lab_name = 'BUN'
    test = labs_before_delirium(lab_name)
    test.to_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\LabFeatures\AllLabsBeforeDelirium\\" + lab_name + ".csv",index=False)