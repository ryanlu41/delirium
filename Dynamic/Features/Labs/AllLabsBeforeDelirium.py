# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 10:25:04 2020

This code isolates all the labs before delirium, just to speed up processing time. 
It calls the function in LabsBeforeDelirium.py.
After running this code, delete the separate bicarbonate.csv, HCO3.csv, and Total CO2.csv files.
Then run the relevant lab feature extraction code.

@author: Kirby
"""
import pandas as pd
import LabsBeforeDelirium as lbd

all_lab_names = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\LabFeatures\LabsList.csv")
all_lab_names_list = all_lab_names.values.tolist()
all_lab_names_list = [item for sublist in all_lab_names_list for item in sublist]
for lab_name in all_lab_names_list:
    labs_before_del = lbd.labs_before_delirium(lab_name)
    labs_before_del.to_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\LabFeatures\AllLabsBeforeDelirium\\" + lab_name + ".csv",index=False)

#%% Combine the 3 different sources of Bicarbonate data (HCO3, Total CO2, bicarbonate)
bicarb = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\LabFeatures\AllLabsBeforeDelirium\bicarbonate.csv")
hco3 = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\LabFeatures\AllLabsBeforeDelirium\HCO3.csv")
total_CO2 = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\LabFeatures\AllLabsBeforeDelirium\Total CO2.csv")
all_data = pd.concat([bicarb,hco3,total_CO2])
all_data.sort_values(['patientunitstayid','labresultoffset'],inplace=True)
all_data.to_csv('AllLabsBeforeDelirium\\bicarbonate_totalCO2_HCO3.csv',index=False)

#Make sure to delete the individual csv files!