# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 19:15:45 2020

Analyze if there are multiple units for each lab type in MIMIC-III.

Also pulls unit types from labs in eICU to compare. 

Lab equivalence between MIMIC-III and eICU was determined manually.

@author: Kirby
"""
#%% Package setup
import pandas as pd

#%% Load in data.

#Pulls all lab info and drop columns we don't need.
labs_list = pd.read_excel(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\LabFeatures\MIMIC_to_eICU_Labs_List.xlsx")
labs_MIMIC = items = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\LABEVENTS_delirium.csv",
                    usecols=['ITEMID','VALUEUOM'])
labs_eICU = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\lab_delirium.csv",
                        usecols=['labname','labmeasurenamesystem'])
items = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\D_LABITEMS.csv",
                    usecols=['ITEMID','LABEL','FLUID'])

#%% Pre-process data.

labs_eICU.drop_duplicates(inplace=True)
labs_MIMIC.drop_duplicates(inplace=True)

#Get MIMIC units
labs_MIMIC = labs_MIMIC[labs_MIMIC['ITEMID'].isin(labs_list['ITEMID'])]

#Get eICU units. 
labs_eICU = labs_eICU[labs_eICU['labname'].isin(labs_list['labname'])]

#Put units together.
labs_list_w_units = labs_list.merge(labs_eICU,how='left',on='labname')
labs_list_w_units = labs_list_w_units.merge(labs_MIMIC,how='left',on='ITEMID')

#Save it off.
labs_list_w_units.to_excel('labs_unit_comparison.xls',index=False)

#Units that need to be converted:
#None.
    
#Also consulted with Dr. Stevens.

#%% Do I need to convert the WBC or platelet numbers by a factor of 1000? 
labs_MIMIC = items = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\SCCM Datathon\mimic-iii-clinical-database-1.4\LABEVENTS_delirium.csv",
                    usecols=['ITEMID','VALUENUM'])
labs_eICU = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\lab_delirium.csv",
                        usecols=['labname','labresult'])

wbc = [51300,51301]
wbc_mimic = labs_MIMIC[labs_MIMIC['ITEMID'].isin(wbc)]
plate_mimic = labs_MIMIC[labs_MIMIC['ITEMID']==51265]

wbc_eicu = labs_eICU[labs_eICU['labname']=='WBC x 1000']
plate_eicu = labs_eICU[labs_eICU['labname']=='platelets x 1000']

#No, to both wbc and platelets. All data is within an order of magnitude. 