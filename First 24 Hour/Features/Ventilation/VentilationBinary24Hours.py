# -*- coding: utf-8 -*-
"""
Created on Sun Apr 26 15:25:10 2020

This code pulls for the prediction/clustering patients whether or not they were 
documented as receiving ventilation during their ICU stay. Checked the 
apacheApsVar, PhysicalExam, or Treatment tables. 

CarePlanGeneral, and respiratory care are less reliable, so not using them.

@author: Kirby
"""
#%% Package setup.
import numpy as np
import pandas as pd

#%% Load in data.
#Load the IDs of the patients we care about.
comp = pd.read_csv(r'C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\complete_patientstayid_list.csv')
comp.rename(columns={'PatientStayID':'patientunitstayid'},inplace=True)

#Get all apacheapsvar
apache = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\apacheapsvar.csv",
                     usecols=['patientunitstayid','vent'])
#Get physicalexam data.
phys = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\physicalexam.csv",
                   usecols=['patientunitstayid','physicalexamoffset',
                            'physicalexamtext'])
#Get treatment
treat = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\treatment.csv",
                    usecols=['patientunitstayid','treatmentoffset',
                            'treatmentstring'])


#%% Get feature and save it off.
#Only keep the stays we care about.
apache = apache[apache['patientunitstayid'].isin(comp['patientunitstayid'])]
phys = phys[phys['patientunitstayid'].isin(comp['patientunitstayid'])]
treat = treat[treat['patientunitstayid'].isin(comp['patientunitstayid'])]

#Only keep data from the first 24 hours.
phys = phys[phys['physicalexamoffset'] >= 0]
phys = phys[phys['physicalexamoffset'] <= 1440]
treat = treat[treat['treatmentoffset'] >= 0]
treat = treat[treat['treatmentoffset'] <= 1440]

#Get ventilation data.
phys = phys[phys['physicalexamtext']=='ventilated']
mech = treat[treat['treatmentstring'].str.contains('mechanical ventilation')]
noninv = treat[treat['treatmentstring'].str.contains('non-invasive ventilation')]

#Just get patientunitstayids that had ventilation in first 24 hours. 
vent_ids = pd.concat([phys[['patientunitstayid']],mech[['patientunitstayid']],
                      noninv[['patientunitstayid']]])
vent_ids.drop_duplicates(inplace=True)

comp['first24hrs_vented'] = comp['patientunitstayid'].isin(vent_ids['patientunitstayid'])

comp.to_csv('First24HoursVented.csv',index=False)