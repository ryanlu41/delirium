# -*- coding: utf-8 -*-
"""
Created on Sat Jun 13 16:37:08 2020

Combines the infection information extracted by HasInfection.py and SOFA scores
from extrac_SOFA_v2_Kirby.R to create a sepsis feature. 

This specific code uses the 24 hour output. 

@author: Kirby
"""

import pandas as pd 
import numpy as np

infect = pd.read_csv('patients_with_infections_24hours.csv')
sofa = pd.read_csv('delirium_SOFA_features_24hr.csv')

infect.rename(columns={'PatientStayID':'patientunitstayid'},inplace=True)

combined = infect.merge(sofa,how='inner',on=['patientunitstayid'])

#Check infection column for True, and suspected sepsis column for 1 to get
#sepsis feature. Can also be used with septic shock.
def get_sepsis(infection,suspected_sepsis):
    if infection == True & suspected_sepsis == 1: 
        return 1
    else:
        return 0
    
combined['24hr_sepsis'] = combined.apply(lambda row: get_sepsis(row['infection'],row['suspected_sepsis']), axis=1)
combined['24hr_septic_shock'] = combined.apply(lambda row: get_sepsis(row['infection'],row['suspected_septic_shock']), axis=1)

feature = combined[['patientunitstayid','infection','suspected_sepsis','24hr_sepsis','suspected_septic_shock','24hr_septic_shock']]
feature.to_csv('24_hour_sepsis_feature.csv',index=False)