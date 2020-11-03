# %% Import packages
import numpy as np
import pandas as pd

# Load all tables
diagnosis = pd.read_csv("../../eICU/diagnosis.csv", usecols=['patientunitstayid', 'icd9code', 'diagnosisoffset'])
lab = pd.read_csv("../../eICU/lab.csv", usecols=['patientunitstayid', 'labresultoffset', 'labname', 'labresult'])
delirium = pd.read_csv('../../Dataset/complete_patientstayid_list.csv')

# Pull metabolic acidosis from diagnosis table
# Pull corresponding metabolic acidosis icd9 codes
metacid_diagnosis = diagnosis[diagnosis['icd9code'].isin(['276.4', '276.2, E87.2', '276.4, E87.4'])]
# Only keep first 24 hours
metacid24_diagnosis = metacid_diagnosis[metacid_diagnosis['diagnosisoffset'] <= 1440]

# Metabolic acidosis is bicarb < 24 and pH <7.35 within 12 hours of each other
# Find bicarbonate from lab
bicarb_lab = lab[lab['labname'].isin(['Total CO2', 'bicarbonate', 'HCO3'])]
# First 24 hours
bicarb24_lab = bicarb_lab[bicarb_lab['labresultoffset'] <= 1440]
bicarb24_lab = bicarb24_lab[bicarb24_lab['labresultoffset'] >= 0]
# Bicarbinate <24
ma_bicarb24_lab = bicarb24_lab[bicarb24_lab['labresult'] < 24]

# Find pH from lab
pH_lab = lab[lab['labname'] == 'pH']
pH24_lab = pH_lab[pH_lab['labresultoffset'] <= 1440]
pH24_lab = pH24_lab[pH24_lab['labresultoffset'] >= 0]
# pH <7.35
ma_pH24_lab = pH24_lab[pH24_lab['labresult'] < 7.35]

# inner join to get only patients who had both lab results and make sure result offsets are within 12 hrs
ma_bipH24_lab = ma_bicarb24_lab.join(ma_pH24_lab.set_index('patientunitstayid'),
                                     on='patientunitstayid', how='inner', lsuffix='_bi', rsuffix='_pH')
ma_bipH24_lab['laboffsetdiff'] = abs(ma_bipH24_lab['labresultoffset_bi'] - ma_bipH24_lab['labresultoffset_pH'])
metacid24_lab = ma_bipH24_lab[ma_bipH24_lab['laboffsetdiff'] <= 720]

# outer join diagnosis and lab for full list
metacid24 = metacid24_diagnosis.join(metacid24_lab.set_index('patientunitstayid'), on='patientunitstayid', how='outer')\
    .drop_duplicates('patientunitstayid')

# Compare to list of delirium patients to add positive or negative metabolic acidosis diagnosis to relevant patients
delirium_metacid = delirium.merge(metacid24, on='patientunitstayid', how='left', indicator=True)
delirium_metacid['metabolicacidosis'] = np.where(delirium_metacid['_merge'] == 'both', 1, 0)

# Drop unnecessary columns
delirium_ma = delirium_metacid[['patientunitstayid', 'metabolicacidosis']]

# Save
delirium_ma.to_csv('first24hr_metabolicacidosis.csv', index=False)
