# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 21:26:42 2020

This code yields a csv/dataframe of all delirium diagnoses and scores,
 whether they were positive/negative, and a time stamp.

@author: Kirby
"""


def all_delirium_tests():
    
    import numpy as np    
    import pandas as pd
    import ICU_LOS as los
    from pathlib import Path
    
    file_path = Path(__file__)
    dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
    eicu_path = file_path.parent.parent.parent.parent.joinpath('eicu')
    
    #Pulls list of Stay IDs.
    comp = pd.read_csv(dataset_path.joinpath('complete_patientstayid_list.csv'))
    
    #Pull LOS info.
    LOS_dict = los.ICU_LOS()
    
    #Get delirium diagnosis info.
    diag = pd.read_csv(eicu_path.joinpath("diagnosis.csv"))
    diag = diag[diag['diagnosisstring']=='neurologic|altered mental status / pain|delirium']
    diag = diag[diag['patientunitstayid'].isin(comp['PatientStayID'])]
    
    #Only keep rows where the diagnosis is after the ICU admission.
    diag = diag[diag['diagnosisoffset'] > 0]
    #Only keep rows where the diagnosis is before discharge time.
    diag['LOS'] = diag.apply(lambda row: LOS_dict.get(row['patientunitstayid']),axis=1)
    diag = diag[diag['diagnosisoffset'] < diag['LOS']]
    
    #Only keep the columns we care about and rename them for concatenation.
    diag = diag[['patientunitstayid','diagnosisoffset']]
    diag.rename(columns={'diagnosisoffset':'offset'},inplace=True)
    diag['delirium'] = True
    
    #Get delirium test info from nurse charting.
    nurse = pd.read_csv(eicu_path.joinpath("nursecharting.csv"))
    nurse = nurse[['patientunitstayid','nursingchartoffset','nursingchartcelltypevalname','nursingchartvalue']]
    nurse = nurse[nurse['nursingchartcelltypevalname']=='Delirium Score']
    nurse = nurse.drop(columns=['nursingchartcelltypevalname'])
    nurse = nurse[nurse['patientunitstayid'].isin(comp['PatientStayID'])]
    nurse = nurse.dropna()
    

    
    #Convert the raw scores data into True or False for delirium presence.
    def make_delirium_col(stay_ID,offset,chart_value):
        #Removes data from before the stay.
        if offset < 0:
            return np.nan
        #Removes data from after the stay.
        elif offset > LOS_dict.get(stay_ID):
            return np.nan
        elif chart_value.lower() == 'no':
            return False
        elif chart_value.lower() == 'yes':
            return True
        elif int(chart_value) < 4:
            return False
        elif int(chart_value) >= 4:
            return True
    
    nurse['delirium'] = nurse.apply(lambda row: make_delirium_col(row['patientunitstayid'],row['nursingchartoffset'],row['nursingchartvalue']),axis=1)
       
    nurse = nurse.dropna()
    
    #Just keeping the data we want.
    nurse = nurse[['patientunitstayid','nursingchartoffset','delirium']]
    nurse.rename(columns={'nursingchartoffset':'offset'},inplace=True)
    
    # concat them together
    all_delirium = pd.concat([diag,nurse])
    all_delirium.sort_values(['patientunitstayid','offset'],inplace=True)
    
    return all_delirium

if __name__ == '__main__':
    all_delirium = all_delirium_tests()
    #save to csv if needed
    all_delirium.to_csv("AllDeliriumTests.csv",index=False)
          
    
    