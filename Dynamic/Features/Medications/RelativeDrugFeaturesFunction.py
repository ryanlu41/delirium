# -*- coding: utf-8 -*-
"""
Created on Sun Jul 26 17:34:41 2020

This function pulls whether a drug was administered to a patient, at least 24
hours before delirium onset. It takes in two file paths, one for a list of drug names
to search for, and one with a list of treatment strings to search for. It then
takes in the number of hours before delirium to examine, which change the file
names it pulls windows from.

@author: Kirby
"""
#Used for testing.
drugSearchListPath=r'C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\MedicationFeatures\DrugNameLists\Vasopressors.csv'
treatmentSearchListPath=r'C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\MedicationFeatures\TreatmentStrings\VasopressorsTreatment.csv'
hours = 24

def RelativeDrugFeature(drugSearchListPath,treatmentSearchListPath,
                        lead_hours,obs_hours):
        
    import numpy as np
    import pandas as pd
    from pathlib import Path
    
    file_path = Path(__file__)
    dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
    eicu_path = file_path.parent.parent.parent.parent.joinpath('eicu')
    
    # get infusion drug info
    infu = pd.read_csv(eicu_path.joinpath("infusiondrug.csv"), 
                       usecols=['patientunitstayid','infusionoffset',
                                'drugname'])
    
    # Get medication table info
    med = pd.read_csv(eicu_path.joinpath("medication.csv"),
                      usecols=['patientunitstayid', 'drugstartoffset', 
                               'drugname', 'drughiclseqno', 'drugstopoffset',
                               'drugordercancelled'])
    # remove cancelled orders
    med = med[med['drugordercancelled']=='No']
    # drop column with drug order info.
    med.drop(columns=['drugordercancelled'],inplace=True)
 
    # Get Treatment table info
    treat = pd.read_csv(eicu_path.joinpath("treatment.csv")
                        ,usecols=['patientunitstayid', 'treatmentoffset', 
                                  'treatmentstring'])
    
    # only keep rows that are relevant to our data set.
    comp = pd.read_csv(
        dataset_path.joinpath('relative_'+ str(lead_hours) +'hr_lead_' + 
                              str(obs_hours) + 'hr_obs_data_set.csv'))
    compInfu = infu[infu['patientunitstayid'].isin(comp['patientunitstayid'])]
    compMed = med[med['patientunitstayid'].isin(comp['patientunitstayid'])]
    compTreat = treat[treat['patientunitstayid'].isin(comp['patientunitstayid'])]
    
    # only keep rows with relevant drugs
    
    # import lists of drug names to search for, and make it all lowercase
    # change this part to get different drug features
    drug=pd.read_csv(drugSearchListPath)
    drug = drug.applymap(lambda s:s.lower() if type(s) == str else s)
    druglist = drug.values.astype(str).tolist()
    druglist = [item.lower() for sublist in druglist for item in sublist]
    
    
    # this csv was generated using Create HICL Drug Name Legend.py
    hicl = pd.read_csv("HICLlegend.csv")
    # make it all lowercase
    hicl = hicl.applymap(lambda s:s.lower() if type(s) == str else s)
    # pull relevant HICL codes
    hicl = hicl[hicl['drugname'].str.contains('|'.join(druglist))]
    hicl = hicl.drop(columns=['drugname'])
    hicl = hicl.drop_duplicates()
    hicllist = hicl.values.astype(float).tolist()
    
    # keep the rows from medication with relevant drugs. 
    compMed = compMed.applymap(lambda s:s.lower() if type(s) == str else s)
    drugMed = compMed[compMed['drugname'].str.contains(
        '|'.join(druglist),na=False)]
    hiclMed = compMed[compMed['drughiclseqno'].isin(hicllist)]
    compMed = pd.concat([drugMed,hiclMed])
    compMed = compMed.drop_duplicates()
    
    # keep the rows from infusion with relevant drugs. 
    compInfu = compInfu.applymap(lambda s:s.lower() if type(s) == str else s)
    compInfu = compInfu[compInfu['drugname'].str.contains(
        '|'.join(druglist),na=False)]
    compInfu['drugstopoffset']=np.nan
    compInfu['drughiclseqno']=np.nan
    compInfu = compInfu.rename(columns={'infusionoffset':'drugstartoffset'})
    
    # keep the rows from treatment with relevant drugs. 
    # pull list of strings to search treatment with. 
    treatStrings = pd.read_csv(treatmentSearchListPath)
    treatStrings = treatStrings.applymap(
        lambda s:s.lower() if type(s) == str else s)
    treatStringsList = treatStrings.values.astype(str).tolist()
    treatStringsList = [
        item.lower() for sublist in treatStringsList for item in sublist]
    
    compTreat = compTreat.applymap(lambda s:s.lower() if type(s) == str else s)
    compTreat = compTreat[
        compTreat['treatmentstring'].str.contains(
            '|'.join(treatStringsList),na=False)]
    compTreat['drugstopoffset']=np.nan
    compTreat['drughiclseqno']=np.nan
    compTreat = compTreat.rename(columns={'treatmentoffset':'drugstartoffset',
                                          'treatmentstring':'drugname'})
    
    # combine the treatment, medication, and infusion info together
    compFeat = pd.concat([compMed,compInfu,compTreat],sort=False)
    compFeat = compFeat.sort_values(by=['patientunitstayid','drugstartoffset',
                                        'drugstopoffset'])
    compFeat = compFeat[['patientunitstayid','drugstartoffset',
                         'drugstopoffset','drugname','drughiclseqno']]
    
    # Only keep the rows where the drug start offset was before our data window,
    # and the stop offset was nan or after the data window. 
    window_lookup = comp.set_index('patientunitstayid')
    def keep_row(current_ID,drug_start,drug_stop):
        window_start = window_lookup.loc[current_ID,'start']
        window_end = window_lookup.loc[current_ID,'end']
        #if drug was given before window, and didn't end, keep it.
        if (drug_start < window_end) & (np.isnan(drug_stop)):
            return 1 
        #If drug was given before window, and stopped after window, keep it. 
        if (drug_start < window_end) & (drug_stop > window_end):
            return 1
        else:
            return 0
    
    compFeat['keep'] = compFeat.apply(lambda row: keep_row(
        row['patientunitstayid'],row['drugstartoffset'],row['drugstopoffset']),
        axis=1)
    compFeat = compFeat[compFeat['keep'] == 1]
    
    # get a list of the stay IDs that have the drug administrations.
    compFeat = compFeat[['patientunitstayid']]
    compFeat = compFeat.drop_duplicates()
    
    # Convert it to a true/false for each stay ID in comp
    newColName = 'relative_'+ str(lead_hours) +'hr_lead_' + str(obs_hours) +   \
    'hr_obs' + drug.columns.values[0]
    comp[newColName] = \
        comp['patientunitstayid'].isin(compFeat['patientunitstayid'])
    
    return comp[newColName]