# -*- coding: utf-8 -*-
"""
Created on Mon Feb 17 00:03:56 2020

This function pulls whether a drug was administered to a patient in the first 
24 hours of their stay. It takes in two file paths, one for a list of drug names
to search for, and one with a list of treatment strings to search for. 

@author: Kirby
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jan 27 20:06:18 2020

@author: Kirby
"""
def TwentyFourHourDrugFeature(drugSearchListPath,treatmentSearchListPath):
        
    import numpy as np
    import pandas as pd
    
    # get infusion drug info
    infu = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\infusiondrug.csv", usecols=['patientunitstayid','infusionoffset','drugname'])
    
    # Get medication table info
    med = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\medication.csv",usecols=['patientunitstayid', 'drugstartoffset', 'drugname', 'drughiclseqno', 'drugstopoffset','drugordercancelled'])
    # remove cancelled orders
    med = med[med['drugordercancelled']=='No']
    # drop column with drug order info.
    med.drop(columns=['drugordercancelled'],inplace=True)
 
    # Get Treatment table info
    treat = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\treatment.csv",usecols=['patientunitstayid', 'treatmentoffset', 'treatmentstring'])
    
    # only keep rows with the complete list of data ids
    comp = pd.read_csv('complete_patientstayid_list.csv')
    compInfu = infu[infu['patientunitstayid'].isin(comp['PatientStayID'])]
    compMed = med[med['patientunitstayid'].isin(comp['PatientStayID'])]
    compTreat = treat[treat['patientunitstayid'].isin(comp['PatientStayID'])]
    
    # only keep rows with relevant drugs
    
    # import lists of drug names to search for, and make it all lowercase
    # change this part to get different drug features
    drug=pd.read_csv(drugSearchListPath)
    drug = drug.applymap(lambda s:s.lower() if type(s) == str else s)
    druglist = drug.values.astype(str).tolist()
    druglist = [item.lower() for sublist in druglist for item in sublist]
    
    
    # this csv was generated using Create HICL Drug Name Legend.py
    hicl = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\HICLlegend.csv")
    # make it all lowercase
    hicl = hicl.applymap(lambda s:s.lower() if type(s) == str else s)
    # pull relevant HICL codes
    hicl = hicl[hicl['drugname'].str.contains('|'.join(druglist))]
    hicl = hicl.drop(columns=['drugname'])
    hicl = hicl.drop_duplicates()
    hicllist = hicl.values.astype(float).tolist()
    
    # keep the rows from medication with relevant drugs. 
    compMed = compMed.applymap(lambda s:s.lower() if type(s) == str else s)
    drugMed = compMed[compMed['drugname'].str.contains('|'.join(druglist),na=False)]
    hiclMed = compMed[compMed['drughiclseqno'].isin(hicllist)]
    compMed = pd.concat([drugMed,hiclMed])
    compMed = compMed.drop_duplicates()
    
    # keep the rows from infusion with relevant drugs. 
    compInfu = compInfu.applymap(lambda s:s.lower() if type(s) == str else s)
    compInfu = compInfu[compInfu['drugname'].str.contains('|'.join(druglist),na=False)]
    compInfu['drugstopoffset']=np.nan
    compInfu['drughiclseqno']=np.nan
    compInfu = compInfu.rename(columns={'infusionoffset':'drugstartoffset'})
    
    # keep the rows from treatment with relevant drugs. 
    # pull list of strings to search treatment with. 
    treatStrings = pd.read_csv(treatmentSearchListPath)
    treatStrings = treatStrings.applymap(lambda s:s.lower() if type(s) == str else s)
    treatStringsList = treatStrings.values.astype(str).tolist()
    treatStringsList = [item.lower() for sublist in treatStringsList for item in sublist]
    
    compTreat = compTreat.applymap(lambda s:s.lower() if type(s) == str else s)
    compTreat = compTreat[compTreat['treatmentstring'].str.contains('|'.join(treatStringsList),na=False)]
    compTreat['drugstopoffset']=np.nan
    compTreat['drughiclseqno']=np.nan
    compTreat = compTreat.rename(columns={'treatmentoffset':'drugstartoffset','treatmentstring':'drugname'})
    
    # combine the treatment, medication, and infusion info together
    compFeat = pd.concat([compMed,compInfu,compTreat],sort=False)
    compFeat = compFeat.sort_values(by=['patientunitstayid','drugstartoffset','drugstopoffset'])
    compFeat = compFeat[['patientunitstayid','drugstartoffset','drugstopoffset','drugname','drughiclseqno']]
    
    # only keep them if the drug start offset was before 24 hours, and after admission. 
    compFeat = compFeat[compFeat['drugstartoffset'] <= 1440 ]
    compFeat = compFeat[compFeat['drugstartoffset'] >= 0 ]
    
    # get a list of the stay IDs that have the drug admissions.
    compFeat = compFeat[['patientunitstayid']]
    compFeat = compFeat.drop_duplicates()
    
    # Convert it to a true/false for each stay ID in comp
    newColName = 'FirstDay' + drug.columns.values[0]
    comp[newColName] = comp['PatientStayID'].isin(compFeat['patientunitstayid'])
    
    return comp[newColName]