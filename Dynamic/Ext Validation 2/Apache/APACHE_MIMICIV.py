# -*- coding: utf-8 -*-
"""
Created on Tue Jul 12 12:19:04 2022

This code manually calculates the APACHE IV score, based on data extracted 
from the first 24 hours. It mean imputes missing individual data points. 

Run time: 1 min
    
@author: kirby
"""

#%% Package setup
import pandas as pd
import numpy as np
from pathlib import Path
from time import time
from sklearn.impute import SimpleImputer

start = time()
file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath('Dataset')
ext_valid_path = file_path.parent.parent.parent.joinpath('Ext Validation 2')
mimic_path = file_path.parent.parent.parent.parent.joinpath('mimic-iv-2.0')


#%% Pull needed data from other extracted feature data. 

comp = pd.read_csv(dataset_path.joinpath("MIMICIV_complete_dataset.csv"),
                   usecols=['stay_id'])

static = pd.read_csv(ext_valid_path.joinpath('Static',
                                             'static_features_MIMICIV.csv'),
                     usecols=['stay_id','Age'])

temp = pd.read_csv(ext_valid_path.joinpath(
    "NurseCharting","first_24hr_temper_feature_MIMICIV.csv"),
    usecols=['stay_id', '24hrMeanTemp', '24hrMinTemp', '24hrMaxTemp'])

#Get MAP, HR, RR.
items = pd.read_csv(mimic_path.joinpath('icu', 'd_items.csv.gz'),
                    usecols = ['itemid', 'label'])
items.loc[:, 'label'] = items['label'].str.lower()
mbp_items = items[items['label'].str.contains('blood pressure mean')]
hr_items = items[items['label'] == 'heart rate']
rr_items = items[items['label'].str.contains('respiratory rate')]
fio2_items = items[items['label'].str.contains('inspired o2 fraction')]

cols_to_use = ['stay_id', 'itemid', 'valuenum']
map_data = pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'),
                          nrows=0,usecols=cols_to_use)
hr_data = map_data.copy()
rr_data = map_data.copy()
fio2_data = map_data.copy()
for chunk in pd.read_csv(mimic_path.joinpath('icu', 'chartevents.csv.gz'),
                         chunksize=1000000, usecols=cols_to_use):
    #Remove erroneous data, cut to just our stay_ids.
    temp_map = chunk.merge(mbp_items, on = 'itemid', how = 'inner')
    map_data = pd.concat([map_data,temp_map])
    temp_hr = chunk.merge(hr_items, on = 'itemid', how = 'inner')
    hr_data = pd.concat([hr_data,temp_hr])
    temp_rr = chunk.merge(rr_items, on = 'itemid', how = 'inner')
    rr_data = pd.concat([rr_data,temp_rr])   
    temp_fio2 = chunk.merge(fio2_items, on = 'itemid', how = 'inner')
    fio2_data = pd.concat([fio2_data,temp_fio2])   
    
#%% Labs
lab = pd.read_csv(
    ext_valid_path.joinpath("Labs","first_24_hour_lab_features_MIMICIV.csv"))
lab.rename(columns={'patientunitstayid':'stay_id'},inplace=True)

gcs = pd.read_csv(ext_valid_path.joinpath(
    "NurseCharting","first_24hr_GCS_feature_MIMICIV.csv"),
    usecols=['stay_id', '24hrMeanMotor','24hrMeanVerbal', '24hrMeanEyes'])

vent = pd.read_csv(ext_valid_path.joinpath('Ventilation',
                                           'First24HoursVented_MIMICIV.csv'),
                   usecols=['stay_id','first24hrs_vented'])

urine = pd.read_csv(ext_valid_path.joinpath('IntakeOutput',
                                           'first_24hr_urine_feature_MIMICIV.csv'))

hist = pd.read_csv(ext_valid_path.joinpath('History',
                                           'history_features_MIMICIV.csv'))
hist = hist.drop(columns=['hadm_id'])


#%% Get worst data.

comp = comp.merge(static, on='stay_id', how='left')

#Get worst value, given min and max in first 24 hours.
def get_worst(norm,min_val,max_val):
    min_diff = abs(norm - min_val)
    max_diff = abs(norm - max_val)
    if min_diff > max_diff:
        return min_val
    else:
        return max_val

temp['worst_temp'] = temp.apply(
    lambda row: get_worst(37.2,row['24hrMinTemp'],row['24hrMaxTemp']),axis=1)

comp = comp.merge(temp[['stay_id','worst_temp']],on='stay_id',how='left')

#Get MAP, HR, RR data. 
#Clean data of erroneous values.
map_data = map_data[(map_data['valuenum']>=5) & (map_data['valuenum']<=300)]
hr_data = hr_data[(hr_data['valuenum']>=20) & (hr_data['valuenum']<=220)]
rr_data = rr_data[(rr_data['valuenum']>=5) & (rr_data['valuenum']<150)]
fio2_data = fio2_data[(fio2_data['valuenum'] > 0) & (fio2_data['valuenum'] <= 100)]

#Get min and max.
min_map = map_data.groupby('stay_id').min().reset_index().rename(
    columns={'valuenum':'min_map'})[['stay_id','min_map']]
max_map = map_data.groupby('stay_id').max().reset_index().rename(
    columns={'valuenum':'max_map'})[['stay_id','max_map']]
min_hr = hr_data.groupby('stay_id').min().reset_index().rename(
    columns={'valuenum':'min_hr'})[['stay_id','min_hr']]
max_hr = hr_data.groupby('stay_id').max().reset_index().rename(
    columns={'valuenum':'max_hr'})[['stay_id','max_hr']]
min_rr = rr_data.groupby('stay_id').min().reset_index().rename(
    columns={'valuenum':'min_rr'})[['stay_id','min_rr']]
max_rr = rr_data.groupby('stay_id').max().reset_index().rename(
    columns={'valuenum':'max_rr'})[['stay_id','max_rr']]
max_fio2 = fio2_data.groupby('stay_id').max().reset_index().rename(
    columns={'valuenum':'max_FiO2'})[['stay_id','max_FiO2']]

#Add info to comp. 
for info in [min_map,max_map,min_hr,max_hr,min_rr,max_rr,max_fio2]:
    comp = comp.merge(info,on='stay_id',how='left')

#Get the worst of it.
comp['worst_map'] = comp.apply(
    lambda row: get_worst(75,row['min_map'],row['max_map']),axis=1)
comp['worst_hr'] = comp.apply(
    lambda row: get_worst(75,row['min_hr'],row['max_hr']),axis=1)
comp['worst_rr'] = comp.apply(
    lambda row: get_worst(15,row['min_rr'],row['max_rr']),axis=1)

comp.drop(columns=['min_map', 'max_map', 'min_hr','max_hr', 'min_rr', 
                   'max_rr'],inplace=True)

comp.rename(columns = {'max_FiO2':'worst_FiO2'}, inplace = True)

comp = comp.merge(vent,on='stay_id',how='left')
comp = comp.merge(urine,on='stay_id',how='left')

#Get worst labs where that's needed (per Dr Stevens).
lab['worst_paO2'] = lab.apply(
    lambda row: get_worst(85,row['min_paO2'],row['max_paO2']),axis=1)
lab['worst_paCO2'] = lab.apply(
    lambda row: get_worst(40,row['min_paCO2'],row['max_paCO2']),axis=1)
lab['worst_pH'] = lab.apply(
    lambda row: get_worst(7.4,row['min_pH'],row['max_pH']),axis=1)
lab['worst_sodium'] = lab.apply(
    lambda row: get_worst(140,row['min_sodium'],row['max_sodium']),axis=1)
lab['worst_creatinine'] = lab.apply(
    lambda row: get_worst(1,row['min_creatinine'],row['max_creatinine']),axis=1)
lab['worst_glucose'] = lab.apply(
    lambda row: get_worst(90,row['min_glucose'],row['max_glucose']),axis=1)
lab['worst_albumin'] = lab.apply(
    lambda row: get_worst(4,row['min_albumin'],row['max_albumin']),axis=1)
lab['worst_Hct'] = lab.apply(
    lambda row: get_worst(40,row['min_Hct'],row['max_Hct']),axis=1)
lab['worst_WBC'] = lab.apply(
    lambda row: get_worst(10,row['min_WBC x 1000'],row['max_WBC x 1000']),axis=1)

#Get max labs (as the worst) of some labs.
lab.rename(columns={'max_BUN':'worst_BUN',
                    'max_total bilirubin':'worst_bilirubin'},inplace=True)

lab = lab[['stay_id', 'worst_paO2', 'worst_paCO2','worst_pH',
           'worst_sodium','worst_creatinine','worst_BUN','worst_glucose',
           'worst_albumin','worst_bilirubin','worst_Hct','worst_WBC']]
comp = comp.merge(lab,on='stay_id',how='left')

#Add on history. 
hist = hist[['stay_id','HistRenalFail','HistCirrhosis','HistLiverFail',
             'HistMetastases','HistLymphoma','HistLeukemia','HistImmuneSuppr',
             'HistAIDS']]
comp = comp.merge(hist,on='stay_id',how='left')

gcs['worst_verbal'] = np.floor(gcs['24hrMeanVerbal'])
gcs['worst_eye'] = np.floor(gcs['24hrMeanEyes'])
gcs['worst_motor'] = np.floor(gcs['24hrMeanMotor'])
gcs = gcs[['stay_id','worst_verbal','worst_eye','worst_motor']]
comp = comp.merge(gcs,on='stay_id',how='left')

#%% Impute individual missing values.
#Numerical.
for col_name in ['Age', 'worst_temp', 'worst_map', 'worst_hr', 'worst_rr',
                 'first_24hr_urine', 'worst_FiO2', 'worst_paO2',
                 'worst_paCO2', 'worst_pH', 'worst_sodium', 'worst_creatinine',
                 'worst_BUN', 'worst_glucose', 'worst_albumin',
                 'worst_bilirubin', 'worst_Hct', 'worst_WBC']:
    imp = SimpleImputer(missing_values=np.nan, strategy='mean')
    comp[col_name] = imp.fit_transform(
        comp[col_name].values.reshape(-1,1))[:,0]

#Categorical.
for col_name in ['first24hrs_vented', 'HistRenalFail', 'HistCirrhosis',
                 'HistLiverFail', 'HistMetastases', 'HistLymphoma', 
                 'HistLeukemia', 'HistImmuneSuppr', 'HistAIDS', 
                 'worst_verbal', 'worst_eye', 'worst_motor']:
    imp = SimpleImputer(missing_values=np.nan, strategy='most_frequent')
    comp[col_name] = imp.fit_transform(
        comp[col_name].values.reshape(-1,1))[:,0]

#%% Calculate the actual score with worst values. 

def calc_apache(temp,mean_ap,hr,rr,vent,fio2,pao2,paco2,ph,sodium,urine,
                renalfail,creatinine,bun,glucose,albumin,bilirubin,hct,wbc,
                gcse,gcsv,gcsm,age,aids,hep_fail,lymph,metastases,leuk,immune,
                cirrhosis):
    
    #Deal with missing values, excluding GCS. 
    for var in [temp,mean_ap,hr,rr,vent,fio2,pao2,paco2,ph,sodium,urine,
                renalfail,creatinine,bun,glucose,albumin,bilirubin,hct,wbc,
                age,aids,hep_fail,lymph,metastases,leuk,immune,cirrhosis]:
        if np.isnan(var) == True:
            return np.nan
    
    score = 0
    
    #Temperature
    if temp < 33: score += 20
    elif temp < 33.5: score += 16
    elif temp < 34: score += 13
    elif temp < 35: score += 8
    elif temp < 36: score += 2
    elif temp < 40: score += 0
    else: score += 4
    
    #MAP
    if mean_ap <= 39: score += 23
    elif mean_ap < 60: score += 15
    elif mean_ap < 70: score += 7
    elif mean_ap < 80: score += 6
    elif mean_ap < 100: score += 0
    elif mean_ap < 120: score += 4
    elif mean_ap < 130: score += 7
    elif mean_ap < 140: score += 9
    else: score += 10
    
    #HR
    if hr < 40: score += 8
    elif hr < 50: score += 5
    elif hr < 100: score += 0
    elif hr < 110: score += 1
    elif hr < 120: score += 5
    elif hr < 140: score += 7
    elif hr < 155: score += 13
    else: score += 17
    
    #RR
    if rr <= 5: score += 17
    elif rr < 12: score += 8
    elif rr < 14: score += 7
    elif rr < 25: score += 0
    elif rr < 35: score += 6
    elif rr < 40: score += 9
    elif rr < 50: score += 11
    else: score += 18
    
    #Calculate AaDO2.
    aad = (fio2/100) * (760-47) - (paco2/0.8) - pao2
    
    #Ventilation
    if vent == 0:
        if pao2 < 50: score += 15
        elif rr < 70: score += 5
        elif rr < 80: score += 2
        else: score += 0
    elif vent == 1:
        if fio2 >= 50:
            if aad < 100: score += 0
            elif aad < 250: score += 7
            elif aad < 350: score += 9
            elif aad < 500: score += 11
            else: score += 14
        elif fio2 < 50:
            if pao2 < 50: score += 15
            elif pao2 < 70: score += 5
            elif pao2 < 80: score += 2
            else: score += 0
            
    #pH
    if ph < 7.2:
        if paco2 < 50: score += 12
        else: score == 4
    elif ph < 7.3:
        if paco2 < 30: score += 9
        elif paco2 < 40: score += 6
        elif paco2 < 50: score += 3
        else: score += 2
    elif ph < 7.35:
        if paco2 < 30: score += 9
        elif paco2 < 45: score += 0
        else: score += 1
    elif ph < 7.45:
        if paco2 < 30: score += 5
        elif paco2 < 45: score += 0
        else: score += 1
    elif ph < 7.5:
        if paco2 < 30: score += 5
        elif paco2 < 40: score += 0
        elif paco2 < 45: score += 2
        else: score += 12
    elif ph < 7.6:
        if paco2 < 40: score += 3
        else: score += 12
    else: 
        score += 12
        
    #Sodium
    if sodium < 120: score += 3
    elif sodium < 135: score += 2
    elif sodium < 155: score += 0
    else: score += 4
    
    #Urine
    if urine < 400: score += 15
    elif urine < 600: score += 8
    elif urine < 900: score += 7
    elif urine < 1500: score += 5
    elif urine < 2000: score += 4
    elif urine < 4000: score += 0
    else: score += 1 
    
    #Creatinine
    if renalfail == 0:
        if (urine < 410) & (creatinine >= 1.5): score += 10
        elif creatinine < 0.5: score += 3
        elif creatinine < 1.5: score += 0
        elif creatinine < 1.95: score += 4
        else: score += 7
    else:
        if (urine < 410) & (creatinine >= 1.5): score += 10
        elif creatinine < 0.5: score += 3
        elif creatinine < 1.5: score += 0
        elif creatinine < 1.95: score += 4
        else: score += 7
    
    #BUN
    if bun < 17: score += 0
    elif bun < 20: score += 2
    elif bun < 40: score += 7
    elif bun < 80: score += 11
    else: score += 12 
    
    #Glucose
    if glucose < 40: score += 8
    elif glucose < 60: score += 9
    elif glucose < 200: score += 0
    elif glucose < 350: score += 3
    else: score += 5 
    
    #Albumin
    if albumin < 2: score += 11
    elif albumin < 2.5: score += 6
    elif albumin < 4.5: score += 0
    else: score += 4
    
    #Bilirubin
    if bilirubin < 2: score += 0
    elif bilirubin < 3: score += 5
    elif bilirubin < 5: score += 6
    elif bilirubin < 8: score += 8
    else: score += 16
    
    #Hematocrit
    if hct < 41: score += 3
    elif hct < 50: score += 0
    else: score += 3
    
    #WBC
    if wbc < 1: score += 19
    elif wbc < 3: score += 5
    elif wbc < 20: score += 0
    elif wbc < 25: score += 1
    else: score += 5
    
    #GCS, modified slightly to allow unlikely GCS combinations.
    if (np.isnan(gcse)==True)|(np.isnan(gcsm)==True)|(np.isnan(gcsv)==True):
        score += 0
    else:
        if gcse == 1:
            if gcsm >= 5: score += 16
            elif gcsm >= 3:
                if gcsv >= 2: score += 24
                else: score += 33
            else: 
                if gcsv >= 2: score += 29
                else: score += 48
        else:
            if gcsm == 6:
                if gcsv == 5: score += 0
                elif gcsv == 4: score += 3
                elif (gcsv == 3) | (gcsv == 2): score +=10
                else: score += 15
            elif gcsm == 5:
                if gcsv == 5: score += 3
                elif gcsv == 4: score += 8
                elif (gcsv == 3) | (gcsv == 2): score +=13
                else: score += 15
            elif (gcsm == 4) | (gcsm == 3):
                if gcsv == 5: score += 3
                elif gcsv == 4: score += 13
                else: score += 24
            else:
                if gcsv == 5: score += 3
                elif gcsv == 4: score += 13
                else: score += 29

    #Age
    if age < 45: score += 0
    elif age < 60: score += 5
    elif age < 65: score += 11
    elif age < 70: score += 13
    elif age < 75: score += 16
    elif age < 85: score += 17
    else: score += 24
    
    #Chronic Conditions
    if aids == 1: score += 23
    elif hep_fail == 1: score += 16
    elif lymph == 1: score += 13
    elif metastases == 1: score += 11
    elif leuk == 1: score += 10
    elif immune == 1: score += 10
    elif cirrhosis == 1: score += 4
    else: score += 0
    
    return score

comp['ApacheIV'] = comp.apply(lambda row:
          calc_apache(row['worst_temp'],row['worst_map'],row['worst_hr'],
                      row['worst_rr'],row['first24hrs_vented'],row['worst_FiO2']
                      ,row['worst_paO2'],row['worst_paCO2'],row['worst_pH'],
                      row['worst_sodium'],row['first_24hr_urine'],
                      row['HistRenalFail'],row['worst_creatinine'],
                      row['worst_BUN'],row['worst_glucose'],
                      row['worst_albumin'],row['worst_bilirubin'],
                      row['worst_Hct'],row['worst_WBC'],
                      row['worst_eye'],row['worst_verbal'],row['worst_motor'],
                      row['Age'],row['HistAIDS'],row['HistLiverFail'],
                      row['HistLymphoma'],row['HistMetastases'],
                      row['HistLeukemia'],row['HistImmuneSuppr'],
                      row['HistCirrhosis']),axis=1)

feature = comp[['stay_id','ApacheIV']]
feature.to_csv('APACHE_IV_MIMICIV.csv')

feature['ApacheIV'].hist()
calc = time() - start
