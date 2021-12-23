# -*- coding: utf-8 -*-
"""
Created on Sun July 19 13:14:56 2020

This code is meant to find out how many ICU discharges there were per hospital,
per year and draw statistics about those numbers. 

Run time: 1 second

@author: Kirby
"""
#%% Package Setup
import numpy as np
import pandas as pd
from pathlib import Path
from time import time
import matplotlib.pyplot as plt

file_path = Path(__file__)
eicu_path = file_path.parent.parent.parent.joinpath('eicu')

start = time()

#%% Inputs

#Pulls patient data. 
pat = pd.read_csv(eicu_path.joinpath("patient.csv"),
                  usecols = ['patientunitstayid', 'hospitalid',
                             'hospitaldischargeyear'])

counts = pat.groupby(['hospitalid','hospitaldischargeyear']).count()

plt.figure()
plt.xlabel('ICU discharges per year per hospital')
plt.ylabel('# of hospitals in bin')
counts['patientunitstayid'].hist()

median = counts['patientunitstayid'].median()
iqr_low = counts['patientunitstayid'].quantile(0.25)
iqr_high = counts['patientunitstayid'].quantile(0.75)
mean = counts['patientunitstayid'].mean()
std = counts['patientunitstayid'].std()
