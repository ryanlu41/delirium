# -*- coding: utf-8 -*-
"""
Created on Thu Dec 23 10:52:08 2021

Calculate median and IQR time between delirium scoring on the same patient in eICU. 

Runtime:

@author: Kirby
"""

#%% Import packages.
import numpy as np
import pandas as pd
import os
#import multiprocessing as mp
import time
from pathlib import Path

start_timer = time.time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.joinpath('Dataset')
eicu_path = file_path.parent.parent.parent.parent.joinpath('eicu')

#%% Get times between delirium testing. 
tests = pd.read_csv(dataset_path.joinpath('AllDeliriumTests.csv'))
tests['last_stayid'] = tests['patientunitstayid'].shift(periods = 1)
tests['last_offset'] = tests['offset'].shift(periods = 1)

def time_since_last_test(curr_id, last_id, curr_test, last_test):
    if curr_id == last_id:
        return curr_test - last_test
    else:
        return np.nan
    
tests['time_since_last_test'] = tests.apply(
    lambda row: time_since_last_test(row['patientunitstayid'], row['last_stayid'], 
                                     row['offset'], row['last_offset']), axis = 1)

# Make it hours instead of minutes. 
tests['time_since_last_test'] = tests['time_since_last_test']/60

median = tests['time_since_last_test'].median()
iqr_low = tests['time_since_last_test'].quantile(0.25)
iqr_high = tests['time_since_last_test'].quantile(0.75)
mean = tests['time_since_last_test'].mean()
std = tests['time_since_last_test'].std()