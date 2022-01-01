import gzip
from pathlib import Path, PurePath
import glob
import os
import numpy as np
import csv
import pandas as pd
import time
import matplotlib.pyplot as plt

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.parent.joinpath('eicu')
print(dataset_path)

lab_file = pd.read_csv(dataset_path.joinpath('lab.csv.gz'), compression='gzip')
lab_prev = lab_file['labname'].value_counts().rename('Total Prevalence').sort_index()

lab_del_file = pd.read_csv(dataset_path.joinpath('lab_delirium.csv'))
lab_del_prev = lab_del_file['labname'].value_counts().rename('Delirium Prevalence').sort_index()


df = pd.concat([lab_prev, lab_del_prev], axis=1)

df.to_csv(file_path.joinpath('LabPrevalence.csv'))


used_list = pd.read_csv(file_path.joinpath('LabsList.csv'))
used_prev = pd.DataFrame()

for i in used_list['Lab Name']:
    try: 
        used_prev = used_prev.append(df.loc[i].copy())
        df = df.drop(i, axis=0)
    except: 
        continue

df.to_csv(file_path.joinpath('UnusedLabPrev.csv'))
used_prev.to_csv(file_path.joinpath('UsedLabPrev.csv'))
# df.to_csv(file_path.joinpath('UnusedLabPrev.csv.gz'), compression='gzip')
# used_prev.to_csv(file_path.joinpath('UsedLabPrev.csv.gz'), compression='gzip')