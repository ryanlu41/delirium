# -*- coding: utf-8 -*-
"""
Created on Mon Nov 2 16:11:04 2020

This code isolates all the labs before delirium, just to speed up 
processing time. This is the version for MIMIC IV.
It calls the function in LabsBeforeDeliriumMIMICIV.py.
Then run the relevant lab feature extraction code.

@author: Kirby
"""
import pandas as pd
from pathlib import Path
import LabsBeforeDeliriumMIMICIV as lbd

file_path = Path(__file__)
parent_path = file_path.parent

all_lab_names = pd.read_excel("MIMICIV_to_eICU_Labs_List.xlsx")
all_lab_names_list = all_lab_names['labname'].drop_duplicates()
for lab_name in all_lab_names_list:
    labs_before_del = lbd.labs_before_delirium(lab_name)
    labs_before_del.to_csv(parent_path.joinpath("AllLabsBeforeDeliriumMIMICIV", lab_name + ".csv"),index=False)
