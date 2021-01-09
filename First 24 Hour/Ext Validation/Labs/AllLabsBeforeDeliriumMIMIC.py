# -*- coding: utf-8 -*-
"""
Created on Mon Nov 2 16:11:04 2020

This code isolates all the labs before delirium, just to speed up 
processing time. This is the version for MIMIC.
It calls the function in LabsBeforeDeliriumMIMIC.py.
Then run the relevant lab feature extraction code.

@author: Kirby
"""
import pandas as pd
from pathlib import Path
import LabsBeforeDeliriumMIMIC as lbd

file_path = Path(__file__)
parent_path = file_path.parent

all_lab_names = pd.read_csv("LabsListMIMIC.csv")
all_lab_names_list = all_lab_names.values.tolist()
all_lab_names_list = [item for sublist in all_lab_names_list for item in sublist]
for lab_name in all_lab_names_list:
    labs_before_del = lbd.labs_before_delirium(lab_name)
    labs_before_del.to_csv(parent_path.joinpath("AllLabsBeforeDeliriumMIMIC",lab_name + ".csv"),index=False)
