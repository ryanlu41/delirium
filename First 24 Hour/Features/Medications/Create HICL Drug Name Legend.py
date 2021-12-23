# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 16:17:19 2020

Creates a legend of drug names to HICL codes, or vice versa, which is used by
DrugFeaturesFunction.py to check medication data where the name is
missing, but HICL is present.

run time: 10 sec

@author: Kirby
"""

import numpy as np
import pandas as pd
from pathlib import Path
from time import time

start = time()

file_path = Path(__file__)
dataset_path = file_path.parent.parent.parent.joinpath("Dataset")
eicu_path = file_path.parent.parent.parent.parent.joinpath('eicu')

# pull relevant HICL codes
hicl = pd.read_csv(eicu_path.joinpath("medication.csv"),
                   usecols=['drugname','drughiclseqno'])
hicl = hicl.dropna()
hicl = hicl.drop_duplicates()
hicl.to_csv("HICLlegend.csv",index=False)

calc = time() - start