# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 16:17:19 2020

Creates a legend of drug names to HICL codes, or vice versa, which is used by
TwentyFourHrDrugFeaturesFunction.py to check medication data where the name is
missing, but HICL is present.

@author: Kirby
"""

import numpy as np
import pandas as pd

# pull relevant HICL codes
hicl = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\medication.csv")
hicl = hicl.drop(columns=['patientunitstayid','medicationid','drugorderoffset','drugstartoffset','drugivadmixture','drugordercancelled','dosage','routeadmin','frequency','loadingdose','prn','drugstopoffset','gtc'])
hicl = hicl.dropna()
hicl = hicl.drop_duplicates()
hicl.to_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\HICLlegend.csv")