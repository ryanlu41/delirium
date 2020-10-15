# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 14:44:53 2020

Pulls whether history was marked for a patient, for each different history
option in eICU's pasthistory table.

@author: Kirby
"""
import numpy as np
import pandas as pd

# read in lists of history paths, and names of lists
paths = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\HistoryFeatureLists.csv")
pathlistlist = paths.values.astype(str).tolist()

names = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\HistoryListNames.csv")
nameslist = names.values.astype(str).tolist()
nameslist = [item for sublist in nameslist for item in sublist]

# import in all history data
hist = pd.read_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\eicu\pastHistory.csv")
hist = hist.drop(columns=['pasthistoryid','pasthistoryoffset','pasthistoryenteredoffset','pasthistorynotetype','pasthistoryvalue','pasthistoryvaluetext'])

# only keep data with relevant patient unit stay ids
comp = pd.read_csv('complete_patientstayid_list.csv')
complist = comp.values.astype(str).tolist()
complist = [item for sublist in complist for item in sublist]
compHist = hist[hist['patientunitstayid'].isin(complist)]

# for each path list, check if there are rows for it. If there are, mark it as such. 
features = comp
#TO DO make this so it simultaneously loops through both, not nested.
for counter in range(0,len(nameslist)):
    # keep rows with relevant paths
    tempHist = compHist[compHist['pasthistorypath'].isin(pathlistlist[counter])]
    tempHist = tempHist.drop(columns=['pasthistorypath'])
    tempHist = tempHist.drop_duplicates()
    tempHistList = tempHist.values.astype(str).tolist()
    tempHistList = [item for sublist in tempHistList for item in sublist]
    features[nameslist[counter]] = (features['PatientStayID'].isin(tempHistList)).to_frame()
        
features.to_csv(r"C:\Users\Kirby\OneDrive\JHU\Precision Care Medicine\HistoryFeatures.csv")