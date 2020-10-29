This folder is for features extracted from the NurseCharting table in eICU. 

Relevant features include GCS scoring, and temperature. 

MeanGCSFirst24hrs.py generates GCS features, including motor, verbal, and eye subscores, as well as totals. It looks over the first 24 hours of ICU stay data, and finds the mean score. 

TemperatureFirst24Hours.py generates mean, min, and max temperature features, again looking over temperature data from the first 24 hours of ICU stay data. 

ComaFirst24Hours.py generates mean, min, and max RASS scores of the first 24 hours. It also checks if the patient was in a coma in their first 24 hours, as indicated by RASS scores of -4 or -5.

The outputs of this code has also been included in this repo. 
