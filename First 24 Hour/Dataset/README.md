We originally did dataset generation in POSTGRESQL, but later decided to re-create it in Python, just to keep the repo as much in Python as possible. 

There's two scripts in here:
1. CreateCompletePatientStayID.py generates a list of all patient stays we might ever use, based on if they had CAM-ICU, ICDSC, or any diagnosis of delirium, and were in the ICU for at least 12 hours. Many feature generation scripts use this, since we figured we'd be using some of the features for clustering work as well.
2. CreateFirst24hrDataset.py generates a list of patient stays that were used in the First 24 Hour prediction model, based only on those patients with CAM-ICUs or ICDSCs, and were in the ICU for at least 24 hours, and developed delirium after 24 hours in the ICU. 


You probably don't need the SQL code for anything, but the original SQL code has also been included, in case you want it for some reason. 

Originally, we first set up a POSTGRESQL server with all of eICU stored on it. Then we ran:

1. CreatTimeStampsTable.sql
2. DeliriumOnsetTimes.sql

Which create tables containing the first delirium onset times for all patients that had info. 

PredictionDataset.sql creates the dataset for this model, including labels of 1 for delirium being present during the ICU stay, and 0 meaning there was never delirium. 
