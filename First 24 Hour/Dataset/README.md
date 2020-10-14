We originally did dataset generation in POSTGRESQL, but later decided to re-create it in Python, just to keep the repo as much in Python as possible. 

There's two scripts in here:
1. CreateCompletePatientStayID.py generates a list of all patient stays we might ever use, based on if they had CAM-ICU, ICDSC, or any diagnosis of delirium, and were in the ICU for at least 12 hours. Many feature generation scripts use this, since we figured we'd be using some of the features for clustering work as well.
2. CreateFirst24hrDataset.py generates a list of patient stays that were used in the First 24 Hour prediction model. These stays had with CAM-ICUs or ICDSCs, and were in the ICU for at least 24 hours, and only developed delirium (identified by diagnoses or delrium scoring) at or after 24 hours in the ICU. 
