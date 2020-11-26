This code is identical to that of the 24 hour model, since patient history does not change over the ICU stay. 

This code creates the history features, which checks eICU's pasthistory table. Our clinician advisor grouped the history descriptions together, which were used to create history categories.  

Just call HistoryFeatures.py, which pulls from HistoryFeatureLists.csv, HistoryListNames.csv, to check through the pastHistory table. 

The end results flags each patientunitstayid as True or False for certain histories. 
