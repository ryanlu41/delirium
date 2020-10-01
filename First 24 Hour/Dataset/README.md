
We first set up a POSTGRESQL server with all of eICU stored on it. Then we ran:

1. CreatTimeStampsTable.sql
2. DeliriumOnsetTimes.sql

Which contains tables containing the first delirium onset times for all patients that had info. 

PredictionDataset.sql creates the dataset for this model, including labels of 1 for delirium being present during the ICU stay, and 0 meaning there was never delirium. 
