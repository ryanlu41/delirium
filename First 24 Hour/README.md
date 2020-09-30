These are the instructions to re-create the First 24 Hour Predictive Model. 

This model takes data from the first 24 hours of ICU data, and tries to use it to predict delirium onset at any point later in the ICU stay. The code in this repository was run in various computers, and so filepaths to data CSVs from eICU or newly created files may vary. Code is generally done in Python, but occasionally uses SQL or R. 

To begin:
1. Create the full data set (all patients that had any delirium testing/diagnoses marked) (27,939 pat unit stays). Use the SQL code in Dataset, and save results as CSV. 
2. Create the feature space using the code in "Features".
