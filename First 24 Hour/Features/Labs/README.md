To create the lab features:

1. Create a folder (I used "AllLabsBeforeDelirium") where all information about labs before delirium will be stored.
2. Create a LabsList.csv, which contains a list of the names of labs you want to pull.
3. Run AllLabsBeforeDelirium.py, which will populate the folder in step 1 with lab information that occurred before delirium onset. 
   a. eICU has 3 sources of bicarbonate information, which my code combines. Be sure to delete the individual csvs, and add to your lab list file the combined name.
4. Create another folder (I used "First24hrsFeatureData") to store the actual lab feature data.
5. Run First24hrLabFeatures.py, which will populate the folder from Step 4 with mean, min, and max lab data in the first 24 hours of ICU stay.
