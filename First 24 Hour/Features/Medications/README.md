The code in this file was used to determine what medications patients received in their first 24 hours. 
It searches across the medication, infusiondrug, and treatment tables. 

Steps:
1. Put together lists of drug names to search for in medication/infusion drug, and treatment strings to search. I've included what I used (DrugNameLists * TreatmentStrings, which were constructed from online websites.
2. Run "Create HICL Drug Name Legend.py", which creates a legend to convert drug names to HICL codes or vice versa.
3. Run 24hrAllDrugFeatures.py, which calls TwentyFourHrDrugFeaturesFunction.py and generates a csv of all the medication features.
