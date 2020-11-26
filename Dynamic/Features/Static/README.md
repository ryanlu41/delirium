This file is identical to the 24 hour model's since static features don't change over the course of the ICU stay. 

This folder includes features that were static, resulting in just one value per ICU stay without further processing. These include demographic information like age or ethnicity, characteristics about each ICU or hospital stay, etc. 

Just run StaticFeatureFirst24Hrs.py, which extracts a host of features from the Patient and Hospital tables, and the apache IVa score from the ApachePatientResult table.

Notes on processing:
An age of >89 was converted to 90. 
Hospital traits were converted from categorical strings to categorical numbers. 

For numbedscategory:
- less than 100 : 1
- 100 - 249 : 2
- 250 - 499 : 3
- greater than 500 : 4

For teaching status:
- t : 1
- f : 0

For region:
- West : 1
- Midwest : 2
- Northeast : 3
- South : 4
