library(comorbidity)
library(tidyverse)
library(magrittr)
`%notin%` <- Negate(`%in%`)

# Read in diagnosis table
diagnosis = read.csv('../EICU Data/diagnosis.csv')

# Read in patients and select codes
pids = read.csv('complete_patientstayid_list.csv') %>%
  rename(patientunitstayid = PatientStayID) %>%
  select(patientunitstayid)
relevant_diagnosis = merge(diagnosis, pids, by='patientunitstayid') %>%
  select(patientunitstayid, icd9code)

# Split codes into ICD9 and ICD10
icd9_codes <- separate(data = relevant_diagnosis, col = icd9code, into = c("icd9", "icd10"), sep = ",") %>%
  select(patientunitstayid, icd9) %>%
  filter(icd9 != "") %>%
  drop_na
icd9_codes <- icd9_codes[order(icd9_codes$patientunitstayid), ]
icd10_codes <- separate(data = relevant_diagnosis, col = icd9code, into = c("icd9", "icd10"), sep = ",") %>%
  select(patientunitstayid, icd10) %>%
  filter(icd10 != "") %>%
  drop_na
icd10_codes <- icd10_codes[order(icd10_codes$patientunitstayid), ]

# Generate Elixhauser Comorbidity Index
elixhauser_icd9 <- comorbidity(x = icd9_codes, id = "patientunitstayid", code = "icd9", score = "elixhauser", icd = "icd9", assign0 = FALSE)
elixhauser_icd10 <- comorbidity(x = icd10_codes, id = "patientunitstayid", code = "icd10", score = "elixhauser", icd = "icd10", assign0 = FALSE)

# Find which patients only have comorbidity indices from ICD9
missing = elixhauser_icd9[which(elixhauser_icd9$patientunitstayid %notin% elixhauser_icd10$patientunitstayid), ]

# Save to csv
write.csv(elixhauser_icd9, 'complete_24hr_comorbidity.csv')