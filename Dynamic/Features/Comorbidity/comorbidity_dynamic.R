library(comorbidity)
library(tidyverse)
library(magrittr)
`%notin%` <- Negate(`%in%`)

# Read in diagnosis table
diagnosis = read.csv('../EICU Data/diagnosis.csv')

# Loop over files
lead_hours = c("1","3","6","12")
obs_hours = c("1","3","6")

for (lead in lead_hours) {
  for (obs in obs_hours) {
    # Read in patients and select codes
    pids = read.csv(paste0('../Dynamic Data/relative_',lead,'hr_lead_',obs,'hr_obs_data_set.csv')) %>%
      select(patientunitstayid, end)
    relevant_diagnosis = merge(diagnosis, pids, by='patientunitstayid') %>%
      filter(diagnosisoffset <= end) %>%
      select(patientunitstayid, icd9code)
    
    # Split codes into ICD9 and ICD10
    icd9_codes <- separate(data = relevant_diagnosis, col = icd9code, into = c("icd9", "icd10"), sep = ",") %>%
      select(patientunitstayid, icd9) %>%
      filter(icd9 != "") %>%
      drop_na
    icd9_codes <- icd9_codes[order(icd9_codes$patientunitstayid), ]
    
    # Generate Elixhauser Comorbidity Index
    elixhauser_icd9 <- comorbidity(x = icd9_codes, id = "patientunitstayid", code = "icd9", score = "elixhauser", icd = "icd9", assign0 = FALSE)
    
    # Save to csv
    write.csv(elixhauser_icd9, paste0('relative_',lead,'hr_lead_',obs,'hr_obs_comorbidity.csv'))
  }
}