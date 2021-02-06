library(comorbidity)
library(tidyverse)
library(magrittr)
`%notin%` <- Negate(`%in%`)

# Read in diagnosis table
diagnosis = read.csv('DIAGNOSES_ICD.csv')

lead_hours = 12 # 1,3,6,12
obs_hours = 12 # 1,3,6,12
infile = paste0('../Dynamic Data/MIMIC/MIMIC_relative_',lead_hours,'hr_lead_',obs_hours,'hr_obs_data_set.csv')
outfile = paste0('MIMIC_relative_',lead_hours,'hr_lead_',obs_hours,'hr_obs_comorbidity.csv')
pids = read.csv(infile) %>%
  select(HADM_ID, ICUSTAY_ID) %>%
  arrange(HADM_ID)
relevant_diagnosis = merge(diagnosis, pids, by='HADM_ID') %>%
  arrange(HADM_ID,SEQ_NUM) %>%
  select(-ROW_ID)

elixhauser_icd9 <- comorbidity(x = relevant_diagnosis, id = "ICUSTAY_ID", code = "ICD9_CODE", score = "elixhauser", icd = "icd9", assign0 = FALSE)

# For relative data
write.csv(elixhauser_icd9, outfile)
