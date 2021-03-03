library(comorbidity)
library(tidyverse)
library(magrittr)
`%notin%` <- Negate(`%in%`)

# Read in diagnosis table
diagnosis = read.csv('DIAGNOSES_ICD.csv')

infile = paste0('MIMIC_first_24hr_prediction_dataset.csv')
outfile = paste0('MIMIC_first_24hr_comorbidity.csv')
pids = read.csv(infile) %>%
  select(HADM_ID, ICUSTAY_ID) %>%
  arrange(HADM_ID)
relevant_diagnosis = merge(diagnosis, pids, by='HADM_ID') %>%
  arrange(HADM_ID,SEQ_NUM) %>%
  select(-ROW_ID)

elixhauser_icd9 <- comorbidity(x = relevant_diagnosis, id = "ICUSTAY_ID", code = "ICD9_CODE", score = "elixhauser", icd = "icd9", assign0 = FALSE)

# For relative data
write.csv(elixhauser_icd9, outfile)
