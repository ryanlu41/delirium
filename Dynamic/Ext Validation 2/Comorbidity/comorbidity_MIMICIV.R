#PUlls comorbidity data for MIMIC IV.

# Run time: a few minutes. 

library(comorbidity)
library(tidyverse)
library(magrittr)
library(data.table)
`%notin%` <- Negate(`%in%`)

# Read in diagnosis table
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
diagnosis = read.csv('../../../mimic-iv-2.0/hosp/diagnoses_icd.csv.gz',stringsAsFactors=FALSE)

    
# Read in patients and select codes
pids = read.csv('../../Dataset/MIMICIV_complete_dataset.csv', stringsAsFactors=FALSE) %>%
  select(hadm_id, stay_id) %>%
  arrange(hadm_id)
rel_diag = merge(diagnosis, pids, by='hadm_id') %>%
  select(stay_id, icd_code, icd_version) %>%
  arrange(stay_id)

diag_9 = rel_diag[rel_diag$icd_version == '9', ]
diag_10 = rel_diag[rel_diag$icd_version == '10', ]

# PUll elixhauser for both icd9 and icd 10 data.
elixhauser_icd9 <- comorbidity(x = diag_9, id = "stay_id", code = "icd_code", score = "elixhauser", icd = "icd9", assign0 = FALSE)
elixhauser_icd10 <- comorbidity(x = diag_10, id = "stay_id", code = "icd_code", score = "elixhauser", icd = "icd10", assign0 = FALSE)

# Put it together, and then find the max per patient for each column.\
# Remove factor columns.
elix_both = rbind(elixhauser_icd9, elixhauser_icd10) %>%
  select(-index, -windex_ahrq, -windex_vw)

# For each column name, excluding the stay id, find the max per stay_id. 
# Attach it all to the full list of pids.
for (col_name in colnames(elix_both)){
  if (col_name == 'stay_id'){
    next
  }
  else{
    feat = aggregate(elix_both[, col_name], by = list(elix_both$stay_id), FUN = max, simplify = TRUE, drop = FALSE)
    names(feat)[names(feat) == 'Group.1'] <- 'stay_id'
    names(feat)[names(feat) == 'x'] <- col_name
  
    pids = merge(pids, feat, by = 'stay_id', all.x = TRUE)
  }
}
# Fill in missing values with 0.
pids[is.na(pids)] <- 0

# Save to csv
write.csv(pids, 'MIMIC_IV_comorbidity.csv')


