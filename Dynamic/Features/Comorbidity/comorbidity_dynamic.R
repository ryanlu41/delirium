#PUlls comorbidity data for the over time risk diagram. 

# Run time: a few minutes. 

library(comorbidity)
library(tidyverse)
library(magrittr)
library(data.table)
`%notin%` <- Negate(`%in%`)

# Read in diagnosis table
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
diagnosis = read.csv('../../../eicu/diagnosis.csv',stringsAsFactors=FALSE)

# Set up lead hours and obs hours.
lead_hours = c("0", "1", "3", "6", "12")
obs_hours = c("1","3","6")
for (lead in lead_hours) {
  for (obs in obs_hours) {
    
    # Read in patients and select codes
    pids = read.csv(paste0('../../Dataset/relative_',lead,'hr_lead_',obs,'hr_obs_data_set.csv'),stringsAsFactors=FALSE) %>%
      select(patientunitstayid, end)
    # Make the index a new unique column. 
    setDT(pids, keep.rownames = 'id')[]
    pids$id = as.integer(pids$id)
    
    # Remove data where diagnosis was before the relevant time period. 
    relevant_diagnosis = merge(diagnosis, pids, by='patientunitstayid', all.x = TRUE) %>%
      filter(diagnosisoffset <= end) %>%
      select(id, icd9code)
    
    # Split codes into ICD9 and ICD10
    icd9_codes <- separate(data = relevant_diagnosis, col = icd9code, into = c("icd9", "icd10"), sep = ",") %>%
      select(id, icd9) %>%
      filter(icd9 != "") %>%
      drop_na
    icd9_codes <- icd9_codes[order(icd9_codes$id), ]
    
    # Generate Elixhauser Comorbidity Index
    elixhauser_icd9 <- comorbidity(x = icd9_codes, id = "id", code = "icd9", score = "elixhauser", icd = "icd9", assign0 = FALSE)

    # Add patientunitstayids back on and drop other columns.
    elixhauser_icd9 = merge( pids, elixhauser_icd9, by = 'id', all.x = TRUE)
    elixhauser_icd9 = subset(elixhauser_icd9, select = -c(id,index,end))
    
    
    # Save to csv
    write.csv(elixhauser_icd9, paste0('relative_',lead,'hr_lead_',obs,'hr_obs_comorbidity.csv'), row.names = FALSE)
    
  }
}

