CREATE TABLE "subtypingdataset" AS 

--Patientstayids with longer than 12 hours ICU stay, at least one positive delirium test, or a positive diagnosis of delirium
--First level of inclusion criteria

SELECT 
	"del"."patientunitstayid",
	1 "delirium?"
FROM
(SELECT DISTINCT 
	"nurse"."patientunitstayid"
FROM "eicu_crd"."nursecharting" "nurse"
WHERE
("nurse"."nursingchartcelltypevalname" = 'Delirium Score'  AND ("nurse"."nursingchartvalue" = 'Yes' OR "nurse"."nursingchartvalue" = 'YES' OR public.convert_to_integer("nurse"."nursingchartvalue") >= 4 ))
UNION
SELECT DISTINCT
	"diag"."patientunitstayid"
FROM "eicu_crd"."diagnosis" "diag"
WHERE "diag"."diagnosisstring" = 'neurologic|altered mental status / pain|delirium'
) "del"

--Gets rid of any patient stay IDs with less than 12 hour stays
INNER JOIN 
(SELECT
	"pat"."patientunitstayid"
FROM "eicu_crd"."patient" "pat"
WHERE "pat"."unitdischargeoffset" > 720) "LOS"
ON "del"."patientunitstayid" = "LOS"."patientunitstayid"


--7 min run time
--7267 rows returned