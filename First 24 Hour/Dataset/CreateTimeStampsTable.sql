CREATE TABLE "DeliriumTimeStamps" AS

SELECT 
	"nurse"."patientunitstayid",
	"nurse"."nursingchartoffset" 
FROM "eicu_crd"."nursecharting" "nurse"
WHERE
("nurse"."nursingchartcelltypevalname" = 'Delirium Score'  
AND ("nurse"."nursingchartvalue" = 'Yes' OR "nurse"."nursingchartvalue" = 'YES' OR public.convert_to_integer("nurse"."nursingchartvalue") >= 4 ))

UNION
SELECT
	"diag"."patientunitstayid",
	"diag"."diagnosisoffset"
FROM "eicu_crd"."diagnosis" "diag"
WHERE "diag"."diagnosisstring" = 'neurologic|altered mental status / pain|delirium'