--Prediction Dataset (Union of Boxes 2.2 and 3.2 in diagram)

--Box 2.2
--Patients with no postive delirium tests, but at least one test and were in ICU for more than 12 hours
SELECT --DISTINCT doesn't change anything
	"del"."patientunitstayid",
	'0'
FROM 
--Pulls patients with a delirium test
(SELECT DISTINCT 
	"nurse"."patientunitstayid"
FROM "eicu_crd"."nursecharting" "nurse"
WHERE
("nurse"."nursingchartcelltypevalname" = 'Delirium Score')) "del"

--Gets rid of any patient stay IDs with less than 12 hour stays
INNER JOIN 
(SELECT
	"pat"."patientunitstayid"
FROM "eicu_crd"."patient" "pat"
WHERE "pat"."unitdischargeoffset" > 1440) "LOS"
ON "del"."patientunitstayid" = "LOS"."patientunitstayid"

--Removes any patient stay IDs with positive delirium test results, or with positive diagnoses
EXCEPT 
SELECT 
	"del"."patientunitstayid",
	'0'
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
--20672 rows returned
--6 min run time

UNION

--Box 3.2
--Patients with + Delirium test or diagnosis after 12 hours in the ICU
SELECT 
	"del"."patientunitstayid",
	'1'
FROM
--pulls all stays with positive scale or diagnosis after 12 hours in ICU.
(SELECT DISTINCT 
	"nurse"."patientunitstayid"
FROM "eicu_crd"."nursecharting" "nurse"
WHERE
("nurse"."nursingchartcelltypevalname" = 'Delirium Score'  
AND ("nurse"."nursingchartvalue" = 'Yes' OR "nurse"."nursingchartvalue" = 'YES' OR public.convert_to_integer("nurse"."nursingchartvalue") >= 4 )
AND "nurse"."nursingchartoffset" >= 1440)
UNION
SELECT DISTINCT
	"diag"."patientunitstayid"
FROM "eicu_crd"."diagnosis" "diag"
WHERE "diag"."diagnosisstring" = 'neurologic|altered mental status / pain|delirium'
AND "diag"."diagnosisoffset" >= 1440
) "del"

--ADD IN  AND "nurse"."nursingchartoffset" >= 720 and "diag"."diagnosisoffset" >= 720

--Gets rid of any patient stay IDs with less than 12 hour stays
INNER JOIN 
(SELECT
	"pat"."patientunitstayid"
FROM "eicu_crd"."patient" "pat"
WHERE "pat"."unitdischargeoffset" > 1440) "LOS"
ON "del"."patientunitstayid" = "LOS"."patientunitstayid"

--5467 rows returned
--7 min run time

--Total run time of the whole query: 10 min
--Rows returned of the whole query: 26,561

--Removes rows for which initial delirium onset was in the first 12 hours.
EXCEPT 
SELECT stamps.patientunitstayid
	,'1'
FROM
(SELECT stamps."patientunitstayid"
	,MIN("nursingchartoffset") "firstonset" 
FROM "eicu_crd"."DeliriumTimeStamps" "stamps"
GROUP BY stamps.patientunitstayid) "stamps"
WHERE stamps.firstonset <=720