CREATE TABLE "DeliriumOnsetTimes" AS 
SELECT "patientunitstayid"
	,MIN("nursingchartoffset")
FROM "eicu_crd"."DeliriumTimeStamps" 
GROUP BY patientunitstayid
ORDER BY patientunitstayid