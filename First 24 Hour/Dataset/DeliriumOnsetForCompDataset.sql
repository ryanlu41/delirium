SELECT data."patientunitstayid"
	,"min" "DeliriumStartOffset"
FROM "eicu_crd"."DeliriumOnsetTimes" onset
RIGHT OUTER JOIN eicu_crd.completedataset data
ON onset.patientunitstayid = data.patientunitstayid
