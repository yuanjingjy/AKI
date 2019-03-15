set search_path to eicu

-- create materialized view yj_akidefination_eicu as
  WITH pid AS (
      SELECT yj_aki_exclusion_eicu.patientunitstayid
      FROM eicu.yj_aki_exclusion_eicu
      WHERE ((yj_aki_exclusion_eicu.exclu_age = 0) AND (yj_aki_exclusion_eicu.exclu_icuduration = 0) AND
             (yj_aki_exclusion_eicu.exclu_icu = 0) AND (yj_aki_exclusion_eicu.exclu_admit = 0) AND
             (yj_aki_exclusion_eicu.exclu_crenum = 0) AND (yj_aki_exclusion_eicu.exclu_crevalue = 0) AND
             (yj_aki_exclusion_eicu.exclu_admittime = 0))
  ), cre_all AS (
      SELECT DISTINCT pid.patientunitstayid, pl.chartoffset, pl.creatinine
      FROM pid
          LEFT JOIN eicu.pivoted_lab pl ON pid.patientunitstayid = pl.patientunitstayid  AND
                                             pl.chartoffset >= '-6' :: integer * 60 AND
                                              pl.chartoffset <= ((7 * 24) * 60)
      and pl.creatinine IS NOT NULL
  ), cre_base_tmp AS (
      SELECT ca.patientunitstayid,
             ca.chartoffset,
             ca.creatinine,
             row_number() OVER (PARTITION BY ca.patientunitstayid ORDER BY ca.chartoffset) AS cre_order
      FROM cre_all ca
  ), cre_basevalue AS (
      SELECT cre_base_tmp.patientunitstayid, cre_base_tmp.chartoffset, cre_base_tmp.creatinine AS cre_base
      FROM cre_base_tmp
      WHERE (cre_base_tmp.cre_order = 1)
  ), creatinine_base AS (
      SELECT ca2.patientunitstayid,
             ca2.chartoffset,
             ca2.creatinine,
             cb.cre_base,
             min(ca2.creatinine) OVER (PARTITION BY cb.patientunitstayid) AS cre_lowest
      FROM (cre_all ca2
          LEFT JOIN cre_basevalue cb ON ((ca2.patientunitstayid = cb.patientunitstayid)))
  ), cre_diff AS (
      SELECT cb2.patientunitstayid,
             cb2.chartoffset,
             cb2.creatinine,
             cb2.cre_base,
             cb2.cre_lowest,
             (cb2.creatinine - cb2.cre_lowest)           AS crediff,
             (cb2.creatinine / (cb2.cre_base + 0.00001)) AS rv
      FROM creatinine_base cb2
  ), akistage AS (
      SELECT cd.patientunitstayid,
             cd.chartoffset,
             cd.creatinine,
             cd.cre_base,
             cd.cre_lowest,
             cd.crediff,
             cd.rv,
             CASE
               WHEN ((cd.rv < 1.5) AND (cd.crediff >= 0.3) AND (cd.chartoffset < (48 * 60))) THEN 1
               WHEN ((cd.rv < 1.5) AND (cd.crediff < 0.3)) THEN 0
               WHEN ((cd.crediff >= 1.5) AND (cd.creatinine >= (4) :: numeric)) THEN 3
               WHEN ((cd.creatinine < (4) :: numeric) AND (cd.rv >= (3) :: numeric)) THEN 3
               WHEN ((cd.creatinine < (4) :: numeric) AND (cd.rv >= (2) :: numeric) AND (cd.rv < (3) :: numeric)) THEN 2
               WHEN ((cd.creatinine < (4) :: numeric) AND (cd.rv >= 1.5) AND (cd.rv < (2) :: numeric)) THEN 1
               ELSE 0
                 END AS akistage
      FROM cre_diff cd
  ), akistageorder AS (
      SELECT aki.patientunitstayid,
             aki.chartoffset,
             aki.creatinine,
             aki.cre_base,
             aki.cre_lowest,
             aki.crediff,
             aki.rv,
             aki.akistage,
             row_number() OVER (PARTITION BY aki.patientunitstayid, aki.akistage ORDER BY aki.chartoffset) AS aki_stage_order,
             row_number() OVER (PARTITION BY aki.patientunitstayid ORDER BY aki.chartoffset DESC)          AS cre_order,
             count(aki.creatinine) OVER (PARTITION BY aki.patientunitstayid)                               AS cre_count
      FROM akistage aki
  ), akiordertmp AS (
      SELECT aso.patientunitstayid,
             aso.chartoffset,
             aso.creatinine,
             aso.cre_base,
             aso.cre_lowest,
             aso.crediff,
             aso.rv,
             aso.akistage,
             aso.aki_stage_order,
             aso.cre_order,
             aso.cre_count,
             row_number() OVER (PARTITION BY aso.patientunitstayid ORDER BY aso.chartoffset) AS akiorder
      FROM akistageorder aso
      WHERE (aso.akistage > 0)
  ), akiorder AS (
      SELECT ao1.patientunitstayid,
             ao1.chartoffset,
             ao1.creatinine,
             ao1.cre_base,
             ao1.cre_lowest,
             ao1.crediff,
             ao1.rv,
             ao1.akistage,
             ao1.aki_stage_order,
             ao1.cre_order,
             ao1.cre_count,
             ao2.akiorder
      FROM (akistageorder ao1
          LEFT JOIN akiordertmp ao2 ON (((ao1.patientunitstayid = ao2.patientunitstayid) AND
                                         (ao1.chartoffset = ao2.chartoffset))))
  )
  SELECT DISTINCT akiorder.patientunitstayid,
                  akiorder.chartoffset,
                  akiorder.creatinine,
                  akiorder.cre_base,
                  akiorder.cre_lowest,
                  akiorder.crediff,
                  akiorder.rv,
                  akiorder.akistage,
                  akiorder.aki_stage_order,
                  akiorder.cre_order,
                  akiorder.cre_count,
                  akiorder.akiorder
  FROM akiorder;




