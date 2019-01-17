create materialized view yj_akidefination as
  WITH cre_all AS (
      SELECT ie.icustay_id, ie.subject_id, ie.hadm_id, ie.intime, ie.outtime, le.charttime, le.valuenum AS creat
      FROM (mimiciii.icustays ie
          LEFT JOIN mimiciii.labevents le ON ((
        (ie.subject_id = le.subject_id) AND (le.itemid = 50912) AND (le.valuenum IS NOT NULL) AND
        ((le.charttime >= (ie.intime - '06:00:00' :: interval hour)) AND
         (le.charttime <= (ie.intime + '7 days' :: interval day))))))
  ), cre_base AS (
      SELECT ca.icustay_id,
             ca.subject_id,
             ca.hadm_id,
             ca.intime,
             ca.outtime,
             ca.charttime,
             ca.creat,
             kc.admcreat     AS crebase,
             kc.admcreattime AS crebasetime,
             kc.lowcreat48hr,
             kc.lowcreat48hrtime
      FROM (cre_all ca
          LEFT JOIN mimiciii.kdigo_creat kc ON (((ca.icustay_id = kc.icustay_id) AND (ca.subject_id = kc.subject_id))))
  ), cre_diff AS (
      SELECT cb.icustay_id,
             cb.subject_id,
             cb.hadm_id,
             cb.intime,
             cb.outtime,
             cb.charttime,
             cb.creat,
             cb.crebase,
             cb.crebasetime,
             cb.lowcreat48hr,
             cb.lowcreat48hrtime,
             (cb.creat - cb.lowcreat48hr)                              AS crediff,
             (cb.creat / (cb.crebase + (0.00001) :: double precision)) AS rv
      FROM cre_base cb
  ), akistage AS (
      SELECT cd.icustay_id,
             cd.subject_id,
             cd.hadm_id,
             cd.intime,
             cd.outtime,
             cd.charttime,
             cd.creat,
             cd.crebase,
             cd.crebasetime,
             cd.lowcreat48hr,
             cd.lowcreat48hrtime,
             cd.crediff,
             cd.rv,
             CASE
               WHEN ((cd.rv < (1.5) :: double precision) AND (cd.crediff >= (0.3) :: double precision) AND
                     (cd.charttime > cd.lowcreat48hrtime) AND
                     (cd.charttime < (cd.intime + '48:00:00' :: interval hour))) THEN 1
               WHEN ((cd.rv < (1.5) :: double precision) AND (cd.crediff < (0.3) :: double precision)) THEN 0
               WHEN ((cd.crediff >= (1.5) :: double precision) AND (cd.creat >= (4) :: double precision)) THEN 3
               WHEN ((cd.creat < (4) :: double precision) AND (cd.rv >= (3) :: double precision)) THEN 3
               WHEN ((cd.creat < (4) :: double precision) AND (cd.rv >= (2) :: double precision) AND
                     (cd.rv < (3) :: double precision)) THEN 2
               WHEN ((cd.creat < (4) :: double precision) AND (cd.rv >= (1.5) :: double precision) AND
                     (cd.rv < (2) :: double precision)) THEN 1
               ELSE 0
                 END AS akistage
      FROM cre_diff cd
  ), akistageorder AS (
      SELECT aki.icustay_id,
             aki.subject_id,
             aki.hadm_id,
             aki.intime,
             aki.outtime,
             aki.charttime,
             aki.creat,
             aki.crebase,
             aki.crebasetime,
             aki.lowcreat48hr,
             aki.lowcreat48hrtime,
             aki.crediff,
             aki.rv,
             aki.akistage,
             row_number() OVER (PARTITION BY aki.icustay_id, aki.subject_id, aki.hadm_id, aki.akistage ORDER BY aki.charttime) AS aki_stage_order,
             row_number() OVER (PARTITION BY aki.icustay_id, aki.subject_id, aki.hadm_id ORDER BY aki.charttime DESC)          AS cre_order,
             count(aki.creat) OVER (PARTITION BY aki.icustay_id, aki.subject_id)                                               AS cre_count
      FROM akistage aki
  ), akiordertmp AS (
      SELECT akistageorder.icustay_id,
             akistageorder.subject_id,
             akistageorder.hadm_id,
             akistageorder.intime,
             akistageorder.outtime,
             akistageorder.charttime,
             akistageorder.creat,
             akistageorder.crebase,
             akistageorder.crebasetime,
             akistageorder.lowcreat48hr,
             akistageorder.lowcreat48hrtime,
             akistageorder.crediff,
             akistageorder.rv,
             akistageorder.akistage,
             akistageorder.aki_stage_order,
             akistageorder.cre_order,
             akistageorder.cre_count,
             row_number() OVER (PARTITION BY akistageorder.icustay_id, akistageorder.subject_id, akistageorder.hadm_id ORDER BY akistageorder.charttime) AS akiorder
      FROM akistageorder
      WHERE (akistageorder.akistage > 0)
  ), akiorder AS (
      SELECT ao1.icustay_id,
             ao1.subject_id,
             ao1.hadm_id,
             ao1.intime,
             ao1.outtime,
             ao1.charttime,
             ao1.creat,
             ao1.crebase,
             ao1.crebasetime,
             ao1.lowcreat48hr,
             ao1.lowcreat48hrtime,
             ao1.crediff,
             ao1.rv,
             ao1.akistage,
             ao1.aki_stage_order,
             ao1.cre_order,
             ao1.cre_count,
             ao2.akiorder
      FROM (akistageorder ao1
          LEFT JOIN akiordertmp ao2 ON (((ao1.icustay_id = ao2.icustay_id) AND (ao1.charttime = ao2.charttime))))
  )
  SELECT akiorder.icustay_id,
         akiorder.subject_id,
         akiorder.hadm_id,
         akiorder.intime,
         akiorder.outtime,
         akiorder.charttime,
         akiorder.creat,
         akiorder.crebase,
         akiorder.crebasetime,
         akiorder.lowcreat48hr,
         akiorder.lowcreat48hrtime,
         akiorder.crediff,
         akiorder.rv,
         akiorder.akistage,
         akiorder.aki_stage_order,
         akiorder.cre_order,
         akiorder.cre_count,
         akiorder.akiorder
  FROM akiorder;

alter materialized view yj_akidefination
  owner to postgres;

