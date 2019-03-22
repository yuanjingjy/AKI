create materialized view yj_akicohort_xiugaiban as
  WITH includeid AS (
      SELECT yae.icustay_id,
             yae.subject_id,
             yae.intime,
             yae.outtime,
             yae.age,
             yad.hadm_id,
             yad.akistage,
             yad.charttime,
             yad.akiorder,
             yad.creat,
             yad.cre_count,
             yad.cre_order,
             yad.aki_stage_order
      FROM (mimiciii.yj_aki_exclusion yae
          LEFT JOIN mimiciii.yj_akidefination_xiugaiban yad ON ((yae.icustay_id = yad.icustay_id)))
      WHERE ((yae.ex_age = 0) AND (yae.ex_los = 0) AND (yae.ex_hos = 0) AND (yae.ex_icufirst = 0) AND
             (yae.ex_crenum = 0) AND (yae.ex_crevalue = 0))
  ), aki_all AS (
      SELECT DISTINCT includeid.icustay_id,
                      includeid.subject_id,
                      includeid.intime,
                      includeid.outtime,
                      includeid.age,
                      includeid.hadm_id,
                      includeid.akistage,
                      includeid.charttime,
                      includeid.akiorder,
                      includeid.creat,
                      includeid.cre_count,
                      includeid.cre_order,
                      includeid.aki_stage_order,
                      row_number() OVER (PARTITION BY includeid.icustay_id ORDER BY includeid.charttime) AS icuakinum
      FROM includeid
      WHERE (includeid.akistage <> 0)
  ), aki_cohort AS (
      SELECT DISTINCT includeid.icustay_id,
                      includeid.subject_id,
                      includeid.intime,
                      includeid.outtime,
                      includeid.age,
                      includeid.hadm_id,
                      includeid.akistage,
                      includeid.charttime,
                      includeid.akiorder,
                      includeid.creat,
                      includeid.cre_count,
                      includeid.cre_order,
                      includeid.aki_stage_order,
                      row_number() OVER (PARTITION BY includeid.icustay_id ORDER BY includeid.charttime) AS icuakinum
      FROM includeid
      WHERE ((includeid.akistage <> 0) AND (includeid.charttime > (includeid.intime + '24:00:00' :: interval hour)))
  ), nonaki_cohort AS (
      SELECT DISTINCT includeid.icustay_id,
                      includeid.subject_id,
                      includeid.intime,
                      includeid.outtime,
                      includeid.age,
                      includeid.hadm_id,
                      includeid.akistage,
                      includeid.charttime,
                      includeid.akiorder,
                      includeid.creat,
                      includeid.cre_count,
                      includeid.cre_order,
                      includeid.aki_stage_order,
                      row_number() OVER (PARTITION BY includeid.icustay_id ORDER BY includeid.charttime DESC) AS icuakinum
      FROM includeid
      WHERE ((NOT (includeid.icustay_id IN (SELECT aki_all.icustay_id FROM aki_all))) AND (includeid.cre_order = 1) AND
             (includeid.charttime > (includeid.intime + '24:00:00' :: interval hour)))
  )
  SELECT aki_cohort.icustay_id,
         aki_cohort.subject_id,
         aki_cohort.intime,
         aki_cohort.outtime,
         aki_cohort.age,
         aki_cohort.hadm_id,
         aki_cohort.akistage,
         aki_cohort.charttime,
         aki_cohort.akiorder,
         aki_cohort.creat,
         aki_cohort.cre_count,
         aki_cohort.cre_order,
         aki_cohort.aki_stage_order,
         aki_cohort.icuakinum
  FROM aki_cohort
  WHERE (aki_cohort.icuakinum = 1)
  UNION
  SELECT nonaki_cohort.icustay_id,
         nonaki_cohort.subject_id,
         nonaki_cohort.intime,
         nonaki_cohort.outtime,
         nonaki_cohort.age,
         nonaki_cohort.hadm_id,
         nonaki_cohort.akistage,
         nonaki_cohort.charttime,
         nonaki_cohort.akiorder,
         nonaki_cohort.creat,
         nonaki_cohort.cre_count,
         nonaki_cohort.cre_order,
         nonaki_cohort.aki_stage_order,
         nonaki_cohort.icuakinum
  FROM nonaki_cohort
  WHERE (nonaki_cohort.icuakinum = 1);

alter materialized view yj_akicohort_xiugaiban
  owner to postgres;

