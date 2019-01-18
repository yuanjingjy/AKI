create materialized view yj_diuretic_cv as
  WITH timeinfo AS (
      SELECT yaf.icustay_id, yaf.akistarttime AS t2, (yaf.akistarttime - '24:00:00' :: interval hour) AS t1
      FROM mimiciii.yj_aki_finaleigen yaf
  ), diuretic_ic AS (
      SELECT tin.icustay_id,
             max(
               CASE
                 WHEN (ic.amount >= (0) :: double precision) THEN 1
                 ELSE 0
                   END) OVER (PARTITION BY tin.icustay_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS diuretic_ic
      FROM (timeinfo tin
          LEFT JOIN mimiciii.inputevents_cv ic ON ((tin.icustay_id = ic.icustay_id)))
      WHERE (((ic.charttime >= tin.t1) AND (ic.charttime <= tin.t2)) AND
             (ic.itemid = ANY (ARRAY[42743, 30123, 40327, 41732, 40030, 42895, 45184, 45211, 45216, 41632, 41408])))
  ), diuretic_cv AS (
      SELECT tin1.icustay_id, di.diuretic_ic
      FROM (timeinfo tin1
          LEFT JOIN diuretic_ic di ON ((tin1.icustay_id = di.icustay_id)))
  )
  SELECT DISTINCT diuretic_cv.icustay_id, diuretic_cv.diuretic_ic
  FROM diuretic_cv;

alter materialized view yj_diuretic_cv
  owner to postgres;

