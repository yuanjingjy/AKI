create materialized view yj_diuretic_mv as
  WITH timeinfo AS (
      SELECT yaf.icustay_id, yaf.akistarttime AS t2, (yaf.akistarttime - '24:00:00' :: interval hour) AS t1
      FROM mimiciii.yj_aki_finaleigen yaf
  ), diuretic_im AS (
      SELECT tin2.icustay_id,
             max(
               CASE
                 WHEN (im.amount >= (0) :: double precision) THEN 1
                 ELSE 0
                   END) OVER (PARTITION BY tin2.icustay_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS diuretic_im
      FROM (timeinfo tin2
          LEFT JOIN mimiciii.inputevents_mv im ON ((tin2.icustay_id = im.icustay_id)))
      WHERE (((im.icustay_id = ANY (ARRAY[220988, 220989, 220990, 220991, 220992, 228340, 221794, 227531])) AND
              ((im.starttime >= tin2.t1) AND (im.starttime <= tin2.t2))) OR
             ((im.endtime >= tin2.t1) AND (im.endtime <= tin2.t2)))
  ), diuretic_mv AS (
      SELECT tin.icustay_id, di2.diuretic_im
      FROM (timeinfo tin
          LEFT JOIN diuretic_im di2 ON ((tin.icustay_id = di2.icustay_id)))
  )
  SELECT DISTINCT diuretic_mv.icustay_id, diuretic_mv.diuretic_im
  FROM diuretic_mv;

alter materialized view yj_diuretic_mv
  owner to postgres;

