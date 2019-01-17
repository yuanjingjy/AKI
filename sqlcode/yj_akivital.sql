create materialized view yj_akivital as
  WITH baseinfo AS (
      SELECT ya.icustay_id,
             ya.subject_id,
             ya.hadm_id,
             ya.intime,
             ya.outtime,
             ya.charttime AS akistarttime,
             CASE
               WHEN (ya.akistage = 0) THEN 0
               ELSE 1
                 END      AS classlabel,
             id.ethnicity,
             id.admission_type,
             id.hospital_expire_flag,
             id.gender
      FROM (mimiciii.yj_akicohort ya
          LEFT JOIN mimiciii.icustay_detail id ON ((ya.icustay_id = id.icustay_id)))
  ), addheightweight AS (
      SELECT bi.icustay_id,
             bi.subject_id,
             bi.hadm_id,
             bi.intime,
             bi.outtime,
             bi.akistarttime,
             bi.classlabel,
             bi.ethnicity,
             bi.admission_type,
             bi.hospital_expire_flag,
             bi.gender,
             hw.weight_max,
             hw.weight_min,
             hw.weight_first,
             hw.height_max,
             hw.height_min,
             hw.height_first
      FROM (baseinfo bi
          LEFT JOIN mimiciii.heightweight hw ON ((bi.icustay_id = hw.icustay_id)))
  ), addvital AS (
      SELECT DISTINCT ahw.icustay_id    AS av_icustayid,
                      max(pv.heartrate) AS hr_max,
                      min(pv.heartrate) AS hr_min,
                      avg(pv.heartrate) AS hr_avg,
                      max(pv.diasbp)    AS dbp_max,
                      min(pv.diasbp)    AS dbp_min,
                      avg(pv.diasbp)    AS dbp_avg,
                      max(pv.sysbp)     AS sbp_max,
                      min(pv.sysbp)     AS sbp_min,
                      avg(pv.sysbp)     AS sbp_avg,
                      max(pv.meanbp)    AS mbp_max,
                      min(pv.meanbp)    AS mbp_min,
                      avg(pv.meanbp)    AS mbp_avg,
                      max(pv.tempc)     AS tem_max,
                      min(pv.tempc)     AS tem_min,
                      avg(pv.tempc)     AS tem_avg,
                      max(pv.resprate)  AS res_max,
                      min(pv.resprate)  AS res_min,
                      avg(pv.resprate)  AS res_avg,
                      max(pv.spo2)      AS spo2_max,
                      min(pv.spo2)      AS spo2_min,
                      avg(pv.spo2)      AS spo2_avg,
                      max(pv.glucose)   AS glu_max,
                      min(pv.glucose)   AS glu_min,
                      avg(pv.glucose)   AS glu_avg
      FROM (addheightweight ahw
          LEFT JOIN mimiciii.pivoted_vital pv ON (((ahw.icustay_id = pv.icustay_id) AND
                                                   ((pv.charttime >= (ahw.akistarttime - '24:00:00' :: interval hour))
                                                    AND (pv.charttime <= ahw.akistarttime)))))
      GROUP BY ahw.icustay_id
  ), vital AS (
      SELECT ahw1.icustay_id,
             ahw1.subject_id,
             ahw1.hadm_id,
             ahw1.intime,
             ahw1.outtime,
             ahw1.akistarttime,
             ahw1.classlabel,
             ahw1.ethnicity,
             ahw1.admission_type,
             ahw1.hospital_expire_flag,
             ahw1.gender,
             ahw1.weight_max,
             ahw1.weight_min,
             ahw1.weight_first,
             ahw1.height_max,
             ahw1.height_min,
             ahw1.height_first,
             av.av_icustayid,
             av.hr_max,
             av.hr_min,
             av.hr_avg,
             av.dbp_max,
             av.dbp_min,
             av.dbp_avg,
             av.sbp_max,
             av.sbp_min,
             av.sbp_avg,
             av.mbp_max,
             av.mbp_min,
             av.mbp_avg,
             av.tem_max,
             av.tem_min,
             av.tem_avg,
             av.res_max,
             av.res_min,
             av.res_avg,
             av.spo2_max,
             av.spo2_min,
             av.spo2_avg,
             av.glu_max,
             av.glu_min,
             av.glu_avg
      FROM (addheightweight ahw1
          LEFT JOIN addvital av ON ((ahw1.icustay_id = av.av_icustayid)))
  ), vent AS (
      SELECT DISTINCT vital.icustay_id,
                      vital.subject_id,
                      vital.hadm_id,
                      vital.intime,
                      vital.outtime,
                      vital.akistarttime,
                      vital.classlabel,
                      vital.ethnicity,
                      vital.admission_type,
                      vital.hospital_expire_flag,
                      vital.gender,
                      vital.weight_max,
                      vital.weight_min,
                      vital.weight_first,
                      vital.height_max,
                      vital.height_min,
                      vital.height_first,
                      vital.av_icustayid,
                      vital.hr_max,
                      vital.hr_min,
                      vital.hr_avg,
                      vital.dbp_max,
                      vital.dbp_min,
                      vital.dbp_avg,
                      vital.sbp_max,
                      vital.sbp_min,
                      vital.sbp_avg,
                      vital.mbp_max,
                      vital.mbp_min,
                      vital.mbp_avg,
                      vital.tem_max,
                      vital.tem_min,
                      vital.tem_avg,
                      vital.res_max,
                      vital.res_min,
                      vital.res_avg,
                      vital.spo2_max,
                      vital.spo2_min,
                      vital.spo2_avg,
                      vital.glu_max,
                      vital.glu_min,
                      vital.glu_avg,
                      max(
                        CASE
                          WHEN (((vd.starttime >= (vital.akistarttime - '24:00:00' :: interval hour)) AND
                                 (vd.starttime <= vital.akistarttime)) OR
                                ((vd.endtime >= (vital.akistarttime - '24:00:00' :: interval hour)) AND
                                 (vd.endtime <= vital.akistarttime))) THEN 1
                          ELSE 0
                            END) OVER (PARTITION BY vd.icustay_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS vent
      FROM (vital
          LEFT JOIN mimiciii.ventdurations vd ON ((vital.icustay_id = vd.icustay_id)))
  ), vaso AS (
      SELECT DISTINCT vent.icustay_id,
                      vent.subject_id,
                      vent.hadm_id,
                      vent.intime,
                      vent.outtime,
                      vent.akistarttime,
                      vent.classlabel,
                      vent.ethnicity,
                      vent.admission_type,
                      vent.hospital_expire_flag,
                      vent.gender,
                      vent.weight_max,
                      vent.weight_min,
                      vent.weight_first,
                      vent.height_max,
                      vent.height_min,
                      vent.height_first,
                      vent.av_icustayid,
                      vent.hr_max,
                      vent.hr_min,
                      vent.hr_avg,
                      vent.dbp_max,
                      vent.dbp_min,
                      vent.dbp_avg,
                      vent.sbp_max,
                      vent.sbp_min,
                      vent.sbp_avg,
                      vent.mbp_max,
                      vent.mbp_min,
                      vent.mbp_avg,
                      vent.tem_max,
                      vent.tem_min,
                      vent.tem_avg,
                      vent.res_max,
                      vent.res_min,
                      vent.res_avg,
                      vent.spo2_max,
                      vent.spo2_min,
                      vent.spo2_avg,
                      vent.glu_max,
                      vent.glu_min,
                      vent.glu_avg,
                      vent.vent,
                      max(
                        CASE
                          WHEN (((vaso_1.starttime >= (vent.akistarttime - '24:00:00' :: interval hour)) AND
                                 (vaso_1.starttime <= vent.akistarttime)) OR
                                ((vaso_1.endtime >= (vent.akistarttime - '24:00:00' :: interval hour)) AND
                                 (vaso_1.endtime <= vent.akistarttime))) THEN 1
                          ELSE 0
                            END) OVER (PARTITION BY vaso_1.icustay_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS vaso
      FROM (vent
          LEFT JOIN mimiciii.vasopressordurations vaso_1 ON ((vent.icustay_id = vaso_1.icustay_id)))
  )
  SELECT DISTINCT vaso.icustay_id,
                  vaso.subject_id,
                  vaso.hadm_id,
                  vaso.intime,
                  vaso.outtime,
                  vaso.akistarttime,
                  vaso.classlabel,
                  vaso.ethnicity,
                  vaso.admission_type,
                  vaso.hospital_expire_flag,
                  vaso.gender,
                  vaso.weight_max,
                  vaso.weight_min,
                  vaso.weight_first,
                  vaso.height_max,
                  vaso.height_min,
                  vaso.height_first,
                  vaso.av_icustayid,
                  vaso.hr_max,
                  vaso.hr_min,
                  vaso.hr_avg,
                  vaso.dbp_max,
                  vaso.dbp_min,
                  vaso.dbp_avg,
                  vaso.sbp_max,
                  vaso.sbp_min,
                  vaso.sbp_avg,
                  vaso.mbp_max,
                  vaso.mbp_min,
                  vaso.mbp_avg,
                  vaso.tem_max,
                  vaso.tem_min,
                  vaso.tem_avg,
                  vaso.res_max,
                  vaso.res_min,
                  vaso.res_avg,
                  vaso.spo2_max,
                  vaso.spo2_min,
                  vaso.spo2_avg,
                  vaso.glu_max,
                  vaso.glu_min,
                  vaso.glu_avg,
                  vaso.vent,
                  vaso.vaso
  FROM vaso;

alter materialized view yj_akivital
  owner to postgres;

