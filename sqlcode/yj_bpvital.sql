create materialized view yj_bpvital as
  WITH ce AS (
      SELECT ce_1.icustay_id,
             ce_1.charttime,
             CASE
               WHEN ((ce_1.itemid = ANY (ARRAY[211, 220045])) AND (ce_1.valuenum > (0) :: double precision) AND
                     (ce_1.valuenum < (300) :: double precision)) THEN ce_1.valuenum
               ELSE NULL :: double precision
                 END AS heartrate,
             CASE
               WHEN ((ce_1.itemid = ANY (ARRAY[51, 6701, 220050])) AND (ce_1.valuenum > (0) :: double precision) AND
                     (ce_1.valuenum < (400) :: double precision)) THEN ce_1.valuenum
               ELSE NULL :: double precision
                 END AS sysbp,
             CASE
               WHEN ((ce_1.itemid = ANY (ARRAY[442, 455, 220179])) AND (ce_1.valuenum > (0) :: double precision) AND
                     (ce_1.valuenum < (400) :: double precision)) THEN ce_1.valuenum
               ELSE NULL :: double precision
                 END AS ni_sbp,
             CASE
               WHEN ((ce_1.itemid = ANY (ARRAY[8368, 8555, 220051])) AND (ce_1.valuenum > (0) :: double precision) AND
                     (ce_1.valuenum < (300) :: double precision)) THEN ce_1.valuenum
               ELSE NULL :: double precision
                 END AS diasbp,
             CASE
               WHEN ((ce_1.itemid = ANY (ARRAY[8440, 8441, 220180])) AND (ce_1.valuenum > (0) :: double precision) AND
                     (ce_1.valuenum < (300) :: double precision)) THEN ce_1.valuenum
               ELSE NULL :: double precision
                 END AS ni_dbp,
             CASE
               WHEN ((ce_1.itemid = ANY (ARRAY[52, 6702, 220052, 225312])) AND
                     (ce_1.valuenum > (0) :: double precision) AND (ce_1.valuenum < (300) :: double precision))
                       THEN ce_1.valuenum
               ELSE NULL :: double precision
                 END AS meanbp,
             CASE
               WHEN ((ce_1.itemid = ANY (ARRAY[456, 443, 220181])) AND (ce_1.valuenum > (0) :: double precision) AND
                     (ce_1.valuenum < (300) :: double precision)) THEN ce_1.valuenum
               ELSE NULL :: double precision
                 END AS ni_mbp,
             CASE
               WHEN ((ce_1.itemid = ANY (ARRAY[615, 618, 220210, 224690])) AND
                     (ce_1.valuenum > (0) :: double precision) AND (ce_1.valuenum < (70) :: double precision))
                       THEN ce_1.valuenum
               ELSE NULL :: double precision
                 END AS resprate,
             CASE
               WHEN ((ce_1.itemid = ANY (ARRAY[223761, 678])) AND (ce_1.valuenum > (70) :: double precision) AND
                     (ce_1.valuenum < (120) :: double precision))
                       THEN ((ce_1.valuenum - (32) :: double precision) / (1.8) :: double precision)
               WHEN ((ce_1.itemid = ANY (ARRAY[223762, 676])) AND (ce_1.valuenum > (10) :: double precision) AND
                     (ce_1.valuenum < (50) :: double precision)) THEN ce_1.valuenum
               ELSE NULL :: double precision
                 END AS tempc,
             CASE
               WHEN ((ce_1.itemid = ANY (ARRAY[646, 220277])) AND (ce_1.valuenum > (0) :: double precision) AND
                     (ce_1.valuenum <= (100) :: double precision)) THEN ce_1.valuenum
               ELSE NULL :: double precision
                 END AS spo2,
             CASE
               WHEN ((ce_1.itemid = ANY (ARRAY[807, 811, 1529, 3745, 3744, 225664, 220621, 226537])) AND
                     (ce_1.valuenum > (0) :: double precision)) THEN ce_1.valuenum
               ELSE NULL :: double precision
                 END AS glucose
      FROM mimiciii.chartevents ce_1
      WHERE ((ce_1.error IS DISTINCT FROM 1) AND (ce_1.itemid = ANY
                                                  (ARRAY[211, 220045, 51, 6701, 220050, 442, 455, 220179, 8368, 8555, 220051, 8440, 8441, 220180, 52, 6702, 220052, 225312, 456, 443, 220181, 618, 615, 220210, 224690, 646, 220277, 807, 811, 1529, 3745, 3744, 225664, 220621, 226537, 223762, 676, 223761, 678])))
  )
  SELECT ce.icustay_id,
         ce.charttime,
         avg(ce.heartrate) AS heartrate,
         avg(ce.sysbp)     AS sysbp,
         avg(ce.ni_sbp)    AS ni_sbp,
         avg(ce.diasbp)    AS diasbp,
         avg(ce.ni_dbp)    AS ni_dbp,
         avg(ce.meanbp)    AS meanbp,
         avg(ce.ni_mbp)    AS ni_mbp,
         avg(ce.resprate)  AS resprate,
         avg(ce.tempc)     AS tempc,
         avg(ce.spo2)      AS spo2,
         avg(ce.glucose)   AS glucose
  FROM ce
  GROUP BY ce.icustay_id, ce.charttime
  ORDER BY ce.icustay_id, ce.charttime;

alter materialized view yj_bpvital
  owner to postgres;

