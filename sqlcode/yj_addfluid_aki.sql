create materialized view yj_addfluid_aki as
  WITH timeinfo AS (
      SELECT yaf.icustay_id, yaf.akistarttime AS t2, (yaf.akistarttime - '24:00:00' :: interval hour) AS t1
      FROM mimiciii.yj_aki_finaleigen yaf
  ), inputfluid AS (
    SELECT ic.icustay_id, ic.amount
    FROM (mimiciii.inputevents_cv ic
        RIGHT JOIN timeinfo ON (((ic.icustay_id = timeinfo.icustay_id) AND
                                 ((ic.charttime >= timeinfo.t1) AND (ic.charttime <= timeinfo.t2)))))
    WHERE ((ic.amountuom) :: text = 'ml' :: text)
    UNION ALL
    SELECT im.icustay_id,
           CASE
             WHEN ((im.starttime >= timeinfo.t1) AND (im.endtime <= timeinfo.t2)) THEN im.totalamount
             WHEN ((im.starttime <= timeinfo.t1) AND ((im.endtime >= timeinfo.t1) AND (im.endtime <= timeinfo.t2)) AND
                   ((im.amountuom) :: text = 'ml' :: text)) THEN CASE
                                                                   WHEN ((im.rateuom) :: text = 'mL/hour' :: text)
                                                                           THEN (
                   ((date_part('epoch' :: text, (im.endtime - timeinfo.t1)) / (60) :: double precision) /
                    (60) :: double precision) * im.rate)
                                                                   WHEN ((im.rateuom) :: text = 'mL/min' :: text) THEN (
                   (date_part('epoch' :: text, (im.endtime - timeinfo.t1)) / (60) :: double precision) * im.rate)
                                                                   WHEN ((im.rateuom) :: text = 'mL/kg/hour' :: text)
                                                                           THEN (
                   (((date_part('epoch' :: text, (im.endtime - timeinfo.t1)) / (60) :: double precision) /
                     (60) :: double precision) * im.patientweight) * im.rate)
                                                                   ELSE NULL :: double precision
               END
             WHEN (((im.starttime >= timeinfo.t1) AND (im.starttime <= timeinfo.t2)) AND (im.endtime >= timeinfo.t2) AND
                   ((im.amountuom) :: text = 'ml' :: text)) THEN CASE
                                                                   WHEN ((im.rateuom) :: text = 'mL/hour' :: text)
                                                                           THEN (
                   ((date_part('epoch' :: text, (timeinfo.t2 - im.starttime)) / (60) :: double precision) /
                    (60) :: double precision) * im.rate)
                                                                   WHEN ((im.rateuom) :: text = 'mL/min' :: text) THEN (
                   (date_part('epoch' :: text, (timeinfo.t2 - im.starttime)) / (60) :: double precision) * im.rate)
                                                                   WHEN ((im.rateuom) :: text = 'mL/kg/hour' :: text)
                                                                           THEN (
                   (((date_part('epoch' :: text, (timeinfo.t2 - im.starttime)) / (60) :: double precision) /
                     (60) :: double precision) * im.patientweight) * im.rate)
                                                                   ELSE NULL :: double precision
               END
             ELSE NULL :: double precision
               END AS amount
    FROM (mimiciii.inputevents_mv im
        RIGHT JOIN timeinfo ON ((im.icustay_id = timeinfo.icustay_id)))
    WHERE ((im.statusdescription) :: text = 'FinishedRunning' :: text)
  ), addfluid AS (
      SELECT inputfluid.icustay_id, sum(inputfluid.amount) OVER (PARTITION BY inputfluid.icustay_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS inputsum
      FROM inputfluid
  )
  SELECT DISTINCT addfluid.icustay_id, addfluid.inputsum
  FROM addfluid;

alter materialized view yj_addfluid_aki
  owner to postgres;

