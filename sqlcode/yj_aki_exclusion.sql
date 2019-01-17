create materialized view yj_aki_exclusion as
  WITH agelos AS (
      SELECT ie.icustay_id,
             ie.subject_id,
             ie.intime,
             ie.outtime,
             date_part('epoch' :: text,
                       ((ie.intime - pa.dob) / (((((60 * 60) * 24)) :: numeric * 365.242)) :: double precision)) AS age,
             date_part('epoch' :: text,
                       ((ie.outtime - ie.intime) / (((60 * 60) * 24)) :: double precision))                      AS icu_length_of_stay,
             rank() OVER (PARTITION BY ie.subject_id ORDER BY ie.intime)                                         AS icu_order
      FROM (mimiciii.icustays ie
          LEFT JOIN mimiciii.patients pa ON ((ie.subject_id = pa.subject_id)))
  ), exage AS (
      SELECT agelos.icustay_id,
             agelos.subject_id,
             agelos.intime,
             agelos.outtime,
             agelos.age,
             agelos.icu_length_of_stay,
             agelos.icu_order,
             CASE
               WHEN (agelos.age < (18) :: double precision) THEN 1
               ELSE 0
                 END AS ex_age,
             CASE
               WHEN (agelos.icu_order <> 1) THEN 1
               ELSE 0
                 END AS ex_icufirst,
             CASE
               WHEN (agelos.icu_length_of_stay < (2) :: double precision) THEN 1
               ELSE 0
                 END AS ex_los
      FROM agelos
  ), exhos AS (
      SELECT exage.icustay_id,
             exage.subject_id,
             exage.intime,
             exage.outtime,
             exage.age,
             exage.icu_length_of_stay,
             exage.icu_order,
             exage.ex_age,
             exage.ex_icufirst,
             exage.ex_los,
             CASE
               WHEN (id.hospstay_seq = 1) THEN 0
               ELSE 1
                 END AS ex_hos
      FROM (exage
          LEFT JOIN mimiciii.icustay_detail id ON (((exage.icustay_id = id.icustay_id) AND
                                                    (exage.subject_id = id.subject_id))))
  ), crevalue AS (
      SELECT eh.icustay_id,
             eh.subject_id,
             eh.intime,
             eh.outtime,
             eh.age,
             eh.icu_length_of_stay,
             eh.icu_order,
             eh.ex_age,
             eh.ex_icufirst,
             eh.ex_los,
             eh.ex_hos,
             le.valuenum                                                                         AS cre_value,
             count(le.valuenum) OVER (PARTITION BY eh.icustay_id, le.subject_id ORDER BY le.charttime
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)                         AS count_cre,
             row_number() OVER (PARTITION BY eh.icustay_id, le.subject_id ORDER BY le.charttime) AS cre_order
      FROM (exhos eh
          LEFT JOIN mimiciii.labevents le ON (((eh.subject_id = le.subject_id) AND (le.itemid = 50912) AND
                                               ((le.charttime >= (eh.intime - '06:00:00' :: interval hour)) AND
                                                (le.charttime <= eh.outtime)))))
  ), excre AS (
      SELECT eh1.icustay_id,
             eh1.subject_id,
             eh1.intime,
             eh1.outtime,
             eh1.age,
             eh1.icu_length_of_stay,
             eh1.icu_order,
             eh1.ex_age,
             eh1.ex_icufirst,
             eh1.ex_los,
             eh1.ex_hos,
             CASE
               WHEN ((cv.count_cre < 2) OR (cv.count_cre IS NULL)) THEN 1
               ELSE 0
                 END AS ex_crenum,
             CASE
               WHEN ((cv.cre_value IS NULL) OR (cv.cre_value > (4) :: double precision)) THEN 1
               ELSE 0
                 END AS ex_crevalue
      FROM (exhos eh1
          LEFT JOIN crevalue cv ON (((eh1.subject_id = cv.subject_id) AND (eh1.icustay_id = cv.icustay_id))))
      WHERE (cv.cre_order = 1)
  )
  SELECT excre.icustay_id,
         excre.subject_id,
         excre.intime,
         excre.outtime,
         excre.age,
         excre.icu_length_of_stay,
         excre.icu_order,
         excre.ex_age,
         excre.ex_icufirst,
         excre.ex_los,
         excre.ex_hos,
         excre.ex_crenum,
         excre.ex_crevalue,
         (((((excre.ex_age + excre.ex_los) + excre.ex_hos) + excre.ex_icufirst) + excre.ex_crenum) +
          excre.ex_crevalue) AS exclusion
  FROM excre;

alter materialized view yj_aki_exclusion
  owner to postgres;

