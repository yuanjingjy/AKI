set search_path to eicu
create materialized view yj_akicohort_eICU as
with aki_all as
(
  select
  yae.*
  ,rank() over (partition by patientunitstayid order by chartoffset) as icuakinum1
  from yj_akidefination_eicu yae
  where yae.akistage <> 0
      and patientunitstayid != 2693435
)
,aki_cohort as
(
  select
  yae2.*
  ,rank() over (partition by patientunitstayid order by chartoffset) as icuakinum
  from yj_akidefination_eicu yae2
  where yae2.akistage<>0
     and patientunitstayid != 2693435
)
,nonaki_cohort as
(
  select distinct
  yae3.*
  ,rank()over (partition by patientunitstayid order by chartoffset) as icuakinum
  from yj_akidefination_eicu yae3
  where (not (yae3.patientunitstayid in (select aki_all.patientunitstayid from aki_all)))
  and yae3.cre_order=1
     and patientunitstayid != 2693435
),tmp as
(
  select distinct aki_cohort.*
  from aki_cohort
  where icuakinum = 1
    and chartoffset >= 24 * 60
  union
  select nonaki_cohort.*
  from nonaki_cohort
  where icuakinum = 1
    and chartoffset >= 24 * 60
)
select
tmp.patientunitstayid
,tmp.chartoffset
,tmp.creatinine
,tmp.cre_base
,tmp.cre_lowest
,crediff
,rv
,case when akistage = 0 then 0 else 1 end as classlabel
from tmp