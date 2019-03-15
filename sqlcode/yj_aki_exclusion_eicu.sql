set search_path to eicu
--筛选只进入一次医院的病人，首先提取唯一的住院编号和病人编号，然后按照uniquepid分组，统计patienthealthsystemstayid的数目
--create materialized view yj_AKI_exclusion_eICU as
with count_admit0 as--提取唯一的住院编号和病人编号
(
    select distinct
           pa.uniquepid,
           pa.patienthealthsystemstayid
    from patient pa
)
     ,count_admit as--统计病人住院次数
    (
     select
     uniquepid
     ,patienthealthsystemstayid
     ,count(patienthealthsystemstayid)over (partition by uniquepid) as count_admit
     from count_admit0
    )
,ex_admit0 as--打标签，识别是需要排除
(
select
patienthealthsystemstayid
,uniquepid
,count_admit
,case when count_admit = 1 then 0 else 1 end as exclu_admit
from count_admit
)
,ex_admit as--和大的patient表合并，排除多次进入医院的患者
    (
     select distinct
     pa.patientunitstayid
     ,pa.uniquepid
     ,pa.patienthealthsystemstayid
     ,ea0.exclu_admit
     ,ea0.count_admit
     from patient pa
     left join ex_admit0 ea0
         on ea0.patienthealthsystemstayid = pa.patienthealthsystemstayid
    )
,count_icu as--统计进入ICU的次数，因为patient表中icustayid唯一，所以可以直接统计
    (
     select
     patientunitstayid
     ,patienthealthsystemstayid
     ,uniquepid
     ,exclu_admit
     ,count_admit
     ,count(patientunitstayid) over(partition by uniquepid) as count_icu
     from ex_admit
    )
,ex_icu as--排除多次进入ICU的
    (
    select
    patientunitstayid,
    patienthealthsystemstayid
    ,uniquepid
    ,exclu_admit
    ,count_icu
    ,count_admit
    ,case when count_icu = 1 then 0 else 1 end as exclu_icu
    from count_icu
    )
,ex_admittime as -- 排除进入医院的时间为正的，先住进医院后住进ICU？
    (
     select
     ei.*
     ,case when pa.hospitaladmitoffset > 0 then 1 else 0 end as exclu_admittime
     from ex_icu ei
     right join patient pa
         on ei.patientunitstayid = pa.patientunitstayid
    )
,ex_icuduration as
    (
     select
     ea.*
     ,case when pa.unitdischargeoffset >= (48*60) then 0 else 1 end as exclu_icuduration
     from ex_admittime ea
     right join patient pa
         on ea.patientunitstayid = pa.patientunitstayid
    )
, ageconvert as
(
     select
     pa.patientunitstayid
     ,cast((case when pa.age = '> 89' then '91.4'
                when pa.age ='' then '-1'
                else age end ) as numeric) as age
     from patient pa
)
,ex_age as
    (
     select
     ei2.*
     ,ac.age
     ,case when age >18 then 0 else 1 end as exclu_age
     from ex_icuduration ei2
     left join ageconvert ac
         on ac.patientunitstayid = ei2.patientunitstayid
    )
,labresults as
    (
     select
     pa.patientunitstayid
     ,lab.labresultoffset
     ,CASE
             WHEN labname = 'creatinine' and labresult > 150 THEN null  else labresult
             END as creatvalue
     from patient pa
     left join lab
         on pa.patientunitstayid = lab.patientunitstayid
     where lab.labresultoffset between -360 and pa.unitdischargeoffset
     and lab.labname = 'creatinine'
     and labresult is not null
     and labresult >0
    )
,creatinine_statis0 as
    (
     select
     lr.patientunitstayid
     ,lr.labresultoffset
     ,lr.creatvalue
     ,count(creatvalue)over (partition by patientunitstayid) as count_crea
     ,row_number()over (partition by patientunitstayid order by labresultoffset) as cre_order
     from labresults lr
     where creatvalue is not null
     and creatvalue > 0
    )
   ,creatinine_statis as
    (
     select
     cs0.labresultoffset,
     cs0.creatvalue,
            cs0.count_crea,
            cs0.cre_order,
            ex_age.patientunitstayid
     from ex_age
     left join creatinine_statis0 cs0
         on cs0.patientunitstayid = ex_age.patientunitstayid
    )
,ex_crea as
    (
     select
     ea2.*
     ,case when cs.count_crea<2 or cs.count_crea is null then 1 else 0 end as exclu_crenum
     ,case when cs.creatvalue is null or cs.creatvalue>=4 then 1 else 0 end as exclu_crevalue
     from ex_age ea2
          left join creatinine_statis cs
         on ea2.patientunitstayid = cs.patientunitstayid
         and cs.cre_order = 1
    )
  select * from ex_crea
-- select  distinct patientunitstayid from ex_crea
--  where exclu_icuduration = 0
-- and exclu_icu = 0
-- and exclu_admit = 0
-- and exclu_age = 0
-- and exclu_crenum = 0
-- and exclu_crevalue = 0


select count(*)
from yj_AKI_exclusion_eICU
where exclu_age = 0
and exclu_icuduration = 0
  and exclu_admit = 0
and exclu_icu = 0
and  exclu_crenum = 0
and exclu_crevalue = 0
and exclu_admittime = 0




