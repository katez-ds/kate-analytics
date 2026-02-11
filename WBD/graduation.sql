create or replace table proddb.katez.wbd_exposure_0126
as
(select 
    case
       when tag = 'control_lt' then 'Control'
       else 'Treatment'
    end as tag_renamed 
    , try_cast(bucket_key as integer) as user_id
    , exposure_time::date as exposed_dt
    from PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
    where experiment_name = 'welcome-back-pricing-experiment'
    and experiment_version >= 7
    and exposure_time >= '2022-09-01' --min in the eligibility table. Otherweise should be 2022-09-01'
    and tag not in ('undefined', 'default')
    and bucket_key != 'unset_bucket_value'
    and NOT (CONTAINS(tag, '_ca') OR CONTAINS(tag, '_can') OR CONTAINS(tag, '_au') OR CONTAINS(tag, 'ca_'))
    and (custom_attributes:submarket_id::int != 24
       or custom_attributes:submarket_id::int is null
       )
    group by 1, 2, 3
    )
;
create or replace table proddb.katez.order_frequency_0126 as
(select  
    consumer_id,
    injected_date as eligible_dt
    from 
        (SELECT consumer_id, injected_date,--L365D_ORDER_COUNT 
        FROM proddb.public.FACT_DYNAMIC_AUDIENCE_WBD_ORDER_FREQUENCY_L365D
        WHERE injected_date >= '2024-10-28'  
            AND injected_date < CURRENT_DATE
        
        UNION ALL
        
        SELECT consumer_id, date as injected_date
        FROM proddb.static.wbd_ocx_of
        WHERE injected_date >= '2022-09-01'
            AND injected_date < CURRENT_DATE
            ) 
group by 1,2
)

create or replace table proddb.katez.wbd_first_eligible_cohort
as
(select
consumer_id,tag_renamed,
min(eligible_dt) first_eligible_dt
from 
    proddb.katez.wbd_exposure_0126 a
join 
    proddb.katez.order_frequency_0126 b 
on a.user_id = b.consumer_id
and a.exposed_dt::date = b.eligible_dt::date
--where year(exposed_dt)=2022 and year(eligible_dt) = 2022
group by 1,2)

with eligiliy as ( -- WBD Cx eligivle on a given date
select  
wbd.consumer_id
from proddb.public.FACT_DYNAMIC_AUDIENCE_WBD_ORDER_FREQUENCY_L365D wbd
left join proddb.public.cx_sensitivity_v2 psm on wbd.consumer_id = psm.consumer_id and prediction_datetime_est = injected_date
where injected_date = date'2025-01-31'
and not (cohort = 'p84d_active_very_insensitive' and L365D_ORDER_COUNT >= 13)
and not (cohort = 'p84d_active_insensitive' and L365D_ORDER_COUNT >= 20)
group by 1
)

SELECT
count(distinct a.consumer_id), 
count(distinct case when b.consumer_id is null then a.consumer_id end) graduated,
count(distinct case when b.consumer_id is null then a.consumer_id end)*1.0000/count(distinct a.consumer_id) graduation_rate
FROM proddb.katez.wbd_first_eligible_cohort a
  left join eligiliy b
  on a.consumer_id = b.consumer_id

COUNT(DISTINCT A.CONSUMER_ID)	GRADUATED	GRADUATION_RATE
142560157	35014995	0.245616
