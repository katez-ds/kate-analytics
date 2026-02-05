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

create or replace table proddb.katez.wbd_all_eligible_cohort
as
(select
consumer_id,tag_renamed,
eligible_dt,
rank() OVER (
            PARTITION BY tag_renamed, consumer_id 
            ORDER BY eligible_dt) exposed_n
from 
    proddb.katez.wbd_exposure_0126 a
join 
    proddb.katez.order_frequency_0126 b 
on a.user_id = b.consumer_id
and a.exposed_dt::date = b.eligible_dt::date
--where year(exposed_dt)=2022 and year(eligible_dt) = 2022
)


--Days between min vs max eligible date

select 
case when days_part >=0 and days_part<365 then '<1Y'
when days_part>=365 and days_part<730 then '1-2Y'
when days_part>=730 and days_part<1095 then '2-3Y'
else '3Y+' end
eligible_tenure,
count(distinct consumer_id)
from 
(
select consumer_id,max(eligible_dt)-min(eligible_dt) days_part
from proddb.katez.wbd_all_eligible_cohort
where tag_renamed = 'Control'
group by 1
)
group by 1
order by 2 desc

ELIGIBLE_TENURE	COUNT(DISTINCT CONSUMER_ID)
<1Y	3328380
1-2Y	1391964
2-3Y	1363162
3Y+	1042061
  

-- Number of Exposures
select 
case when exposed_n <30 then '<30'
when exposed_n>=30 and exposed_n<=60 then '30-60'
when exposed_n>=60 and exposed_n<=90 then '60-90'
else '>90' end exposed_n,
count(distinct consumer_id)
from 
(
select consumer_id,max(exposed_n) exposed_n
from proddb.katez.wbd_all_eligible_cohort
where tag_renamed = 'Control'
group by 1
)
group by 1
order by 2 desc

EXPOSED_N	COUNT(DISTINCT CONSUMER_ID)
<30	4358826
30-60	1448747
60-90	678785
>90	639209
