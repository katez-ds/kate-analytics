create or replace table proddb.katez.xs_exposure_0126
as
(select 
    case
       when tag = 'control_lt' then 'Control'
       else 'Treatment'
    end as tag_renamed 
    , try_cast(bucket_key as integer) as user_id
    , exposure_time::date as exposed_dt
    from PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
    where experiment_name = 'cross_shopping_customers_pricing_v2_us'
    --and experiment_version >= 7
    and exposure_time >= '2023-05-04' --min in the eligibility table. Otherweise should be 2022-09-01'
    and tag not in ('undefined', 'default')
    and bucket_key != 'unset_bucket_value'
    and NOT (CONTAINS(tag, '_ca') OR CONTAINS(tag, '_can') OR CONTAINS(tag, '_au') OR CONTAINS(tag, 'ca_'))
    and (custom_attributes:submarket_id::int != 24
       or custom_attributes:submarket_id::int is null
       )
    group by 1, 2, 3
    )
;
create or replace table proddb.katez.xs_eligible_0126 as
(  
select consumer_id,import_date eligible_dt
from edw.pad.cross_shopper_daily_snapshot_cross_shopper_customer_v3_daily_snapshot
group by 1,2
)

create or replace table proddb.katez.xs_first_eligible_cohort
as
(select
consumer_id,tag_renamed,
min(eligible_dt) first_eligible_dt
from 
    proddb.katez.xs_exposure_0126 a
join 
    proddb.katez.xs_eligible_0126 b 
on a.user_id = b.consumer_id
and a.exposed_dt::date = b.eligible_dt::date
--where year(exposed_dt)=2022 and year(eligible_dt) = 2022
group by 1,2)
