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

select year(first_eligible_dt), count(*) from
proddb.katez.wbd_first_eligible_cohort
group by 1

/*
with exposure as --exposed used (holdout vs treatment opened app)
(select 
case
   when tag = 'control_lt' then 'Control'
   else 'Treatment'
end as tag_renamed 
, try_cast(bucket_key as integer) as user_id
, exposure_time as exposed_dt
from PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
where experiment_name = 'welcome-back-pricing-experiment'
and experiment_version >= 7
and exposure_time >= '2024-02-22' --min in the eligibility table. Otherweise should be 2022-09-01'
and tag not in ('undefined', 'default')
and bucket_key != 'unset_bucket_value'
and NOT (CONTAINS(tag, '_ca') OR CONTAINS(tag, '_can') OR CONTAINS(tag, '_au') OR CONTAINS(tag, 'ca_'))
and (custom_attributes:submarket_id::int != 24
   or custom_attributes:submarket_id::int is null
   )
group by 1, 2, 3
)

, eligiliy as ( -- WBD Cx eligivle on a given date
select  
wbd.consumer_id,
injected_date as eligible_dt
from proddb.public.FACT_DYNAMIC_AUDIENCE_WBD_ORDER_FREQUENCY_L365D wbd
left join cx_sensitivity_v2 psm on wbd.consumer_id = psm.consumer_id and prediction_datetime_est = injected_date
where injected_date >= '2024-02-22' --min in the table
and not (cohort = 'p84d_active_very_insensitive' and L365D_ORDER_COUNT >= 13)
and not (cohort = 'p84d_active_insensitive' and L365D_ORDER_COUNT >= 20)
group by 1,2
)
*/

CREATE OR REPLACE TABLE proddb.katez.wbd_first_eligible_cohort AS
WITH exposures AS (
    SELECT 
        CASE
            WHEN tag = 'control_lt' THEN 'Control'
            ELSE 'Treatment'
        END AS tag_renamed,
        TRY_CAST(bucket_key AS INTEGER) AS consumer_id,
        exposure_time::DATE AS exposed_dt
    FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
    WHERE experiment_name = 'welcome-back-pricing-experiment'
        AND experiment_version >= 7
        AND exposure_time >= '2022-09-01'
        AND exposure_time < CURRENT_DATE  -- Add upper bound
        AND tag NOT IN ('undefined', 'default')
        AND bucket_key != 'unset_bucket_value'
        AND NOT (CONTAINS(tag, '_ca') OR CONTAINS(tag, '_can') OR CONTAINS(tag, '_au') OR CONTAINS(tag, 'ca_'))
        AND (custom_attributes:submarket_id::INT != 24 OR custom_attributes:submarket_id::INT IS NULL)
    GROUP BY 1, 2, 3  -- Dedupe at source
),
eligibility AS (
    SELECT  
        wbd.consumer_id,
        wbd.injected_date::DATE AS eligible_dt
    FROM (
        SELECT consumer_id, injected_date,L365D_ORDER_COUNT 
        FROM proddb.public.FACT_DYNAMIC_AUDIENCE_WBD_ORDER_FREQUENCY_L365D
        WHERE injected_date >= '2024-10-28'  
            AND injected_date < CURRENT_DATE
        
        UNION ALL
        
        SELECT consumer_id, date as injected_date,L365D_ORDER_COUNT
        FROM proddb.static.wbd_ocx_of
        WHERE injected_date >= '2022-09-01'
            AND injected_date < CURRENT_DATE
    ) wbd
    --LEFT JOIN proddb.public.cx_sensitivity_v2 psm 
        --ON wbd.consumer_id = psm.consumer_id 
        --AND psm.prediction_datetime_est = wbd.injected_date
    --WHERE NOT (psm.cohort = 'p84d_active_very_insensitive' AND wbd.L365D_ORDER_COUNT >= 13)
        --AND NOT (psm.cohort = 'p84d_active_insensitive' AND wbd.L365D_ORDER_COUNT >= 20)
    GROUP BY 1, 2  -- Dedupe after filtering
)
SELECT 
    e.consumer_id,
    exp.tag_renamed,
    MIN(e.eligible_dt) AS first_eligible_dt
FROM eligibility e
INNER JOIN exposures exp 
    ON e.consumer_id = exp.consumer_id
    AND e.eligible_dt = exp.exposed_dt
GROUP BY 1, 2

