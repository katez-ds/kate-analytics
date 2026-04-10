
-- Control vs Treatment
set exp_name = 'discount_engine_deals_us_growth';
set start_time = '2026-04-09'::date;
set end_time = '2026-04-09'::date;
set segment = 'Users';

with universal_be as (
select
  experiment_name
  , EXPERIMENT_VERSION version
  , case when tag = 'treatment_0' then 'Treatment'
  else 'Control' end tag
  , try_cast(bucket_key as integer) as user_id
  , min(cast(EXPOSURE_TIME as date)) as first_exposed
from PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
where experiment_name = $exp_name
        --and experiment_version between {{experiment_version_start-1}} and {{experiment_version_end-1}}
        and exposure_time between $start_time and $end_time
        --and (tag in {{control_1-1}} or tag in {{treatment_1-1}})
        and SEGMENT = $segment
        and try_cast(BUCKET_KEY as integer) != 1505155093 -- 1505155093 is the high volume account (100s of orders per day across 100+ sites) that DashMart uses to MFF (micro fulfill) items for our warehouse operations.
group by all
)
,
pad_impressions as (
select  
event_timestamp::date impression_dt,
case when event_properties:container_id::string = 'pad_gtm_v3_t1' then event_properties:facet_vertical_position::string else
event_properties:vertical_position::string end as position,
unified_consumer_events.user_id user_id
from edw.consumer.unified_consumer_events
where impression_dt between $start_time and $end_time
and event_name like '%card_view%'
and (
    (event_properties:container::string in ('merchandisingunit_component_store_carousel', 'merchandisingunit_component_store_carousel_uc') 
and event_properties:container_id::string = '60e58852-64bb-49a4-be75-9f1ed482b487')
or (event_properties:container_id::string = 'pad_gtm_v3_t1')
)
--and platform != 'web'
  --and country_id = {{country}}
  and submarket_id = 38
group by all
)

select tag,
count(distinct a.user_id) exposed_users,
count(distinct a.user_id) *1.0000/sum(count(distinct a.user_id)) over() perc_of_total_exposed,
count(distinct b.user_id) users_ordered_with_impressions,
count(distinct b.user_id)*1.0000/count(distinct a.user_id) impression_rate
from universal_be a
left join pad_impressions b
  on a.user_id = b.user_id and b.impression_dt>= a.first_exposed
group by all
order by all

-- Daily Trended

set start_time = '2026-04-09'::date;
set end_time = '2026-04-09'::date;


with pad_impressions as (
select  
event_timestamp::date impression_dt,
case when event_properties:container_id::string = 'pad_gtm_v3_t1' then event_properties:facet_vertical_position::string else
event_properties:vertical_position::string end as position,
unified_consumer_events.user_id||event_timestamp::date user_id
from edw.consumer.unified_consumer_events
where impression_dt between $start_time-3 and $end_time
and event_name like '%card_view%'
and (
    (event_properties:container::string in ('merchandisingunit_component_store_carousel', 'merchandisingunit_component_store_carousel_uc') 
and event_properties:container_id::string = '60e58852-64bb-49a4-be75-9f1ed482b487')
or (event_properties:container_id::string = 'pad_gtm_v3_t1')
)
--and platform != 'web'
  --and country_id = {{country}}
  and submarket_id = 38
group by all
)
select impression_dt, 
count(distinct user_id) impressed_users,
avg(position) avg_position
from pad_impressions
group by 1
order by 1
