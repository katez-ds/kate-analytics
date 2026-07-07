--Usually you can use placement_type = 'benefit reminder / homepage usi' from edw.growth.dl_fact_placements, but there is an issue with edw.growth.dl_fact_placements during long weekend. DE is trying to resolve it.

-- 1-day impressed users count for benefit reminder / homepage USI on 2026-06-01
select
  event_date,
  count(distinct consumer_id) as impressed_users_count
from
  edw.growth.dl_fact_placements
where
  event_date = '2026-06-01'
  and lower(placement_type) = 'benefit reminder / homepage usi'
group by
  1
order by
  1;

004177: SQL Execution Error: User provided authentication credentials or scopes are invalid for catalog integration UC_CATALOG_INTEGRATION__EDW_GROWTH due to error: Malformed request: Table 'edw.growth.dl_fact_placements' is not an Iceberg compatible table. [ErrorCode: 1000].

--Now, you can use query below:
--mobile:
select count(distinct consumer_id)
  from IGUAZU.CONSUMER.M_PLACEMENTS_MODEL_VIEW f
    where f.IGUAZU_INGEST_TIMESTAMP::date = '2026-07-01'
    and (lower(f.location) like '%home%page%' and lower(placement_component) in ('wallet', 'benefit_footer'))

2632704
  
--Web:
select count(distinct consumer_id)
from IGUAZU.server_events_production.w_placements_model_view_ice f
where lower(f.page) like '%home%page%'
and f.IGUAZU_TIMESTAMP::date = '2026-07-01'

112879

--mobile + web
select count(distinct consumer_id)
  from
(
select consumer_id
  from IGUAZU.CONSUMER.M_PLACEMENTS_MODEL_VIEW f
    where f.IGUAZU_INGEST_TIMESTAMP::date = '2026-07-01'
    and (lower(f.location) like '%home%page%' and lower(placement_component) in ('wallet', 'benefit_footer'))

union
  
select consumer_id
from IGUAZU.server_events_production.w_placements_model_view_ice f
where lower(f.page) like '%home%page%'
and f.IGUAZU_TIMESTAMP::date = '2026-07-01'
)

2724214
  
-- Total unique DoorDash visitors on app + web for 2026-07-01
select
  event_date,
  count(distinct dd_device_id) as total_mobile_web_visitors
from
  proddb.public.fact_unique_visitors_full_pt
where
  event_date = '2026-07-01'
  and lower(experience) = 'doordash'
  and is_bot = 0
  --and subchannel in ('App', 'Web')
group by
  1
order by
  1;
13144638

20.7%


-- Total unique DoorDash visitors on 2026-07-01
-- limited to users present in the WBD eligibility audience snapshot for that same date
select
  v.event_date,
  count(distinct v.user_id) as total_wbd_eligible_doordash_visitors
from
  proddb.public.fact_unique_visitors_full_pt v
  inner join proddb.public.fact_dynamic_audience_wbd_order_frequency_l365d wbd on v.user_id = wbd.consumer_id
  and v.event_date = wbd.injected_date
where
  v.event_date = '2026-07-01'
  and lower(v.experience) = 'doordash'
  and v.is_bot = 0
group by
  1
order by
  1;

2482605


-- Distinct WBD-eligible consumers who saw homepage wallet / benefit_footer placements on 2026-07-01
select count(distinct p.consumer_id) as wbd_eligible_consumer_cnt
from (
    select consumer_id
    from iguazu.consumer.m_placements_model_view f
    where f.iguazu_ingest_timestamp::date = '2026-07-01'
      and lower(f.location) like '%home%page%'
      and lower(f.placement_component) in ('wallet', 'benefit_footer')

    union

    select consumer_id
    from iguazu.server_events_production.w_placements_model_view_ice f
    where f.iguazu_timestamp::date = '2026-07-01'
      and lower(f.page) like '%home%page%'
) p
inner join proddb.public.fact_dynamic_audience_wbd_order_frequency_l365d wbd
    on try_to_number(p.consumer_id) = wbd.consumer_id
   and wbd.injected_date = '2026-07-01';

896269
36%

