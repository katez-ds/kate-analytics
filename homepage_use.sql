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


--Now, you can use query below:
--mobile:
  from IGUAZU.CONSUMER.M_PLACEMENTS_MODEL_VIEW f
    join campaign c ON f.campaign_id = c.campaign_id
    where f.IGUAZU_INGEST_TIMESTAMP::date >= '2025-08-15'
    and (lower(f.location) like '%home%page%' and lower(placement_component) in ('wallet', 'benefit_footer'))

--Web:
    from IGUAZU.server_events_production.w_placements_model_view_ice
    where (lower(f.page) like '%home%page%')
