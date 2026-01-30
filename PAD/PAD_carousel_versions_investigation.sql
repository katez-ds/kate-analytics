-- Investigate new/old PAD carousel version impressions post Spotlight V2
select c.name country_name,platform,app_version,
count(distinct case when event_properties:container_id = '60e58852-64bb-49a4-be75-9f1ed482b487' then a.user_id||event_timestamp::date end) as new_carousel_views,
count(distinct case when event_properties:container_id = 'pad_gtm_v3_t1' then a.user_id||event_timestamp::date end) as old_carousel_views
from edw.consumer.unified_consumer_events a
left JOIN geo_intelligence.public.maindb_market b ON a.submarket_id = b.id
	LEFT JOIN geo_intelligence.public.maindb_country c ON b.country_id = c.id
where event_timestamp::date = date'2026-01-25'
and event_name like '%card_view%'
and (
    (event_properties:container::string in ('merchandisingunit_component_store_carousel', 'merchandisingunit_component_store_carousel_uc') 
and event_properties:container_id::string = '60e58852-64bb-49a4-be75-9f1ed482b487')
or (event_properties:container_id::string = 'pad_gtm_v3_t1')
)
and (
(lower(platform) like '%ios%' and app_version_major>=7 and app_version_minor>=12)
    or 
(lower(platform) like '%android%' and app_version_major>=15 and app_version_minor>=231)
    )
and country_name <> 'Canada'
group by 1,2,3
order by 1,2,3

