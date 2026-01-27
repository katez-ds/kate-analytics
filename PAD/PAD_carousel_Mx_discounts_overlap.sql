-- Understand PAD Carousel Data
-- Event Name
select
EVENT_CATEGORY,
EVENT_NAME,
EVENT_LABEL
from edw.consumer.unified_consumer_events
where event_timestamp::date = current_date - 1
and event_name like '%card_view%'
and (
    (event_properties:container::string in ('merchandisingunit_component_store_carousel', 'merchandisingunit_component_store_carousel_uc') 
and event_properties:container_id::string = '60e58852-64bb-49a4-be75-9f1ed482b487')
or (event_properties:container_id::string = 'pad_gtm_v3_t1')
)
group by 1,2,3

-- Containers and Position
select
event_properties:container::string as container,
event_properties:container_id::string as container_id,
event_properties:vertical_position::string as vertical_position
from edw.consumer.unified_consumer_events
where event_timestamp::date = current_date -1
and event_name like '%card_view%'
and (
    (event_properties:container::string in ('merchandisingunit_component_store_carousel', 'merchandisingunit_component_store_carousel_uc') 
and event_properties:container_id::string = '60e58852-64bb-49a4-be75-9f1ed482b487')
or (event_properties:container_id::string = 'pad_gtm_v3_t1')
)
group by 1,2,3


-- Trended PAD Carousel Impression by Position
select 
impression_dt,
case when position < 3 then '0-2'
when position is null then 'Unknown'
when position >9 then '>8'
else position end as position,
sum(num_viewed_cx) impressions
from
(select  
event_timestamp::date impression_dt,
case when event_properties:container_id::string = 'pad_gtm_v3_t1' then event_properties:facet_vertical_position::string else
event_properties:vertical_position::string end as position,
count(distinct unified_consumer_events.user_id||event_timestamp::date) as num_viewed_cx
from edw.consumer.unified_consumer_events
where event_timestamp::date >= current_date - 7
and event_name like '%card_view%'
and (
    (event_properties:container::string in ('merchandisingunit_component_store_carousel', 'merchandisingunit_component_store_carousel_uc') 
and event_properties:container_id::string = '60e58852-64bb-49a4-be75-9f1ed482b487')
or (event_properties:container_id::string = 'pad_gtm_v3_t1')
)
and platform != 'web'
group by 1,2
)
group by 1,2
order by 1,2

-- Average PAD Carousel Impressed Position

select  
average(case when event_properties:container_id::string = 'pad_gtm_v3_t1' then event_properties:facet_vertical_position::string else
event_properties:vertical_position::string end) as avg_position
from edw.consumer.unified_consumer_events
where event_timestamp::date >= current_date - 7
and event_name like '%card_view%'
and (
    (event_properties:container::string in ('merchandisingunit_component_store_carousel', 'merchandisingunit_component_store_carousel_uc') 
and event_properties:container_id::string = '60e58852-64bb-49a4-be75-9f1ed482b487')
or (event_properties:container_id::string = 'pad_gtm_v3_t1')
)
and platform != 'web'

-- 4.8

-- PAD Offers with Mx discount
select 
count(distinct case when mx_funded_cx_discount = 0 then delivery_id end) PAD_only_orders,
count(distinct case when mx_funded_cx_discount > 0 then delivery_id end) PAD_and_Mx_orders,
count(distinct delivery_id) all_PAD_orders,
avg(pad_df_promo_discount) PAD_discount,
avg(case when mx_funded_cx_discount >0 then mx_funded_cx_discount end) Mx_discount_upon_PAD
--delivery_id, pad_df_promo_discount, mx_funded_cx_discount
from proddb.static.df_sf_promo_discount_delivery_level
where pad_df_promo_discount > 0 
and year(created_at) = 2025

PAD_ONLY_ORDERS	PAD_AND_MX_ORDERS	ALL_PAD_ORDERS	PAD_DISCOUNT	MX_DISCOUNT_UPON_PAD
44878790	8411525	53290315	1.725605016596	5.870248874015

-- PAD vs Mx
select 
case when pad_df_promo_discount > 0 then 1 else 0 end PAD_ind,
case when mx_funded_cx_discount > 0 then 1 else 0 end Mx_ind,
count(distinct delivery_id) orders,
avg(case when pad_df_promo_discount >0 then pad_df_promo_discount end) PAD_discount,
avg(case when mx_funded_cx_discount >0 then mx_funded_cx_discount end) Mx_discount_upon_PAD
--delivery_id, pad_df_promo_discount, mx_funded_cx_discount
from proddb.static.df_sf_promo_discount_delivery_level
where (pad_df_promo_discount > 0 or mx_funded_cx_discount>0)
and year(created_at) = 2025
group by 1,2
order by 1,2

PAD_IND	MX_IND	ORDERS	PAD_DISCOUNT	MX_DISCOUNT_UPON_PAD
0	1	353961210		6.351877167303
1	0	44878790	1.731528773169	
1	1	8411525	1.693999448376	5.870248874015
-- PAD orders with users clicked on PAD carousel (same day attribution)
WITH pad_orders AS (           -- 1. identify PAD orders
    SELECT
        d.delivery_id,
        d.creator_id AS consumer_id,
        d.created_at AS order_ts_utc
    FROM edw.finance.dimension_deliveries d
    JOIN proddb.static.df_sf_promo_discount_delivery_level dfp
        ON d.delivery_id = dfp.delivery_id
        and pad_df_promo_discount>0
    WHERE d.is_filtered_core = TRUE
        AND d.fulfillment_type NOT IN ('dine_in', 'shipping', 'merchant_fleet', 'virtual') -- excluding dine-in, shipping, merchant fleet, and virtual orders (giftcards)
        AND d.is_from_store_to_us = FALSE -- excluding store-to-us orders
        --AND d.country_id = 1 -- US only 
        AND d.created_at::date BETWEEN date'2026-01-19' AND date'2026-01-25'
)

, pad_clicked as (
select  
event_date,user_id
from edw.consumer.unified_consumer_events
where event_date BETWEEN date'2026-01-19' AND date'2026-01-25'
and event_name like '%card_click%'
and (
    (event_properties:container::string in ('merchandisingunit_component_store_carousel', 'merchandisingunit_component_store_carousel_uc') 
and event_properties:container_id::string = '60e58852-64bb-49a4-be75-9f1ed482b487')
or (event_properties:container_id::string = 'pad_gtm_v3_t1')
)
--and platform != 'web'
group by all
)

SELECT
    COUNT(distinct delivery_id) AS PAD_orders,
    COUNT(distinct case when c.user_id is not null then delivery_id end) AS PAD_carousel_attributed,
    ROUND(COUNT(distinct case when c.user_id is not null then delivery_id end)/COUNT(distinct delivery_id),4) AS pct_of_pad_orders
FROM pad_orders o
left join pad_clicked c
on o.consumer_id = c.user_id

-- Investigate # of Order per User on a day
WITH pad_orders AS (           -- 1. identify PAD orders
    SELECT
        d.delivery_id,
        d.creator_id AS consumer_id
    FROM edw.finance.dimension_deliveries d
    JOIN proddb.static.df_sf_promo_discount_delivery_level dfp
        ON d.delivery_id = dfp.delivery_id
        and pad_df_promo_discount>0
    WHERE d.is_filtered_core = TRUE
        AND d.fulfillment_type NOT IN ('dine_in', 'shipping', 'merchant_fleet', 'virtual') -- excluding dine-in, shipping, merchant fleet, and virtual orders (giftcards)
        AND d.is_from_store_to_us = FALSE -- excluding store-to-us orders
        --AND d.country_id = 1 -- US only 
        AND d.created_at::date = date'2026-01-19'
        group by 1,2
)
select orders,count(distinct consumer_id) users
from
(select consumer_id, count(distinct delivery_id) orders
from pad_orders
group by 1
having count(distinct delivery_id)>1)
group by 1
order by 2 desc

ORDERS	USERS
2	3617
3	139
4	11
5	2






