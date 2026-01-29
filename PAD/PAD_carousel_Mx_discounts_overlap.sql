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


-- PAD Carousel CTR
with impressions AS (
select  
count(distinct unified_consumer_events.user_id||event_timestamp::date) as num_viewed_cx
from edw.consumer.unified_consumer_events
where event_timestamp::date between date'2026-01-19' and date'2026-01-25'
and event_name like '%card_view%'
and (
    (event_properties:container::string in ('merchandisingunit_component_store_carousel', 'merchandisingunit_component_store_carousel_uc') 
and event_properties:container_id::string = '60e58852-64bb-49a4-be75-9f1ed482b487')
or (event_properties:container_id::string = 'pad_gtm_v3_t1')
)
and platform != 'web'
group by all
)
, clicks as (
select  
count(distinct unified_consumer_events.user_id||event_timestamp::date) as num_clicked_cx
from edw.consumer.unified_consumer_events
where event_timestamp::date between date'2026-01-19' and date'2026-01-25'
and event_name like '%card_click%'
and (
    (event_properties:container::string in ('merchandisingunit_component_store_carousel', 'merchandisingunit_component_store_carousel_uc') 
and event_properties:container_id::string = '60e58852-64bb-49a4-be75-9f1ed482b487')
or (event_properties:container_id::string = 'pad_gtm_v3_t1')
)
and platform != 'web'
group by all
)
select
num_viewed_cx as "Impression",
num_clicked_cx / num_viewed_cx as "CTR"
from impressions
cross join clicks

Impression	CTR
3006787	0.181031

-- Mx Funded Stores on a given day (excl. ads)
select --campaign_id, --entity_id as store_id, 
case when lower(promo_title) like '%free item%' then 'Free Item'
when lower(promo_title) like '%delivery fee%' then 'Delivery Fee: Set Value'
when lower(promo_title) like '%off%items%' then 'Order Item: % Off'
when lower(promo_title) like '%$%off%' or lower(incentive_value_type) like '%flat%' then 'Subtotal: Flat Amount Off'
when lower(promo_title) like '%off%' then 'Subtotal: % Off'
when lower(promo_title) like '%spend%save%' then 'Subtotal: Flat Amount Off'
when lower(promo_title) like '%buy%get%' then 'BOGO'
when lower(incentive_target_type) like  '%subtotal%' and lower(incentive_value_type) like '%percent%' then 'Subtotal: % Off'
when lower(incentive_target_type) like  '%item%' and lower(incentive_value_type) like '%percent%' then 'Order Item: % Off'
when lower(incentive_target_type) like  '%item%' and lower(incentive_value_type) like '%set%' then 'Order Item: Set Value'
when lower(incentive_target_type) like  '%delivery%' and lower(incentive_value_type) like '%set%' then 'Delivery Fee: Set Value'
else incentive_target_type end campaign_type,
count(distinct entity_id) stores
from
proddb.public.fact_daily_ad_store_campaigns
where TP = date'2026-01-19'
and is_active_campaign = TRUE
and is_merchant_funded = TRUE
and PRODUCT_TYPE <> 'ad'
group by 1
order by 2 desc

CAMPAIGN_TYPE	STORES
Subtotal: % Off	194395
Subtotal: Flat Amount Off	176204
BOGO	110793
Delivery Fee: Set Value	88219
Order Item: % Off	84604
INCENTIVE_TARGET_TYPE_SMART	82056
Free Item	37685
Order Item: Set Value	249
Delivery: Set Value	0

PRODUCT_TYPE	SUB_PRODUCT_TYPE	INCENTIVE_TARGET_TYPE	INCENTIVE_VALUE_TYPE	STORES
ad	ad_sponsored_listing			498653
promotion	promotion	INCENTIVE_TARGET_TYPE_SUBTOTAL	INCENTIVE_VALUE_TYPE_PERCENT_OFF	194644
promotion	promotion	INCENTIVE_TARGET_TYPE_ORDER_ITEM	INCENTIVE_VALUE_TYPE_PERCENT_OFF	176322
promotion	promotion	INCENTIVE_TARGET_TYPE_SUBTOTAL	INCENTIVE_VALUE_TYPE_FLAT_AMOUNT_OFF	175279
ad	ad_homepage_banner			102304
promotion	promotion	INCENTIVE_TARGET_TYPE_DELIVERY_FEE	INCENTIVE_VALUE_TYPE_SET_VALUE	88219
promotion	promotion	INCENTIVE_TARGET_TYPE_SMART		82056
promotion	promotion	INCENTIVE_TARGET_TYPE_ORDER_ITEM	INCENTIVE_VALUE_TYPE_SET_VALUE	1770
promotion	promotion			1

select
count(distinct entity_id) stores
from
proddb.public.fact_daily_ad_store_campaigns
where TP = date'2026-01-19'
and is_active_campaign = TRUE
and is_merchant_funded = TRUE
and PRODUCT_TYPE <> 'ad'

495338

-- Pad Carousel vs Mx Eligible Store
with pad_carousel_eligible as
(SELECT 
    f.value AS store_id
    --,IGUAZU_PARTITION_DATE pad_dt
FROM IGUAZU.SERVER_EVENTS_PRODUCTION.discount_candidates_event_ice,
    LATERAL FLATTEN(input => ELIGIBLE_CANDIDATES) f
WHERE IGUAZU_PARTITION_DATE = '2026-01-19'  -- Replace with your desired date (format: YYYY-MM-DD)
group by 1)
,
mx_discount as
(select entity_id
from
proddb.public.fact_daily_ad_store_campaigns
where TP = date'2026-01-19'
and is_active_campaign = TRUE
and is_merchant_funded = TRUE
and PRODUCT_TYPE<> 'ad'
group by 1
)
select 
case when store_id is not null and entity_id is null then 'PAD Only'
when store_id is null and entity_id is not null then 'Mx Only'
when store_id is not null and entity_id is not null then 'PAD + Mx'
end cohort,
count(*) orders
from pad_carousel_eligible p
full outer join mx_discount m
on p.store_id = m.entity_id
group by 1

COHORT      orders
PAD Only	281690
PAD + Mx	248075
Mx Only	247264

-- Mx Orders and $Discounts by Campaign Type

-- Query to identify which merchants are on promos, promo type, and daily Mx-funded CX discounts
with dd as 
(select dd.delivery_id
    FROM proddb.public.dimension_deliveries dd
    LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv 
            ON dd.store_id = nv.store_id AND nv.is_filtered_mp_vertical = 1
    WHERE dd.is_filtered_core = TRUE
        -- AND dd.is_consumer_pickup = FALSE -- optional to exclude pickup orders
        AND dd.fulfillment_type NOT IN ('dine_in', 'shipping', 'merchant_fleet', 'virtual') -- excluding dine-in, shipping, merchant fleet, and virtual orders (giftcards)
        AND dd.is_from_store_to_us = FALSE -- excluding store-to-us orders
        AND dd.is_bundle_order = FALSE -- excluding bundle orders -- an optional column to filter out DoubleDash
        AND nv.business_line IS NULL -- excluding non-restaurant orders -- an optional column to exclude NV
        AND dd.country_id = 1 -- US only 
        AND cast(created_at as date) between date'2026-01-19' and date'2026-01-25'
        AND is_subscribed_consumer = FALSE
    group by 1) 
,

daily_mx_discounts AS (
    -- Calculate daily Mx-funded CX discounts from FPOR
    SELECT
        --fpor.delivery_active_date,
        --fpor.store_id,
        fpor.campaign_id,
        case when x.management_type IN ('ENTERPRISE', 'MID MARKET') then 0 else 1 end as smb_flag,
        COUNT(DISTINCT fpor.delivery_id) AS orders,
        SUM(fpor.discount_subsidy_usd / 100.0) AS total_cx_discount_usd
    FROM edw.ads.fact_promo_order_redemption fpor
    join dd on fpor.delivery_id = dd.delivery_id
    LEFT JOIN proddb.public.dimension_store_ext x ON fpor.store_id = x.store_id
    WHERE fpor.delivery_active_date between date'2026-01-19' and date'2026-01-25'  -- Adjust date range
        AND fpor.sub_transaction_funding_entity_type = 'SUB_TRANSACTION_FUNDED_ENTITY_TYPE_MERCHANT'
        AND fpor.fee_inducing_entity_type = 'BILLABLE_EVENT_ENTITY_ID_TYPE_CAMPAIGN'
    GROUP BY 1,2
),
campaign_metadata AS (
select campaign_id, --entity_id as store_id, 
case when lower(promo_title) like '%free item%' then 'Free Item'
when lower(promo_title) like '%delivery fee%' then 'Delivery Fee: Set Value'
when lower(promo_title) like '%off%items%' then 'Order Item: % Off'
when lower(promo_title) like '%$%off%' or lower(incentive_value_type) like '%flat%' then 'Subtotal: Flat Amount Off'
when lower(promo_title) like '%off%' then 'Subtotal: % Off'
when lower(promo_title) like '%spend%save%' then 'Subtotal: Flat Amount Off'
when lower(promo_title) like '%buy%get%' then 'BOGO'
when lower(incentive_target_type) like  '%subtotal%' and lower(incentive_value_type) like '%percent%' then 'Subtotal: % Off'
when lower(incentive_target_type) like  '%item%' and lower(incentive_value_type) like '%percent%' then 'Order Item: % Off'
when lower(incentive_target_type) like  '%item%' and lower(incentive_value_type) like '%set%' then 'Order Item: Set Value'
when lower(incentive_target_type) like  '%delivery%' and lower(incentive_value_type) like '%set%' then 'Delivery Fee: Set Value'
else incentive_value_type end campaign_type,
--concat(incentive_target_type, incentive_value_type),
promo_title
from
proddb.public.fact_daily_ad_store_campaigns
where 1=1
--and TP between date'2026-01-19' and date'2026-01-25'
and is_active_campaign = TRUE
and is_merchant_funded = TRUE
--and PRODUCT_TYPE<> 'ad'
group by all
)
-- Final output: Stores on promos with daily discount amounts
SELECT
    --smb_flag,
    cm.campaign_type,--promo_title,
    sum(dmd.orders) orders,
    sum(dmd.total_cx_discount_usd) total_cx_discount_usd,
    round(sum(dmd.total_cx_discount_usd)/sum(dmd.orders),4) avg_cx_discount_per_order
FROM daily_mx_discounts dmd
LEFT JOIN campaign_metadata cm
    ON cm.campaign_id = dmd.campaign_id
    --AND cm.store_id = dmd.store_id
    --AND cm.active_date = dmd.delivery_active_date
--LEFT JOIN proddb.public.dimension_store ds
    --ON ds.store_id = dmd.store_id
group by 1
order by 2 desc

CAMPAIGN_TYPE	ORDERS	TOTAL_CX_DISCOUNT_USD	AVG_CX_DISCOUNT_PER_ORDER
	599041	3981042.510000	6.6457
BOGO	446185	4140789.910000	9.2804
Subtotal: Flat Amount Off	401962	1866092.150000	4.6425
Subtotal: % Off	313274	1744610.940000	5.5690
Free Item	250699	1792019.990000	7.1481
Delivery Fee: Set Value	125457	316934.500000	2.5262
Order Item: % Off	122772	703090.540000	5.7268
Order Item: Set Value	9	24.950000	2.7722


-- Mx Discount Orders: SMB vs ENT

-- Query to identify which merchants are on promos, promo type, and daily Mx-funded CX discounts
with dd as 
(select dd.delivery_id
    FROM proddb.public.dimension_deliveries dd
    LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv 
            ON dd.store_id = nv.store_id AND nv.is_filtered_mp_vertical = 1
    WHERE dd.is_filtered_core = TRUE
        -- AND dd.is_consumer_pickup = FALSE -- optional to exclude pickup orders
        AND dd.fulfillment_type NOT IN ('dine_in', 'shipping', 'merchant_fleet', 'virtual') -- excluding dine-in, shipping, merchant fleet, and virtual orders (giftcards)
        AND dd.is_from_store_to_us = FALSE -- excluding store-to-us orders
        AND dd.is_bundle_order = FALSE -- excluding bundle orders -- an optional column to filter out DoubleDash
        AND nv.business_line IS NULL -- excluding non-restaurant orders -- an optional column to exclude NV
        AND dd.country_id = 1 -- US only 
        AND cast(created_at as date) between date'2026-01-19' and date'2026-01-25'
        AND is_subscribed_consumer = FALSE
    group by 1) 
,

daily_mx_discounts AS (
    -- Calculate daily Mx-funded CX discounts from FPOR
    SELECT
        --fpor.delivery_active_date,
        --fpor.store_id,
        fpor.campaign_id,
        case when x.management_type IN ('ENTERPRISE', 'MID MARKET') then 0 else 1 end as smb_flag,
        COUNT(DISTINCT fpor.delivery_id) AS orders,
        SUM(fpor.discount_subsidy_usd / 100.0) AS total_cx_discount_usd
    FROM edw.ads.fact_promo_order_redemption fpor
    join dd on fpor.delivery_id = dd.delivery_id
    LEFT JOIN proddb.public.dimension_store_ext x ON fpor.store_id = x.store_id
    WHERE fpor.delivery_active_date between date'2026-01-19' and date'2026-01-25'  -- Adjust date range
        AND fpor.sub_transaction_funding_entity_type = 'SUB_TRANSACTION_FUNDED_ENTITY_TYPE_MERCHANT'
        AND fpor.fee_inducing_entity_type = 'BILLABLE_EVENT_ENTITY_ID_TYPE_CAMPAIGN'
    GROUP BY 1,2
),
campaign_metadata AS (
select campaign_id, --entity_id as store_id, 
case when lower(promo_title) like '%free item%' then 'Free Item'
when lower(promo_title) like '%delivery fee%' then 'Delivery Fee: Set Value'
when lower(promo_title) like '%off%items%' then 'Order Item: % Off'
when lower(promo_title) like '%$%off%' or lower(incentive_value_type) like '%flat%' then 'Subtotal: Flat Amount Off'
when lower(promo_title) like '%off%' then 'Subtotal: % Off'
when lower(promo_title) like '%spend%save%' then 'Subtotal: Flat Amount Off'
when lower(promo_title) like '%buy%get%' then 'BOGO'
when lower(incentive_target_type) like  '%subtotal%' and lower(incentive_value_type) like '%percent%' then 'Subtotal: % Off'
when lower(incentive_target_type) like  '%item%' and lower(incentive_value_type) like '%percent%' then 'Order Item: % Off'
when lower(incentive_target_type) like  '%item%' and lower(incentive_value_type) like '%set%' then 'Order Item: Set Value'
when lower(incentive_target_type) like  '%delivery%' and lower(incentive_value_type) like '%set%' then 'Delivery Fee: Set Value'
else incentive_value_type end campaign_type,
--concat(incentive_target_type, incentive_value_type),
promo_title
from
proddb.public.fact_daily_ad_store_campaigns
where 1=1
--and TP between date'2026-01-19' and date'2026-01-25'
and is_active_campaign = TRUE
and is_merchant_funded = TRUE
--and PRODUCT_TYPE<> 'ad'
group by all
)
-- Final output: Stores on promos with daily discount amounts
SELECT
    smb_flag,
    cm.campaign_type,--promo_title,
    sum(dmd.orders) orders,
    sum(dmd.total_cx_discount_usd) total_cx_discount_usd,
    round(sum(dmd.total_cx_discount_usd)/sum(dmd.orders),4) avg_cx_discount_per_order
FROM daily_mx_discounts dmd
LEFT JOIN campaign_metadata cm
    ON cm.campaign_id = dmd.campaign_id
    --AND cm.store_id = dmd.store_id
    --AND cm.active_date = dmd.delivery_active_date
--LEFT JOIN proddb.public.dimension_store ds
    --ON ds.store_id = dmd.store_id
group by 1,2
order by 1,3 desc

SMB_FLAG	CAMPAIGN_TYPE	ORDERS	TOTAL_CX_DISCOUNT_USD	AVG_CX_DISCOUNT_PER_ORDER
0	BOGO	300190	2387955.480000	7.9548
0	Subtotal: Flat Amount Off	298154	1345601.420000	4.5131
0	Free Item	246598	1761046.090000	7.1414
0		244058	1560427.910000	6.3937
0	Subtotal: % Off	148115	800165.920000	5.4023
0	Order Item: % Off	39419	181584.360000	4.6065
0	Delivery Fee: Set Value	38512	123292.460000	3.2014
1		354983	2420614.600000	6.8190
1	Subtotal: % Off	165159	944445.020000	5.7184
1	BOGO	145995	1752834.430000	12.0061
1	Subtotal: Flat Amount Off	103808	520490.730000	5.0140
1	Delivery Fee: Set Value	86945	193642.040000	2.2272
1	Order Item: % Off	83353	521506.180000	6.2566
1	Free Item	4101	30973.900000	7.5528
1	Order Item: Set Value	9	24.950000	2.7722
