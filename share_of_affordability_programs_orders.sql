--Weekly Restaurant Only Orders
create or replace table proddb.katez.weekly_orders
AS (
    SELECT 
        c.first_date_of_week,
        dd.country_id,
        dd.delivery_id,
        dd.creator_id,
        dd.is_consumer_pickup::INT AS pickup, -- pickup flag
        dd.is_subscribed_consumer::INT AS dashpass, -- dashpass cx at the time of order flag
        case 
        when dfp.wbd_df_promo_discount >0 then 'WBD'
        when dfp.cs_df_promo_discount>0 then 'XS'
        when dfp.pad_fee_promo_discount>0 then 'PAD'
        else 'Others' end cohort,
        CASE 
            WHEN nv.business_line IS NULL THEN 'Restaurant' 
            ELSE 'New Verticals' 
        END AS vertical_business_line --Restaurant, New Vertical classification

    FROM proddb.public.dimension_deliveries dd
    join proddb.GLAUBERVASCONCELOS.DIMENSION_DATES c
    on dd.created_at::date = c.calendar_date::date
    LEFT JOIN proddb.public.fact_delivery_allocation fda ON dd.delivery_id = fda.delivery_id
    LEFT JOIN proddb.public.fact_delivery_distances fdd ON dd.delivery_id = fdd.delivery_id
    LEFT JOIN proddb.public.fact_core_delivery_metrics fcdm ON fcdm.delivery_id = dd.delivery_id
    LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv 
        ON dd.store_id = nv.store_id AND nv.is_filtered_mp_vertical = 1
    LEFT JOIN proddb.static.df_sf_promo_discount_delivery_level dfp -- fyi only populated from 7/1/2023 onwards. Wiki here: https://doordash.atlassian.net/wiki/spaces/DATA/pages/4476961078/DF+SF+Promo+Discount+Static+Table
        ON dd.delivery_id = dfp.delivery_id
    LEFT JOIN public.dimension_store_ext x ON dd.store_id = x.store_id
    WHERE dd.is_filtered_core = TRUE
        -- AND dd.is_consumer_pickup = FALSE -- optional to exclude pickup orders
        AND dd.fulfillment_type NOT IN ('dine_in', 'shipping', 'merchant_fleet', 'virtual') -- excluding dine-in, shipping, merchant fleet, and virtual orders (giftcards)
        AND dd.is_from_store_to_us = FALSE -- excluding store-to-us orders
        -- AND dd.is_bundle_order = FALSE -- excluding bundle orders -- an optional column to filter out DoubleDash
        --AND vertical_business_line = 'Restaurant' -- excluding non-restaurant orders -- an optional column to exclude NV
        --AND dd.country_id = 1 -- US only 
        AND dd.created_at BETWEEN '2022-09-01' AND '2026-01-31' -- date range
group by all
)

--Weekly US, Restaurant, Classic, Delivered Only Orders
select first_date_of_week,  
cohort, 
count(distinct delivery_id) orders
from proddb.katez.weekly_orders
where vertical_business_line = 'Restaurant'
and pickup = 0
and dashpass=0
and country_id = 1
group by 1,2
order by 1,2

--Country Breakdown: Restaurant, Classic, Delivered Only Orders
select --first_date_of_week,  
country_id, 
count(distinct case when cohort <> 'Others' then delivery_id end) affordability_orders,
count(distinct delivery_id) total_orders,
count(distinct case when cohort <> 'Others' then delivery_id end)*1.00000/count(distinct delivery_id) share
from proddb.katez.weekly_orders
where vertical_business_line = 'Restaurant'
and pickup = 0
and dashpass=0
and first_date_of_week>='2025-01-01'
group by 1
order by 1

COUNTRY_ID	AFFORDABILITY_ORDERS	TOTAL_ORDERS	SHARE
1	257911545	639665645	0.403197
2	10221554	29836576	0.342585
5	6141265	16171139	0.379767
503	959612	3078677	0.311696

--Country Breakdown: All restaurant Orders
select --first_date_of_week,  
country_id, 
count(distinct case when cohort <> 'Others' then delivery_id end) affordability_orders,
count(distinct delivery_id) total_orders,
count(distinct case when cohort <> 'Others' then delivery_id end)*1.00000/count(distinct delivery_id) share
from proddb.katez.weekly_orders
where 1=1
--and vertical_business_line = 'Restaurant'
--and pickup = 0
--and dashpass=0
and first_date_of_week>='2025-01-01'
group by 1
order by 1

COUNTRY_ID	AFFORDABILITY_ORDERS	TOTAL_ORDERS	SHARE
1	258702791	2034810321	0.127139
2	10247152	74812848	0.136970
5	6154365	41673092	0.147682
503	961137	5567011	0.172649

-- 2025 Affordability orders and spend
select 
count(distinct o.delivery_id) orders,
sum(wbd_df_promo_discount+pad_fee_promo_discount+cs_fee_promo_discount) discount_spend
from proddb.static.df_sf_promo_discount_delivery_level o
where year(created_at) = 2025
and (wbd_df_promo_discount > 0 or pad_fee_promo_discount>0 or cs_fee_promo_discount>0)
