create or replace table proddb.

with core_delivery_data AS (
    SELECT 
        -- Delivery identifiers
        dd.delivery_id
        , dd.creator_id
        , dd.store_id
        --, dd.order_cart_id
        , dd.submarket_id
        , dd.submarket_name
        , dd.created_at
        , CONVERT_TIMEZONE('UTC','America/Los_Angeles', dd.created_at) AS created_at_pst
        
        -- Customer attributes
        , dd.is_consumer_pickup::INT AS pickup -- pickup flag
        , dd.is_subscribed_consumer::INT AS dashpass -- dashpass cx at the time of order flag
        , dd.is_subscription_discount_applied::INT AS dashpass_eligible -- dashpass eligible pricing flag (ie a typical $5 Rx order would be 0 here but 1 for dashpass in the row above
        , dd.is_bundle_order -- flags whether order is bundled (Doubledash)
        --, dd.bundle_group -- Flag for whether the DoubleDash primary order or bundle order is checked out via 'pre_checkout' flow or 'post_checkout' flow
        --, dd.bundle_order_role -- ‘primary_order’: Primary order if there’s a bundle order placed. ‘bundle_order’: DoubleDash child bundle order
        
        -- Distance metrics
        --, fdd.straightline_r2c_distance AS r2c_sl_meters
        --, fdd.ROAD_R2C_DISTANCE AS r2c_road_meters
        , fdd.straightline_r2c_distance/1609.34 AS r2c_sl_miles
        --, fdd.ROAD_R2C_DISTANCE/1609.34 AS r2c_road_miles

        , NTILE(10) OVER(PARTITION BY dd.submarket_id ORDER BY fdd.straightline_r2c_distance) AS distance_percentile
        
        -- Fee components (gross amounts)
        , dd.delivery_fee/100.0 AS gross_delivery_fee
        , dd.service_fee/100.0 AS gross_service_fee
        , dd.small_order_fee/100.0 AS gross_small_order_fee
        , dd.expand_range_fee/100.0 AS gross_expand_range_fee
        , dd.legislative_fee/100.0 AS gross_legislative_fee
        , dd.priority_fee/100.0 AS express_fee

        -- Discount amounts
        , COALESCE(dfp.total_df_promo_discount, 0) AS df_discount_amount
        , COALESCE(dfp.total_sf_promo_discount, 0) AS sf_discount_amount
        , COALESCE(dfp.total_fee_promo_discount, 0) AS total_fee_discount_amount
        
        -- Individual program fee discounts
        , dfp.wbd_df_promo_discount -- Welcome Back Discount (WBD) df fee promo
        , dfp.cs_df_promo_discount -- Cross Shopper (CS or XS) df fee promo
        , dfp.pad_fee_promo_discount -- PAD GTM total fee promo
        , dfp.pad_df_promo_discount -- PAD GTM df fee promo
        , dfp.pad_sf_promo_discount -- PAD GTM sf fee promo
        
        -- Net fee calculations (after discounts)
        , GREATEST(dd.delivery_fee/100.0 - COALESCE(df_discount_amount, 0), 0) AS net_delivery_fee
        , GREATEST(dd.service_fee/100.0 - COALESCE(sf_discount_amount, 0), 0) AS net_service_fee
        
        -- Total fees calculation
        , net_delivery_fee + net_service_fee + gross_small_order_fee + gross_expand_range_fee + gross_legislative_fee AS total_net_fees -- gross fees included don't currently have any fee promo discounts as of 7/1/2025

        -- Mx and Doordash funded promotions applied
        , dfp.mx_funded_cx_discount -- mx funded promotion spend, in dollars
        , dfp.dd_funded_cx_discount -- dd funded promotion spend, in dollars
  
        -- Order value metrics
        , dd.gov/100.0 AS aov -- average order value
        , dd.subtotal/100.0 AS subtotal -- subtotal in dollars
        , dd.tip/100.0 AS tip -- tip in dollars 
        , dd.tax/100.0 AS tax -- tax in dollars
        
        -- Variable profit calculation
        , COALESCE(fda.variable_profit_ex_alloc, fda.variable_profit + fda.payment_to_customers, fda.variable_profit) AS unit_vp -- Unit VP
        , dd.subscription_alloc/100.0 AS subscription_alloc
        , unit_vp - dd.subscription_alloc/100.0 AS trans_vp -- Trans VP, removing subscription alloc for DP orders
        
        -- Vertical classification
        , CASE 
            WHEN nv.business_line IS NULL THEN 'Restaurant' 
            ELSE nv.business_line 
        END AS vertical_business_line --Restaurant, New Vertical classification
        /*        
        -- Quality metrics
        , fcdm.is_high_quality_delivery_mp
        , fcdm.is_20_min_late
    
        -- Timing metrics
        , dd.distinct_active_duration/3600.0 AS delivery_duration_hours
        , DATEDIFF('minute', dd.created_at, dd.actual_delivery_time) AS asap_minutes
        , DATEDIFF('minute', dd.quoted_delivery_time, dd.actual_delivery_time) AS lateness_minutes

        -- Variable Profit component breakout 
        ---- The below components including those already calculated above will sum to variable_profit
        , dd.GROSS_FEE/100.0 as GROSS_FEE
        -- components already calculated above: gross_delivery_fee, gross_service_fee, gross_small_order_fee, gross_expand_range_fee, gross_legislative_fee, express_fee,

        , fda.SUBSCRIPTION_ALLOC as SUBSCRIPTION_ALLOC
        , fda.DASHER_COST
        , fda.REFUNDS_CREDITS
        , fda.PROMOTIONS - fda.payment_to_customers as promotions
        , fda.COST_OF_SALES
        , fda.SUPPORT
        , fda.commission_alloc
        , fda.gross_fee - dd.DELIVERY_FEE/100.0 - dd.SERVICE_FEE/100.0 - dd.LEGISLATIVE_FEE/100.0 
                - dd.expand_range_fee/100.0 - dd.priority_fee/100.0 - fda.SUBSCRIPTION_ALLOC - fda.commission_alloc 
                + fda.SUPPORT_PAYROLL_TE_ALLOC as other_fees
      
        -- the below US markets into regulatory (California, Seattle, NYC) and non-Regulatory (all others)
        -- Regulatory are markets with higher Dasher Pay standards leading to significantly higher cost per mile.
        , case when dd.market_id in (1, 2) then 'California'
                when dd.submarket_id in (22) then 'Seattle'
                when dd.submarket_id in (8, 17, 72, 304, 7347) then 'NYC'
                else 'Non-Regulatory' end as reg_filter

        -- daypart
        , case
            when date_part('hour',convert_timezone('UTC',dd.timezone,dd.QUOTED_DELIVERY_TIME)) <5 THEN 'early_morning'
            when date_part('hour',convert_timezone('UTC',dd.timezone,dd.QUOTED_DELIVERY_TIME)) between 5 and 10 then 'breakfast'
            when date_part('hour',convert_timezone('UTC',dd.timezone,dd.QUOTED_DELIVERY_TIME)) between 11 and 13 then 'lunch'
            when date_part('hour',convert_timezone('UTC',dd.timezone,dd.QUOTED_DELIVERY_TIME)) between 14 and 16 then 'snack'
            when date_part('hour',convert_timezone('UTC',dd.timezone,dd.QUOTED_DELIVERY_TIME)) between 17 and 20 then 'dinner'
            when date_part('hour',convert_timezone('UTC',dd.timezone,dd.QUOTED_DELIVERY_TIME)) between 21 and 23 then 'latenight' else null
          end as daypart
          */       
        -- SMB vs. Ent
        , case when x.management_type IN ('ENTERPRISE', 'MID MARKET') then 0 else 1 end as smb_flag

    FROM proddb.public.dimension_deliveries dd
    --LEFT JOIN proddb.public.fact_delivery_allocation fda ON dd.delivery_id = fda.delivery_id
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
        -- AND vertical_business_line = 'Restaurant' -- excluding non-restaurant orders -- an optional column to exclude NV
        AND dd.country_id = 1 -- US only 
        AND dd.created_at >= '2025-11-12' 
),

be as(
    select 
    /*
        case 
            WHEN fde.tag in ('control') THEN 'Control'
             else fde.tag
             end as tag_renamed,
    */
        try_cast(fde.bucket_key as integer) as user_id 
         , min(fde.exposure_time) as first_exposed
        --, fde.experiment_version 
        --, min(fde.exposure_time) as exposure_time
    from 
          PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE fde
           inner join proddb.public.fact_unique_visitors_full_utc fuv
          on try_to_Numeric(fde.bucket_key) = try_to_numeric(fuv.user_id)
          and fde.exposure_time::date = fuv.event_date
    where experiment_name = 'new_fee_structure_v4'
        and experiment_version between 2 and 100
        and exposure_time::Date >= '2025-11-12' 
        and tag = 'treatment4'
        -- and convert_timezone('UTC','America/Los_Angeles',exposure_time) between $start_time and $end_time
        -- and (tag in ('control') or tag in ( ))
        and bucket_key not in ('1505155093') --Exclude Dashmart re-stocking cx_id
        and SEGMENT in ('Users')
        and fuv.COUNTRY_NAME = 'United States'  -- United States
        AND fuv.event_date >= '2025-11-12' 
        AND fuv.user_id IS NOT NULL  -- Only signed-in users
        AND fuv.UNIQUE_CORE_VISITOR = 1 
    group by all
    )
    
-- SF Distribution
with base as
(select * from proddb.katez.nfs_vs_t4_orders_0326
where VERTICAL_BUSINESS_LINE = 'Restaurant'
and pickup = 0
)

select dashpass,--dashpass_eligible,mb_flag,
    case
    when gross_service_fee > 0
    and gross_service_fee <= 1 then '$0 ~ $1'
    when gross_service_fee > 1
    and gross_service_fee <= 2 then '$1 ~ $2'
    when gross_service_fee > 2
    and gross_service_fee <= 3 then '$2 ~ $3'
    when gross_service_fee > 3
    and gross_service_fee <= 4 then '$3 ~ $4'
    else '$4+'
    end gross_services_fee,
   COUNT(DISTINCT delivery_id) AS orders
from base
group by all
order by 1,2

DASHPASS	DISCOUNT_AMOUNT	ORDERS
0	$0 ~ $1	8902
0	$1 ~ $2	259577
0	$2 ~ $3	1867557
0	$3 ~ $4	1206562
0	$4+	1959433
1	$0 ~ $1	3926768
1	$1 ~ $2	5509700
1	$2 ~ $3	1994406
1	$3 ~ $4	672146
1	$4+	998781
    
-- DashPass vs Classic
with base as
(select * from proddb.katez.nfs_vs_t4_orders_0326
where VERTICAL_BUSINESS_LINE = 'Restaurant'
and pickup = 0
)

select dashpass,--dashpass_eligible,mb_flag,
   COUNT(DISTINCT delivery_id) AS orders,
    COUNT(DISTINCT creator_id) AS consumers,

    AVG(aov) AS avg_gov,
    AVG(subtotal) AS avg_subtotal,
        
    AVG(df_discount_amount) AS avg_df_discount,
    AVG(sf_discount_amount) AS avg_sf_discount,
    AVG(WBD_df_promo_discount+cs_df_promo_discount+pad_fee_promo_discount) AS avg_affordability_discount,
    AVG(total_fee_discount_amount) AS avg_total_fee_discount,
    
    AVG(gross_delivery_fee) AS avg_gross_df,
    AVG(gross_service_fee) AS avg_gross_sf,

    AVG(net_delivery_fee) AS avg_net_df,
    AVG(net_service_fee) AS avg_net_sf,
    --AVG(total_net_fees) AS avg_total_net_fee,

    SUM(CASE WHEN df_discount_amount > 0 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) AS pct_with_df_discount,
    SUM(CASE WHEN sf_discount_amount > 0 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) AS pct_with_sf_discount,
    sum(df_discount_amount) / NULLIF(sum(gross_delivery_fee), 0) AS df_discount_rate,
    sum(sf_discount_amount) / NULLIF(sum(gross_service_fee), 0) AS sf_discount_rate,
    sum(total_fee_discount_amount) / NULLIF(sum(gross_delivery_fee+gross_service_fee), 0) fee_discount_rate,
    sum(WBD_df_promo_discount+cs_df_promo_discount+pad_fee_promo_discount) / NULLIF(sum(gross_delivery_fee), 0) afforability_discount_rate,
    SUM(CASE WHEN gross_delivery_fee > 0 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) AS pct_with_df,
    SUM(CASE WHEN gross_service_fee > 0 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) AS pct_with_sf

from base
group by 1

-- SMB vs ENT
with base as
(select * from proddb.katez.nfs_vs_t4_orders_0326
where VERTICAL_BUSINESS_LINE = 'Restaurant'
and pickup = 0
)

select dashpass,smb_flag,
    COUNT(DISTINCT delivery_id) AS orders,
    AVG(aov) AS gov,
    AVG(gross_delivery_fee) AS avg_gross_df,
    AVG(gross_service_fee) AS avg_gross_sf,
    AVG(gross_service_fee) * 1.0000/AVG(gross_delivery_fee) gross_sf_df_ratio,

    AVG(net_delivery_fee) AS avg_net_df,
    AVG(net_service_fee) AS avg_net_sf,
    AVG(net_service_fee) * 1.0000/AVG(net_delivery_fee) net_sf_df_ratio,

    AVG(WBD_df_promo_discount+cs_df_promo_discount+pad_fee_promo_discount) AS avg_affordability_discount,

    sum(WBD_df_promo_discount+cs_df_promo_discount+pad_fee_promo_discount) / NULLIF(sum(gross_delivery_fee), 0) afforability_discount_rate,
    sum(total_fee_discount_amount) / NULLIF(sum(gross_delivery_fee+gross_service_fee), 0) fee_discount_rate
from base
group by all
order by 1,2

DASHPASS	SMB_FLAG	ORDERS	AVG_GROSS_DF	AVG_GROSS_SF	GROSS_SF_DF_RATIO	AVG_NET_DF	AVG_NET_SF	NET_SF_DF_RATIO	AVG_AFFORDABILITY_DISCOUNT	AFFORABILITY_DISCOUNT_RATE	FEE_DISCOUNT_RATE
0	0	3538515	2.634332975839	3.782794474518	1.435959124838	1.870939693629	3.774237825755	2.017295286752	0.659072571404	0.250185750036	0.120295775360
0	1	1763516	2.341511922772	4.759229556182	2.032545514672	1.772824907741	4.713854980618	2.658951236546	0.452443748738	0.193227181266	0.086478756277
1	0	8454877	0.219960917232	1.612090210183	7.328984759973	0.177177437354	1.611544654050	9.095653928159	0.000145844818	0.000663048782	0.023650754167
1	1	4646924	0.227118876917	2.200176572718	9.687334679460	0.197096505129	2.193664613409	11.129901121145	0.000094393625	0.000415613295	0.015051456180

-- Order Distance
with base as
(select * from proddb.katez.nfs_vs_t4_orders_0326
where VERTICAL_BUSINESS_LINE = 'Restaurant'
and pickup = 0
)

select dashpass,
CASE
    WHEN r2c_sl_miles < 1 THEN '00-01 mi'
    WHEN r2c_sl_miles < 3 THEN '01-03 mi'
    WHEN r2c_sl_miles < 5 THEN '03-05 mi'
    WHEN r2c_sl_miles < 8 THEN '05-08 mi'
    WHEN r2c_sl_miles < 12 THEN '08-12 mi'
    ELSE '12+  mi'
  END AS r2c_distance_segment,
    COUNT(DISTINCT delivery_id) AS orders,
    AVG(aOV) AS gov,
    AVG(gross_delivery_fee) AS avg_gross_df,
    AVG(gross_service_fee) AS avg_gross_sf,
    AVG(gross_service_fee) * 1.0000/AVG(gross_delivery_fee) gross_sf_df_ratio,

    AVG(net_delivery_fee) AS avg_net_df,
    AVG(net_service_fee) AS avg_net_sf,
    AVG(net_service_fee) * 1.0000/AVG(net_delivery_fee) net_sf_df_ratio,

    AVG(WBD_df_promo_discount+cs_df_promo_discount+pad_fee_promo_discount) AS avg_affordability_discount,

    sum(WBD_df_promo_discount+cs_df_promo_discount+pad_fee_promo_discount) / NULLIF(sum(gross_delivery_fee), 0) afforability_discount_rate,
    sum(total_fee_discount_amount) / NULLIF(sum(gross_delivery_fee+gross_service_fee), 0) fee_discount_rate
from base
group by all
order by 1,2

-- GOV
with base as
(select * from proddb.katez.nfs_vs_t4_orders_0326
where VERTICAL_BUSINESS_LINE = 'Restaurant'
and pickup = 0
)

select dashpass,
CASE
    WHEN aov < 20 THEN '$0-20'
    WHEN aov < 40 THEN '$20-40'
    WHEN aov < 60 THEN '$40-60'
    WHEN aov < 80 THEN '$60-80'
    WHEN aov < 100 THEN '$80-100'
    ELSE '$100+'
  END AS gov_segment,
    COUNT(DISTINCT delivery_id) AS orders,
    
    AVG(gross_delivery_fee) AS avg_gross_df,
    AVG(gross_service_fee) AS avg_gross_sf,
    AVG(gross_service_fee) * 1.0000/AVG(gross_delivery_fee) gross_sf_df_ratio,

    AVG(net_delivery_fee) AS avg_net_df,
    AVG(net_service_fee) AS avg_net_sf,
    AVG(net_service_fee) * 1.0000/AVG(net_delivery_fee) net_sf_df_ratio,

    AVG(WBD_df_promo_discount+cs_df_promo_discount+pad_fee_promo_discount) AS avg_affordability_discount,

    sum(WBD_df_promo_discount+cs_df_promo_discount+pad_fee_promo_discount) / NULLIF(sum(gross_delivery_fee), 0) afforability_discount_rate,
    sum(total_fee_discount_amount) / NULLIF(sum(gross_delivery_fee+gross_service_fee), 0) fee_discount_rate
from base
group by all
order by 1,2
    
-- L365D OF, PSMv2, PSMv3

with base as
(select * from proddb.katez.nfs_vs_t4_orders_0326
where VERTICAL_BUSINESS_LINE = 'Restaurant'
and pickup = 0
)
,
L365D_of as
(select
      creator_id user_id,L360_ORDERS L365D_orders,dte
    from
      proddb.mattheitz.mh_customer_authority
    where
      dte between '2025-11-12' and '2026-03-11'
    group by all
)

select dashpass,
    case
    when L365D_orders <20 then '0-20'
    when L365D_orders <40 then '20-40'
    when L365D_orders <60 then '40-60'
    when L365D_orders <80 then '60-80'
    when L365D_orders <100 then '80-100'
    else '100+'
  end L365D_orders,
   COUNT(DISTINCT delivery_id) AS orders,
    AVG(gross_delivery_fee) AS avg_gross_df,
    AVG(gross_service_fee) AS avg_gross_sf,
    AVG(gross_service_fee) * 1.0000/AVG(gross_delivery_fee) gross_sf_df_ratio,

    AVG(net_delivery_fee) AS avg_net_df,
    AVG(net_service_fee) AS avg_net_sf,
    AVG(net_service_fee) * 1.0000/AVG(net_delivery_fee) net_sf_df_ratio,

    AVG(WBD_df_promo_discount+cs_df_promo_discount+pad_fee_promo_discount) AS avg_affordability_discount,

    sum(WBD_df_promo_discount+cs_df_promo_discount+pad_fee_promo_discount) / NULLIF(sum(gross_delivery_fee), 0) afforability_discount_rate,
    sum(total_fee_discount_amount) / NULLIF(sum(gross_delivery_fee+gross_service_fee), 0) fee_discount_rate

from base a
    left join L365D_of b on a.creator_id = b.user_id
      and b.dte = a.order_dt
    --left join proddb.public.cx_sensitivity_v2 psm on a.user_id = psm.consumer_id and prediction_datetime_est = a.order_dt
group by 1,2
order by 1,2


-- PSMv3
with base as
(select * from proddb.katez.nfs_vs_t4_orders_0326
where VERTICAL_BUSINESS_LINE = 'Restaurant'
and pickup = 0
)
,
psm AS (
    select
      consumer_id,
      active_date dte,
      V3_SENSITIVITY_COHORT cohort
    from
      proddb.ml.cx_sensitivity_v3
    where
      active_date between '2025-11-12' and '2026-03-11'
    group by all
  )

select 
    cohort,
   COUNT(DISTINCT delivery_id) AS orders,
    avg(aov) AS gov,
    AVG(gross_delivery_fee) AS avg_gross_df,
    AVG(gross_service_fee) AS avg_gross_sf,
    AVG(gross_service_fee) * 1.0000/AVG(gross_delivery_fee) gross_sf_df_ratio,

    AVG(net_delivery_fee) AS avg_net_df,
    AVG(net_service_fee) AS avg_net_sf,
    AVG(net_service_fee) * 1.0000/AVG(net_delivery_fee) net_sf_df_ratio,

    AVG(WBD_df_promo_discount+cs_df_promo_discount+pad_fee_promo_discount) AS avg_affordability_discount,

    sum(WBD_df_promo_discount+cs_df_promo_discount+pad_fee_promo_discount) / NULLIF(sum(gross_delivery_fee), 0) afforability_discount_rate,
    sum(total_fee_discount_amount) / NULLIF(sum(gross_delivery_fee+gross_service_fee), 0) fee_discount_rate

from base a
    left join psm on a.user_id = psm.consumer_id and dte = a.order_dt
group by 1
order by 1


