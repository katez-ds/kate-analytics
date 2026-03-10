

with core_delivery_data AS (
    SELECT 
        -- Delivery identifiers
        dd.delivery_id
        , dd.creator_id
        , dd.store_id
        , dd.order_cart_id
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
        , fdd.straightline_r2c_distance AS r2c_sl_meters
        , fdd.ROAD_R2C_DISTANCE AS r2c_road_meters
        , fdd.straightline_r2c_distance/1609.34 AS r2c_sl_miles
        , fdd.ROAD_R2C_DISTANCE/1609.34 AS r2c_road_miles

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
        
        -- Quality metrics
        , fcdm.is_high_quality_delivery_mp
        , fcdm.is_20_min_late
        /*
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
        -- AND vertical_business_line = 'Restaurant' -- excluding non-restaurant orders -- an optional column to exclude NV
        AND dd.country_id = 1 -- US only 
        AND dd.created_at BETWEEN '2025-01-01' AND '2026-03-09'
)

SELECT * 
FROM core_delivery_data;
