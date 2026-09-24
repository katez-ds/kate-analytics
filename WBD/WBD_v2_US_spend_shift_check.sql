-- ============================================================================
-- WBD 2.0 V2 US -- Spend / Discount Analysis by Treatment Arm and Cx Segment
-- Experiment: discount_engine_us_wbd_v2, version >= 22 (standing rule -- see
-- project_wbd_v2_daily_trend_tag_switch_bug memory: v22+ avoids a tag-
-- reshuffle double-count bug present in earlier versions).
-- Window: 2026-07-29 to 2026-09-22 (data confirmed fresh through 9/22).
-- Population: Classic Rx delivery orders (is_filtered_core, US, non-pickup,
-- non-DashPass, restaurant, excl. non-delivery fulfillment types), for each
-- arm's exposed Cx on/after their first_exposed date.
-- DashMart re-stocking bot (bucket_key 1505155093) excluded throughout.
--
-- Six queries, run independently (each is a full standalone statement):
--   1. Discount-amount % distribution by arm (WBD/PAD-discounted orders only)
--   2. Discount-amount bucket $ spend by arm (WBD/PAD-discounted orders only)
--   3. Cx Segment % + spend + avg discount/order by arm (discounted orders only)
--   4. Order-level coverage % by segment/arm (denominator = ALL Classic Rx orders)
--   5. User-level coverage % by segment/arm (denominator = ALL exposed users)
--   6. Spend by segment/arm across Affordability/CRM/Mx-funded/Total programs
--      (denominator = ALL Classic Rx orders, not discount-filtered)
-- ============================================================================


-- ============================================================================
-- 1. DISCOUNT-AMOUNT % DISTRIBUTION BY ARM
-- % of WBD/PAD-discounted Classic Rx delivery orders by $ discount-amount
-- bucket, broken out by treatment arm. Buckets: $0-1/$1-2/$2-3/>$3.
-- ============================================================================
WITH params AS (
    SELECT
        DATE '2026-07-29' AS start_date,
        DATE '2026-09-22' AS end_date
),

exposure_cohort AS (
    SELECT
        tag,
        TRY_CAST(bucket_key AS INTEGER)  AS consumer_id,
        MIN(CAST(exposure_time AS DATE)) AS first_exposed
    FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
    WHERE experiment_name = 'discount_engine_us_wbd_v2'
      AND experiment_version BETWEEN 22 AND 100
      AND exposure_time >= '2026-07-29'
      AND segment = 'Users'
      AND TRY_CAST(bucket_key AS INTEGER) != 1505155093
    GROUP BY ALL
),

base_orders AS (
    SELECT
        dd.delivery_id,
        dd.creator_id      AS consumer_id,
        ec.tag,
        dd.store_id
    FROM proddb.public.dimension_deliveries dd
    JOIN edw.merchant.dimension_store ds
      ON ds.store_id = dd.store_id
    JOIN exposure_cohort ec
      ON ec.consumer_id = dd.creator_id
     AND CAST(dd.created_at AS DATE) >= ec.first_exposed
    CROSS JOIN params p
    WHERE CAST(dd.created_at AS DATE) BETWEEN p.start_date AND p.end_date
      AND dd.is_filtered_core       = TRUE
      AND dd.country_id            = 1
      AND dd.is_consumer_pickup     = FALSE
      AND dd.is_subscribed_consumer = FALSE
      AND NVL(ds.is_restaurant, 0)  = 1
      AND NVL(dd.fulfillment_type, '') NOT IN
          ('merchant_fleet', 'shipping', 'digital', 'virtual', 'dine_in', 'drone')
),

discounts AS (
    SELECT
        delivery_id,
        NVL(SUM(wbd_df_promo_discount), 0) AS wbd_discount,
        NVL(SUM(pad_df_promo_discount), 0) AS pad_discount
    FROM proddb.static.df_sf_promo_discount_delivery_level
    GROUP BY 1
),

attributed AS (
    SELECT
        b.tag,
        b.delivery_id,
        (d.wbd_discount + d.pad_discount) AS wbd_pad_discount
    FROM base_orders b
    JOIN discounts d ON d.delivery_id = b.delivery_id
    WHERE (d.wbd_discount + d.pad_discount) > 0   -- WBD/PAD-discounted orders only
),

bucketed AS (
    SELECT
        tag,
        delivery_id,
        CASE
            WHEN wbd_pad_discount > 0 AND wbd_pad_discount <= 1 THEN '1. $0 ~ $1'
            WHEN wbd_pad_discount > 1 AND wbd_pad_discount <= 2 THEN '2. $1 ~ $2'
            WHEN wbd_pad_discount > 2 AND wbd_pad_discount <= 3 THEN '3. $2 ~ $3'
            ELSE '4. >$3'
        END AS discount_amount_bucket
    FROM attributed
)

SELECT
    tag                                                          AS arm,
    discount_amount_bucket,
    COUNT(DISTINCT delivery_id)                                  AS orders,
    ROUND(100.0 * COUNT(DISTINCT delivery_id)
        / NULLIF(SUM(COUNT(DISTINCT delivery_id)) OVER (PARTITION BY tag), 0), 2)
                                                                  AS pct_of_arm_wbd_pad_discounted_orders
FROM bucketed
GROUP BY ALL
ORDER BY tag, discount_amount_bucket
;


-- ============================================================================
-- 2. DISCOUNT-AMOUNT BUCKET $ SPEND BY ARM
-- Same buckets as query 1, but showing total $ spend per bucket/arm instead
-- of order-count %.
-- ============================================================================
WITH params AS (
    SELECT
        DATE '2026-07-29' AS start_date,
        DATE '2026-09-22' AS end_date
),

exposure_cohort AS (
    SELECT
        tag,
        TRY_CAST(bucket_key AS INTEGER)  AS consumer_id,
        MIN(CAST(exposure_time AS DATE)) AS first_exposed
    FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
    WHERE experiment_name = 'discount_engine_us_wbd_v2'
      AND experiment_version BETWEEN 22 AND 100
      AND exposure_time >= '2026-07-29'
      AND segment = 'Users'
      AND TRY_CAST(bucket_key AS INTEGER) != 1505155093
    GROUP BY ALL
),

base_orders AS (
    SELECT
        dd.delivery_id,
        dd.creator_id      AS consumer_id,
        ec.tag,
        dd.store_id
    FROM proddb.public.dimension_deliveries dd
    JOIN edw.merchant.dimension_store ds
      ON ds.store_id = dd.store_id
    JOIN exposure_cohort ec
      ON ec.consumer_id = dd.creator_id
     AND CAST(dd.created_at AS DATE) >= ec.first_exposed
    CROSS JOIN params p
    WHERE CAST(dd.created_at AS DATE) BETWEEN p.start_date AND p.end_date
      AND dd.is_filtered_core       = TRUE
      AND dd.country_id            = 1
      AND dd.is_consumer_pickup     = FALSE
      AND dd.is_subscribed_consumer = FALSE
      AND NVL(ds.is_restaurant, 0)  = 1
      AND NVL(dd.fulfillment_type, '') NOT IN
          ('merchant_fleet', 'shipping', 'digital', 'virtual', 'dine_in', 'drone')
),

discounts AS (
    SELECT
        delivery_id,
        NVL(SUM(wbd_df_promo_discount), 0) AS wbd_discount,
        NVL(SUM(pad_df_promo_discount), 0) AS pad_discount
    FROM proddb.static.df_sf_promo_discount_delivery_level
    GROUP BY 1
),

attributed AS (
    SELECT
        b.tag,
        b.delivery_id,
        (d.wbd_discount + d.pad_discount) AS wbd_pad_discount
    FROM base_orders b
    JOIN discounts d ON d.delivery_id = b.delivery_id
    WHERE (d.wbd_discount + d.pad_discount) > 0
),

bucketed AS (
    SELECT
        tag,
        delivery_id,
        wbd_pad_discount,
        CASE
            WHEN wbd_pad_discount > 0 AND wbd_pad_discount <= 1 THEN '1. $0 ~ $1'
            WHEN wbd_pad_discount > 1 AND wbd_pad_discount <= 2 THEN '2. $1 ~ $2'
            WHEN wbd_pad_discount > 2 AND wbd_pad_discount <= 3 THEN '3. $2 ~ $3'
            ELSE '4. >$3'
        END AS discount_amount_bucket
    FROM attributed
)

SELECT
    tag                                                          AS arm,
    discount_amount_bucket,
    COUNT(DISTINCT delivery_id)                                  AS orders,
    ROUND(SUM(wbd_pad_discount), 2)                              AS total_wbd_pad_df_spend
FROM bucketed
GROUP BY ALL
ORDER BY tag, discount_amount_bucket
;


-- ============================================================================
-- 3. CX SEGMENT % + SPEND + AVG DISCOUNT/ORDER BY ARM
-- % of WBD/PAD-discounted orders by Cx segment (per-order-day segment, joined
-- on creator_id/dte=order_dt -- matches existing dashboard convention), plus
-- total spend and avg discount depth per discounted order, by arm.
-- ============================================================================
WITH params AS (
    SELECT
        DATE '2026-07-29' AS start_date,
        DATE '2026-09-22' AS end_date
),

exposure_cohort AS (
    SELECT
        tag,
        TRY_CAST(bucket_key AS INTEGER)  AS consumer_id,
        MIN(CAST(exposure_time AS DATE)) AS first_exposed
    FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
    WHERE experiment_name = 'discount_engine_us_wbd_v2'
      AND experiment_version BETWEEN 22 AND 100
      AND exposure_time >= '2026-07-29'
      AND segment = 'Users'
      AND TRY_CAST(bucket_key AS INTEGER) != 1505155093
    GROUP BY ALL
),

base_orders AS (
    SELECT
        dd.delivery_id,
        dd.creator_id      AS consumer_id,
        ec.tag,
        CAST(dd.created_at AS DATE) AS order_dt
    FROM proddb.public.dimension_deliveries dd
    JOIN edw.merchant.dimension_store ds
      ON ds.store_id = dd.store_id
    JOIN exposure_cohort ec
      ON ec.consumer_id = dd.creator_id
     AND CAST(dd.created_at AS DATE) >= ec.first_exposed
    CROSS JOIN params p
    WHERE CAST(dd.created_at AS DATE) BETWEEN p.start_date AND p.end_date
      AND dd.is_filtered_core       = TRUE
      AND dd.country_id            = 1
      AND dd.is_consumer_pickup     = FALSE
      AND dd.is_subscribed_consumer = FALSE
      AND NVL(ds.is_restaurant, 0)  = 1
      AND NVL(dd.fulfillment_type, '') NOT IN
          ('merchant_fleet', 'shipping', 'digital', 'virtual', 'dine_in', 'drone')
),

discounts AS (
    SELECT
        delivery_id,
        NVL(SUM(wbd_df_promo_discount), 0) AS wbd_discount,
        NVL(SUM(pad_df_promo_discount), 0) AS pad_discount
    FROM proddb.static.df_sf_promo_discount_delivery_level
    GROUP BY 1
),

attributed AS (
    SELECT
        b.tag,
        b.delivery_id,
        b.consumer_id,
        b.order_dt,
        (d.wbd_discount + d.pad_discount) AS wbd_pad_discount
    FROM base_orders b
    JOIN discounts d ON d.delivery_id = b.delivery_id
    WHERE (d.wbd_discount + d.pad_discount) > 0   -- WBD/PAD DF-discounted orders only
),

segmented AS (
    SELECT
        a.tag,
        a.delivery_id,
        a.wbd_pad_discount,
        CASE
            WHEN ca.days_since_first_purchase < 29              THEN '1. New/FMX'
            WHEN ca.days_since_last_purchase > 90               THEN '2. Churned'
            WHEN ca.days_since_last_purchase BETWEEN 29 AND 90  THEN '3. Dormant'
            WHEN ca.l28_orders BETWEEN 1 AND 2                  THEN '4. Active - Occasional'
            WHEN ca.l28_orders BETWEEN 3 AND 4                  THEN '5. Active - Habituating'
            WHEN ca.l28_orders >= 5                             THEN '6. Active - RDP'
            ELSE '7. Unmatched'
        END AS segment
    FROM attributed a
    LEFT JOIN proddb.mattheitz.mh_customer_authority ca
      ON ca.creator_id = a.consumer_id
     AND ca.dte = a.order_dt
)

SELECT
    tag                                                          AS arm,
    segment,
    COUNT(DISTINCT delivery_id)                                  AS orders,
    ROUND(100.0 * COUNT(DISTINCT delivery_id)
        / NULLIF(SUM(COUNT(DISTINCT delivery_id)) OVER (PARTITION BY tag), 0), 2)
                                                                  AS pct_of_arm_wbd_pad_discounted_orders,
    ROUND(SUM(wbd_pad_discount), 2)                              AS total_wbd_pad_df_spend,
    ROUND(AVG(wbd_pad_discount), 4)                              AS avg_wbd_pad_df_discount_per_order
FROM segmented
GROUP BY ALL
ORDER BY tag, segment
;


-- ============================================================================
-- 4. ORDER-LEVEL COVERAGE % BY SEGMENT/ARM
-- % of ALL Classic Rx delivery orders (not just discounted ones) that got a
-- nonzero WBD/PAD DF discount, by Cx segment and arm -- decomposes spend
-- differences into coverage vs. depth vs. volume.
-- ============================================================================
WITH params AS (
    SELECT
        DATE '2026-07-29' AS start_date,
        DATE '2026-09-22' AS end_date
),

exposure_cohort AS (
    SELECT
        tag,
        TRY_CAST(bucket_key AS INTEGER)  AS consumer_id,
        MIN(CAST(exposure_time AS DATE)) AS first_exposed
    FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
    WHERE experiment_name = 'discount_engine_us_wbd_v2'
      AND experiment_version BETWEEN 22 AND 100
      AND exposure_time >= '2026-07-29'
      AND segment = 'Users'
      AND TRY_CAST(bucket_key AS INTEGER) != 1505155093
    GROUP BY ALL
),

base_orders AS (
    SELECT
        dd.delivery_id,
        dd.creator_id                AS consumer_id,
        ec.tag,
        CAST(dd.created_at AS DATE)  AS order_dt
    FROM proddb.public.dimension_deliveries dd
    JOIN edw.merchant.dimension_store ds
      ON ds.store_id = dd.store_id
    JOIN exposure_cohort ec
      ON ec.consumer_id = dd.creator_id
     AND CAST(dd.created_at AS DATE) >= ec.first_exposed
    CROSS JOIN params p
    WHERE CAST(dd.created_at AS DATE) BETWEEN p.start_date AND p.end_date
      AND dd.is_filtered_core       = TRUE
      AND dd.country_id            = 1
      AND dd.is_consumer_pickup     = FALSE
      AND dd.is_subscribed_consumer = FALSE
      AND NVL(ds.is_restaurant, 0)  = 1
      AND NVL(dd.fulfillment_type, '') NOT IN
          ('merchant_fleet', 'shipping', 'digital', 'virtual', 'dine_in', 'drone')
    -- NOTE: NO discount>0 filter here -- ALL Classic Rx delivery orders
),

discounts AS (
    SELECT
        delivery_id,
        NVL(SUM(wbd_df_promo_discount), 0) AS wbd_discount,
        NVL(SUM(pad_df_promo_discount), 0) AS pad_discount
    FROM proddb.static.df_sf_promo_discount_delivery_level
    GROUP BY 1
),

joined AS (
    SELECT
        b.tag,
        b.delivery_id,
        b.consumer_id,
        b.order_dt,
        NVL(d.wbd_discount + d.pad_discount, 0) AS wbd_pad_discount
    FROM base_orders b
    LEFT JOIN discounts d ON d.delivery_id = b.delivery_id
),

segmented AS (
    SELECT
        j.tag,
        j.delivery_id,
        j.wbd_pad_discount,
        CASE
            WHEN ca.days_since_first_purchase < 29              THEN '1. New/FMX'
            WHEN ca.days_since_last_purchase > 90               THEN '2. Churned'
            WHEN ca.days_since_last_purchase BETWEEN 29 AND 90  THEN '3. Dormant'
            WHEN ca.l28_orders BETWEEN 1 AND 2                  THEN '4. Active - Occasional'
            WHEN ca.l28_orders BETWEEN 3 AND 4                  THEN '5. Active - Habituating'
            WHEN ca.l28_orders >= 5                             THEN '6. Active - RDP'
            ELSE '7. Unmatched'
        END AS segment
    FROM joined j
    LEFT JOIN proddb.mattheitz.mh_customer_authority ca
      ON ca.creator_id = j.consumer_id
     AND ca.dte = j.order_dt
)

SELECT
    tag                                                           AS arm,
    segment,
    COUNT(DISTINCT delivery_id)                                   AS total_classic_rx_orders,
    COUNT(DISTINCT CASE WHEN wbd_pad_discount > 0 THEN delivery_id END)
                                                                   AS discounted_orders,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN wbd_pad_discount > 0 THEN delivery_id END)
        / NULLIF(COUNT(DISTINCT delivery_id), 0), 2)              AS pct_coverage,
    ROUND(SUM(wbd_pad_discount), 2)                               AS total_wbd_pad_df_spend,
    ROUND(SUM(wbd_pad_discount) / NULLIF(COUNT(DISTINCT CASE WHEN wbd_pad_discount > 0 THEN delivery_id END), 0), 4)
                                                                   AS avg_depth_per_discounted_order
FROM segmented
GROUP BY ALL
ORDER BY tag, segment
;


-- ============================================================================
-- 5. USER-LEVEL COVERAGE % BY SEGMENT/ARM
-- % of distinct users in each Cx segment who had >=1 order with a nonzero
-- WBD/PAD DF discount, by segment and arm. A user can appear in multiple
-- segments over the window (segment assigned per order-day); "in segment X"
-- here means "had >=1 Classic Rx delivery order that landed in segment X."
-- ============================================================================
WITH params AS (
    SELECT
        DATE '2026-07-29' AS start_date,
        DATE '2026-09-22' AS end_date
),

exposure_cohort AS (
    SELECT
        tag,
        TRY_CAST(bucket_key AS INTEGER)  AS consumer_id,
        MIN(CAST(exposure_time AS DATE)) AS first_exposed
    FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
    WHERE experiment_name = 'discount_engine_us_wbd_v2'
      AND experiment_version BETWEEN 22 AND 100
      AND exposure_time >= '2026-07-29'
      AND segment = 'Users'
      AND TRY_CAST(bucket_key AS INTEGER) != 1505155093
    GROUP BY ALL
),

base_orders AS (
    SELECT
        dd.delivery_id,
        dd.creator_id                AS consumer_id,
        ec.tag,
        CAST(dd.created_at AS DATE)  AS order_dt
    FROM proddb.public.dimension_deliveries dd
    JOIN edw.merchant.dimension_store ds
      ON ds.store_id = dd.store_id
    JOIN exposure_cohort ec
      ON ec.consumer_id = dd.creator_id
     AND CAST(dd.created_at AS DATE) >= ec.first_exposed
    CROSS JOIN params p
    WHERE CAST(dd.created_at AS DATE) BETWEEN p.start_date AND p.end_date
      AND dd.is_filtered_core       = TRUE
      AND dd.country_id            = 1
      AND dd.is_consumer_pickup     = FALSE
      AND dd.is_subscribed_consumer = FALSE
      AND NVL(ds.is_restaurant, 0)  = 1
      AND NVL(dd.fulfillment_type, '') NOT IN
          ('merchant_fleet', 'shipping', 'digital', 'virtual', 'dine_in', 'drone')
),

discounts AS (
    SELECT
        delivery_id,
        NVL(SUM(wbd_df_promo_discount), 0) AS wbd_discount,
        NVL(SUM(pad_df_promo_discount), 0) AS pad_discount
    FROM proddb.static.df_sf_promo_discount_delivery_level
    GROUP BY 1
),

joined AS (
    SELECT
        b.tag,
        b.delivery_id,
        b.consumer_id,
        b.order_dt,
        NVL(d.wbd_discount + d.pad_discount, 0) AS wbd_pad_discount
    FROM base_orders b
    LEFT JOIN discounts d ON d.delivery_id = b.delivery_id
),

segmented AS (
    SELECT
        j.tag,
        j.consumer_id,
        j.wbd_pad_discount,
        CASE
            WHEN ca.days_since_first_purchase < 29              THEN '1. New/FMX'
            WHEN ca.days_since_last_purchase > 90               THEN '2. Churned'
            WHEN ca.days_since_last_purchase BETWEEN 29 AND 90  THEN '3. Dormant'
            WHEN ca.l28_orders BETWEEN 1 AND 2                  THEN '4. Active - Occasional'
            WHEN ca.l28_orders BETWEEN 3 AND 4                  THEN '5. Active - Habituating'
            WHEN ca.l28_orders >= 5                             THEN '6. Active - RDP'
            ELSE '7. Unmatched'
        END AS segment
    FROM joined j
    LEFT JOIN proddb.mattheitz.mh_customer_authority ca
      ON ca.creator_id = j.consumer_id
     AND ca.dte = j.order_dt
)

SELECT
    tag                                                            AS arm,
    segment,
    COUNT(DISTINCT consumer_id)                                    AS total_users,
    COUNT(DISTINCT CASE WHEN wbd_pad_discount > 0 THEN consumer_id END)
                                                                    AS discounted_users,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN wbd_pad_discount > 0 THEN consumer_id END)
        / NULLIF(COUNT(DISTINCT consumer_id), 0), 2)               AS pct_user_coverage
FROM segmented
GROUP BY ALL
ORDER BY tag, segment
;


-- ============================================================================
-- 6. SPEND BY SEGMENT/ARM ACROSS AFFORDABILITY/CRM/MX-FUNDED/TOTAL PROGRAMS
-- Across ALL Classic Rx delivery orders for each arm's exposed Cx (NOT
-- restricted to WBD/PAD-discounted orders -- broader population than
-- queries 1-5 above).
-- "Affordability" = WBD + XS(cs) + PAD, DF-only (matches queries 1-5's
-- discount definition).
-- "CRM" = dd_funded_cx_discount (DoorDash-funded consumer discount, e.g.
-- promo codes) -- no column literally named "CRM" exists in this table;
-- this is the closest match and the only other DD-funded (non-Mx-funded,
-- non-fee) discount bucket available.
-- "Mx funded" = mx_funded_cx_discount.
-- "Total" here uses affordability_full_discount (DF+SF combined, not
-- DF-only) + CRM + Mx-funded -- a full-program-spend comparison, distinct
-- from the DF-only Affordability column and from queries 1-5's DF-only
-- lens. Does NOT include the smaller legacy FDF/LDF/Other fee-promo buckets.
-- ============================================================================
WITH params AS (
    SELECT
        DATE '2026-07-29' AS start_date,
        DATE '2026-09-22' AS end_date
),

exposure_cohort AS (
    SELECT
        tag,
        TRY_CAST(bucket_key AS INTEGER)  AS consumer_id,
        MIN(CAST(exposure_time AS DATE)) AS first_exposed
    FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
    WHERE experiment_name = 'discount_engine_us_wbd_v2'
      AND experiment_version BETWEEN 22 AND 100
      AND exposure_time >= '2026-07-29'
      AND segment = 'Users'
      AND TRY_CAST(bucket_key AS INTEGER) != 1505155093
    GROUP BY ALL
),

base_orders AS (
    SELECT
        dd.delivery_id,
        dd.creator_id      AS consumer_id,
        ec.tag,
        CAST(dd.created_at AS DATE) AS order_dt
    FROM proddb.public.dimension_deliveries dd
    JOIN edw.merchant.dimension_store ds
      ON ds.store_id = dd.store_id
    JOIN exposure_cohort ec
      ON ec.consumer_id = dd.creator_id
     AND CAST(dd.created_at AS DATE) >= ec.first_exposed
    CROSS JOIN params p
    WHERE CAST(dd.created_at AS DATE) BETWEEN p.start_date AND p.end_date
      AND dd.is_filtered_core       = TRUE
      AND dd.country_id            = 1
      AND dd.is_consumer_pickup     = FALSE
      AND dd.is_subscribed_consumer = FALSE
      AND NVL(ds.is_restaurant, 0)  = 1
      AND NVL(dd.fulfillment_type, '') NOT IN
          ('merchant_fleet', 'shipping', 'digital', 'virtual', 'dine_in', 'drone')
    -- NOTE: no discount>0 filter -- ALL Classic Rx delivery orders
),

discounts AS (
    SELECT
        delivery_id,
        NVL(SUM(wbd_df_promo_discount), 0)
            + NVL(SUM(cs_df_promo_discount), 0)
            + NVL(SUM(pad_df_promo_discount), 0)       AS affordability_df_discount,
        NVL(SUM(wbd_fee_promo_discount), 0)
            + NVL(SUM(cs_fee_promo_discount), 0)
            + NVL(SUM(pad_fee_promo_discount), 0)      AS affordability_full_discount,
        NVL(SUM(dd_funded_cx_discount), 0)             AS crm_discount,
        NVL(SUM(mx_funded_cx_discount), 0)             AS mx_funded_discount
    FROM proddb.static.df_sf_promo_discount_delivery_level
    GROUP BY 1
),

attributed AS (
    SELECT
        b.tag,
        b.delivery_id,
        b.consumer_id,
        b.order_dt,
        NVL(d.affordability_df_discount, 0)   AS affordability_df_discount,
        NVL(d.affordability_full_discount, 0) AS affordability_full_discount,
        NVL(d.crm_discount, 0)                AS crm_discount,
        NVL(d.mx_funded_discount, 0)          AS mx_funded_discount
    FROM base_orders b
    LEFT JOIN discounts d ON d.delivery_id = b.delivery_id
),

segmented AS (
    SELECT
        a.tag,
        a.delivery_id,
        a.affordability_df_discount,
        a.affordability_full_discount,
        a.crm_discount,
        a.mx_funded_discount,
        CASE
            WHEN ca.days_since_first_purchase < 29              THEN '1. New/FMX'
            WHEN ca.days_since_last_purchase > 90               THEN '2. Churned'
            WHEN ca.days_since_last_purchase BETWEEN 29 AND 90  THEN '3. Dormant'
            WHEN ca.l28_orders BETWEEN 1 AND 2                  THEN '4. Active - Occasional'
            WHEN ca.l28_orders BETWEEN 3 AND 4                  THEN '5. Active - Habituating'
            WHEN ca.l28_orders >= 5                             THEN '6. Active - RDP'
            ELSE '7. Unmatched'
        END AS segment
    FROM attributed a
    LEFT JOIN proddb.mattheitz.mh_customer_authority ca
      ON ca.creator_id = a.consumer_id
     AND ca.dte = a.order_dt
)

SELECT
    tag                                                          AS arm,
    segment,
    COUNT(DISTINCT delivery_id)                                  AS total_orders,
    ROUND(SUM(affordability_df_discount), 2)                     AS affordability_df_spend,
    ROUND(SUM(crm_discount), 2)                                  AS crm_spend,
    ROUND(SUM(mx_funded_discount), 2)                            AS mx_funded_spend,
    ROUND(SUM(affordability_full_discount + crm_discount + mx_funded_discount), 2)
                                                                  AS total_spend
FROM segmented
GROUP BY ALL
ORDER BY tag, segment
;
