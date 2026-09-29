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


-- ============================================================
-- WBD 2.0 V2 US: Cx<>Mx Orders + Mx Subtotal bucket queries
-- Corrected (order-level) logic, All Cx and New/FMX segments
-- Generated 2026-09-29 13:09 PDT
-- ============================================================

-- ============================================================
-- 1) ALL CX -- L90D Cx<>Mx Orders bucket
-- ============================================================
-- WBD 2.0 V2 US: ALL Cx -- Affordability (DF) spend + coverage by
-- L90D Cx<>Mx Orders bucket -- CORRECTED: order-level, not user-aggregated.
-- For each order O by consumer C at restaurant merchant S, bucket = COUNT
-- of C's OWN prior orders at S in the 90 days strictly before O (excludes
-- O itself). Restaurant-only (Rx). Version >= 22, window 2026-07-29 to
-- 2026-09-22.

WITH params AS (
    SELECT DATE '2026-07-29' AS start_date, DATE '2026-09-22' AS end_date
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

all_cx_all AS (
    SELECT
        dd.delivery_id,
        dd.creator_id AS consumer_id,
        dd.store_id,
        dd.created_at,
        ec.tag,
        ec.first_exposed
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

-- Same consumers' full Rx delivery history (any experiment status), bounded
-- to [analysis_start-90d, analysis_end], needed to look back before 7/29
-- for orders anchored near the start of the window.
cx_order_history AS (
    SELECT
        dd.creator_id AS consumer_id,
        dd.store_id,
        dd.created_at
    FROM proddb.public.dimension_deliveries dd
    JOIN edw.merchant.dimension_store ds
      ON ds.store_id = dd.store_id
     AND NVL(ds.is_restaurant, 0) = 1
    CROSS JOIN params p
    WHERE dd.is_filtered_core       = TRUE
      AND dd.country_id            = 1
      AND dd.is_consumer_pickup     = FALSE
      AND dd.is_subscribed_consumer = FALSE
      AND dd.creator_id IN (SELECT DISTINCT consumer_id FROM all_cx_all)
      AND dd.created_at >= DATEADD(day, -90, p.start_date)
      AND dd.created_at <= p.end_date
),

order_bucketed AS (
    SELECT
        a.delivery_id,
        a.consumer_id,
        a.tag,
        CASE
            WHEN COUNT(h.consumer_id) = 0 THEN '0. First Order at Mx (90D)'
            WHEN COUNT(h.consumer_id) = 1 THEN '1. 1 Prior Order'
            WHEN COUNT(h.consumer_id) = 2 THEN '2. 2 Prior Orders'
            ELSE '3. 3+ Prior Orders'
        END AS cxmx_bucket
    FROM all_cx_all a
    LEFT JOIN cx_order_history h
      ON h.consumer_id = a.consumer_id
     AND h.store_id = a.store_id
     AND h.created_at >= DATEADD(day, -90, a.created_at)
     AND h.created_at < a.created_at
    GROUP BY 1, 2, 3
),

discounts AS (
    SELECT
        delivery_id,
        NVL(SUM(wbd_df_promo_discount), 0) AS wbd_discount,
        NVL(SUM(pad_df_promo_discount), 0) AS pad_discount
    FROM proddb.static.df_sf_promo_discount_delivery_level
    GROUP BY 1
),

orders_flagged AS (
    SELECT
        ob.tag,
        ob.delivery_id,
        ob.consumer_id,
        ob.cxmx_bucket,
        (d.wbd_discount + d.pad_discount) AS wbd_pad_discount,
        CASE WHEN (d.wbd_discount + d.pad_discount) > 0 THEN 1 ELSE 0 END AS is_discounted
    FROM order_bucketed ob
    LEFT JOIN discounts d ON d.delivery_id = ob.delivery_id
),

psm_v4 AS (
    SELECT
        consumer_id,
        CAST(active_date AS DATE) AS active_date,
        v4_fee_vp_hybrid_ptile_by_country_classic AS v4_score
    FROM proddb.cx_econ.cx_sensitivity_v4_fee
    CROSS JOIN params p
    WHERE country_id = 1
      AND is_dashpass_active = 0
      AND CAST(active_date AS DATE) BETWEEN p.start_date AND p.end_date
),

median_psm AS (
    SELECT
        o.tag,
        o.cxmx_bucket,
        MEDIAN(v4.v4_score) AS median_v4_score,
        COUNT(v4.v4_score)  AS users_with_v4
    FROM orders_flagged o
    JOIN all_cx_all a ON a.delivery_id = o.delivery_id
    LEFT JOIN psm_v4 v4
      ON v4.consumer_id = o.consumer_id AND v4.active_date = a.first_exposed
    GROUP BY 1, 2
)

SELECT
    o.tag,
    o.cxmx_bucket,
    COUNT(DISTINCT o.consumer_id)                                          AS total_users,
    COUNT(DISTINCT CASE WHEN o.is_discounted=1 THEN o.consumer_id END)     AS users_with_discount,
    COUNT(DISTINCT CASE WHEN o.is_discounted=1 THEN o.consumer_id END) * 1.0
        / COUNT(DISTINCT o.consumer_id)                                   AS user_coverage_pct,
    COUNT(*)                                                               AS total_orders,
    SUM(o.is_discounted)                                                   AS discounted_orders,
    SUM(o.is_discounted) * 1.0 / COUNT(*)                                  AS order_coverage_pct,
    SUM(CASE WHEN o.is_discounted=1 THEN o.wbd_pad_discount ELSE 0 END)    AS total_wbd_pad_spend,
    mp.median_v4_score,
    mp.users_with_v4
FROM orders_flagged o
LEFT JOIN median_psm mp ON mp.tag = o.tag AND mp.cxmx_bucket = o.cxmx_bucket
GROUP BY 1, 2, 10, 11
ORDER BY 1, 2;


-- ============================================================
-- 2) ALL CX -- P90D Mx Subtotal bucket
-- ============================================================
-- WBD 2.0 V2 US: ALL Cx -- Affordability (DF) spend + coverage by
-- P90D Mx Subtotal bucket -- TRUE CORRECTED VERSION: ORDER-level bucketing.
-- For each order O at merchant S on date D, bucket = merchant S's own
-- ROLLING average subtotal across ALL its customers' orders in the 90 days
-- strictly before D (evaluated as of that order's own date, not a static
-- whole-window average, and not the user's own spend). Restaurant-only.
-- Version >= 22, window 2026-07-29 to 2026-09-22.

WITH params AS (
    SELECT DATE '2026-07-29' AS start_date, DATE '2026-09-22' AS end_date
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

all_cx_all AS (
    SELECT
        dd.delivery_id,
        dd.creator_id AS consumer_id,
        dd.store_id,
        CAST(dd.created_at AS DATE) AS order_dt,
        ec.tag,
        ec.first_exposed
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

-- ALL restaurant orders (any customer) at the stores our cohort actually
-- visited, bounded to [start-90d, end], to build each store's own daily
-- volume/subtotal so we can roll a 90D trailing average as of any date.
store_daily_agg AS (
    SELECT
        dd.store_id,
        CAST(dd.created_at AS DATE) AS order_dt,
        SUM(dd.subtotal) / 100.0    AS daily_subtotal_sum,
        COUNT(*)                    AS daily_order_cnt
    FROM proddb.public.dimension_deliveries dd
    JOIN edw.merchant.dimension_store ds
      ON ds.store_id = dd.store_id
     AND NVL(ds.is_restaurant, 0) = 1
    CROSS JOIN params p
    WHERE dd.is_filtered_core = TRUE
      AND dd.country_id = 1
      AND dd.store_id IN (SELECT DISTINCT store_id FROM all_cx_all)
      AND dd.created_at >= DATEADD(day, -90, p.start_date)
      AND dd.created_at <= p.end_date
    GROUP BY 1, 2
),

store_rolling AS (
    SELECT
        store_id,
        order_dt,
        SUM(daily_subtotal_sum) OVER (
            PARTITION BY store_id ORDER BY order_dt
            RANGE BETWEEN 90 PRECEDING AND 1 PRECEDING
        ) AS rolling_subtotal_sum,
        SUM(daily_order_cnt) OVER (
            PARTITION BY store_id ORDER BY order_dt
            RANGE BETWEEN 90 PRECEDING AND 1 PRECEDING
        ) AS rolling_order_cnt
    FROM store_daily_agg
),

order_bucketed AS (
    SELECT
        a.delivery_id,
        a.consumer_id,
        a.tag,
        CASE
            WHEN NVL(sr.rolling_order_cnt, 0) = 0 THEN '0. No Merchant History (90D)'
            WHEN sr.rolling_subtotal_sum / sr.rolling_order_cnt < 15 THEN '1. <$15'
            WHEN sr.rolling_subtotal_sum / sr.rolling_order_cnt < 25 THEN '2. $15-25'
            WHEN sr.rolling_subtotal_sum / sr.rolling_order_cnt < 35 THEN '3. $25-35'
            WHEN sr.rolling_subtotal_sum / sr.rolling_order_cnt < 50 THEN '4. $35-50'
            ELSE '5. $50+'
        END AS subtotal_bucket
    FROM all_cx_all a
    LEFT JOIN store_rolling sr
      ON sr.store_id = a.store_id AND sr.order_dt = a.order_dt
),

discounts AS (
    SELECT
        delivery_id,
        NVL(SUM(wbd_df_promo_discount), 0) AS wbd_discount,
        NVL(SUM(pad_df_promo_discount), 0) AS pad_discount
    FROM proddb.static.df_sf_promo_discount_delivery_level
    GROUP BY 1
),

orders_flagged AS (
    SELECT
        ob.tag,
        ob.delivery_id,
        ob.consumer_id,
        ob.subtotal_bucket,
        (d.wbd_discount + d.pad_discount) AS wbd_pad_discount,
        CASE WHEN (d.wbd_discount + d.pad_discount) > 0 THEN 1 ELSE 0 END AS is_discounted
    FROM order_bucketed ob
    LEFT JOIN discounts d ON d.delivery_id = ob.delivery_id
),

psm_v4 AS (
    SELECT
        consumer_id,
        CAST(active_date AS DATE) AS active_date,
        v4_fee_vp_hybrid_ptile_by_country_classic AS v4_score
    FROM proddb.cx_econ.cx_sensitivity_v4_fee
    CROSS JOIN params p
    WHERE country_id = 1
      AND is_dashpass_active = 0
      AND CAST(active_date AS DATE) BETWEEN p.start_date AND p.end_date
),

median_psm AS (
    SELECT
        o.tag,
        o.subtotal_bucket,
        MEDIAN(v4.v4_score) AS median_v4_score,
        COUNT(v4.v4_score)  AS users_with_v4
    FROM orders_flagged o
    JOIN all_cx_all a ON a.delivery_id = o.delivery_id
    LEFT JOIN psm_v4 v4
      ON v4.consumer_id = o.consumer_id AND v4.active_date = a.first_exposed
    GROUP BY 1, 2
)

SELECT
    o.tag,
    o.subtotal_bucket,
    COUNT(DISTINCT o.consumer_id)                                          AS total_users,
    COUNT(DISTINCT CASE WHEN o.is_discounted=1 THEN o.consumer_id END)     AS users_with_discount,
    COUNT(DISTINCT CASE WHEN o.is_discounted=1 THEN o.consumer_id END) * 1.0
        / COUNT(DISTINCT o.consumer_id)                                   AS user_coverage_pct,
    COUNT(*)                                                               AS total_orders,
    SUM(o.is_discounted)                                                   AS discounted_orders,
    SUM(o.is_discounted) * 1.0 / COUNT(*)                                  AS order_coverage_pct,
    SUM(CASE WHEN o.is_discounted=1 THEN o.wbd_pad_discount ELSE 0 END)    AS total_wbd_pad_spend,
    mp.median_v4_score,
    mp.users_with_v4
FROM orders_flagged o
LEFT JOIN median_psm mp ON mp.tag = o.tag AND mp.subtotal_bucket = o.subtotal_bucket
GROUP BY 1, 2, 10, 11
ORDER BY 1, 2;


-- ============================================================
-- 3) NEW/FMX -- L90D Cx<>Mx Orders bucket
-- ============================================================
-- WBD 2.0 V2 US: New/FMX Cx -- Affordability (DF) spend + coverage by
-- L90D Cx<>Mx Orders bucket -- CORRECTED: order-level, not user-aggregated.
-- For each order O by consumer C at restaurant merchant S, bucket = COUNT
-- of C's OWN prior orders at S in the 90 days strictly before O (excludes
-- O itself). Restaurant-only (Rx). Version >= 22, window 2026-07-29 to
-- 2026-09-22.

WITH params AS (
    SELECT DATE '2026-07-29' AS start_date, DATE '2026-09-22' AS end_date
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
        dd.creator_id AS consumer_id,
        dd.store_id,
        dd.created_at,
        ec.tag,
        ec.first_exposed
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

segment_tagged AS (
    SELECT
        b.*,
        mh.days_since_first_purchase
    FROM base_orders b
    LEFT JOIN proddb.mattheitz.mh_customer_authority mh
      ON mh.creator_id = b.consumer_id
     AND mh.dte        = CAST(b.created_at AS DATE)
),

all_cx_all AS (
    SELECT delivery_id, consumer_id, store_id, created_at, tag, first_exposed
    FROM segment_tagged WHERE days_since_first_purchase < 29
),

cx_order_history AS (
    SELECT
        dd.creator_id AS consumer_id,
        dd.store_id,
        dd.created_at
    FROM proddb.public.dimension_deliveries dd
    JOIN edw.merchant.dimension_store ds
      ON ds.store_id = dd.store_id
     AND NVL(ds.is_restaurant, 0) = 1
    CROSS JOIN params p
    WHERE dd.is_filtered_core       = TRUE
      AND dd.country_id            = 1
      AND dd.is_consumer_pickup     = FALSE
      AND dd.is_subscribed_consumer = FALSE
      AND dd.creator_id IN (SELECT DISTINCT consumer_id FROM all_cx_all)
      AND dd.created_at >= DATEADD(day, -90, p.start_date)
      AND dd.created_at <= p.end_date
),

order_bucketed AS (
    SELECT
        a.delivery_id,
        a.consumer_id,
        a.tag,
        CASE
            WHEN COUNT(h.consumer_id) = 0 THEN '0. First Order at Mx (90D)'
            WHEN COUNT(h.consumer_id) = 1 THEN '1. 1 Prior Order'
            WHEN COUNT(h.consumer_id) = 2 THEN '2. 2 Prior Orders'
            ELSE '3. 3+ Prior Orders'
        END AS cxmx_bucket
    FROM all_cx_all a
    LEFT JOIN cx_order_history h
      ON h.consumer_id = a.consumer_id
     AND h.store_id = a.store_id
     AND h.created_at >= DATEADD(day, -90, a.created_at)
     AND h.created_at < a.created_at
    GROUP BY 1, 2, 3
),

discounts AS (
    SELECT
        delivery_id,
        NVL(SUM(wbd_df_promo_discount), 0) AS wbd_discount,
        NVL(SUM(pad_df_promo_discount), 0) AS pad_discount
    FROM proddb.static.df_sf_promo_discount_delivery_level
    GROUP BY 1
),

orders_flagged AS (
    SELECT
        ob.tag,
        ob.delivery_id,
        ob.consumer_id,
        ob.cxmx_bucket,
        (d.wbd_discount + d.pad_discount) AS wbd_pad_discount,
        CASE WHEN (d.wbd_discount + d.pad_discount) > 0 THEN 1 ELSE 0 END AS is_discounted
    FROM order_bucketed ob
    LEFT JOIN discounts d ON d.delivery_id = ob.delivery_id
),

psm_v4 AS (
    SELECT
        consumer_id,
        CAST(active_date AS DATE) AS active_date,
        v4_fee_vp_hybrid_ptile_by_country_classic AS v4_score
    FROM proddb.cx_econ.cx_sensitivity_v4_fee
    CROSS JOIN params p
    WHERE country_id = 1
      AND is_dashpass_active = 0
      AND CAST(active_date AS DATE) BETWEEN p.start_date AND p.end_date
),

median_psm AS (
    SELECT
        o.tag,
        o.cxmx_bucket,
        MEDIAN(v4.v4_score) AS median_v4_score,
        COUNT(v4.v4_score)  AS users_with_v4
    FROM orders_flagged o
    JOIN all_cx_all a ON a.delivery_id = o.delivery_id
    LEFT JOIN psm_v4 v4
      ON v4.consumer_id = o.consumer_id AND v4.active_date = a.first_exposed
    GROUP BY 1, 2
)

SELECT
    o.tag,
    o.cxmx_bucket,
    COUNT(DISTINCT o.consumer_id)                                          AS total_users,
    COUNT(DISTINCT CASE WHEN o.is_discounted=1 THEN o.consumer_id END)     AS users_with_discount,
    COUNT(DISTINCT CASE WHEN o.is_discounted=1 THEN o.consumer_id END) * 1.0
        / COUNT(DISTINCT o.consumer_id)                                   AS user_coverage_pct,
    COUNT(*)                                                               AS total_orders,
    SUM(o.is_discounted)                                                   AS discounted_orders,
    SUM(o.is_discounted) * 1.0 / COUNT(*)                                  AS order_coverage_pct,
    SUM(CASE WHEN o.is_discounted=1 THEN o.wbd_pad_discount ELSE 0 END)    AS total_wbd_pad_spend,
    mp.median_v4_score,
    mp.users_with_v4
FROM orders_flagged o
LEFT JOIN median_psm mp ON mp.tag = o.tag AND mp.cxmx_bucket = o.cxmx_bucket
GROUP BY 1, 2, 10, 11
ORDER BY 1, 2;


-- ============================================================
-- 4) NEW/FMX -- P90D Mx Subtotal bucket
-- ============================================================
-- WBD 2.0 V2 US: New/FMX Cx -- Affordability (DF) spend + coverage by
-- P90D Mx Subtotal bucket -- TRUE CORRECTED VERSION: ORDER-level bucketing.
-- For each order O at merchant S on date D, bucket = merchant S's own
-- ROLLING average subtotal across ALL its customers' orders in the 90 days
-- strictly before D. Restaurant-only. Version >= 22, window 2026-07-29 to
-- 2026-09-22.

WITH params AS (
    SELECT DATE '2026-07-29' AS start_date, DATE '2026-09-22' AS end_date
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
        dd.creator_id AS consumer_id,
        dd.store_id,
        CAST(dd.created_at AS DATE) AS order_dt,
        ec.tag,
        ec.first_exposed
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

segment_tagged AS (
    SELECT
        b.*,
        mh.days_since_first_purchase
    FROM base_orders b
    LEFT JOIN proddb.mattheitz.mh_customer_authority mh
      ON mh.creator_id = b.consumer_id
     AND mh.dte        = b.order_dt
),

all_cx_all AS (
    SELECT delivery_id, consumer_id, store_id, order_dt, tag, first_exposed
    FROM segment_tagged WHERE days_since_first_purchase < 29
),

store_daily_agg AS (
    SELECT
        dd.store_id,
        CAST(dd.created_at AS DATE) AS order_dt,
        SUM(dd.subtotal) / 100.0    AS daily_subtotal_sum,
        COUNT(*)                    AS daily_order_cnt
    FROM proddb.public.dimension_deliveries dd
    JOIN edw.merchant.dimension_store ds
      ON ds.store_id = dd.store_id
     AND NVL(ds.is_restaurant, 0) = 1
    CROSS JOIN params p
    WHERE dd.is_filtered_core = TRUE
      AND dd.country_id = 1
      AND dd.store_id IN (SELECT DISTINCT store_id FROM all_cx_all)
      AND dd.created_at >= DATEADD(day, -90, p.start_date)
      AND dd.created_at <= p.end_date
    GROUP BY 1, 2
),

store_rolling AS (
    SELECT
        store_id,
        order_dt,
        SUM(daily_subtotal_sum) OVER (
            PARTITION BY store_id ORDER BY order_dt
            RANGE BETWEEN 90 PRECEDING AND 1 PRECEDING
        ) AS rolling_subtotal_sum,
        SUM(daily_order_cnt) OVER (
            PARTITION BY store_id ORDER BY order_dt
            RANGE BETWEEN 90 PRECEDING AND 1 PRECEDING
        ) AS rolling_order_cnt
    FROM store_daily_agg
),

order_bucketed AS (
    SELECT
        a.delivery_id,
        a.consumer_id,
        a.tag,
        CASE
            WHEN NVL(sr.rolling_order_cnt, 0) = 0 THEN '0. No Merchant History (90D)'
            WHEN sr.rolling_subtotal_sum / sr.rolling_order_cnt < 15 THEN '1. <$15'
            WHEN sr.rolling_subtotal_sum / sr.rolling_order_cnt < 25 THEN '2. $15-25'
            WHEN sr.rolling_subtotal_sum / sr.rolling_order_cnt < 35 THEN '3. $25-35'
            WHEN sr.rolling_subtotal_sum / sr.rolling_order_cnt < 50 THEN '4. $35-50'
            ELSE '5. $50+'
        END AS subtotal_bucket
    FROM all_cx_all a
    LEFT JOIN store_rolling sr
      ON sr.store_id = a.store_id AND sr.order_dt = a.order_dt
),

discounts AS (
    SELECT
        delivery_id,
        NVL(SUM(wbd_df_promo_discount), 0) AS wbd_discount,
        NVL(SUM(pad_df_promo_discount), 0) AS pad_discount
    FROM proddb.static.df_sf_promo_discount_delivery_level
    GROUP BY 1
),

orders_flagged AS (
    SELECT
        ob.tag,
        ob.delivery_id,
        ob.consumer_id,
        ob.subtotal_bucket,
        (d.wbd_discount + d.pad_discount) AS wbd_pad_discount,
        CASE WHEN (d.wbd_discount + d.pad_discount) > 0 THEN 1 ELSE 0 END AS is_discounted
    FROM order_bucketed ob
    LEFT JOIN discounts d ON d.delivery_id = ob.delivery_id
),

psm_v4 AS (
    SELECT
        consumer_id,
        CAST(active_date AS DATE) AS active_date,
        v4_fee_vp_hybrid_ptile_by_country_classic AS v4_score
    FROM proddb.cx_econ.cx_sensitivity_v4_fee
    CROSS JOIN params p
    WHERE country_id = 1
      AND is_dashpass_active = 0
      AND CAST(active_date AS DATE) BETWEEN p.start_date AND p.end_date
),

median_psm AS (
    SELECT
        o.tag,
        o.subtotal_bucket,
        MEDIAN(v4.v4_score) AS median_v4_score,
        COUNT(v4.v4_score)  AS users_with_v4
    FROM orders_flagged o
    JOIN all_cx_all a ON a.delivery_id = o.delivery_id
    LEFT JOIN psm_v4 v4
      ON v4.consumer_id = o.consumer_id AND v4.active_date = a.first_exposed
    GROUP BY 1, 2
)

SELECT
    o.tag,
    o.subtotal_bucket,
    COUNT(DISTINCT o.consumer_id)                                          AS total_users,
    COUNT(DISTINCT CASE WHEN o.is_discounted=1 THEN o.consumer_id END)     AS users_with_discount,
    COUNT(DISTINCT CASE WHEN o.is_discounted=1 THEN o.consumer_id END) * 1.0
        / COUNT(DISTINCT o.consumer_id)                                   AS user_coverage_pct,
    COUNT(*)                                                               AS total_orders,
    SUM(o.is_discounted)                                                   AS discounted_orders,
    SUM(o.is_discounted) * 1.0 / COUNT(*)                                  AS order_coverage_pct,
    SUM(CASE WHEN o.is_discounted=1 THEN o.wbd_pad_discount ELSE 0 END)    AS total_wbd_pad_spend,
    mp.median_v4_score,
    mp.users_with_v4
FROM orders_flagged o
LEFT JOIN median_psm mp ON mp.tag = o.tag AND mp.subtotal_bucket = o.subtotal_bucket
GROUP BY 1, 2, 10, 11
ORDER BY 1, 2;

