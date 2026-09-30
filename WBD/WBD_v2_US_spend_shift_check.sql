-- ============================================================================
-- WBD 2.0 V2 US -- Spend / Discount Analysis by Treatment Arm and Cx Segment
-- CORRECTED FULL VERSION -- 2026-09-29 22:18 PDT
--
-- Corrections applied vs. original WBD_v2_US_spend_shift_check.sql:
--   Query 5 (User-Level Coverage %) -- population bug fixed: was gated on
--     placing an order in-window (base_orders INNER JOIN dimension_deliveries),
--     which silently excluded any exposed Cx with zero orders and inflated
--     coverage%. Now uses the full exposed cohort, restricted to Classic
--     (non-DashPass) app visitors (>=1 app visit in trailing 365D before
--     first_exposed, FACT_UNIQUE_VISITORS_FULL_UTC unique_visitor=1 AND
--     is_dashpass=0), segment anchored at first_exposed (not per-order-day,
--     since non-orderers have no order_dt to anchor to).
--   Queries 1-4, 6: reviewed, no population bug found (order-level/discounted-
--     orders-only by design) -- left unchanged from original.
--   Cx<>Mx Orders / Mx Subtotal queries (order-level, unchanged logic) --
--     stale bucket label '0. First Order at Mx (90D)' renamed to the
--     accurate '0. No Prior Orders (90D)' (a user could have ordered from
--     that merchant outside the 90D lookback -- this isn't necessarily
--     their literal first order there).
--   NEW: Cart Page Conversion Rate and GOV per Order bucket queries (All Cx +
--     New/FMX) -- not in the original file. Built on the SAME corrected
--     population as Query 5 (full exposed cohort, Classic app visitors,
--     trailing-365D lookback), not gated on in-window orders. Includes
--     median PSM v4 score (Classic-scoped) per bucket.
--   NEW: PSM v2-vs-v4 score comparison by 6-way Cx Segment -- not in the
--     original file. Same corrected population. Includes match rates,
--     matched-only medians, imputed-80-for-unmatched sensitivity check, and
--     diff-in-diff (each model's segment deviation from its OWN overall
--     mean, to avoid comparing two differently-centered percentile scales
--     directly).
--
-- Population baseline (all queries): discount_engine_us_wbd_v2, version >= 22
-- (v22 rollout is concentrated ~2026-09-16 to 09-22 in practice), window
-- 2026-07-29 to 2026-09-22. Classic Rx delivery orders where order-level:
-- is_filtered_core, US, non-pickup, non-DashPass, restaurant, excl. non-
-- delivery fulfillment types. DashMart re-stocking bot (bucket_key
-- 1505155093) excluded throughout.
-- ============================================================================

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
-- 5. USER-LEVEL COVERAGE % BY SEGMENT/ARM -- CORRECTED
-- Population = FULL EXPOSED COHORT, restricted to Classic (non-DashPass) app
-- visitors (trailing 365D before first_exposed), NOT gated on placing a
-- subsequent order. Segment anchored at first_exposed (single value per
-- user). 'Covered' = had >=1 Classic Rx delivery order in the window with a
-- nonzero WBD/PAD DF discount (0 if they placed no orders at all).
-- ============================================================================
-- CORRECTED Query 5: USER-LEVEL COVERAGE % BY SEGMENT/ARM
-- Population = FULL EXPOSED COHORT (everyone assigned to
-- discount_engine_us_wbd_v2 v22+, first_exposed in window), NOT gated on
-- placing a subsequent order. Segment anchored at first_exposed (single
-- value per user), not per-order-day, since a non-orderer has no order_dt
-- to anchor to. "Covered" = had >=1 Classic Rx delivery order in the window
-- with a nonzero WBD/PAD DF discount (0 if they placed no orders at all).

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

exposed_users AS (
    SELECT consumer_id, tag, first_exposed
    FROM exposure_cohort
    CROSS JOIN params p
    WHERE first_exposed BETWEEN p.start_date AND p.end_date
),

-- App-visited AND Classic (non-DashPass) on that same visit day, trailing
-- 365D before first_exposed -- fixes the earlier bug where "full exposed"
-- population silently included DashPass Cx once the order-level
-- is_subscribed_consumer filter was removed.
app_visited AS (
    SELECT DISTINCT
        u.consumer_id,
        u.tag
    FROM exposed_users u
    JOIN proddb.public.fact_unique_visitors_full_utc uv
      ON CAST(uv.user_id AS BIGINT) = u.consumer_id
     AND CAST(uv.event_date AS DATE) >= DATEADD(day, -365, u.first_exposed)
     AND CAST(uv.event_date AS DATE) <= u.first_exposed
    WHERE uv.unique_visitor = 1
      AND uv.is_dashpass = 0
),

distinct_users AS (
    SELECT eu.consumer_id, eu.tag, eu.first_exposed
    FROM exposed_users eu
    JOIN app_visited av ON av.consumer_id = eu.consumer_id AND av.tag = eu.tag
),

cx_segment AS (
    SELECT
        u.consumer_id,
        u.tag,
        u.first_exposed,
        CASE
            WHEN ca.days_since_first_purchase < 29              THEN '1. New/FMX'
            WHEN ca.days_since_last_purchase > 90               THEN '2. Churned'
            WHEN ca.days_since_last_purchase BETWEEN 29 AND 90  THEN '3. Dormant'
            WHEN ca.l28_orders BETWEEN 1 AND 2                  THEN '4. Active - Occasional'
            WHEN ca.l28_orders BETWEEN 3 AND 4                  THEN '5. Active - Habituating'
            WHEN ca.l28_orders >= 5                             THEN '6. Active - RDP'
            ELSE '7. Unmatched'
        END AS segment
    FROM distinct_users u
    LEFT JOIN proddb.mattheitz.mh_customer_authority ca
      ON ca.creator_id = u.consumer_id
     AND ca.dte        = u.first_exposed
),

orders_in_window AS (
    SELECT
        dd.delivery_id,
        dd.creator_id AS consumer_id,
        ec.tag
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

user_discount_flag AS (
    SELECT
        o.consumer_id,
        o.tag,
        MAX(CASE WHEN (d.wbd_discount + d.pad_discount) > 0 THEN 1 ELSE 0 END) AS has_discount
    FROM orders_in_window o
    LEFT JOIN discounts d ON d.delivery_id = o.delivery_id
    GROUP BY 1, 2
)

SELECT
    cs.tag                                                          AS arm,
    cs.segment,
    COUNT(DISTINCT cs.consumer_id)                                  AS total_users,
    COUNT(DISTINCT CASE WHEN NVL(udf.has_discount,0)=1 THEN cs.consumer_id END)
                                                                     AS discounted_users,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN NVL(udf.has_discount,0)=1 THEN cs.consumer_id END)
        / NULLIF(COUNT(DISTINCT cs.consumer_id), 0), 2)              AS pct_user_coverage
FROM cx_segment cs
LEFT JOIN user_discount_flag udf
  ON udf.consumer_id = cs.consumer_id AND udf.tag = cs.tag
GROUP BY 1, 2
ORDER BY 1, 2;
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


-- ============================================================================
-- 7-10. Cx<>Mx Orders + Mx Subtotal bucket queries (All Cx + New/FMX)
-- Order-level logic, unchanged from original -- reviewed, no population bug
-- (each order gets its own bucket by definition). Bucket label corrected:
-- '0. First Order at Mx (90D)' -> '0. No Prior Orders (90D)'.
-- ============================================================================

-- 7) ALL CX -- L90D Cx<>Mx Orders bucket
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
            WHEN COUNT(h.consumer_id) = 0 THEN '0. No Prior Orders (90D)'
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


-- 8) ALL CX -- P90D Mx Subtotal bucket
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


-- 9) NEW/FMX -- L90D Cx<>Mx Orders bucket
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
            WHEN COUNT(h.consumer_id) = 0 THEN '0. No Prior Orders (90D)'
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


-- 10) NEW/FMX -- P90D Mx Subtotal bucket
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


-- ============================================================================
-- 11-14. NEW: Cart Page Conversion Rate + GOV per Order bucket queries
-- (All Cx + New/FMX). Same corrected population as Query 5. Includes median
-- PSM v4 score (Classic-scoped) per bucket.
-- ============================================================================

-- 11) ALL CX -- Cart Page Conversion Rate (Trailing 365D) bucket
-- CORRECTED: ALL Cx -- Cart Page Conversion Rate (Trailing 365D) bucket,
-- spend + coverage. Population = FULL EXPOSED COHORT (not gated on placing
-- an order in-window). CVR bucket computed from trailing-365D-before-
-- first_exposed history (works regardless of in-window order activity).
-- Orders/spend/coverage computed via LEFT JOIN to in-window Classic Rx
-- orders (0 if none).

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

exposed_users AS (
    SELECT consumer_id, tag, first_exposed
    FROM exposure_cohort
    CROSS JOIN params p
    WHERE first_exposed BETWEEN p.start_date AND p.end_date
),

app_visited AS (
    SELECT DISTINCT
        u.consumer_id,
        u.tag
    FROM exposed_users u
    JOIN proddb.public.fact_unique_visitors_full_utc uv
      ON CAST(uv.user_id AS BIGINT) = u.consumer_id
     AND CAST(uv.event_date AS DATE) >= DATEADD(day, -365, u.first_exposed)
     AND CAST(uv.event_date AS DATE) <= u.first_exposed
    WHERE uv.unique_visitor = 1
      AND uv.is_dashpass = 0
),

distinct_users AS (
    SELECT eu.consumer_id, eu.tag, eu.first_exposed
    FROM exposed_users eu
    JOIN app_visited av ON av.consumer_id = eu.consumer_id AND av.tag = eu.tag
),

traffic AS (
    SELECT
        u.consumer_id,
        u.tag,
        u.first_exposed,
        CAST(uv.event_date AS DATE) AS event_date,
        CASE WHEN SUM(uv.unique_purchaser) > 0 THEN 1 ELSE 0 END AS purchases,
        CASE WHEN SUM(uv.unique_order_cart_page_visitor) > 0 THEN 1 ELSE 0 END AS cart_visits
    FROM distinct_users u
    JOIN proddb.public.fact_unique_visitors_full_utc uv
      ON CAST(uv.user_id AS BIGINT) = u.consumer_id
     AND CAST(uv.event_date AS DATE) >= DATEADD(day, -365, u.first_exposed)
     AND CAST(uv.event_date AS DATE) <= u.first_exposed
    GROUP BY 1, 2, 3, 4
),

traffic_agg AS (
    SELECT
        consumer_id,
        tag,
        SUM(CASE WHEN event_date < first_exposed THEN cart_visits ELSE 0 END) AS cart_visits_l365d,
        SUM(CASE WHEN event_date < first_exposed THEN purchases ELSE 0 END)   AS purchases_l365d
    FROM traffic
    GROUP BY 1, 2
),

bucketed_users AS (
    SELECT
        u.consumer_id,
        u.tag,
        u.first_exposed,
        CASE
            WHEN NVL(t.cart_visits_l365d, 0) = 0 THEN '0. No Cart Visits (365D)'
            WHEN t.purchases_l365d * 1.0 / t.cart_visits_l365d < 0.20 THEN '1. <20%'
            WHEN t.purchases_l365d * 1.0 / t.cart_visits_l365d < 0.40 THEN '2. 20-40%'
            WHEN t.purchases_l365d * 1.0 / t.cart_visits_l365d < 0.60 THEN '3. 40-60%'
            WHEN t.purchases_l365d * 1.0 / t.cart_visits_l365d < 0.80 THEN '4. 60-80%'
            ELSE '5. 80-100%'
        END AS cvr_bucket
    FROM distinct_users u
    LEFT JOIN traffic_agg t ON t.consumer_id = u.consumer_id AND t.tag = u.tag
),

orders_in_window AS (
    SELECT
        dd.delivery_id,
        dd.creator_id AS consumer_id,
        ec.tag
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

orders_flagged AS (
    SELECT
        o.tag,
        o.delivery_id,
        o.consumer_id,
        (d.wbd_discount + d.pad_discount) AS wbd_pad_discount,
        CASE WHEN (d.wbd_discount + d.pad_discount) > 0 THEN 1 ELSE 0 END AS is_discounted
    FROM orders_in_window o
    LEFT JOIN discounts d ON d.delivery_id = o.delivery_id
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
)

SELECT
    bu.tag,
    bu.cvr_bucket,
    COUNT(DISTINCT bu.consumer_id)                                          AS total_users,
    COUNT(DISTINCT CASE WHEN ofl.is_discounted=1 THEN ofl.consumer_id END)    AS users_with_discount,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN ofl.is_discounted=1 THEN ofl.consumer_id END)
        / NULLIF(COUNT(DISTINCT bu.consumer_id), 0), 2)                    AS user_coverage_pct,
    COUNT(ofl.delivery_id)                                                   AS total_orders,
    SUM(NVL(ofl.is_discounted,0))                                            AS discounted_orders,
    ROUND(100.0 * SUM(NVL(ofl.is_discounted,0)) / NULLIF(COUNT(ofl.delivery_id), 0), 2)
                                                                             AS order_coverage_pct,
    ROUND(SUM(CASE WHEN ofl.is_discounted=1 THEN ofl.wbd_pad_discount ELSE 0 END), 2)
                                                                             AS total_wbd_pad_spend,
    MEDIAN(v4.v4_score)                                                     AS median_v4_score,
    COUNT(v4.v4_score)                                                      AS users_with_v4
FROM bucketed_users bu
LEFT JOIN orders_flagged ofl ON ofl.consumer_id = bu.consumer_id AND ofl.tag = bu.tag
LEFT JOIN psm_v4 v4 ON v4.consumer_id = bu.consumer_id AND v4.active_date = bu.first_exposed
GROUP BY 1, 2
ORDER BY 1, 2;


-- 12) ALL CX -- GOV per Order (Trailing 365D) bucket
-- CORRECTED: ALL Cx -- GOV per Order (Trailing 365D) bucket, spend +
-- coverage. Population = FULL EXPOSED COHORT (not gated on placing an
-- order in-window). GOV bucket computed from trailing-365D-before-
-- first_exposed history.

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

exposed_users AS (
    SELECT consumer_id, tag, first_exposed
    FROM exposure_cohort
    CROSS JOIN params p
    WHERE first_exposed BETWEEN p.start_date AND p.end_date
),

app_visited AS (
    SELECT DISTINCT
        u.consumer_id,
        u.tag
    FROM exposed_users u
    JOIN proddb.public.fact_unique_visitors_full_utc uv
      ON CAST(uv.user_id AS BIGINT) = u.consumer_id
     AND CAST(uv.event_date AS DATE) >= DATEADD(day, -365, u.first_exposed)
     AND CAST(uv.event_date AS DATE) <= u.first_exposed
    WHERE uv.unique_visitor = 1
      AND uv.is_dashpass = 0
),

distinct_users AS (
    SELECT eu.consumer_id, eu.tag, eu.first_exposed
    FROM exposed_users eu
    JOIN app_visited av ON av.consumer_id = eu.consumer_id AND av.tag = eu.tag
),

gov_hist AS (
    SELECT
        u.consumer_id,
        u.tag,
        COUNT(*)             AS orders_l365d,
        SUM(dd.gov) / 100.0  AS total_gov_l365d
    FROM distinct_users u
    JOIN proddb.public.dimension_deliveries dd
      ON dd.creator_id = u.consumer_id
     AND dd.is_filtered_core = TRUE
     AND dd.country_id = 1
     AND CAST(dd.created_at AS DATE) >= DATEADD(day, -365, u.first_exposed)
     AND CAST(dd.created_at AS DATE) <  u.first_exposed
    GROUP BY 1, 2
),

bucketed_users AS (
    SELECT
        u.consumer_id,
        u.tag,
        u.first_exposed,
        CASE
            WHEN NVL(g.orders_l365d, 0) = 0 THEN '0. No Orders (365D)'
            WHEN g.total_gov_l365d / g.orders_l365d < 15 THEN '1. <$15'
            WHEN g.total_gov_l365d / g.orders_l365d < 25 THEN '2. $15-25'
            WHEN g.total_gov_l365d / g.orders_l365d < 35 THEN '3. $25-35'
            WHEN g.total_gov_l365d / g.orders_l365d < 50 THEN '4. $35-50'
            ELSE '5. $50+'
        END AS gov_bucket
    FROM distinct_users u
    LEFT JOIN gov_hist g ON g.consumer_id = u.consumer_id AND g.tag = u.tag
),

orders_in_window AS (
    SELECT
        dd.delivery_id,
        dd.creator_id AS consumer_id,
        ec.tag
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

orders_flagged AS (
    SELECT
        o.tag,
        o.delivery_id,
        o.consumer_id,
        (d.wbd_discount + d.pad_discount) AS wbd_pad_discount,
        CASE WHEN (d.wbd_discount + d.pad_discount) > 0 THEN 1 ELSE 0 END AS is_discounted
    FROM orders_in_window o
    LEFT JOIN discounts d ON d.delivery_id = o.delivery_id
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
)

SELECT
    bu.tag,
    bu.gov_bucket,
    COUNT(DISTINCT bu.consumer_id)                                          AS total_users,
    COUNT(DISTINCT CASE WHEN ofl.is_discounted=1 THEN ofl.consumer_id END)    AS users_with_discount,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN ofl.is_discounted=1 THEN ofl.consumer_id END)
        / NULLIF(COUNT(DISTINCT bu.consumer_id), 0), 2)                    AS user_coverage_pct,
    COUNT(ofl.delivery_id)                                                   AS total_orders,
    SUM(NVL(ofl.is_discounted,0))                                            AS discounted_orders,
    ROUND(100.0 * SUM(NVL(ofl.is_discounted,0)) / NULLIF(COUNT(ofl.delivery_id), 0), 2)
                                                                             AS order_coverage_pct,
    ROUND(SUM(CASE WHEN ofl.is_discounted=1 THEN ofl.wbd_pad_discount ELSE 0 END), 2)
                                                                             AS total_wbd_pad_spend,
    MEDIAN(v4.v4_score)                                                     AS median_v4_score,
    COUNT(v4.v4_score)                                                      AS users_with_v4
FROM bucketed_users bu
LEFT JOIN orders_flagged ofl ON ofl.consumer_id = bu.consumer_id AND ofl.tag = bu.tag
LEFT JOIN psm_v4 v4 ON v4.consumer_id = bu.consumer_id AND v4.active_date = bu.first_exposed
GROUP BY 1, 2
ORDER BY 1, 2;


-- 13) NEW/FMX -- Cart Page Conversion Rate (Trailing 365D) bucket
-- CORRECTED: New/FMX -- Cart Page Conversion Rate (Trailing 365D) bucket,
-- spend + coverage. Population = FULL EXPOSED COHORT filtered to New/FMX
-- (days_since_first_purchase < 29 as of first_exposed), NOT gated on
-- placing an order in-window.

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

exposed_users AS (
    SELECT consumer_id, tag, first_exposed
    FROM exposure_cohort
    CROSS JOIN params p
    WHERE first_exposed BETWEEN p.start_date AND p.end_date
),

app_visited AS (
    SELECT DISTINCT
        u.consumer_id,
        u.tag
    FROM exposed_users u
    JOIN proddb.public.fact_unique_visitors_full_utc uv
      ON CAST(uv.user_id AS BIGINT) = u.consumer_id
     AND CAST(uv.event_date AS DATE) >= DATEADD(day, -365, u.first_exposed)
     AND CAST(uv.event_date AS DATE) <= u.first_exposed
    WHERE uv.unique_visitor = 1
      AND uv.is_dashpass = 0
),

distinct_users AS (
    SELECT eu.consumer_id, eu.tag, eu.first_exposed
    FROM exposed_users eu
    JOIN app_visited av ON av.consumer_id = eu.consumer_id AND av.tag = eu.tag
    JOIN proddb.mattheitz.mh_customer_authority ca
      ON ca.creator_id = eu.consumer_id
     AND ca.dte        = eu.first_exposed
     AND ca.days_since_first_purchase < 29
),

traffic AS (
    SELECT
        u.consumer_id,
        u.tag,
        u.first_exposed,
        CAST(uv.event_date AS DATE) AS event_date,
        CASE WHEN SUM(uv.unique_purchaser) > 0 THEN 1 ELSE 0 END AS purchases,
        CASE WHEN SUM(uv.unique_order_cart_page_visitor) > 0 THEN 1 ELSE 0 END AS cart_visits
    FROM distinct_users u
    JOIN proddb.public.fact_unique_visitors_full_utc uv
      ON CAST(uv.user_id AS BIGINT) = u.consumer_id
     AND CAST(uv.event_date AS DATE) >= DATEADD(day, -365, u.first_exposed)
     AND CAST(uv.event_date AS DATE) <= u.first_exposed
    GROUP BY 1, 2, 3, 4
),

traffic_agg AS (
    SELECT
        consumer_id,
        tag,
        SUM(CASE WHEN event_date < first_exposed THEN cart_visits ELSE 0 END) AS cart_visits_l365d,
        SUM(CASE WHEN event_date < first_exposed THEN purchases ELSE 0 END)   AS purchases_l365d
    FROM traffic
    GROUP BY 1, 2
),

bucketed_users AS (
    SELECT
        u.consumer_id,
        u.tag,
        u.first_exposed,
        CASE
            WHEN NVL(t.cart_visits_l365d, 0) = 0 THEN '0. No Cart Visits (365D)'
            WHEN t.purchases_l365d * 1.0 / t.cart_visits_l365d < 0.20 THEN '1. <20%'
            WHEN t.purchases_l365d * 1.0 / t.cart_visits_l365d < 0.40 THEN '2. 20-40%'
            WHEN t.purchases_l365d * 1.0 / t.cart_visits_l365d < 0.60 THEN '3. 40-60%'
            WHEN t.purchases_l365d * 1.0 / t.cart_visits_l365d < 0.80 THEN '4. 60-80%'
            ELSE '5. 80-100%'
        END AS cvr_bucket
    FROM distinct_users u
    LEFT JOIN traffic_agg t ON t.consumer_id = u.consumer_id AND t.tag = u.tag
),

orders_in_window AS (
    SELECT
        dd.delivery_id,
        dd.creator_id AS consumer_id,
        ec.tag
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
      AND dd.creator_id IN (SELECT consumer_id FROM distinct_users)
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
        o.tag,
        o.delivery_id,
        o.consumer_id,
        (d.wbd_discount + d.pad_discount) AS wbd_pad_discount,
        CASE WHEN (d.wbd_discount + d.pad_discount) > 0 THEN 1 ELSE 0 END AS is_discounted
    FROM orders_in_window o
    LEFT JOIN discounts d ON d.delivery_id = o.delivery_id
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
)

SELECT
    bu.tag,
    bu.cvr_bucket,
    COUNT(DISTINCT bu.consumer_id)                                          AS total_users,
    COUNT(DISTINCT CASE WHEN ofl.is_discounted=1 THEN ofl.consumer_id END)    AS users_with_discount,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN ofl.is_discounted=1 THEN ofl.consumer_id END)
        / NULLIF(COUNT(DISTINCT bu.consumer_id), 0), 2)                    AS user_coverage_pct,
    COUNT(ofl.delivery_id)                                                   AS total_orders,
    SUM(NVL(ofl.is_discounted,0))                                            AS discounted_orders,
    ROUND(100.0 * SUM(NVL(ofl.is_discounted,0)) / NULLIF(COUNT(ofl.delivery_id), 0), 2)
                                                                             AS order_coverage_pct,
    ROUND(SUM(CASE WHEN ofl.is_discounted=1 THEN ofl.wbd_pad_discount ELSE 0 END), 2)
                                                                             AS total_wbd_pad_spend,
    MEDIAN(v4.v4_score)                                                     AS median_v4_score,
    COUNT(v4.v4_score)                                                      AS users_with_v4
FROM bucketed_users bu
LEFT JOIN orders_flagged ofl ON ofl.consumer_id = bu.consumer_id AND ofl.tag = bu.tag
LEFT JOIN psm_v4 v4 ON v4.consumer_id = bu.consumer_id AND v4.active_date = bu.first_exposed
GROUP BY 1, 2
ORDER BY 1, 2;


-- 14) NEW/FMX -- GOV per Order (Trailing 365D) bucket
-- CORRECTED: New/FMX -- GOV per Order (Trailing 365D) bucket, spend +
-- coverage. Population = FULL EXPOSED COHORT filtered to New/FMX
-- (days_since_first_purchase < 29 as of first_exposed), NOT gated on
-- placing an order in-window.

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

exposed_users AS (
    SELECT consumer_id, tag, first_exposed
    FROM exposure_cohort
    CROSS JOIN params p
    WHERE first_exposed BETWEEN p.start_date AND p.end_date
),

app_visited AS (
    SELECT DISTINCT
        u.consumer_id,
        u.tag
    FROM exposed_users u
    JOIN proddb.public.fact_unique_visitors_full_utc uv
      ON CAST(uv.user_id AS BIGINT) = u.consumer_id
     AND CAST(uv.event_date AS DATE) >= DATEADD(day, -365, u.first_exposed)
     AND CAST(uv.event_date AS DATE) <= u.first_exposed
    WHERE uv.unique_visitor = 1
      AND uv.is_dashpass = 0
),

distinct_users AS (
    SELECT eu.consumer_id, eu.tag, eu.first_exposed
    FROM exposed_users eu
    JOIN app_visited av ON av.consumer_id = eu.consumer_id AND av.tag = eu.tag
    JOIN proddb.mattheitz.mh_customer_authority ca
      ON ca.creator_id = eu.consumer_id
     AND ca.dte        = eu.first_exposed
     AND ca.days_since_first_purchase < 29
),

gov_hist AS (
    SELECT
        u.consumer_id,
        u.tag,
        COUNT(*)             AS orders_l365d,
        SUM(dd.gov) / 100.0  AS total_gov_l365d
    FROM distinct_users u
    JOIN proddb.public.dimension_deliveries dd
      ON dd.creator_id = u.consumer_id
     AND dd.is_filtered_core = TRUE
     AND dd.country_id = 1
     AND CAST(dd.created_at AS DATE) >= DATEADD(day, -365, u.first_exposed)
     AND CAST(dd.created_at AS DATE) <  u.first_exposed
    GROUP BY 1, 2
),

bucketed_users AS (
    SELECT
        u.consumer_id,
        u.tag,
        u.first_exposed,
        CASE
            WHEN NVL(g.orders_l365d, 0) = 0 THEN '0. No Orders (365D)'
            WHEN g.total_gov_l365d / g.orders_l365d < 15 THEN '1. <$15'
            WHEN g.total_gov_l365d / g.orders_l365d < 25 THEN '2. $15-25'
            WHEN g.total_gov_l365d / g.orders_l365d < 35 THEN '3. $25-35'
            WHEN g.total_gov_l365d / g.orders_l365d < 50 THEN '4. $35-50'
            ELSE '5. $50+'
        END AS gov_bucket
    FROM distinct_users u
    LEFT JOIN gov_hist g ON g.consumer_id = u.consumer_id AND g.tag = u.tag
),

orders_in_window AS (
    SELECT
        dd.delivery_id,
        dd.creator_id AS consumer_id,
        ec.tag
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
      AND dd.creator_id IN (SELECT consumer_id FROM distinct_users)
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
        o.tag,
        o.delivery_id,
        o.consumer_id,
        (d.wbd_discount + d.pad_discount) AS wbd_pad_discount,
        CASE WHEN (d.wbd_discount + d.pad_discount) > 0 THEN 1 ELSE 0 END AS is_discounted
    FROM orders_in_window o
    LEFT JOIN discounts d ON d.delivery_id = o.delivery_id
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
)

SELECT
    bu.tag,
    bu.gov_bucket,
    COUNT(DISTINCT bu.consumer_id)                                          AS total_users,
    COUNT(DISTINCT CASE WHEN ofl.is_discounted=1 THEN ofl.consumer_id END)    AS users_with_discount,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN ofl.is_discounted=1 THEN ofl.consumer_id END)
        / NULLIF(COUNT(DISTINCT bu.consumer_id), 0), 2)                    AS user_coverage_pct,
    COUNT(ofl.delivery_id)                                                   AS total_orders,
    SUM(NVL(ofl.is_discounted,0))                                            AS discounted_orders,
    ROUND(100.0 * SUM(NVL(ofl.is_discounted,0)) / NULLIF(COUNT(ofl.delivery_id), 0), 2)
                                                                             AS order_coverage_pct,
    ROUND(SUM(CASE WHEN ofl.is_discounted=1 THEN ofl.wbd_pad_discount ELSE 0 END), 2)
                                                                             AS total_wbd_pad_spend,
    MEDIAN(v4.v4_score)                                                     AS median_v4_score,
    COUNT(v4.v4_score)                                                      AS users_with_v4
FROM bucketed_users bu
LEFT JOIN orders_flagged ofl ON ofl.consumer_id = bu.consumer_id AND ofl.tag = bu.tag
LEFT JOIN psm_v4 v4 ON v4.consumer_id = bu.consumer_id AND v4.active_date = bu.first_exposed
GROUP BY 1, 2
ORDER BY 1, 2;


-- ============================================================================
-- 15. NEW: PSM v2-vs-v4 Score Comparison by 6-way Cx Segment
-- Population = Classic app visitors (trailing 365D before first_exposed).
-- Includes match rates, matched-only medians/means, imputed-80-for-unmatched
-- sensitivity check, and diff-in-diff (each model's own-baseline deviation,
-- v4 minus v2) for both the matched-only and imputed versions.
-- ============================================================================
-- Same as psm_v2_v4_segment_full_exposed.sql, but population further
-- restricted to APP-VISITED Classic Cx: exposed Cx with
-- unique_visitor = 1 on at least one day in the trailing 365D before
-- first_exposed, in PRODDB.PUBLIC.FACT_UNIQUE_VISITORS_FULL_UTC (the
-- broadest/earliest funnel flag in that table, confirmed working
-- definition, distinct from cart-page-visit or purchase).

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

exposed_users AS (
    SELECT consumer_id, tag, first_exposed
    FROM exposure_cohort
    CROSS JOIN params p
    WHERE first_exposed BETWEEN p.start_date AND p.end_date
),

-- App-visited AND Classic (non-DashPass) on that same visit day -- both
-- conditions checked directly on FACT_UNIQUE_VISITORS_FULL_UTC, which
-- carries IS_DASHPASS natively (no separate subscription-table join
-- needed for this app-visit-anchored check).
app_visited AS (
    SELECT DISTINCT
        u.consumer_id,
        u.tag
    FROM exposed_users u
    JOIN proddb.public.fact_unique_visitors_full_utc uv
      ON CAST(uv.user_id AS BIGINT) = u.consumer_id
     AND CAST(uv.event_date AS DATE) >= DATEADD(day, -365, u.first_exposed)
     AND CAST(uv.event_date AS DATE) <= u.first_exposed
    WHERE uv.unique_visitor = 1
      AND uv.is_dashpass = 0
),

distinct_users AS (
    SELECT eu.consumer_id, eu.tag, eu.first_exposed
    FROM exposed_users eu
    JOIN app_visited av ON av.consumer_id = eu.consumer_id AND av.tag = eu.tag
),

cx_segment AS (
    SELECT
        u.consumer_id,
        u.tag,
        u.first_exposed,
        CASE
            WHEN ca.days_since_first_purchase < 29              THEN 'New/FMX'
            WHEN ca.days_since_last_purchase > 90               THEN 'Churned'
            WHEN ca.days_since_last_purchase BETWEEN 29 AND 90  THEN 'Dormant'
            WHEN ca.l28_orders BETWEEN 1 AND 2                  THEN 'Active - Occasional'
            WHEN ca.l28_orders BETWEEN 3 AND 4                  THEN 'Active - Habituating'
            WHEN ca.l28_orders >= 5                             THEN 'Active - RDP'
        END AS segment
    FROM distinct_users u
    LEFT JOIN proddb.mattheitz.mh_customer_authority ca
      ON ca.creator_id = u.consumer_id
     AND ca.dte        = u.first_exposed
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

v2_windowed AS (
    SELECT
        consumer_id,
        CAST(prediction_datetime_est AS DATE) AS pred_date,
        country_id,
        caf_is_dp_active,
        sens_score_raw_bps
    FROM proddb.public.cx_sensitivity_v2
    CROSS JOIN params p
    WHERE country_id = 1
      AND TRY_TO_TIMESTAMP(prediction_datetime_est) IS NOT NULL
      AND CAST(prediction_datetime_est AS DATE) BETWEEN p.start_date AND p.end_date
),

v2_ptile AS (
    SELECT
        consumer_id,
        pred_date,
        caf_is_dp_active,
        (NTILE(100) OVER (
            PARTITION BY pred_date, country_id, caf_is_dp_active
            ORDER BY sens_score_raw_bps DESC
        )) AS v2_ptile_by_country
    FROM v2_windowed
),

v2_ptile_classic AS (
    SELECT
        consumer_id,
        pred_date,
        v2_ptile_by_country AS v2_score
    FROM v2_ptile
    WHERE caf_is_dp_active = 0
),

joined AS (
    SELECT
        cs.consumer_id,
        cs.tag,
        cs.segment,
        v4.v4_score,
        v2.v2_score,
        NVL(v4.v4_score, 80) AS v4_score_imputed80,
        NVL(v2.v2_score, 80) AS v2_score_imputed80
    FROM cx_segment cs
    LEFT JOIN psm_v4 v4
      ON v4.consumer_id = cs.consumer_id AND v4.active_date = cs.first_exposed
    LEFT JOIN v2_ptile_classic v2
      ON v2.consumer_id = cs.consumer_id AND v2.pred_date = cs.first_exposed
),

with_overall_avg AS (
    SELECT
        *,
        AVG(v4_score) OVER (PARTITION BY tag)            AS overall_mean_v4,
        AVG(v2_score) OVER (PARTITION BY tag)            AS overall_mean_v2,
        AVG(v4_score_imputed80) OVER (PARTITION BY tag)  AS overall_mean_v4_imputed80,
        AVG(v2_score_imputed80) OVER (PARTITION BY tag)  AS overall_mean_v2_imputed80
    FROM joined
)

SELECT
    tag,
    NVL(segment, '(unmatched in mh_customer_authority)') AS segment,
    COUNT(*)                          AS total_users,
    MEDIAN(v4_score)                  AS median_v4_matched,
    AVG(v4_score)                     AS seg_mean_v4,
    COUNT(v4_score)                   AS users_with_v4,
    MEDIAN(v2_score)                  AS median_v2_matched,
    AVG(v2_score)                     AS seg_mean_v2,
    COUNT(v2_score)                   AS users_with_v2,
    AVG(overall_mean_v4)              AS overall_mean_v4,
    AVG(overall_mean_v2)              AS overall_mean_v2,
    (AVG(v4_score) - AVG(overall_mean_v4)) - (AVG(v2_score) - AVG(overall_mean_v2)) AS diff_in_diff_matched,
    AVG(v4_score_imputed80)           AS seg_mean_v4_imputed80,
    AVG(v2_score_imputed80)           AS seg_mean_v2_imputed80,
    AVG(overall_mean_v4_imputed80)    AS overall_mean_v4_imputed80,
    AVG(overall_mean_v2_imputed80)    AS overall_mean_v2_imputed80,
    (AVG(v4_score_imputed80) - AVG(overall_mean_v4_imputed80)) - (AVG(v2_score_imputed80) - AVG(overall_mean_v2_imputed80)) AS diff_in_diff_imputed80
FROM with_overall_avg
GROUP BY 1, 2
ORDER BY 1, 2;


-- ============================================================================
-- 16. DIAGNOSTIC VARIANT of query 15: app-visit checked over a UNIFORM
-- 9/16-9/22 window (the actual v22 rollout span) for every user, instead of
-- each user's own trailing-365D lookback. Used to test whether PSM match%
-- improves when requiring app activity concurrent with PSM's own scoring
-- week -- it does, sharply, for Active/Dormant tiers (~92-98% match), but
-- NOT for New/FMX or Churned, ruling out simple recency as their explanation.
-- Not the standing population definition -- kept as a reference diagnostic.
-- ============================================================================
-- Same as psm_v2_v4_segment_full_exposed.sql, but population further
-- restricted to APP-VISITED Classic Cx: exposed Cx with
-- unique_visitor = 1 on at least one day in the trailing 365D before
-- first_exposed, in PRODDB.PUBLIC.FACT_UNIQUE_VISITORS_FULL_UTC (the
-- broadest/earliest funnel flag in that table, confirmed working
-- definition, distinct from cart-page-visit or purchase).

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

exposed_users AS (
    SELECT consumer_id, tag, first_exposed
    FROM exposure_cohort
    CROSS JOIN params p
    WHERE first_exposed BETWEEN p.start_date AND p.end_date
),

-- VARIANT: app-visit checked over a UNIFORM window (9/16-9/22, the
-- actual v22 rollout span) for every user, regardless of their individual
-- first_exposed date -- avoids the bias of a per-user variable-length
-- window (someone exposed on 9/22 would otherwise only get a 1-day check).
app_visited AS (
    SELECT DISTINCT
        u.consumer_id,
        u.tag
    FROM exposed_users u
    JOIN proddb.public.fact_unique_visitors_full_utc uv
      ON CAST(uv.user_id AS BIGINT) = u.consumer_id
     AND CAST(uv.event_date AS DATE) >= '2026-09-16'
     AND CAST(uv.event_date AS DATE) <= '2026-09-22'
    WHERE uv.unique_visitor = 1
      AND uv.is_dashpass = 0
),

distinct_users AS (
    SELECT eu.consumer_id, eu.tag, eu.first_exposed
    FROM exposed_users eu
    JOIN app_visited av ON av.consumer_id = eu.consumer_id AND av.tag = eu.tag
),

cx_segment AS (
    SELECT
        u.consumer_id,
        u.tag,
        u.first_exposed,
        CASE
            WHEN ca.days_since_first_purchase < 29              THEN 'New/FMX'
            WHEN ca.days_since_last_purchase > 90               THEN 'Churned'
            WHEN ca.days_since_last_purchase BETWEEN 29 AND 90  THEN 'Dormant'
            WHEN ca.l28_orders BETWEEN 1 AND 2                  THEN 'Active - Occasional'
            WHEN ca.l28_orders BETWEEN 3 AND 4                  THEN 'Active - Habituating'
            WHEN ca.l28_orders >= 5                             THEN 'Active - RDP'
        END AS segment
    FROM distinct_users u
    LEFT JOIN proddb.mattheitz.mh_customer_authority ca
      ON ca.creator_id = u.consumer_id
     AND ca.dte        = u.first_exposed
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

v2_windowed AS (
    SELECT
        consumer_id,
        CAST(prediction_datetime_est AS DATE) AS pred_date,
        country_id,
        caf_is_dp_active,
        sens_score_raw_bps
    FROM proddb.public.cx_sensitivity_v2
    CROSS JOIN params p
    WHERE country_id = 1
      AND TRY_TO_TIMESTAMP(prediction_datetime_est) IS NOT NULL
      AND CAST(prediction_datetime_est AS DATE) BETWEEN p.start_date AND p.end_date
),

v2_ptile AS (
    SELECT
        consumer_id,
        pred_date,
        caf_is_dp_active,
        (NTILE(100) OVER (
            PARTITION BY pred_date, country_id, caf_is_dp_active
            ORDER BY sens_score_raw_bps DESC
        )) AS v2_ptile_by_country
    FROM v2_windowed
),

v2_ptile_classic AS (
    SELECT
        consumer_id,
        pred_date,
        v2_ptile_by_country AS v2_score
    FROM v2_ptile
    WHERE caf_is_dp_active = 0
),

joined AS (
    SELECT
        cs.consumer_id,
        cs.tag,
        cs.segment,
        v4.v4_score,
        v2.v2_score,
        NVL(v4.v4_score, 80) AS v4_score_imputed80,
        NVL(v2.v2_score, 80) AS v2_score_imputed80
    FROM cx_segment cs
    LEFT JOIN psm_v4 v4
      ON v4.consumer_id = cs.consumer_id AND v4.active_date = cs.first_exposed
    LEFT JOIN v2_ptile_classic v2
      ON v2.consumer_id = cs.consumer_id AND v2.pred_date = cs.first_exposed
),

with_overall_avg AS (
    SELECT
        *,
        AVG(v4_score) OVER (PARTITION BY tag)            AS overall_mean_v4,
        AVG(v2_score) OVER (PARTITION BY tag)            AS overall_mean_v2,
        AVG(v4_score_imputed80) OVER (PARTITION BY tag)  AS overall_mean_v4_imputed80,
        AVG(v2_score_imputed80) OVER (PARTITION BY tag)  AS overall_mean_v2_imputed80
    FROM joined
)

SELECT
    tag,
    NVL(segment, '(unmatched in mh_customer_authority)') AS segment,
    COUNT(*)                          AS total_users,
    MEDIAN(v4_score)                  AS median_v4_matched,
    AVG(v4_score)                     AS seg_mean_v4,
    COUNT(v4_score)                   AS users_with_v4,
    MEDIAN(v2_score)                  AS median_v2_matched,
    AVG(v2_score)                     AS seg_mean_v2,
    COUNT(v2_score)                   AS users_with_v2,
    AVG(overall_mean_v4)              AS overall_mean_v4,
    AVG(overall_mean_v2)              AS overall_mean_v2,
    (AVG(v4_score) - AVG(overall_mean_v4)) - (AVG(v2_score) - AVG(overall_mean_v2)) AS diff_in_diff_matched,
    AVG(v4_score_imputed80)           AS seg_mean_v4_imputed80,
    AVG(v2_score_imputed80)           AS seg_mean_v2_imputed80,
    AVG(overall_mean_v4_imputed80)    AS overall_mean_v4_imputed80,
    AVG(overall_mean_v2_imputed80)    AS overall_mean_v2_imputed80,
    (AVG(v4_score_imputed80) - AVG(overall_mean_v4_imputed80)) - (AVG(v2_score_imputed80) - AVG(overall_mean_v2_imputed80)) AS diff_in_diff_imputed80
FROM with_overall_avg
GROUP BY 1, 2
ORDER BY 1, 2;
