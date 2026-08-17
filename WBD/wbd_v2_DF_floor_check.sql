/*
Population — who's included:
1. Consumers exposed to discount_engine_us_wbd_v2, experiment_version >= 7, excluding treatment_0 (control has no WBD arm) and the known bench user.
2. Their US classic (non-DashPass), restaurant-marketplace orders with gross_df > 0, from 2026-08-13 through "today" (grows nightly).
3. Only orders where WBD fired (wbd_cents > 0) — whether or not PAD also fired on the same order (WBD takes priority, so PAD's presence doesn't exclude the order anymore).
4. Orders touched by some other non-WBD/non-PAD promo (Mx-funded, XS, etc.) are flagged (has_other_promo) and excluded from the % calc — tracked separately as n_excluded_other_promo.

Per-consumer floor:
- l365d_orders = count of their orders in the trailing 365 days, as of the window's start date.
- floor_cents = 0 (waived) if l365d_orders < 5.49).

The check itself:
- net_df_after_wbd = gross_df − wbd_discount (PAD's amount is irrelevant to this calc either way).
- is_below_floor = net_df_after_wbd < floor_ce
*/

WITH params AS (
    SELECT
        DATE '2026-08-13'  AS start_date,   -- inclusive
        DATE '2026-08-17'  AS end_date,     -- inclusive (today)
        49                 AS floor_cents,
        5                  AS l365d_order_waiver
),

exposure_cohort AS (
    SELECT
        tag,
        TRY_CAST(bucket_key AS INTEGER) AS consumer_id
    FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
    WHERE experiment_name = 'discount_engine_
      AND experiment_version >= 7
      AND exposure_time BETWEEN '2026-08-13'
      AND segment = 'Users'
      AND tag != 'treatment_0'  -- control hafloor on
      AND TRY_CAST(bucket_key AS INTEGER) != 1505155093
    GROUP BY ALL
),

-- US classic restaurant marketplace deliveries in the window, restricted
-- to consumers exposed to the selected exper
base_orders AS (
    SELECT
        dd.delivery_id,
        dd.active_date,
        dd.creator_id      AS consumer_id,
        ec.tag,
        dd.store_id,
        dd.submarket_id,
        dd.fee             AS gross_df_cents
    FROM proddb.public.dimension_deliveries d
    JOIN edw.merchant.dimension_store ds
      ON ds.store_id = dd.store_id
    JOIN exposure_cohort ec
      ON ec.consumer_id = dd.creator_id
    CROSS JOIN params p
    WHERE dd.active_date BETWEEN p.start_date
      AND dd.is_filtered_core     = TRUE
      AND dd.country_id           = 1
      AND dd.is_consumer_pickup   = FALSE
      AND dd.is_subscribed_consumer = FALSE  s $0 DF
      AND NVL(ds.is_restaurant, 0) = 1
      AND NVL(dd.fulfillment_type, '') NOT IN
          ('merchant_fleet', 'shipping', 'digital', 'virtual', 'dine_in', 'drone')
      AND dd.fee > 0                         oor to violate
),

-- Per-delivery affordability spend. Source table is in DOLLARS -> convert to cents.
discounts AS (
    SELECT
        delivery_id,
        ROUND(100 * NVL(SUM(wbd_df_promo_discount),   0)) AS wbd_cents,
        ROUND(100 * NVL(SUM(pad_df_promo_disc
        ROUND(100 * NVL(SUM(total_df_promo_discount), 0)) AS total_df_discount_cents
    FROM proddb.static.df_sf_promo_discount_d
    GROUP BY 1
),

-- Trailing-365-day order count, which decideved
-- for this consumer. Window is [start_date - 365d, start_date), i.e. as
-- of the start of the check window rather th
l365d AS (
    SELECT
        dd.creator_id            AS consumer_id,
        COUNT(*)                 AS l365d_ord
    FROM proddb.public.dimension_deliveries dd
    JOIN (SELECT DISTINCT consumer_id FROM ba
      ON c.consumer_id = dd.creator_id
    CROSS JOIN params p
    WHERE dd.is_filtered_core = TRUE
      AND dd.active_date >= DATEADD('day', -3
      AND dd.active_date < p.start_date
    GROUP BY 1
),

evaluated AS (
    SELECT
        b.delivery_id,
        b.active_date,
        b.tag,
        b.submarket_id,
        b.gross_df_cents,
        NVL(d.wbd_cents, 0)
        NVL(d.pad_cents, 0)                       AS pad_cents,
        NVL(l.l365d_orders, 0)

        -- Any DF promo that is neither WBD n).
        GREATEST(0, NVL(d.total_df_discount_cents, 0)
                    - NVL(d.wbd_cents, 0) - Ner_promo_cents,

        -- The consumer's own floor: waived ft.
        IFF(NVL(l.l365d_orders, 0) < p.l365d_order_waiver, 0, p.floor_cents)


        -- What WBD alone left on the fee. WB so
        -- this is evaluated the same way regardless of whether PAD also
        -- fired on the same order.
        b.gross_df_cents - NVL(d.wbd_cents, 0)    AS net_df_after_wbd_cents
    FROM base_orders b
    CROSS JOIN params p
    LEFT JOIN discounts d ON d.delivery_id =
    LEFT JOIN l365d      l ON l.consumer_id = b.consumer_id
    WHERE NVL(d.wbd_cents, 0) > 0    -- WBD fave)
),

-- Flag rather than filter, so the excluded orders can still be counted in
-- the output instead of vanishing silently.
testable AS (
    SELECT *,
           other_promo_cents > 1                            AS has_other_promo,
           net_df_after_wbd_cents < floor_cen_floor,
           GREATEST(0, floor_cents - net_df_after_wbd_cents) AS shortfall_cents
    FROM evaluated
)

-- ---------------- RESULT: one row per arm x cohort, plus an explicit verdict ----------------
SELECT
    tag                                                              AS arm,
    IFF(floor_cents = 0,
        'floor WAIVED  (<  5 L365D orders)',
        'floor BOUND   (>= 5 L365D orders)') ort,

    COUNT_IF(NOT has_other_promo)            n_wbd_orders,
    COUNT_IF(NOT has_other_promo AND is_below_floor)                AS n_below_floor,
    ROUND(100.0 * COUNT_IF(NOT has_other_prom
                / NULLIF(COUNT_IF(NOT has_other_promo), 0), 2)      AS pct_below_floor,

    ROUND(AVG(IFF(NOT has_other_promo, gross_df_cents, NULL))         / 100.0, 2) AS avg_gross_df,
    ROUND(AVG(IFF(NOT has_other_promo, wbd_ce 100.0, 2) AS avg_wbd_discount,
    ROUND(AVG(IFF(NOT has_other_promo, net_df_after_wbd_cents, NULL)) / 100.0, 2) AS avg_net_df_after_wbd,
    ROUND(MEDIAN(IFF(NOT has_other_promo, l36        AS median_l365d_orders,

    ROUND(AVG(IFF(NOT has_other_promo AND is_ts, NULL))
          / 100.0, 2)                                               AS avg_shortfall_dollars,
    ROUND(SUM(IFF(NOT has_other_promo, shortf
                                                                    AS total_shortfall_dollars,

    -- Not silently dropped: orders where another DF promo also moved the fee, so WBD alone
    -- cannot be held responsible for the net
    COUNT_IF(has_other_promo)                                       AS n_excluded_other_promo,

    CASE
        WHEN floor_cents = 0 THEN
            'EXPECTED - floor is waived for this cohort by policy, not a violation'
        WHEN COUNT_IF(NOT has_other_promo AND
            'PASS - floor enforced on every order'
        WHEN 100.0 * COUNT_IF(NOT has_other_p
                   / NULLIF(COUNT_IF(NOT has_other_promo), 0) < 1 THEN
            'MINOR - under 1% below floor, lis'
        ELSE
            'FAIL - floor not enforced'
    END                                                             AS verdict
FROM testable
GROUP BY tag, floor_cents
ORDER BY tag, floor_cents DESC
