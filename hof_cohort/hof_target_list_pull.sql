Pipeline (unchanged from source query):
--   0. CLASSIC consumers only (dp_sub_flag_start != 1) — DashPass excluded.
--   1. Consumers with L365D orders BETWEEN 50 AND 100.
--   2. Exclude the top 10% LEAST price-sensitive consumers (drop the lowest-
--      sensitivity decile of the cohort).
--   3. Among the survivors, keep those whose L28D weekly order frequency has
--      dropped >= 50% vs their L180D weekly baseline.
--   4. keep only those with L180D weekly OF in [0, 3].


SET snap = '2026-08-10';

WITH
-- (1) Cohort: US, Classic-only, tenured, L365D orders in [50, 100], @ snapshot.
cohort AS (
    SELECT
        ca.creator_id,
        'Classic' AS consumer_type,
        ca.l360_orders AS l365d_orders,
        ca.l28_orders
    FROM proddb.mattheitz.mh_customer_authority ca
    WHERE ca.dte = $snap::DATE
      AND ca.acquisition_country_id = 1
      AND ca.dp_sub_flag_start IS DISTINCT FROM 1   -- Classic only (DashPass excluded; NULL -> Classic)
      AND ca.days_since_first_purchase > 0
      AND ca.l360_orders BETWEEN 50 AND 100
),

-- (2) Attach PSM v3 sensitivity and rank the cohort into sensitivity deciles.
--     Only SCORED consumers are ranked; unscored are carried separately (kept).
scored AS (
    SELECT
        c.creator_id, c.consumer_type, c.l365d_orders, c.l28_orders,
        s.v3_relative_sensitivity,
        NTILE(10) OVER (ORDER BY s.v3_relative_sensitivity ASC) AS sens_decile  -- 1 = least sensitive
    FROM cohort c
    JOIN proddb.ml.cx_sensitivity_v3 s
      ON s.consumer_id = c.creator_id
     AND s.active_date = $snap::DATE
),
unscored AS (
    SELECT
        c.creator_id, c.consumer_type, c.l365d_orders, c.l28_orders,
        NULL::FLOAT AS v3_relative_sensitivity, NULL::INT AS sens_decile
    FROM cohort c
    LEFT JOIN proddb.ml.cx_sensitivity_v3 s
      ON s.consumer_id = c.creator_id AND s.active_date = $snap::DATE
    WHERE s.consumer_id IS NULL
),
-- Keep everyone EXCEPT the least-sensitive decile (decile 1). Unscored are kept.
kept AS (
    SELECT creator_id, consumer_type, l365d_orders, l28_orders, v3_relative_sensitivity FROM scored   WHERE sens_decile > 1
    UNION ALL
    SELECT creator_id, consumer_type, l365d_orders, l28_orders, v3_relative_sensitivity FROM unscored
),

-- (3) L180D orders from dimension_deliveries (last 180 calendar days ending on snapshot).
l180 AS (
    SELECT dd.creator_id, COUNT(DISTINCT dd.delivery_id) AS l180d_orders
    FROM proddb.public.dimension_deliveries dd
    JOIN kept k ON k.creator_id = dd.creator_id
    WHERE dd.is_filtered_core = TRUE
      AND dd.created_at::DATE BETWEEN DATEADD('day', -179, $snap::DATE) AND $snap::DATE
    GROUP BY 1
),

-- (4) Weekly rates + the >=50% recent-drop filter.
metrics AS (
    SELECT
        k.creator_id,
        k.consumer_type,
        k.v3_relative_sensitivity,
        k.l365d_orders,
        k.l28_orders,
        COALESCE(l.l180d_orders, 0)              AS l180d_orders,
        COALESCE(l.l180d_orders, 0) / (180.0/7)  AS l180_weekly,
        k.l28_orders / 4.0                        AS l28_weekly
    FROM kept k
    LEFT JOIN l180 l ON l.creator_id = k.creator_id
),
decel AS (
    SELECT *
    FROM metrics
    WHERE l180_weekly > 0                       -- measurable baseline
      AND l28_weekly <= 0.5 * l180_weekly       -- >= 50% drop in recent (L28D) weekly OF
)

-- (5) FINAL STEP (new): restrict to L180D weekly OF 0-3, return consumer-level list.
SELECT
    creator_id,
    consumer_type,
    l365d_orders,
    l28_orders,
    l180d_orders,
    l180_weekly,
    l28_weekly,
    v3_relative_sensitivity
FROM decel
WHERE l180_weekly <= 3                          -- 0-3 band cutoff (bands "1. 0-1" + "2. 1-2" + "3. 2-3")
ORDER BY creator_id;
