-- =============================================================================
-- HOF cohort — recent-OF-deceleration cut, by L180D weekly order-frequency band
-- =============================================================================
-- Ask (snapshot = 2026-07-01):
--   0. CLASSIC consumers only (dp_sub_flag_start != 1) — DashPass excluded.
--   1. Consumers with L365D orders BETWEEN 50 AND 100.
--   2. Exclude the top 10% LEAST price-sensitive consumers (drop the lowest-
--      sensitivity decile of the cohort).
--   3. Among the survivors, keep those whose L28D weekly order frequency has
--      dropped >= 50% vs their L180D weekly baseline.
--   4. Break the resulting consumer count down by L180D weekly OF band
--      (0-1, 1-2, 2-3, 3-4, 4+).
--
-- Sources (same lineage as hof_cohort_data_asks.sql / high_OF_Cx_exploration.sql):
--   proddb.mattheitz.mh_customer_authority  -- cohort def @ dte snapshot: l360_orders
--                                              (L365D proxy), l28_orders, consumer_type
--   proddb.public.dimension_deliveries       -- L180D order count (mh has no L180 field)
--   proddb.ml.cx_sensitivity_v3              -- price sensitivity (PSM v3) @ active_date
--
-- Definitions / assumptions (flagged so they're easy to change):
--   * L365D orders   = mh_customer_authority.l360_orders (the canonical L365D proxy).
--   * L28D orders    = mh_customer_authority.l28_orders. L28D weekly = l28_orders / 4.
--   * L180D orders   = COUNT(DISTINCT delivery_id) from dimension_deliveries over the
--                      180 calendar days ending on the snapshot (is_filtered_core = TRUE).
--                      L180D weekly = l180d_orders / (180/7).
--   * Price sensitivity: PSM v3 `v3_relative_sensitivity` — HIGHER = MORE sensitive
--     (verified: 0.dp_very_insensitive avg~0.05 .. 9.classic_very_sensitive avg~4.83).
--     "Top 10% least price sensitive" = the lowest-sensitivity decile WITHIN the cohort
--     (NTILE(10) ascending, decile 1) -> excluded. Consumers with no v3 score are KEPT
--     (mirrors high_OF_Cx_exploration.sql treating missing PSM as price-sensitive).
--   * ">= 50% drop" baseline = the L180D weekly rate: l28_weekly <= 0.5 * l180_weekly.
--     Requires l180_weekly > 0 (no baseline -> not a measurable drop).
-- =============================================================================
SET snap = '2026-07-01';

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
    SELECT creator_id, consumer_type, l365d_orders, l28_orders FROM scored   WHERE sens_decile > 1
    UNION ALL
    SELECT creator_id, consumer_type, l365d_orders, l28_orders FROM unscored
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

-- (4) Weekly rates + the >=50% recent-drop filter, then band by L180D weekly OF.
metrics AS (
    SELECT
        k.creator_id,
        k.consumer_type,
        COALESCE(l.l180d_orders, 0)              AS l180d_orders,
        COALESCE(l.l180d_orders, 0) / (180.0/7)  AS l180_weekly,
        k.l28_orders / 4.0                        AS l28_weekly
    FROM kept k
    LEFT JOIN l180 l ON l.creator_id = k.creator_id
),
decel AS (
    SELECT
        *,
        CASE
            WHEN l180_weekly < 1 THEN '1. 0-1'
            WHEN l180_weekly < 2 THEN '2. 1-2'
            WHEN l180_weekly < 3 THEN '3. 2-3'
            WHEN l180_weekly < 4 THEN '4. 3-4'
            ELSE                      '5. 4+'
        END AS l180_weekly_band
    FROM metrics
    WHERE l180_weekly > 0                       -- measurable baseline
      AND l28_weekly <= 0.5 * l180_weekly       -- >= 50% drop in recent (L28D) weekly OF
)

SELECT
    l180_weekly_band,
    COUNT(*)                                        AS cx,
    COUNT(*) * 1.0 / SUM(COUNT(*)) OVER ()          AS pct_of_cx
FROM decel
GROUP BY 1
ORDER BY 1;

-- =============================================================================
-- CLASSIC-ONLY cohort size (snapshot 2026-07-01): 2,374,213 cx (100% v3-scored).
-- Validated result numbers pending the Classic-only re-run (2026-07-07).
--
-- For reference, the ALL-CONSUMER run (Classic + DashPass, 7,312,834 cohort)
-- produced 1,700,351 decelerating cx: 0-1 =562,059 / 1-2 =952,094 /
-- 2-3 =173,367 / 3-4 =12,830 / 4+ =1. The 3-4 / 4+ bands are near-empty by
-- construction (cohort capped at 100 L365D orders ~1.9/wk).
-- =============================================================================

L180_WEEKLY_BAND	CX	PCT_OF_CX
1. 0-1	323912	0.416502
2. 1-2	384949	0.494986
3. 2-3	63817	0.082059
4. 3-4	5019	0.006454
