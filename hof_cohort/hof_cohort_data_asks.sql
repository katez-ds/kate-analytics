-- =============================================================================
-- HOF cohort — efficiency & affordability-coverage data asks
-- =============================================================================
-- Goal: support the "high-order-frequency (HOF) cx are an efficient affordability
-- target" case, and find a clean HOF cutoff.
--
--   HOF := high-order-frequency consumer, L365D order frequency > 30.
--          (Uses MH `l360_orders` — the 360-day count is the canonical "L365D" proxy.)
--
-- Data asks covered:
--   ASK 1  VP per order for HOF        (high VP -> low CPIO -> stronger efficiency case)
--   ASK 2a Promo coverage HOF vs avg   (under-covered + decelerating[= recent OF drop] = stronger case)
--   ASK 2b Coverage by FUNDER          (affordability / Mx-funded / CRM-marketing-funded)
--   ASK 3  Avg discount/order by OF cohort -> where it DROPS OFF = candidate HOF cutoff
--
-- Sources (reused from the TAM / backtest tools, so they're known-good):
--   proddb.public.dimension_deliveries          -- order grain, gov  (is_filtered_core = TRUE)
--   proddb.public.fact_delivery_allocation      -- VP per order (matches the backtest's `ue`)
--   proddb.mattheitz.mh_customer_authority      -- HOF/OF/momentum from the 2026-04-15 snapshot (l360_orders, l28_orders)
--   proddb.static.df_sf_promo_discount_delivery_level  -- affordability (WBD/XS/PAD) + Mx-funded (mx_funded_cx_discount) $/delivery
--   proddb.public.fact_order_discounts_and_promotions_extended  -- CRM/marketing (campaign) funded $/delivery
--   (funder split mirrors kate-analytics/promo_orders_overlap.sql)
--
-- VP/order is defined to MATCH the Sensitivity Backtest tool EXACTLY (its `ue`):
--   ue = COALESCE(fda.variable_profit_ex_alloc,
--                 fda.variable_profit + fda.payment_to_customers)
--   from proddb.public.fact_delivery_allocation (joined on delivery_id, active_date-bounded),
--   over proddb.public.dimension_deliveries WHERE is_filtered_core = TRUE.
-- =============================================================================

-- Date methodology:
--   * HOF cohort is defined on the MH snapshot = 2026-04-15 (l360_orders > 30) — no order scan
--   * behavior-metric window = the 28 days AFTER the snapshot (2026-04-16 .. 2026-05-13):
--     classify at the snapshot, then observe promo coverage / discount / VP forward.
SET snapshot_dte = '2026-04-15';      -- mh_customer_authority.dte / PSM active_date (cohort definition)
SET window_days  = 28;                -- behavior-metric window length, POST the snapshot

WITH params AS (
    SELECT $snapshot_dte::DATE AS snapshot_dte, $window_days::INT AS window_days
),

-- Order (delivery) grain over the window. Base + VP aligned to the Sensitivity
-- Backtest tool: proddb.public.dimension_deliveries (is_filtered_core = TRUE),
-- VP from fact_delivery_allocation (identical COALESCE to the backtest's `ue`).
base_dd AS (
    SELECT
        dd.delivery_id,
        dd.creator_id,
        dd.created_at::DATE        AS order_date,
        dd.gov,
        COALESCE(fda.variable_profit_ex_alloc,
                 fda.variable_profit + fda.payment_to_customers) AS vp
    FROM params p
    CROSS JOIN proddb.public.dimension_deliveries dd
    -- active_date-bounded so fact_delivery_allocation (~686M rows) partition-prunes; ±21d buffer
    -- absorbs lag between created_at and active_date (same pattern as the backtest's core_dd).
    LEFT JOIN proddb.public.fact_delivery_allocation fda
        ON fda.delivery_id = dd.delivery_id
       AND fda.active_date BETWEEN DATEADD('day', -21, p.snapshot_dte)
                               AND DATEADD('day', p.window_days + 21, p.snapshot_dte)
    -- Behavior window = the `window_days` days AFTER the snapshot (forward-looking).
    WHERE dd.created_at::DATE BETWEEN DATEADD('day', 1, p.snapshot_dte)
                                  AND DATEADD('day', p.window_days, p.snapshot_dte)
      AND dd.is_filtered_core = TRUE
),

-- HOF / OF / momentum classified on the FIXED MH snapshot (matches high_OF_Cx_exploration.sql):
-- one row per consumer at snapshot_dte, attached to all their in-window orders below.
cx_cohort AS (
    SELECT
        ca.creator_id,
        -- Classic vs DashPass at the snapshot (mirrors high_OF_Cx_exploration.sql).
        CASE WHEN ca.dp_sub_flag_start = 1 THEN 'DashPass' ELSE 'Classic' END AS consumer_type,
        ca.l360_orders AS l365d_of,
        ca.l28_orders,
        IFF(ca.l360_orders > 30, 'HOF (L365D OF > 30)', 'Non-HOF') AS hof_flag,
        -- Fine OF bins around 20-40 so the discount/order drop-off (Ask 3) is visible.
        CASE
            WHEN ca.l360_orders IS NULL THEN 'unknown'
            WHEN ca.l360_orders <= 0    THEN '1. 0'
            WHEN ca.l360_orders <= 5    THEN '2. 1-5'
            WHEN ca.l360_orders <= 10   THEN '3. 6-10'
            WHEN ca.l360_orders <= 20   THEN '4. 11-20'
            WHEN ca.l360_orders <= 30   THEN '5. 21-30'
            WHEN ca.l360_orders <= 50   THEN '6. 31-50'
            WHEN ca.l360_orders <= 100  THEN '7. 51-100'
            ELSE                            '8. 100+'
        END AS of_bucket,
        -- OF momentum / deceleration: recent weekly rate (L28/4) vs long-run (L360/360*7).
        CASE
            WHEN ca.l28_orders IS NULL OR ca.l360_orders IS NULL THEN 'unknown'
            WHEN ca.l360_orders / 360.0 * 7 = ca.l28_orders / 4.0 THEN 'OF Same'
            WHEN ca.l360_orders / 360.0 * 7 < ca.l28_orders / 4.0 THEN 'OF Increase'
            ELSE 'OF Drop'
        END AS of_momentum,
        IFF(ca.l360_orders / 360.0 * 7 > ca.l28_orders / 4.0, 1, 0) AS is_of_drop
    FROM proddb.mattheitz.mh_customer_authority ca
    CROSS JOIN params p
    WHERE ca.dte = p.snapshot_dte
      AND ca.acquisition_country_id = 1
),

-- Discount $ per delivery, split by FUNDER. Logic mirrors kate-analytics/promo_orders_overlap.sql:
--   affordability program (WBD/XS/PAD) and Mx-funded (mx_funded_cx_discount) both live in
--   df_sf_promo_discount_delivery_level; CRM (marketing/campaign) funded is derived from
--   fact_order_discounts_and_promotions_extended (campaign-tagged FDA components).
disc AS (
    SELECT
        d.delivery_id,
        SUM(d.wbd_fee_promo_discount + d.cs_fee_promo_discount + d.pad_fee_promo_discount) AS affordability_discount,
        SUM(d.wbd_fee_promo_discount) AS wbd_discount,
        SUM(d.cs_fee_promo_discount)  AS xs_discount,
        SUM(d.pad_fee_promo_discount) AS pad_discount,
        SUM(d.mx_funded_cx_discount)  AS mx_funded_discount,   -- merchant-funded
        SUM(d.total_fee_promo_discount) AS total_discount
    FROM proddb.static.df_sf_promo_discount_delivery_level d
    JOIN base_dd b ON b.delivery_id = d.delivery_id
    GROUP BY d.delivery_id
),
-- CRM / marketing-team funded discount per delivery (campaign-tagged FDA components).
crm AS (
    SELECT delivery_id, SUM(crm_disc) AS crm_discount
    FROM (
        SELECT
            e.delivery_id,
            CASE WHEN e.campaign_id IS NOT NULL
                 THEN COALESCE(e.FDA_OTHER_PROMOTIONS_BASE + e.FDA_PROMOTION_CATCH_ALL
                             + e.FDA_CONSUMER_RETENTION - e.FDA_BUNDLES_PRICING_DISCOUNT, 0)
                 ELSE 0 END AS crm_disc
        FROM proddb.public.fact_order_discounts_and_promotions_extended e
        JOIN base_dd b ON b.delivery_id = e.delivery_id
    )
    GROUP BY delivery_id
    HAVING SUM(crm_disc) > 0          -- keep only CRM-funded deliveries (crm_ind = present)
),

orders AS (
    SELECT
        b.delivery_id,
        b.creator_id,
        b.gov,
        b.vp,
        -- Cohort attributes — classified once on the snapshot (consumer-level), per high-OF methodology.
        c.consumer_type,
        c.l365d_of,
        c.l28_orders,
        c.hof_flag,
        c.of_bucket,
        c.of_momentum,
        c.is_of_drop,
        -- Funder coverage segment (overlap-aware; 'Uncovered' = no funded discount).
        CASE
            WHEN COALESCE(dc.affordability_discount, 0) = 0
             AND COALESCE(dc.mx_funded_discount, 0) = 0
             AND cr.delivery_id IS NULL THEN 'Uncovered'
            ELSE CONCAT_WS('+',
                    IFF(COALESCE(dc.affordability_discount, 0) > 0, 'Affordability', NULL),
                    IFF(COALESCE(dc.mx_funded_discount, 0) > 0,     'Mx',            NULL),
                    IFF(cr.delivery_id IS NOT NULL,                 'CRM',           NULL))
        END AS coverage_segment,
        COALESCE(dc.affordability_discount, 0) AS affordability_discount,
        COALESCE(dc.wbd_discount, 0)           AS wbd_discount,
        COALESCE(dc.xs_discount,  0)           AS xs_discount,
        COALESCE(dc.pad_discount, 0)           AS pad_discount,
        COALESCE(dc.mx_funded_discount, 0)     AS mx_funded_discount,   -- merchant-funded
        COALESCE(cr.crm_discount, 0)           AS crm_discount,         -- marketing/CRM-funded
        COALESCE(dc.total_discount, 0)         AS total_discount,
        -- Coverage flags per funder (order has any discount of that type).
        IFF(COALESCE(dc.affordability_discount, 0) > 0, 1, 0) AS is_afford_order,
        IFF(COALESCE(dc.mx_funded_discount, 0)     > 0, 1, 0) AS is_mx_order,
        IFF(cr.delivery_id IS NOT NULL, 1, 0)                 AS is_crm_order,
        -- Combined coverage: order has ANY funded discount (affordability OR Mx OR CRM).
        IFF(COALESCE(dc.affordability_discount, 0) > 0
            OR COALESCE(dc.mx_funded_discount, 0) > 0
            OR cr.delivery_id IS NOT NULL, 1, 0)              AS is_any_promo_order
    FROM base_dd b
    JOIN cx_cohort c  ON c.creator_id  = b.creator_id      -- snapshot cohort (INNER: in-snapshot consumers only)
    LEFT JOIN disc dc ON dc.delivery_id = b.delivery_id
    LEFT JOIN crm  cr ON cr.delivery_id = b.delivery_id
)

-- =============================================================================
-- MAIN PULL — ASK 1 + 2a + 3 by L365D-OF bucket
--   vp_per_order            (ASK 1: high VP -> low CPIO -> efficiency)
--   avg_discount_per_order  (ASK 3: where it drops off = candidate cutoff)
--   promo_coverage          (ASK 2a: HOF vs avg; under-covered = stronger case)
-- =============================================================================
-- Column order + naming follow the "Followups from Gayatri" table in the High OF Cx
-- Exploration doc: L365D OF, Cx, Cx %, ORDERS, VP/Order, GOV/Order, Promo Coverage
-- (= Affordability+Mx+CRM), Affordability/Mx Funded/CRM Coverage, then Avg discount $/order
-- per funder. (consumer_type kept as the leading Classic-vs-DashPass breakdown.)
SELECT
    consumer_type,
    of_bucket                         AS l365d_of,                  -- "L365D OF"
    COUNT(DISTINCT creator_id)        AS cx,                        -- "Cx"
    COUNT(DISTINCT creator_id) * 1.0
        / SUM(COUNT(DISTINCT creator_id)) OVER (PARTITION BY consumer_type) AS cx_pct,   -- "Cx %"
    COUNT(*)                          AS orders,                    -- "ORDERS"
    AVG(vp)                           AS vp_per_order,              -- "VP per Order"
    AVG(gov)                          AS gov_per_order,             -- "GOV per Order"
    AVG(is_any_promo_order)           AS promo_coverage,            -- "Promo Coverage" (Affordability+Mx+CRM)
    AVG(is_afford_order)              AS affordability_coverage,    -- "Affordability Coverage"
    AVG(is_mx_order)                  AS mx_funded_coverage,        -- "Mx Funded Coverage"
    AVG(is_crm_order)                 AS crm_coverage,              -- "CRM Coverage"
    AVG(affordability_discount)       AS avg_affordability_discount,-- "Avg. Affordability Discount"
    AVG(mx_funded_discount)           AS avg_mx_funded_discount,    -- "Avg. Mx Funded Discount"
    AVG(crm_discount)                 AS avg_crm_discount           -- "Avg. CRM Discount"
FROM orders
GROUP BY consumer_type, of_bucket
ORDER BY consumer_type, of_bucket;

-- =============================================================================
-- ASK 2 (i) — OVERALL HOF: HOF (>30) vs Non-HOF vs ALL (avg). Headline coverage +
-- VP + discount. "Under-covered" = HOF any_promo_coverage vs ALL.
-- =============================================================================
SELECT
    consumer_type,
    COALESCE(hof_flag, 'ALL')         AS cohort,
    COUNT(*)                          AS orders,
    COUNT(DISTINCT creator_id)        AS cx,
    AVG(vp)                           AS vp_per_order,
    AVG(is_any_promo_order)           AS any_promo_coverage,                   -- affordability + Mx + CRM (under-covered = HOF vs ALL here)
    AVG(is_afford_order)              AS afford_coverage,
    AVG(is_mx_order)                  AS mx_coverage,
    AVG(is_crm_order)                 AS crm_coverage,
    AVG(affordability_discount)       AS avg_afford_disc_per_order,
    AVG(mx_funded_discount)           AS avg_mx_disc_per_order,
    AVG(crm_discount)                 AS avg_crm_disc_per_order
FROM orders
GROUP BY consumer_type, ROLLUP(hof_flag)
ORDER BY consumer_type, cohort;

-- =============================================================================
-- DECELERATION CUT — HOF cx by OF momentum  (the "decelerating = stronger case" view)
-- -----------------------------------------------------------------------------
-- Restricts to HOF (L365D OF > 30) and splits by OF momentum. "OF Drop" = recent
-- weekly rate (L28/4) below long-run (L360/360*7) = decelerating high-OF cx.
-- Stronger case = HOF + OF Drop that is ALSO under-covered (low any_promo_coverage)
-- and high VP/order. Logic mirrors high_OF_Cx_exploration.sql.
-- =============================================================================
SELECT
    consumer_type,
    of_momentum,
    COUNT(*)                          AS orders,
    COUNT(DISTINCT creator_id)        AS cx,
    AVG(vp)                           AS vp_per_order,
    AVG(is_any_promo_order)           AS any_promo_coverage,   -- under-covered?
    AVG(is_afford_order)              AS afford_coverage,
    AVG(is_mx_order)                  AS mx_coverage,
    AVG(is_crm_order)                 AS crm_coverage,
    AVG(affordability_discount)       AS avg_afford_disc_per_order,
    AVG(mx_funded_discount)           AS avg_mx_disc_per_order,
    AVG(crm_discount)                 AS avg_crm_disc_per_order
FROM orders
WHERE l365d_of > 30                   -- HOF only
GROUP BY consumer_type, of_momentum
ORDER BY consumer_type, of_momentum;

-- =============================================================================
-- ASK 2 (ii) — HOF x COVERAGE breakdown: how HOF orders split across funder
-- coverage (overlap-aware; 'Uncovered' = no funded discount). pct_of_orders shows
-- how much of HOF volume each segment is; 'Uncovered' share = the under-coverage.
-- =============================================================================
SELECT
    consumer_type,
    coverage_segment,
    COUNT(*)                          AS orders,
    COUNT(DISTINCT creator_id)        AS cx,
    COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY consumer_type)  AS pct_of_orders,
    AVG(vp)                           AS vp_per_order,
    AVG(affordability_discount + mx_funded_discount + crm_discount) AS avg_total_disc_per_order
FROM orders
WHERE l365d_of > 30                   -- HOF only
GROUP BY consumer_type, coverage_segment
ORDER BY consumer_type, orders DESC;

-- =============================================================================
-- ASK 2 (iii) — DECELERATING HOF x COVERAGE breakdown: same as (ii) but restricted
-- to decelerating HOF (of_momentum = 'OF Drop'). High 'Uncovered' share here +
-- high VP/order = the strongest case (high-value, slipping, under-supported).
-- =============================================================================
SELECT
    consumer_type,
    coverage_segment,
    COUNT(*)                          AS orders,
    COUNT(DISTINCT creator_id)        AS cx,
    COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY consumer_type)  AS pct_of_orders,
    AVG(vp)                           AS vp_per_order,
    AVG(affordability_discount + mx_funded_discount + crm_discount) AS avg_total_disc_per_order
FROM orders
WHERE l365d_of > 30                   -- HOF
  AND of_momentum = 'OF Drop'         -- decelerating
GROUP BY consumer_type, coverage_segment
ORDER BY consumer_type, orders DESC;

-- =============================================================================
-- ASK 2b — Promo coverage by FUNDER (affordability / Mx-funded / CRM-marketing)
-- -----------------------------------------------------------------------------
-- IMPLEMENTED above: afford_coverage / mx_coverage / crm_coverage (and avg $/order
-- per funder) are in BOTH the by-OF-bucket pull and the HOF-vs-ALL rollup.
-- Funder logic mirrors kate-analytics/promo_orders_overlap.sql:
--   * affordability = WBD + XS + PAD            (df_sf_promo_discount_delivery_level)
--   * Mx-funded     = mx_funded_cx_discount     (same table)
--   * CRM/marketing = campaign-tagged FDA comps (fact_order_discounts_and_promotions_extended)
-- => "under-covered" reads off any_promo_coverage (affordability + Mx + CRM, union) for
--    HOF vs ALL; the per-funder afford/mx/crm columns show which funder drives the gap.
--
-- "Decelerating" = HOF cx whose recent order frequency has DROPPED (recent weekly rate
--   L28/4 below long-run L360/360*7) — see the DECELERATION CUT query above
--   (of_momentum = 'OF Drop'). Stronger case = HOF + OF Drop + under-covered + high VP.
-- =============================================================================

-- =============================================================================
-- ASK 1 (CPIO framing): the VP/order above is the BASELINE level. For the
-- experiment-based "low CPIO = efficient" cut on the HOF cohort, use the
-- Sensitivity Backtest tool with cohort dim `of_bucket` (set OF lookback = 365)
-- -> CPIO/GPLO + VP Impact per OF cohort. (CPIO is a treatment effect, so it needs
-- a relevant experiment; the backtest already computes it per cohort.)
-- =============================================================================
