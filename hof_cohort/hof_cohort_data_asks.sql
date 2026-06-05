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
--   proddb.mattheitz.mh_customer_authority      -- L365D OF (l360_orders), as-of order date
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

SET window_days = 90;                 -- trailing window for the pull (edit as needed)
SET end_dte     = CURRENT_DATE - 1;   -- last full day

WITH params AS (
    SELECT $window_days::INT AS window_days, $end_dte::DATE AS end_dte
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
       AND fda.active_date BETWEEN DATEADD('day', -(p.window_days - 1) - 21, p.end_dte)
                               AND DATEADD('day', 21, p.end_dte)
    WHERE dd.created_at::DATE BETWEEN DATEADD('day', -(p.window_days - 1), p.end_dte) AND p.end_dte
      AND dd.is_filtered_core = TRUE
),

-- L365D order frequency as-of the order date (HOF = l360_orders > 30).
cx_of AS (
    SELECT
        a.delivery_id,
        ca.l360_orders AS l365d_of,
        ca.l28_orders                       -- recent rate, for OF-drop / deceleration
    FROM base_dd a
    LEFT JOIN proddb.mattheitz.mh_customer_authority ca
        ON ca.creator_id            = a.creator_id
       AND ca.dte                   = a.order_date
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
        o.l365d_of,
        o.l28_orders,
        -- OF momentum / deceleration (mirrors high_OF_Cx_exploration.sql): recent weekly
        -- rate (L28/4) vs long-run weekly rate (L360/360*7). "OF Drop" = decelerating.
        CASE
            WHEN o.l28_orders IS NULL OR o.l365d_of IS NULL THEN 'unknown'
            WHEN o.l365d_of / 360.0 * 7 = o.l28_orders / 4.0 THEN 'OF Same'
            WHEN o.l365d_of / 360.0 * 7 < o.l28_orders / 4.0 THEN 'OF Increase'
            ELSE 'OF Drop'
        END AS of_momentum,
        IFF(o.l365d_of / 360.0 * 7 > o.l28_orders / 4.0, 1, 0) AS is_of_drop,
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
            OR cr.delivery_id IS NOT NULL, 1, 0)              AS is_any_promo_order,
        -- Fine OF bins around 20-40 so the discount/order drop-off (Ask 3) is visible.
        CASE
            WHEN o.l365d_of IS NULL THEN 'unknown'
            WHEN o.l365d_of <= 0    THEN '00_zero'
            WHEN o.l365d_of <= 5    THEN '01_1-5'
            WHEN o.l365d_of <= 10   THEN '02_6-10'
            WHEN o.l365d_of <= 20   THEN '03_11-20'
            WHEN o.l365d_of <= 30   THEN '04_21-30'
            WHEN o.l365d_of <= 50   THEN '05_31-50'
            WHEN o.l365d_of <= 100  THEN '06_51-100'
            ELSE                         '07_100+'
        END AS of_bucket,
        IFF(o.l365d_of > 30, 'HOF (L365D OF > 30)', 'Non-HOF') AS hof_flag
    FROM base_dd b
    JOIN cx_of o      ON o.delivery_id  = b.delivery_id
    LEFT JOIN disc dc ON dc.delivery_id = b.delivery_id
    LEFT JOIN crm  cr ON cr.delivery_id = b.delivery_id
)

-- =============================================================================
-- MAIN PULL — ASK 1 + 2a + 3 by L365D-OF bucket
--   vp_per_order            (ASK 1: high VP -> low CPIO -> efficiency)
--   avg_discount_per_order  (ASK 3: where it drops off = candidate cutoff)
--   promo_coverage          (ASK 2a: HOF vs avg; under-covered = stronger case)
-- =============================================================================
SELECT
    of_bucket,
    COUNT(*)                          AS orders,
    COUNT(DISTINCT creator_id)        AS cx,
    AVG(vp)                           AS vp_per_order,
    AVG(gov)                          AS gov_per_order,
    -- Promo coverage (ASK 2a + 2b). any_promo_coverage = the combined affordability+Mx+CRM
    -- coverage that defines "under-covered" (compare HOF vs ALL); then the per-funder split.
    AVG(is_any_promo_order)           AS any_promo_coverage,                   -- affordability + Mx + CRM (union)
    AVG(is_afford_order)              AS afford_coverage,                      -- affordability program (WBD/XS/PAD)
    AVG(is_mx_order)                  AS mx_coverage,                          -- merchant-funded
    AVG(is_crm_order)                 AS crm_coverage,                         -- marketing/CRM-funded
    -- Avg discount $/order by funder (ASK 3: where affordability drops off = cutoff).
    AVG(affordability_discount)       AS avg_afford_disc_per_order,
    AVG(mx_funded_discount)           AS avg_mx_disc_per_order,
    AVG(crm_discount)                 AS avg_crm_disc_per_order,
    AVG(IFF(is_afford_order = 1, affordability_discount, NULL))
                                      AS avg_afford_disc_among_covered,
    AVG(wbd_discount)                 AS avg_wbd_per_order,
    AVG(xs_discount)                  AS avg_xs_per_order,
    AVG(pad_discount)                 AS avg_pad_per_order
FROM orders
GROUP BY of_bucket
ORDER BY of_bucket;

-- HOF (>30) vs Non-HOF vs grand-total rollup — same headline metrics.
SELECT
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
GROUP BY ROLLUP(hof_flag)
ORDER BY cohort;

-- =============================================================================
-- DECELERATION CUT — HOF cx by OF momentum  (the "decelerating = stronger case" view)
-- -----------------------------------------------------------------------------
-- Restricts to HOF (L365D OF > 30) and splits by OF momentum. "OF Drop" = recent
-- weekly rate (L28/4) below long-run (L360/360*7) = decelerating high-OF cx.
-- Stronger case = HOF + OF Drop that is ALSO under-covered (low any_promo_coverage)
-- and high VP/order. Logic mirrors high_OF_Cx_exploration.sql.
-- =============================================================================
SELECT
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
GROUP BY of_momentum
ORDER BY of_momentum;

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
