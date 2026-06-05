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
--   ASK 2a Promo coverage HOF vs avg   (under-covered + decelerating = stronger case)
--   ASK 2b Coverage by FUNDER          (merchant- vs marketing-funded)  -> DATA GAP, see bottom
--   ASK 3  Avg discount/order by OF cohort -> where it DROPS OFF = candidate HOF cutoff
--
-- Sources (reused from the TAM / backtest tools, so they're known-good):
--   proddb.public.dimension_deliveries          -- order grain, gov  (is_filtered_core = TRUE)
--   proddb.public.fact_delivery_allocation      -- VP per order (matches the backtest's `ue`)
--   proddb.mattheitz.mh_customer_authority      -- L365D OF (l360_orders), as-of order date
--   proddb.static.df_sf_promo_discount_delivery_level  -- affordability discount $ per delivery
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
        ca.l360_orders AS l365d_of
    FROM base_dd a
    LEFT JOIN proddb.mattheitz.mh_customer_authority ca
        ON ca.creator_id            = a.creator_id
       AND ca.dte                   = a.order_date
       AND ca.acquisition_country_id = 1
),

-- Affordability discount $ per delivery.  PROGRAM split (WBD / XS / PAD) — NOT funder.
disc AS (
    SELECT
        d.delivery_id,
        SUM(d.wbd_fee_promo_discount + d.cs_fee_promo_discount + d.pad_fee_promo_discount) AS affordability_discount,
        SUM(d.wbd_fee_promo_discount) AS wbd_discount,
        SUM(d.cs_fee_promo_discount)  AS xs_discount,
        SUM(d.pad_fee_promo_discount) AS pad_discount
    FROM proddb.static.df_sf_promo_discount_delivery_level d
    JOIN base_dd b ON b.delivery_id = d.delivery_id
    GROUP BY d.delivery_id
),

orders AS (
    SELECT
        b.delivery_id,
        b.creator_id,
        b.gov,
        b.vp,
        o.l365d_of,
        COALESCE(dc.affordability_discount, 0) AS affordability_discount,
        COALESCE(dc.wbd_discount, 0)           AS wbd_discount,
        COALESCE(dc.xs_discount,  0)           AS xs_discount,
        COALESCE(dc.pad_discount, 0)           AS pad_discount,
        IFF(COALESCE(dc.affordability_discount, 0) > 0, 1, 0) AS is_discount_order,
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
    AVG(affordability_discount)       AS avg_discount_per_order,
    AVG(is_discount_order)            AS promo_coverage,                       -- % of orders w/ any affordability discount
    AVG(IFF(is_discount_order = 1, affordability_discount, NULL))
                                      AS avg_discount_among_discounted,
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
    AVG(affordability_discount)       AS avg_discount_per_order,
    AVG(is_discount_order)            AS promo_coverage
FROM orders
GROUP BY ROLLUP(hof_flag)
ORDER BY cohort;

-- =============================================================================
-- ASK 2b — Promo coverage by FUNDER (merchant-funded vs marketing/company-funded)
-- -----------------------------------------------------------------------------
-- DATA GAP: df_sf_promo_discount_delivery_level splits by affordability PROGRAM
-- (WBD / XS / PAD), NOT by funding ENTITY. There is no merchant-vs-marketing
-- funder field in this source, so this cut CANNOT be produced from the tables above.
--
-- To produce it, wire in a promotions/campaign source that carries a funding-entity /
-- cost-owner field (confirm the canonical one with Promo/Pricing data eng), e.g.:
--   <promotions table>.funding_type / cost_owner / funded_by  joined on delivery_id
--   (or campaign_id), then split `promo_coverage` and `avg_discount_per_order` by funder.
--
-- "Decelerating" (trend): add a time grain (e.g. DATE_TRUNC('week', order_date)) to the
-- GROUP BY once a funder source exists, to show HOF coverage trend over time.
-- =============================================================================

-- =============================================================================
-- ASK 1 (CPIO framing): the VP/order above is the BASELINE level. For the
-- experiment-based "low CPIO = efficient" cut on the HOF cohort, use the
-- Sensitivity Backtest tool with cohort dim `of_bucket` (set OF lookback = 365)
-- -> CPIO/GPLO + VP Impact per OF cohort. (CPIO is a treatment effect, so it needs
-- a relevant experiment; the backtest already computes it per cohort.)
-- =============================================================================
