-- DV A (discount_engine_global_holdout_us) DF backtest by Cx Segment
-- Rebuilt to follow the ACTUAL reference logic in
-- katez-ds/kate-analytics/SF_Discount/sensitivity_backtest.sql (fetched 2026-09-24),
-- adapted DF-for-SF, with Cx Segment = pre-exposure consumer-level lifecycle
-- (New/FMX, Churned, Dormant, Active-Occasional/Habituating/RDP).
WITH params AS (
    -- Original 3-month window (Kate's initial ask), DF discount logic
    -- corrected to match production (TOTAL_DF_PROMO_DISCOUNT, not wbd+pad+cs).
    SELECT DATE '2026-03-12' AS start_date, DATE '2026-06-12' AS end_date,
           DATE '2026-06-12' AS order_end_date
),

exp AS (
    -- Matched to the actual production be-table load (us_universal_dv_a_be__load.sql):
    -- requires a confirmed US visitor record on the exposure date, version>=6,
    -- result IS NOT NULL. Dropped the DashMart-bot exclusion (not in production)
    -- and kept the ILIKE 'control%' collapse (functionally same as production's
    -- LIKE 'control_%' given the confirmed tag set).
    SELECT
        CASE
            WHEN tag ILIKE 'control%' THEN 'Control'
            ELSE 'Treatment'
        END AS tag_renamed,
        TRY_CAST(bucket_key AS INTEGER)  AS user_id,
        MIN(CAST(exposure_time AS DATE)) AS first_exposed
    FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE ee
    JOIN proddb.public.fact_unique_visitors_full_UTC b
      ON TRY_TO_NUMBER(ee.bucket_key) = b.user_id
     AND b.event_date = CAST(ee.exposure_time AS DATE)
     AND b.country_name = 'United States'
    , params p
    WHERE experiment_name = 'discount_engine_global_holdout_us'
      AND experiment_version >= 6
      AND segment = 'Users'
      AND result IS NOT NULL
      AND exposure_time >= p.start_date
      AND exposure_time <  DATEADD(day, 1, p.end_date)
    GROUP BY ALL
),

be AS (
    SELECT
        e.*,
        CASE
            WHEN ca.days_since_first_purchase < 29              THEN '1. New/FMX'
            WHEN ca.days_since_last_purchase > 90               THEN '2. Churned'
            WHEN ca.days_since_last_purchase BETWEEN 29 AND 90  THEN '3. Dormant'
            WHEN ca.l28_orders BETWEEN 1 AND 2                  THEN '4. Active - Occasional'
            WHEN ca.l28_orders BETWEEN 3 AND 4                  THEN '5. Active - Habituating'
            WHEN ca.l28_orders >= 5                             THEN '6. Active - RDP'
            ELSE '7. Unmatched'
        END AS cohort
    FROM exp e
    LEFT JOIN proddb.mattheitz.mh_customer_authority ca
      ON ca.creator_id = e.user_id
     AND ca.dte = e.first_exposed
),

df_promo AS (
    -- Corrected to match production's actual Net DF calc
    -- (us_universal_dv_core_dd__load.sql): TOTAL_DF_PROMO_DISCOUNT, the full
    -- discount total across all categories (wbd+cs+pad+fdf+other), not just
    -- wbd+pad+cs.
    SELECT
        delivery_id,
        NVL(SUM(total_df_promo_discount), 0) AS df_discount_use
    FROM proddb.static.df_sf_promo_discount_delivery_level
    GROUP BY 1
),

core_dd AS (
    SELECT
        dd.delivery_id,
        dd.creator_id AS consumer_id,
        dd.created_at,
        dd.gov,
        dd.subtotal,
        dd.delivery_fee,
        NVL(dfp.df_discount_use, 0) AS df_discount_use,
        GREATEST(dd.delivery_fee / 100.0 - NVL(dfp.df_discount_use, 0), 0) AS actual_df_paid_by_cx,
        COALESCE(fda.variable_profit_ex_alloc, fda.variable_profit + fda.payment_to_customers) AS ue
    -- Removed is_consumer_pickup/restaurant-only/fulfillment_type filters --
    -- not present in production's us_universal_dv_core_dd__load.sql (which
    -- only filters is_filtered_core + country_id=1). Order Rate should be
    -- over the broader order definition, matching production.
    FROM proddb.public.dimension_deliveries dd
    LEFT JOIN proddb.public.fact_delivery_allocation fda ON fda.delivery_id = dd.delivery_id
    LEFT JOIN df_promo dfp ON dfp.delivery_id = dd.delivery_id
    CROSS JOIN params p
    WHERE dd.is_filtered_core       = TRUE
      AND dd.country_id            = 1
      AND CAST(dd.created_at AS DATE) BETWEEN p.start_date AND p.order_end_date
),

dp_signup AS (
    SELECT
        e.user_id,
        CASE WHEN dsa.is_in_intraday_trial_balance = TRUE
              AND dsa.is_new_subscription_date = TRUE THEN 1 ELSE 0 END
        +
        CASE WHEN dsa.is_in_intraday_pay_balance = TRUE
              AND dsa.is_new_paying_subscription_date = TRUE
              AND dsa.is_direct_to_pay_date = TRUE
              AND dsa.billing_period IS NOT NULL THEN 1 ELSE 0 END AS dashpass_signup
    FROM exp e
    JOIN edw.consumer.fact_consumer_subscription__daily dsa
      ON e.user_id = dsa.consumer_id
     AND DATEADD(second, 600, COALESCE(dsa.elected_time, dsa.start_time)) BETWEEN e.first_exposed AND CURRENT_DATE
    WHERE dsa.is_new_subscription_date = TRUE
      AND dsa.country_id_subscribed_from = 1
      AND dsa.consumer_subscription_plan_id != 10002416
      AND dsa.subscription_status != 'cancelled_subscription_creation_failed'
),

dp_adoption AS (
    SELECT user_id AS consumer_id
    FROM dp_signup
    WHERE dashpass_signup >= 1
    GROUP BY 1
),

comb AS (
    SELECT
        a.tag_renamed,
        a.user_id AS consumer_id,
        a.first_exposed,
        a.cohort,
        dpa.consumer_id AS dp_sign_up,
        c.delivery_id,
        c.gov,
        c.subtotal,
        c.delivery_fee,
        c.actual_df_paid_by_cx,
        c.ue
    FROM be a
    LEFT JOIN core_dd c ON c.consumer_id = a.user_id
     AND CAST(c.created_at AS DATE) >= a.first_exposed
    LEFT JOIN dp_adoption dpa ON a.user_id = dpa.consumer_id
)

SELECT *
FROM (
    SELECT
        cohort,
        COUNT(DISTINCT CASE WHEN tag_renamed = 'Control'   THEN consumer_id END) AS total_cx_c,
        COUNT(DISTINCT CASE WHEN tag_renamed = 'Treatment' THEN consumer_id END) AS total_cx_t,
        COUNT(DISTINCT CASE WHEN tag_renamed = 'Control'   THEN delivery_id END) AS volume_control,
        COUNT(DISTINCT CASE WHEN tag_renamed = 'Treatment' THEN delivery_id END) AS volume_treatment,
        volume_control   * 1.0 / NULLIF(total_cx_c, 0) AS order_rate_control,
        volume_treatment * 1.0 / NULLIF(total_cx_t, 0) AS order_rate_treatment,
        order_rate_treatment / NULLIF(order_rate_control, 0) - 1 AS or_lift,
        volume_treatment - volume_control * total_cx_t * 1.0 / NULLIF(total_cx_c, 0) AS volume_impact,
        SUM(CASE WHEN tag_renamed = 'Treatment' THEN ue END)
          - SUM(CASE WHEN tag_renamed = 'Control' THEN ue END) * total_cx_t * 1.0 / NULLIF(total_cx_c, 0) AS vp_impact,
        - vp_impact / NULLIF(volume_impact, 0) AS gplo_cpio,
        SUM(CASE WHEN tag_renamed = 'Treatment' THEN gov / 100.0 END)
          - SUM(CASE WHEN tag_renamed = 'Control' THEN gov / 100.0 END) * total_cx_t * 1.0 / NULLIF(total_cx_c, 0) AS gov_impact,
        gov_impact / NULLIF((SUM(CASE WHEN tag_renamed = 'Control' THEN gov / 100.0 END) * total_cx_t * 1.0 / NULLIF(total_cx_c, 0)), 0) AS gov_lift,
        AVG(CASE WHEN tag_renamed = 'Control'   THEN ue END) AS ue_control,
        AVG(CASE WHEN tag_renamed = 'Treatment' THEN ue END) AS ue_treatment,
        AVG(CASE WHEN tag_renamed = 'Control'   THEN actual_df_paid_by_cx END) AS net_df_control,
        AVG(CASE WHEN tag_renamed = 'Treatment' THEN actual_df_paid_by_cx END) AS net_df_treatment,
        net_df_treatment - net_df_control AS net_df_delta,
        COUNT(DISTINCT CASE WHEN tag_renamed = 'Control'   THEN dp_sign_up END) * 1.0 / NULLIF(total_cx_c, 0) AS dp_signup_rate_control,
        COUNT(DISTINCT CASE WHEN tag_renamed = 'Treatment' THEN dp_sign_up END) * 1.0 / NULLIF(total_cx_t, 0) AS dp_signup_rate_treatment,
        dp_signup_rate_treatment / NULLIF(dp_signup_rate_control, 0) - 1 AS dp_signup_lift,
        dp_signup_lift / NULLIF(net_df_delta, 0) AS dp_signup_sensitivity,
        - or_lift / NULLIF(net_df_delta, 0) AS price_sensitivity_df
    FROM comb
    GROUP BY 1

    UNION ALL

    SELECT
        'Overall' AS cohort,
        COUNT(DISTINCT CASE WHEN tag_renamed = 'Control'   THEN consumer_id END) AS total_cx_c,
        COUNT(DISTINCT CASE WHEN tag_renamed = 'Treatment' THEN consumer_id END) AS total_cx_t,
        COUNT(DISTINCT CASE WHEN tag_renamed = 'Control'   THEN delivery_id END) AS volume_control,
        COUNT(DISTINCT CASE WHEN tag_renamed = 'Treatment' THEN delivery_id END) AS volume_treatment,
        volume_control   * 1.0 / NULLIF(total_cx_c, 0) AS order_rate_control,
        volume_treatment * 1.0 / NULLIF(total_cx_t, 0) AS order_rate_treatment,
        order_rate_treatment / NULLIF(order_rate_control, 0) - 1 AS or_lift,
        volume_treatment - volume_control * total_cx_t * 1.0 / NULLIF(total_cx_c, 0) AS volume_impact,
        SUM(CASE WHEN tag_renamed = 'Treatment' THEN ue END)
          - SUM(CASE WHEN tag_renamed = 'Control' THEN ue END) * total_cx_t * 1.0 / NULLIF(total_cx_c, 0) AS vp_impact,
        - vp_impact / NULLIF(volume_impact, 0) AS gplo_cpio,
        SUM(CASE WHEN tag_renamed = 'Treatment' THEN gov / 100.0 END)
          - SUM(CASE WHEN tag_renamed = 'Control' THEN gov / 100.0 END) * total_cx_t * 1.0 / NULLIF(total_cx_c, 0) AS gov_impact,
        gov_impact / NULLIF((SUM(CASE WHEN tag_renamed = 'Control' THEN gov / 100.0 END) * total_cx_t * 1.0 / NULLIF(total_cx_c, 0)), 0) AS gov_lift,
        AVG(CASE WHEN tag_renamed = 'Control'   THEN ue END) AS ue_control,
        AVG(CASE WHEN tag_renamed = 'Treatment' THEN ue END) AS ue_treatment,
        AVG(CASE WHEN tag_renamed = 'Control'   THEN actual_df_paid_by_cx END) AS net_df_control,
        AVG(CASE WHEN tag_renamed = 'Treatment' THEN actual_df_paid_by_cx END) AS net_df_treatment,
        net_df_treatment - net_df_control AS net_df_delta,
        COUNT(DISTINCT CASE WHEN tag_renamed = 'Control'   THEN dp_sign_up END) * 1.0 / NULLIF(total_cx_c, 0) AS dp_signup_rate_control,
        COUNT(DISTINCT CASE WHEN tag_renamed = 'Treatment' THEN dp_sign_up END) * 1.0 / NULLIF(total_cx_t, 0) AS dp_signup_rate_treatment,
        dp_signup_rate_treatment / NULLIF(dp_signup_rate_control, 0) - 1 AS dp_signup_lift,
        dp_signup_lift / NULLIF(net_df_delta, 0) AS dp_signup_sensitivity,
        - or_lift / NULLIF(net_df_delta, 0) AS price_sensitivity_df
    FROM comb
)
ORDER BY cohort;
