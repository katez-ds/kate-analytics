-- Cohort: experiment exposure (universal_be) ∩ SF-discount deliveries (discount_orders).
-- Each row is one delivery; eligibility columns are 1 if that order passes the rule, 0 if not.
-- Final grain: experiment tag — counts / percents are over deliveries (not deduped users).
--
-- REPLACE template tokens {{...}} with your dashboard / Curie bindings.
--
-- Assumptions (adjust if your eligibility spec differs):
--   - R2C "eligible": aligns with sf_r2c_percentile_v1 training SQL (nearby_only_v1 pattern):
--       delivery universe + NTILE(100) BY submarket_id ON r2c_miles, threshold = MAX(r2c_miles) for ptile=90;
--       submarkets with volume <= 1000 use national NTILE(100) ptile=90 threshold. Compare cohort order
--       straight-line r2c (miles) to threshold (miles). Lookback: active_date in [end_date - 90d, end_date]
--       matches model default lookback to as_of (set {{end_time-1}} = as-of date for refresh parity).
--   - "120 days" / DP tenure: matches SF_Discount/graduation_rules_check.sql `new_dp1` (L365D window):
--       paid_days and partner_days = distinct subscription `dte` in [order_dt - 1 year, order_dt] with paid/partner logic;
--       order eligible iff CASE paid/partner/trial current state on order_dt AND paid_days <= 120 AND partner_days <= 120.
--       Source: https://github.com/katez-ds/kate-analytics/blob/main/SF_Discount/graduation_rules_check.sql
--   - DP order: dimension_deliveries.is_subscribed_consumer = TRUE at order time.
--   - Prior-window counts: distinct deliveries with created_at::date in [order_dt - N days, order_dt - 1 day]
--       (strictly before checkout date; does not double-count current order).
--
-- Performance: prior-window counts use correlated aggregates; for large cohorts, rewrite with window frames or temp tables.

WITH universal_be AS (
    SELECT
        experiment_name,
        experiment_version AS version,
        tag,
        TRY_CAST(bucket_key AS INTEGER) AS user_id,
        MIN(CAST(exposure_time AS DATE)) AS first_exposed
    FROM proddb.public.fact_dedup_experiment_exposure
    WHERE experiment_name = {{experiment_name-1}}
      AND experiment_version BETWEEN {{experiment_version_start-1}} AND {{experiment_version_end-1}}
      AND exposure_time BETWEEN {{start_time-1}} AND {{end_time-1}}
      AND (tag IN {{control_1-1}} OR tag IN {{treatment_1-1}})
      AND segment IN {{segment-1}}
      AND TRY_CAST(bucket_key AS INTEGER) != 1505155093
    GROUP BY ALL
),

discount_orders AS (
    SELECT
        a.delivery_id,
        a.creator_id AS user_id,
        CAST(a.created_at AS DATE) AS order_dt,
        SUM(
            wbd_fee_promo_discount + cs_fee_promo_discount + pad_fee_promo_discount
        ) AS affordability_program_discount,
        SUM(wbd_fee_promo_discount + cs_fee_promo_discount) AS wbd_xs_discount,
        SUM(pad_fee_promo_discount) AS pad_discount,
        SUM(wbd_sf_promo_discount + cs_sf_promo_discount + pad_sf_promo_discount) AS affordability_program_sf_discount,
        SUM(wbd_sf_promo_discount + cs_sf_promo_discount) AS wbd_xs_sf_discount,
        SUM(pad_sf_promo_discount) AS pad_sf_discount
    FROM proddb.static.df_sf_promo_discount_delivery_level a
    LEFT JOIN proddb.public.dimension_deliveries b
        ON a.delivery_id = b.delivery_id
    WHERE CAST(a.created_at AS DATE) BETWEEN {{start_time-1}} AND {{end_time-1}}
      AND CAST(b.created_at AS DATE) BETWEEN {{start_time-1}} AND {{end_time-1}}
      AND b.country_id = {{country}}
      AND (
          wbd_sf_promo_discount > 0
          OR cs_sf_promo_discount > 0
          OR pad_sf_promo_discount > 0
      )
    GROUP BY 1, 2, 3
),

-- ---------------------------------------------------------------------------
-- sf_r2c_percentile_v1 distribution (construct_main_sql + construct_submarket_sql + overall fallback)
-- ---------------------------------------------------------------------------
r2c_model_delivery_base AS (
    SELECT
        dd.submarket_id,
        dd.delivery_id,
        fdd.straightline_r2c_distance / 1609.34 AS r2c_miles
    FROM proddb.public.dimension_deliveries dd
    INNER JOIN proddb.public.fact_delivery_distances fdd
        ON dd.delivery_id = fdd.delivery_id
    INNER JOIN edw.merchant.dimension_store ds
        ON dd.store_id = ds.store_id
    WHERE dd.is_filtered_core = TRUE
      AND dd.is_consumer_pickup = FALSE
      AND dd.country_id = {{country}}
      AND ds.is_restaurant = 1
      AND dd.is_subscribed_consumer = FALSE
      AND dd.is_bundle_order = FALSE
      AND dd.fulfillment_type NOT IN ('dine_in', 'shipping', 'merchant_fleet', 'virtual')
      AND dd.active_date BETWEEN DATEADD('DAY', -90, {{end_time-1}}) AND {{end_time-1}}
      AND fdd.straightline_r2c_distance IS NOT NULL
      AND dd.submarket_id IS NOT NULL
),

r2c_ranked_by_submarket AS (
    SELECT
        submarket_id,
        delivery_id,
        r2c_miles,
        NTILE(100) OVER (PARTITION BY submarket_id ORDER BY r2c_miles) AS ptile
    FROM r2c_model_delivery_base
),

r2c_submarket_volume AS (
    SELECT
        submarket_id,
        COUNT(DISTINCT delivery_id) AS volume
    FROM r2c_model_delivery_base
    GROUP BY 1
),

r2c_submarket_ntile_thresholds AS (
    SELECT
        r.submarket_id,
        r.ptile,
        MAX(r.r2c_miles) AS r2c_miles_threshold
    FROM r2c_ranked_by_submarket r
    INNER JOIN r2c_submarket_volume v
        ON r.submarket_id = v.submarket_id
    WHERE v.volume > 1000
    GROUP BY 1, 2
),

r2c_submarket_ntile90 AS (
    SELECT
        submarket_id,
        r2c_miles_threshold AS ntile90_max_r2c_miles
    FROM r2c_submarket_ntile_thresholds
    WHERE ptile = 90
),

r2c_national_ranked AS (
    SELECT
        r2c_miles,
        NTILE(100) OVER (ORDER BY r2c_miles) AS ptile
    FROM r2c_model_delivery_base
),

r2c_national_ntile90 AS (
    SELECT
        MAX(r2c_miles) AS ntile90_max_r2c_miles
    FROM r2c_national_ranked
    WHERE ptile = 90
),

-- Distinct (consumer, order_date) for cohort SF-discount deliveries (drives L365 tenure + state joins).
cohort_order_dates AS (
    SELECT DISTINCT
        ub.user_id,
        do.order_dt
    FROM universal_be ub
    INNER JOIN discount_orders do
        ON ub.user_id = do.user_id
),

-- Subscription spine for cohort users; wide date range so L365 from each order_dt is covered.
dp_subscription_days AS (
    SELECT
        d.consumer_id AS user_id,
        d.dte,
        d.is_in_paid_balance,
        d.is_partner_plan,
        d.is_in_trial_period,
        d.dynamic_subscription_status
    FROM edw.consumer.fact_consumer_subscription__daily d
    INNER JOIN (SELECT DISTINCT user_id FROM universal_be) u
        ON d.consumer_id = u.user_id
    WHERE d.dte BETWEEN DATEADD('day', -400, {{start_time-1}}) AND {{end_time-1}}
),

-- L365D paid/partner day counts as of each order_dt (same window as new_dp1: DATEADD(year,-1, as_of) .. as_of).
dp_tenure_l365_by_order AS (
    SELECT
        cod.user_id,
        cod.order_dt,
        COUNT(DISTINCT IFF(
            sd.is_in_paid_balance = TRUE
            OR sd.dynamic_subscription_status ILIKE 'active_paid%',
            sd.dte,
            NULL
        )) AS paid_days_l365,
        COUNT(DISTINCT IFF(
            sd.is_partner_plan = TRUE
            AND (
                sd.dynamic_subscription_status ILIKE 'active%'
                OR sd.dynamic_subscription_status ILIKE '%free_subscription%'
            ),
            sd.dte,
            NULL
        )) AS partner_days_l365
    FROM cohort_order_dates cod
    LEFT JOIN dp_subscription_days sd
        ON cod.user_id = sd.user_id
       AND sd.dte BETWEEN DATEADD('year', -1, cod.order_dt) AND cod.order_dt
    GROUP BY 1, 2
),

-- Subscription "current state" on the calendar day of the order (new_dp1 uses WHERE dte = as_of_date).
dp_state_on_order_dt AS (
    SELECT
        d.consumer_id AS user_id,
        d.dte AS order_dt,
        MAX(IFF(
            d.is_in_paid_balance = TRUE
            OR d.dynamic_subscription_status ILIKE 'active_paid%',
            TRUE,
            FALSE
        )) AS is_dashpass_paid,
        MAX(IFF(
            d.is_partner_plan = TRUE
            AND (
                d.dynamic_subscription_status ILIKE 'active%'
                OR d.dynamic_subscription_status ILIKE '%free_subscription%'
            ),
            TRUE,
            FALSE
        )) AS is_dashpass_partner,
        MAX(IFF(
            d.is_in_trial_period = TRUE
            OR d.dynamic_subscription_status ILIKE 'trial%',
            TRUE,
            FALSE
        )) AS is_trial_active
    FROM edw.consumer.fact_consumer_subscription__daily d
    INNER JOIN cohort_order_dates cod
        ON d.consumer_id = cod.user_id
       AND d.dte = cod.order_dt
    GROUP BY 1, 2
),

cohort_sf_deliveries AS (
    SELECT
        ub.experiment_name,
        ub.version,
        ub.tag,
        ub.user_id,
        ub.first_exposed,
        do.delivery_id,
        do.order_dt,
        do.affordability_program_sf_discount,
        dd.submarket_id,
        dd.store_id,
        fdd.straightline_r2c_distance AS r2c_sl_meters,
        fdd.straightline_r2c_distance / 1609.34 AS r2c_sl_miles,
        COALESCE(
            sm90.ntile90_max_r2c_miles,
            (SELECT n.ntile90_max_r2c_miles FROM r2c_national_ntile90 n)
        ) AS ntile90_threshold_r2c_miles,
        ten.paid_days_l365,
        ten.partner_days_l365,
        st.is_dashpass_paid,
        st.is_dashpass_partner,
        st.is_trial_active,
        (
            SELECT COUNT(DISTINCT dd_p90.delivery_id)
            FROM proddb.public.dimension_deliveries dd_p90
            WHERE dd_p90.creator_id = ub.user_id
              AND dd_p90.is_filtered_core = TRUE
              AND dd_p90.is_subscribed_consumer = TRUE
              AND CAST(dd_p90.created_at AS DATE) >= DATEADD('DAY', -90, do.order_dt)
              AND CAST(dd_p90.created_at AS DATE) < do.order_dt
        ) AS prior90d_dp_orders,
        (
            SELECT COUNT(DISTINCT dd_p28.delivery_id)
            FROM proddb.public.dimension_deliveries dd_p28
            WHERE dd_p28.creator_id = ub.user_id
              AND dd_p28.is_filtered_core = TRUE
              AND dd_p28.is_subscribed_consumer = TRUE
              AND CAST(dd_p28.created_at AS DATE) >= DATEADD('DAY', -28, do.order_dt)
              AND CAST(dd_p28.created_at AS DATE) < do.order_dt
        ) AS prior28d_dp_orders,
        (
            SELECT COUNT(DISTINCT dd_cs.delivery_id)
            FROM proddb.public.dimension_deliveries dd_cs
            WHERE dd_cs.creator_id = ub.user_id
              AND dd_cs.is_filtered_core = TRUE
              AND dd_cs.store_id = dd.store_id
              AND CAST(dd_cs.created_at AS DATE) >= DATEADD('DAY', -90, do.order_dt)
              AND CAST(dd_cs.created_at AS DATE) < do.order_dt
        ) AS prior90d_consumer_store_orders
    FROM universal_be ub
    INNER JOIN discount_orders do
        ON ub.user_id = do.user_id
    INNER JOIN proddb.public.dimension_deliveries dd
        ON do.delivery_id = dd.delivery_id
    LEFT JOIN proddb.public.fact_delivery_distances fdd
        ON do.delivery_id = fdd.delivery_id
    LEFT JOIN r2c_submarket_ntile90 sm90
        ON dd.submarket_id = sm90.submarket_id
    LEFT JOIN dp_tenure_l365_by_order ten
        ON ub.user_id = ten.user_id
       AND do.order_dt = ten.order_dt
    LEFT JOIN dp_state_on_order_dt st
        ON ub.user_id = st.user_id
       AND do.order_dt = st.order_dt
),

order_eligibility AS (
    SELECT
        *,
        -- Order passes R2C rule if straight-line miles <= NTILE(100) ptile=90 threshold (submarket or national fallback).
        IFF(
            r2c_sl_miles IS NOT NULL
            AND ntile90_threshold_r2c_miles IS NOT NULL
            AND r2c_sl_miles <= ntile90_threshold_r2c_miles,
            1,
            0
        ) AS eligible_order_r2c_le_submarket_ntile90,
        -- L365D DashPass paid/partner day counts <= 120 each, with paid/partner/trial path (graduation_rules_check new_dp1).
        IFF(
            CASE
                WHEN COALESCE(is_dashpass_paid, FALSE)
                    OR COALESCE(is_dashpass_partner, FALSE)
                    THEN (COALESCE(paid_days_l365, 0) <= 120)
                        AND (COALESCE(partner_days_l365, 0) <= 120)
                WHEN COALESCE(is_trial_active, FALSE)
                    THEN (COALESCE(paid_days_l365, 0) <= 120)
                        AND (COALESCE(partner_days_l365, 0) <= 120)
                ELSE FALSE
            END,
            1,
            0
        ) AS eligible_order_dp_l365_paid_partner_days_le_120,
        IFF(prior90d_dp_orders <= 20, 1, 0) AS eligible_order_prior90d_dp_orders_le_20,
        IFF(prior28d_dp_orders <= 8, 1, 0) AS eligible_order_prior28d_dp_orders_le_8,
        IFF(prior90d_consumer_store_orders <= 4, 1, 0) AS eligible_order_prior90d_cx_store_le_4
    FROM cohort_sf_deliveries
)

SELECT
    experiment_name,
    tag,
    COUNT(*) AS n_sf_discount_deliveries,
    COUNT(DISTINCT user_id) AS n_distinct_users_with_sf_discount_delivery,
    -- Order fails rule iff eligible_* = 0 (delivery-level flag).
    SUM(IFF(eligible_order_r2c_le_submarket_ntile90 = 0, 1, 0)) AS n_orders_not_eligible_r2c_le_submarket_ntile90,
    SUM(IFF(eligible_order_r2c_le_submarket_ntile90 = 0, 1, 0)) / NULLIF(COUNT(*), 0)
        AS pct_orders_not_eligible_r2c_le_submarket_ntile90,

    SUM(IFF(eligible_order_dp_l365_paid_partner_days_le_120 = 0, 1, 0)) AS n_orders_not_eligible_dp_l365_active_days_le_120,
    SUM(IFF(eligible_order_dp_l365_paid_partner_days_le_120 = 0, 1, 0)) / NULLIF(COUNT(*), 0)
        AS pct_orders_not_eligible_dp_l365_active_days_le_120,

    SUM(IFF(eligible_order_prior90d_dp_orders_le_20 = 0, 1, 0)) AS n_orders_not_eligible_prior90d_dp_le_20,
    SUM(IFF(eligible_order_prior90d_dp_orders_le_20 = 0, 1, 0)) / NULLIF(COUNT(*), 0)
        AS pct_orders_not_eligible_prior90d_dp_le_20,

    SUM(IFF(eligible_order_prior28d_dp_orders_le_8 = 0, 1, 0)) AS n_orders_not_eligible_prior28d_dp_le_8,
    SUM(IFF(eligible_order_prior28d_dp_orders_le_8 = 0, 1, 0)) / NULLIF(COUNT(*), 0)
        AS pct_orders_not_eligible_prior28d_dp_le_8,

    SUM(IFF(eligible_order_prior90d_cx_store_le_4 = 0, 1, 0)) AS n_orders_not_eligible_prior90d_cx_store_le_4,
    SUM(IFF(eligible_order_prior90d_cx_store_le_4 = 0, 1, 0)) / NULLIF(COUNT(*), 0)
        AS pct_orders_not_eligible_prior90d_cx_store_le_4
FROM order_eligibility
GROUP BY 1, 2
ORDER BY 1, 2;
