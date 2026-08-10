WITH
exposure_cohort AS (
    SELECT
        TRY_CAST(bucket_key AS INTEGER) AS user_id,
        MIN(CAST(exposure_time AS DATE)) AS first_exposed
    FROM proddb.public.fact_dedup_experiment_exposure
    WHERE experiment_name = 'discount_engine_us_wbd_v2'
      AND tag = 'treatment_3'
      AND segment = 'Users'
      AND TRY_CAST(bucket_key AS INTEGER) IS NOT NULL
      AND TRY_CAST(bucket_key AS INTEGER) != 1505155093
    GROUP BY ALL
),
sampled_cohort AS (
    SELECT user_id, first_exposed
    FROM exposure_cohort
    QUALIFY ROW_NUMBER() OVER (ORDER BY HASH(user_id)) <= 6000
),
cohort_orders AS (
    SELECT d.creator_id AS user_id, d.delivery_id, CAST(d.created_at AS DATE) AS order_dt
    FROM proddb.public.dimension_deliveries d
    INNER JOIN sampled_cohort sc ON d.creator_id = sc.user_id
    WHERE d.is_filtered_core = TRUE AND d.is_subscribed_consumer = TRUE
      AND CAST(d.created_at AS DATE) >= sc.first_exposed
      AND CAST(d.created_at AS DATE) <= CURRENT_DATE
),
cohort_user_order_dates AS (
    SELECT DISTINCT user_id, order_dt FROM cohort_orders
),
dp_hist_for_cohort AS (
    SELECT d.creator_id AS user_id, d.delivery_id, CAST(d.created_at AS DATE) AS order_date_d
    FROM proddb.public.dimension_deliveries d
    INNER JOIN sampled_cohort sc ON d.creator_id = sc.user_id
    WHERE d.is_filtered_core = TRUE AND d.is_subscribed_consumer = TRUE
      AND CAST(d.created_at AS DATE) >= DATEADD('day', -365, sc.first_exposed)
      AND CAST(d.created_at AS DATE) < CURRENT_DATE
),
prior_of_windows AS (
    SELECT
        k.user_id, k.order_dt,
        COUNT(DISTINCT IFF(h.order_date_d >= DATEADD('day', -84, k.order_dt) AND h.order_date_d < k.order_dt, h.delivery_id, NULL)) AS p84d_of,
        COUNT(DISTINCT IFF(h.order_date_d >= DATEADD('day', -365, k.order_dt) AND h.order_date_d < k.order_dt, h.delivery_id, NULL)) AS p365d_of
    FROM cohort_user_order_dates k
    LEFT JOIN dp_hist_for_cohort h
      ON h.user_id = k.user_id
     AND h.order_date_d >= DATEADD('day', -365, k.order_dt) AND h.order_date_d < k.order_dt
    GROUP BY 1, 2
),
traffic AS (
    SELECT
        v.user_id, v.event_date,
        CASE WHEN SUM(v.unique_purchaser) > 0 THEN 1 ELSE 0 END AS purchases,
        CASE WHEN SUM(v.unique_core_visitor) > 0 THEN 1 ELSE 0 END AS visits
    FROM proddb.public.fact_unique_visitors_full_utc v
    INNER JOIN sampled_cohort sc ON v.user_id = sc.user_id
    WHERE v.user_id IS NOT NULL
      AND v.event_date >= DATEADD('day', -28, sc.first_exposed)
      AND v.event_date < CURRENT_DATE
    GROUP BY 1, 2
),
cvr_windows AS (
    SELECT
        k.user_id, k.order_dt,
        COALESCE(
            SUM(IFF(t.event_date >= DATEADD('day', -28, k.order_dt) AND t.event_date < k.order_dt, t.purchases, NULL))
            / NULLIF(SUM(IFF(t.event_date >= DATEADD('day', -28, k.order_dt) AND t.event_date < k.order_dt, t.visits, NULL)), 0),
            0
        ) AS p28d_cvr
    FROM cohort_user_order_dates k
    LEFT JOIN traffic t
      ON t.user_id = k.user_id
     AND t.event_date >= DATEADD('day', -28, k.order_dt) AND t.event_date < k.order_dt
    GROUP BY 1, 2
),
base AS (
    SELECT
        co.user_id, co.delivery_id, co.order_dt,
        pw.p84d_of, pw.p365d_of, cw.p28d_cvr,
        IFF(cw.p28d_cvr < 0.55, 1, 0) AS cvr_pass
    FROM cohort_orders co
    JOIN prior_of_windows pw ON pw.user_id = co.user_id AND pw.order_dt = co.order_dt
    JOIN cvr_windows cw ON cw.user_id = co.user_id AND cw.order_dt = co.order_dt
),
grid AS (
    SELECT column1 AS p84d_thresh FROM VALUES (5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15)
)
SELECT
    g.p84d_thresh,
    g.p84d_thresh * 4 AS p365d_thresh,
    COUNT(*) AS total_orders,
    SUM(IFF((b.p84d_of < g.p84d_thresh AND b.p365d_of < g.p84d_thresh * 4) OR b.cvr_pass = 1, 1, 0)) AS eligible_orders,
    ROUND(100.0 * SUM(IFF((b.p84d_of < g.p84d_thresh AND b.p365d_of < g.p84d_thresh * 4) OR b.cvr_pass = 1, 1, 0)) / COUNT(*), 3) AS pct_eligible
FROM base b
CROSS JOIN grid g
GROUP BY g.p84d_thresh
ORDER BY g.p84d_thresh;
