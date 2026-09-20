WITH params AS (
    SELECT
        DATE '2026-06-22' AS start_date,
        DATE '2026-09-13' AS end_date,
        5   AS l365d_order_waiver,
        49  AS floor_cents
),

base_orders AS (
    SELECT
        dd.delivery_id,
        dd.creator_id AS consumer_id,
        dd.store_id,
        CAST(dd.created_at AS DATE) AS order_date,
        dd.fee AS gross_df_cents
    FROM proddb.public.dimension_deliveries dd
    JOIN edw.merchant.dimension_store ds
      ON ds.store_id = dd.store_id
    CROSS JOIN params p
    WHERE CAST(dd.created_at AS DATE) BETWEEN p.start_date AND p.end_date
      AND dd.is_filtered_core = TRUE
      AND dd.country_id = 1
      AND dd.is_consumer_pickup = FALSE
      AND dd.is_subscribed_consumer = FALSE
      AND NVL(ds.is_restaurant, 0) = 1
      AND NVL(dd.fulfillment_type, '') NOT IN
          ('merchant_fleet', 'shipping', 'digital', 'virtual', 'dine_in', 'drone')
),

discounts AS (
    SELECT
        delivery_id,
        ROUND(100 * NVL(SUM(wbd_df_promo_discount), 0)) AS wbd_cents,
        ROUND(100 * NVL(SUM(pad_df_promo_discount), 0)) AS pad_cents
    FROM proddb.static.df_sf_promo_discount_delivery_level
    GROUP BY 1
),

l365d AS (
    SELECT
        dd.creator_id AS consumer_id,
        COUNT(*) AS l365d_orders
    FROM proddb.public.dimension_deliveries dd
    JOIN (SELECT DISTINCT consumer_id FROM base_orders) c
      ON c.consumer_id = dd.creator_id
    CROSS JOIN params p
    WHERE dd.is_filtered_core = TRUE
      AND dd.country_id = 1
      AND CAST(dd.created_at AS DATE) >= DATEADD('day', -365, p.start_date)
      AND CAST(dd.created_at AS DATE) < p.start_date
    GROUP BY 1
),

evaluated AS (
    SELECT
        b.delivery_id,
        b.consumer_id,
        b.store_id,
        b.order_date,
        b.gross_df_cents,
        NVL(d.wbd_cents, 0) + NVL(d.pad_cents, 0) AS actual_discount_cents,
        NVL(l.l365d_orders, 0) AS l365d_orders,
        IFF(NVL(l.l365d_orders, 0) <= p.l365d_order_waiver, 0, p.floor_cents) AS own_floor_cents,
        b.gross_df_cents - NVL(d.wbd_cents, 0) - NVL(d.pad_cents, 0) AS net_df_cents
    FROM base_orders b
    CROSS JOIN params p
    LEFT JOIN discounts d ON d.delivery_id = b.delivery_id
    LEFT JOIN l365d l ON l.consumer_id = b.consumer_id
),

classified AS (
    SELECT
        *,
        (actual_discount_cents > 0) AS got_wbd_pad_discount,
        CASE
            WHEN actual_discount_cents = 0 THEN NULL
            WHEN own_floor_cents = 49 AND net_df_cents <= 49 THEN 'hit_049_floor'
            WHEN own_floor_cents = 0 AND net_df_cents <= 0 THEN 'hit_0_floor'
            ELSE 'did_not_hit_floor'
        END AS floor_status
    FROM evaluated
),

floor_hit_orders AS (
    SELECT * FROM classified WHERE floor_status IN ('hit_049_floor', 'hit_0_floor')
),

model_log AS (
    SELECT
        consumer_id,
        store_id,
        event_date,
        MAX(delivery_fee_discount) AS model_delivery_fee_discount_cents
    FROM proddb.ml.discount_engine_log_v1
    WHERE event_date BETWEEN '2026-06-22' AND '2026-09-13'
      AND country_id = 1
      AND is_df_discount_eligible = TRUE
    GROUP BY 1, 2, 3
),

joined AS (
    SELECT
        f.*,
        m.model_delivery_fee_discount_cents,
        (m.consumer_id IS NOT NULL) AS matched_to_model_log
    FROM floor_hit_orders f
    LEFT JOIN model_log m
      ON m.consumer_id = f.consumer_id
     AND m.store_id = TO_VARCHAR(f.store_id)
     AND m.event_date = f.order_date
)

SELECT
    floor_status,
    COUNT(*) AS total_floor_hit_orders,
    ROUND(100.0 * COUNT_IF(matched_to_model_log) / COUNT(*), 0) AS pct_matched_to_model_log,
    COUNT_IF(matched_to_model_log AND model_delivery_fee_discount_cents > actual_discount_cents) AS model_recommends_higher,
    ROUND(100.0 * COUNT_IF(matched_to_model_log AND model_delivery_fee_discount_cents > actual_discount_cents) / COUNT(*), 0) AS pct_of_floor_hit_orders,
    ROUND(100.0 * COUNT_IF(matched_to_model_log AND model_delivery_fee_discount_cents > actual_discount_cents) / 114600835, 2) AS pct_of_all_classic_orders
FROM joined
GROUP BY floor_status
ORDER BY 1;

-- Table 1 — Order funnel by OF tier

┌────────────┬───────────────┬──────────────┬────────────┬──────────┬─────────┬──────────────────┬─────────────────┐
│            │  Classic Rx   │   WBD/PAD    │  % with    │   Hit    │ Hit $0  │ % Hit Floor Out  │ % Hit Floor Out │
│     OF     │   Delivery    │  Discounted  │  Discount  │  $0.49   │  Floor  │ of all Classic   │   of Discount   │
│            │    Orders     │              │            │  Floor   │         │      Orders      │     Orders      │
├────────────┼───────────────┼──────────────┼────────────┼──────────┼─────────┼──────────────────┼─────────────────┤
│ ≤5 L365D   │ 25.6M         │ 16.6M        │ 64.7%      │ 0M       │ 14.2M   │ 55.6%            │ 85.9%           │
│ (floor $0) │               │              │            │          │         │                  │                 │
├────────────┼───────────────┼──────────────┼────────────┼──────────┼─────────┼──────────────────┼─────────────────┤
│ >5 L365D   │               │              │            │          │         │                  │                 │
│ (floor     │ 89.0M         │ 28.4M        │ 31.9%      │ 20.5M    │ 0M      │ 23.1%            │ 72.2%           │
│ $0.49)     │               │              │            │          │         │                  │                 │
├────────────┼───────────────┼──────────────┼────────────┼──────────┼─────────┼──────────────────┼─────────────────┤
│ ALL        │ 114.6M        │ 45.0M        │ 39.3%      │ 20.5M    │ 14.2M   │ 30.3%            │ 77.2%           │
└────────────┴───────────────┴──────────────┴────────────┴──────────┴─────────┴──────────────────┴─────────────────┘

--Note: Window 2026-06-22 to 2026-09-13 (~12 weeks), Classic (non-DashPass) restaurant delivery orders, L365D OF anchored at window start. "Hit floor" now means net DF ≤ own floor (at-or-below), not exact-match only — this is the corrected definition; it captures the floor-violation population previously excluded.

--Table 2 — Model-upside check on floor-hit orders

┌────────────┬────────────────────┬──────────────┬──────────────────────┬────────────────────┬────────────────────┐
│     OF     │    Classic Rx      │ Matched to   │ ML Recommends More   │ % Out of Floor-hit │   % Out of all     │
│            │  Delivery Orders   │    ML Log    │       Discount       │       Orders       │   Classic Orders   │
├────────────┼────────────────────┼──────────────┼──────────────────────┼────────────────────┼────────────────────┤
│ Hit $0     │ 14.2M              │ 92%          │ 12.4M                │ 87%                │ 10.83%             │
│ Floor      │                    │              │                      │                    │                    │
├────────────┼────────────────────┼──────────────┼──────────────────────┼────────────────────┼────────────────────┤
│ Hit $0.49  │ 20.5M              │ 92%          │ 14.2M                │ 69%                │ 12.42%             │
│ Floor      │                    │              │                      │                    │                    │
└────────────┴────────────────────┴──────────────┴──────────────────────┴────────────────────┴────────────────────┘
