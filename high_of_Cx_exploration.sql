-- Distribution of consumers by annual order-frequency band using mh_customer_authority
-- Assumptions:
-- 1) Use the latest available snapshot date
-- 2) "l365d_orders" maps to L360_ORDERS, which the table describes as the last 364 days of is_filtered_core orders
-- 3) Classic vs DashPass is based on DP_SUB_FLAG_START


SELECT
    --x.dte AS snapshot_date,
    x.consumer_type,
    x.order_frequency_band,
    x.consumers,
    x.consumers / NULLIF(SUM(x.consumers) OVER (PARTITION BY x.dte, x.consumer_type), 0) AS pct_within_consumer_type
FROM (
    SELECT
        ca.dte,
        CASE
            WHEN ca.dp_sub_flag_start = 1 THEN 'DashPass'
            ELSE 'Classic'
        END AS consumer_type,
        CASE
            WHEN ca.l360_orders = 0 THEN '1. 0 Order'
            WHEN ca.l360_orders <=4 THEN '2. 1-4 Orders'
            WHEN ca.l360_orders <=15 THEN '3. 5-15 Orders'
            WHEN ca.l360_orders <=30 THEN '4. 16-30 Orders'
            WHEN ca.l360_orders <=52 THEN '5. 31-52 Orders'
            WHEN ca.l360_orders >= 53 THEN '6. 53+ Orders'
        END AS order_frequency_band,
        SUM(ca.unique_customer) AS consumers
    FROM proddb.mattheitz.mh_customer_authority ca
    WHERE ca.dte = '2026-04-15'
      AND ca.days_since_first_purchase > 0
      AND acquisition_country_id = 1
    GROUP BY 1, 2, 3
) x
ORDER BY 1,2
