-- Distribution of consumers by annual order-frequency band using mh_customer_authority
-- Assumptions:
-- 1) Use the latest available snapshot date
-- 2) "l365d_orders" maps to L360_ORDERS, which the table describes as the last 364 days of is_filtered_core orders
-- 3) Classic vs DashPass is based on DP_SUB_FLAG_START


create or replace table proddb.katez.cx_orders as
(SELECT
        ca.*,
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
        END AS l365d_of
    FROM proddb.mattheitz.mh_customer_authority ca
    --left join proddb.mattheitz.mh_customer_authority b on ca.creator_id 
    WHERE ca.dte = '2026-04-15'
      AND ca.days_since_first_purchase > 0
      AND acquisition_country_id = 1
)

-- L365d of
SELECT
    --x.dte AS snapshot_date,
    x.consumer_type,
    x.l365d_of,
    count(distinct x.creator_id) consumers,
    consumers / NULLIF(SUM(consumers) OVER (PARTITION BY x.consumer_type), 0) AS pct_cx,
    sum(l7_orders) weekly_orders,
    weekly_orders / NULLIF(SUM(weekly_orders) OVER (PARTITION BY x.consumer_type), 0) AS pct_orders
FROM proddb.katez.cx_orders x
group by 1,2
order by 1,2

-- L365d of>30, L365d NV Orders
SELECT
    --x.dte AS snapshot_date,
    x.consumer_type,
    case when L364_nv_orders = 0 then '1. 0 Order'
    when L364_nv_orders <=2 then '2. 1-2 Orders'
    when L364_nv_orders <=5 then '3. 3-5 Orders'
    when L364_nv_orders <=10 then '4. 6-10 Orders'
    when L364_nv_orders >10 then '5. 11+ Orders'
    end l365d_nv_of,
    count(distinct x.creator_id) consumers,
    consumers / NULLIF(SUM(consumers) OVER (PARTITION BY x.consumer_type), 0) AS pct_cx,
    sum(l7_orders) weekly_orders,
    weekly_orders / NULLIF(SUM(weekly_orders) OVER (PARTITION BY x.consumer_type), 0) AS pct_orders
FROM proddb.katez.cx_orders x
where l360_orders > 30
group by 1,2
order by 1,2
