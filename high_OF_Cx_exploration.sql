, case
          when date_part('hour',convert_timezone('UTC',dd.timezone,dd.QUOTED_DELIVERY_TIME)) <5 THEN 'early_morning'
          when date_part('hour',convert_timezone('UTC',dd.timezone,dd.QUOTED_DELIVERY_TIME)) between 5 and 10 THEN 'breakfast'
          when  date_part('hour',convert_timezone('UTC',dd.timezone,dd.QUOTED_DELIVERY_TIME)) between 11 and 13 then 'lunch'
          when date_part('hour',convert_timezone('UTC',dd.timezone,dd.QUOTED_DELIVERY_TIME)) between 14 and 16 then 'snack'
          when date_part('hour',convert_timezone('UTC',dd.timezone,dd.QUOTED_DELIVERY_TIME)) between 17 and 20 then 'dinner'
          when date_part('hour',convert_timezone('UTC',dd.timezone,dd.QUOTED_DELIVERY_TIME)) between 21 and 23 then 'latenight' else null
        end as daypart



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

-- L365 of>30, OF momentum change
        
SELECT
    --x.dte AS snapshot_date,
    x.consumer_type,
    case when L360_orders/360*7 = L28_orders/4 then 'OF Same'
    when L360_orders/360*7 < L28_orders/4 then 'OF Increase'
    when L360_orders/360*7 > L28_orders/4 then 'OF Drop'
    end of_change,
    count(distinct x.creator_id) consumers,
    consumers / NULLIF(SUM(consumers) OVER (PARTITION BY x.consumer_type), 0) AS pct_cx,
    sum(l7_orders) weekly_orders,
    weekly_orders / NULLIF(SUM(weekly_orders) OVER (PARTITION BY x.consumer_type), 0) AS pct_orders
FROM proddb.katez.cx_orders x
where l360_orders > 30
group by 1,2
order by 1,2


-- L365d of>30, L365d NV Orders
/*
L360_ALCOHOL_ORDERS
L360_DASHMART_ORDERS
L364_RETAIL_ORDERS
L360_CONVENIENCE_ORDERS
L360_grocery_orders
*/
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

-- By Price Sensitivity (PSMv3)

SELECT
    --x.dte AS snapshot_date,
    x.consumer_type,
    v3_sensitivity_cohort,
    count(distinct x.creator_id) consumers,
    consumers / NULLIF(SUM(consumers) OVER (PARTITION BY x.consumer_type), 0) AS pct_cx,
    sum(l7_orders) weekly_orders,
    weekly_orders / NULLIF(SUM(weekly_orders) OVER (PARTITION BY x.consumer_type), 0) AS pct_orders
FROM proddb.katez.cx_orders x
left join proddb.ml.cx_sensitivity_v3 psm
    on x.creator_id = psm.consumer_id and active_date = date'2026-04-15'
where l360_orders > 30
group by 1,2
order by 1,2

-- By Order Time
with orders AS (
    SELECT
        creator_id,
        --delivery_id,
        --created_at::date order_dt,
        CASE
            WHEN DAYOFWEEKISO(CONVERT_TIMEZONE('UTC', timezone, QUOTED_DELIVERY_TIME)) IN (6, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS weekpart,
        CASE
            WHEN HOUR(convert_timezone('UTC', timezone, QUOTED_DELIVERY_TIME)) BETWEEN 5 AND 10 THEN 'Breakfast'
            WHEN HOUR(convert_timezone('UTC', timezone, QUOTED_DELIVERY_TIME)) BETWEEN 11 AND 15 THEN 'Lunch'
            WHEN HOUR(convert_timezone('UTC', timezone, QUOTED_DELIVERY_TIME)) BETWEEN 16 AND 21 THEN 'Dinner'
            ELSE 'Other'
        END AS mealpart,
        count(distinct delivery_id) orders
    FROM proddb.public.dimension_deliveries 
where 1=1
       AND is_filtered_core = TRUE
       AND created_at::date BETWEEN '2025-04-16' and '2026-04-15'
group by all
),
cx_pattern AS (
SELECT
        creator_id,
        sum(case when weekpart = 'Weekday' then orders else 0 end) *100 / sum(orders) share_of_weekday_orders,
        sum(case when weekpart = 'Weekend' then orders else 0 end) *100 / sum(orders) share_of_weekend_orders,
        sum(case when mealpart = 'Lunch' then orders else 0 end) *100 / sum(orders) share_of_lunch_orders,
        sum(case when mealpart = 'Dinner' then orders else 0 end) *100 / sum(orders) share_of_dinner_orders
    FROM orders
group by all
)

SELECT
    x.consumer_type,
       
    case when share_of_weekend_orders = 0 then '1. 0'
    when share_of_weekend_orders <= 25 then '2. 0-25%'
    when share_of_weekend_orders <= 50 then '3. 25%-50%'
    when share_of_weekend_orders <= 75 then '4. 50%-75%'
    when share_of_weekend_orders <= 95 then '5. 75%-95%'
    when share_of_weekend_orders < 100 then '5. 95%-99%'
    when share_of_weekend_orders = 100 then '6. 100%'
    end weekend_share,
 /*
    case when share_of_lunch_orders = 0 then '1. 0'
    when share_of_lunch_orders <= 25 then '2. 0-25%'
    when share_of_lunch_orders <= 50 then '3. 25%-50%'
    when share_of_lunch_orders <= 75 then '4. 50%-75%'
    when share_of_lunch_orders <= 95 then '5. 75%-95%'
    when share_of_lunch_orders < 100 then '5. 95%-99%'
    when share_of_lunch_orders = 100 then '6. 100%'
    end lunch_share,
  
    case when share_of_dinner_orders = 0 then '1. 0'
    when share_of_dinner_orders <= 25 then '2. 0-25%'
    when share_of_dinner_orders <= 50 then '3. 25%-50%'
    when share_of_dinner_orders <= 75 then '4. 50%-75%'
    when share_of_dinner_orders <= 95 then '5. 75%-95%'
    when share_of_dinner_orders < 100 then '5. 95%-99%'
    when share_of_dinner_orders = 100 then '6. 100%'
    end dinner_share,
      */
    count(distinct x.creator_id) consumers,
    consumers / NULLIF(SUM(consumers) OVER (PARTITION BY x.consumer_type), 0) AS pct_cx,
    sum(l7_orders) weekly_orders,
    weekly_orders / NULLIF(SUM(weekly_orders) OVER (PARTITION BY x.consumer_type), 0) AS pct_orders
FROM proddb.katez.cx_orders x
left join cx_pattern b
    on x.creator_id = b.creator_id
where l360_orders > 30
group by 1,2
order by 1,2


-- High OF (L360 orders > 30 on mh snapshot): L90d daypart behavior vs Classic / DashPass
-- Mirrors assumptions in https://github.com/katez-ds/kate-analytics/blob/main/high_OF_Cx_exploration.sql
--   - mh_customer_authority: snapshot dte, acquisition_country_id = 1, days_since_first_purchase > 0
--   - High OF: l360_orders > 30
--   - consumer_type: dp_sub_flag_start = 1 -> DashPass else Classic
--   - Daypart hour bands (local TZ from dimension_deliveries.timezone on QUOTED_DELIVERY_TIME)
-- L90d order window: last 90 calendar days ending on snapshot_dte (inclusive), using created_at::date like the sample "By Order Time" block

WITH params AS (
    SELECT DATE '2026-05-01' AS snapshot_dte  -- set to your mh_customer_authority.dte
),

base_cx AS (
    SELECT
        ca.creator_id,
        CASE
            WHEN ca.dp_sub_flag_start = 1 THEN 'DashPass'
            ELSE 'Classic'
        END AS consumer_type
    FROM proddb.mattheitz.mh_customer_authority ca
    CROSS JOIN params p
    WHERE ca.dte = p.snapshot_dte
      AND ca.days_since_first_purchase > 0
      AND ca.acquisition_country_id = 1
      AND ca.l360_orders > 30
),

totals AS (
    SELECT
        consumer_type,
        COUNT(DISTINCT creator_id) AS total_high_of_users
    FROM base_cx
    GROUP BY 1
),

dayparts (daypart) AS (
    SELECT column1
    FROM VALUES
        ('early_morning'),
        ('breakfast'),
        ('lunch'),
        ('snack'),
        ('dinner'),
        ('latenight')
),

orders_l90d AS (
    SELECT
        dd.creator_id,
        CASE
            WHEN DATE_PART(
                'hour',
                CONVERT_TIMEZONE('UTC', dd.timezone, dd.QUOTED_DELIVERY_TIME)
            ) < 5 THEN 'early_morning'
            WHEN DATE_PART(
                'hour',
                CONVERT_TIMEZONE('UTC', dd.timezone, dd.QUOTED_DELIVERY_TIME)
            ) BETWEEN 5 AND 10 THEN 'breakfast'
            WHEN DATE_PART(
                'hour',
                CONVERT_TIMEZONE('UTC', dd.timezone, dd.QUOTED_DELIVERY_TIME)
            ) BETWEEN 11 AND 13 THEN 'lunch'
            WHEN DATE_PART(
                'hour',
                CONVERT_TIMEZONE('UTC', dd.timezone, dd.QUOTED_DELIVERY_TIME)
            ) BETWEEN 14 AND 16 THEN 'snack'
            WHEN DATE_PART(
                'hour',
                CONVERT_TIMEZONE('UTC', dd.timezone, dd.QUOTED_DELIVERY_TIME)
            ) BETWEEN 17 AND 20 THEN 'dinner'
            WHEN DATE_PART(
                'hour',
                CONVERT_TIMEZONE('UTC', dd.timezone, dd.QUOTED_DELIVERY_TIME)
            ) BETWEEN 21 AND 23 THEN 'latenight'
        END AS daypart,
        COUNT(DISTINCT dd.delivery_id) AS n_orders_in_daypart
    FROM proddb.public.dimension_deliveries dd
    CROSS JOIN params p
    WHERE dd.is_filtered_core = TRUE
      AND dd.created_at::DATE BETWEEN DATEADD('day', -89, p.snapshot_dte) AND p.snapshot_dte
      AND dd.QUOTED_DELIVERY_TIME IS NOT NULL
      AND dd.timezone IS NOT NULL
    GROUP BY 1, 2
    HAVING daypart IS NOT NULL
),

user_daypart AS (
    SELECT
        b.creator_id,
        b.consumer_type,
        d.daypart,
        COALESCE(o.n_orders_in_daypart, 0) AS n_orders_in_daypart
    FROM base_cx b
    CROSS JOIN dayparts d
    LEFT JOIN orders_l90d o
        ON b.creator_id = o.creator_id
       AND d.daypart = o.daypart
)

SELECT
    u.consumer_type,
    u.daypart,
    SUM(IFF(u.n_orders_in_daypart = 0, 1, 0)) AS users_never_this_daypart_l90d,
    SUM(IFF(u.n_orders_in_daypart BETWEEN 1 AND 2, 1, 0)) AS users_1_to_2_this_daypart_l90d,
    SUM(IFF(u.n_orders_in_daypart >= 3, 1, 0)) AS users_3plus_this_daypart_l90d,
    t.total_high_of_users,
    SUM(IFF(u.n_orders_in_daypart = 0, 1, 0))
        / NULLIF(t.total_high_of_users, 0) AS pct_of_consumer_type_never,
    SUM(IFF(u.n_orders_in_daypart BETWEEN 1 AND 2, 1, 0))
        / NULLIF(t.total_high_of_users, 0) AS pct_of_consumer_type_1_to_2
FROM user_daypart u
JOIN totals t
    ON u.consumer_type = t.consumer_type
GROUP BY
    u.consumer_type,
    u.daypart,
    t.total_high_of_users
ORDER BY
    u.consumer_type,
    CASE u.daypart
        WHEN 'early_morning' THEN 1
        WHEN 'breakfast' THEN 2
        WHEN 'lunch' THEN 3
        WHEN 'snack' THEN 4
        WHEN 'dinner' THEN 5
        WHEN 'latenight' THEN 6
    END;

