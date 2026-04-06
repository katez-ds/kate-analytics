-- Average daily first-ever DashPass signups of any kind in the US over the last 30 complete days
SELECT
  AVG(daily_first_ever_signups) AS avg_daily_first_ever_dashpass_signups_us_last_30d
FROM
  (
    SELECT
      active_date,
      COUNT(DISTINCT consumer_id) AS daily_first_ever_signups
    FROM
      (
        SELECT
          consumer_id,
          active_date,
          country,
          subscription_event_time,
          subscription_id
        FROM
          edw.growth.fact_consumer_dashpass_signups
        QUALIFY
          ROW_NUMBER() OVER (
            PARTITION BY
              consumer_id
            ORDER BY
              subscription_event_time,
              subscription_id
          ) = 1
      ) first_signup
    WHERE
      active_date >= DATEADD(day, -30, CURRENT_DATE())
      AND active_date < CURRENT_DATE()
      AND country = 'United States'
    GROUP BY
      active_date
  ) d;

52236 * 4.7% = 2455


-- All DP signups (not new to DP)
with dp_signup as (
  select 
  SUBSCRIPTION_ID, 
  START_TIME,
  CASE WHEN is_in_intraday_trial_balance = true and is_new_subscription_date = true THEN 1 ELSE 0 END AS dashpass_trial_signup,
  CASE WHEN is_in_intraday_pay_balance = true
       and is_new_paying_subscription_date = true
       and is_direct_to_pay_date = true
       and billing_period is not null 
  THEN 1 ELSE 0 END AS dashpass_dtp_signup,
  dashpass_trial_signup + dashpass_dtp_signup AS dashpass_signup
FROM edw.consumer.fact_consumer_subscription__daily dsa
--LEFT JOIN
  --proddb.static.dashpass_annual_plan_ids b ON dsa.consumer_subscription_plan_id = b.consumer_subscription_plan_id
where is_new_subscription_date = TRUE
  and COUNTRY_ID_SUBSCRIBED_FROM = 1
  and dsa.consumer_subscription_plan_id != 10002416
  and dsa.subscription_status != 'cancelled_subscription_creation_failed'
  and dte between current_date - 30 and current_date
  ),
daily as (
select START_TIME::date signup_dt, 
sum(dashpass_trial_signup) dashpass_trial_signup,
sum(dashpass_dtp_signup) dashpass_dtp_signup,
sum(dashpass_signup) dashpass_signup
from dp_signup
group by all)
select avg(dashpass_signup)
from daily

-- L30D
DASHPASS_TRIAL_SIGNUP	DASHPASS_DTP_SIGNUP	DASHPASS_SIGNUP
2147897	828614	2976511

AVG(DASHPASS_SIGNUP)
99217

99217*4.7% = 4663


-- Current US DashPass subscribers who are either in trial
-- or within their first 3 paid months
SELECT
  COUNT(DISTINCT dsa.consumer_id) AS dashpass_subscribers_in_trial_or_first_3_paid_months_us
FROM
  edw.consumer.fact_consumer_subscription__daily dsa
  INNER JOIN edw.consumer.dimension_consumer_subscription_plan sp ON dsa.consumer_subscription_plan_id = sp.consumer_subscription_plan_id
WHERE
  dsa.dte = (
    SELECT
      MAX(dte)
    FROM
      edw.consumer.fact_consumer_subscription__daily
    WHERE
      dte <= CURRENT_DATE()
  )
  AND dsa.country_id_subscribed_from = 1
  AND sp.plan_type = 'DASHPASS'
  AND COALESCE(dsa.subscription_status, '') <> 'cancelled_subscription_creation_failed'
  AND dsa.consumer_subscription_plan_id <> 10002416
  AND (
    (
      dsa.dynamic_subscription_status = 'active_trial'
      AND dsa.is_in_trial_balance = TRUE
    )
    OR (
      dsa.dynamic_subscription_status = 'active_paid'
      AND dsa.is_in_paid_balance = TRUE
      AND dsa.billing_period IS NOT NULL
      AND dsa.first_subscription_successful_charge_date IS NOT NULL
      AND dsa.dte < DATEADD(
        month,
        3,
        dsa.first_subscription_successful_charge_date
      )
    )
  );
7213040

-- US DashPass subscribers as of 2026-03-30
-- Includes consumers who are either:
-- 1) currently in trial, or
-- 2) currently active paid and in their first 3 paid months lifetime
-- "first 3 paid months (lifetime)" is interpreted as monthly_period IN (0, 1, 2)
SELECT
  COUNT(DISTINCT dsa.consumer_id) AS dashpass_subscribers_in_trial_or_first_3_paid_months_us
FROM
  edw.consumer.fact_consumer_subscription__daily AS dsa
  INNER JOIN edw.consumer.dimension_consumer_subscription_plan AS sp ON dsa.consumer_subscription_plan_id = sp.consumer_subscription_plan_id
WHERE
  dsa.dte = '2026-03-30'
  AND dsa.country_id_subscribed_from = 1
  AND sp.plan_type = 'DASHPASS'
  AND COALESCE(dsa.subscription_status, '') <> 'cancelled_subscription_creation_failed'
  AND (
    (
      dsa.dynamic_subscription_status = 'active_trial'
      AND dsa.is_in_trial_balance = TRUE
    )
    OR (
      dsa.dynamic_subscription_status = 'active_paid'
      AND dsa.is_in_paid_balance = TRUE
      AND dsa.monthly_period IN (0, 1, 2)
    )
  );

7148174
7148174*4.7% = 335964


-- In first 90 days of DP
with dp_tenure as
(
select consumer_id, 
count(distinct dte) as tenure
from edw.consumer.fact_consumer_subscription__daily dp 
where (
    (is_in_paid_balance = true and billing_period is not null) -- paid
    or is_in_trial_balance = true -- trial
    )
group by all)
select count(distinct consumer_id) 
from dp_tenure
where tenure<90 


36266937

-- Average daily US DashPass signups over the last 30 complete days
SELECT
  AVG(daily_signups) AS avg_daily_us_dashpass_signups_last_30d
FROM
  (
    SELECT
      active_date,
      COUNT(DISTINCT subscription_id) AS daily_signups
    FROM
      edw.growth.fact_consumer_dashpass_signups
    WHERE
      active_date >= DATEADD(day, -30, CURRENT_DATE())
      AND active_date < CURRENT_DATE()
      AND country = 'United States'
    GROUP BY
      1
  ) s

80256

-- Distinct US consumers with a DashPass signup event in the last 30/60/90 days
-- who are still active on DashPass as of the latest available snapshot on or before today
SELECT
  COUNT(DISTINCT CASE
    WHEN s.active_date BETWEEN DATEADD(day, -29, CURRENT_DATE()) AND CURRENT_DATE()
    THEN s.consumer_id
  END) AS active_dp_users_signed_up_last_30d_us,
  COUNT(DISTINCT CASE
    WHEN s.active_date BETWEEN DATEADD(day, -59, CURRENT_DATE()) AND CURRENT_DATE()
    THEN s.consumer_id
  END) AS active_dp_users_signed_up_last_60d_us,
  COUNT(DISTINCT CASE
    WHEN s.active_date BETWEEN DATEADD(day, -89, CURRENT_DATE()) AND CURRENT_DATE()
    THEN s.consumer_id
  END) AS active_dp_users_signed_up_last_90d_us
FROM edw.growth.fact_consumer_dashpass_signups AS s
INNER JOIN (
  SELECT DISTINCT
    dsa.consumer_id
  FROM edw.consumer.fact_consumer_subscription__daily AS dsa
  INNER JOIN edw.consumer.dimension_consumer_subscription_plan AS sp
    ON dsa.consumer_subscription_plan_id = sp.consumer_subscription_plan_id
  WHERE dsa.dte = (
      SELECT MAX(dte)
      FROM edw.consumer.fact_consumer_subscription__daily
      WHERE dte <= CURRENT_DATE()
    )
    AND dsa.country_id_subscribed_from = 1
    AND sp.plan_type = 'DASHPASS'
    AND (
      dsa.is_in_trial_balance = TRUE
      OR dsa.is_in_paid_balance = TRUE
    )
    AND COALESCE(dsa.subscription_status, '') <> 'cancelled_subscription_creation_failed'
) AS active_dp
  ON s.consumer_id = active_dp.consumer_id
WHERE s.country_id = 1
  AND s.active_date BETWEEN DATEADD(day, -89, CURRENT_DATE()) AND CURRENT_DATE();

ACTIVE_DP_USERS_SIGNED_UP_LAST_30D_US	ACTIVE_DP_USERS_SIGNED_UP_LAST_60D_US	ACTIVE_DP_USERS_SIGNED_UP_LAST_90D_US
2030858	3855676	5114662


   
-- new DP sigups orders
with new_dp as
(SELECT
  dsa.consumer_id
FROM
  edw.consumer.fact_consumer_subscription__daily AS dsa
  INNER JOIN edw.consumer.dimension_consumer_subscription_plan AS sp ON dsa.consumer_subscription_plan_id = sp.consumer_subscription_plan_id
WHERE
  dsa.dte = '2026-03-21'
  AND dsa.country_id_subscribed_from = 1
  AND sp.plan_type = 'DASHPASS'
  AND COALESCE(dsa.subscription_status, '') <> 'cancelled_subscription_creation_failed'
  AND (
    (
      dsa.dynamic_subscription_status = 'active_trial'
      AND dsa.is_in_trial_balance = TRUE
    )
    OR (
      dsa.dynamic_subscription_status = 'active_paid'
      AND dsa.is_in_paid_balance = TRUE
      AND dsa.monthly_period IN (0, 1, 2)
    )
  )
group by 1)
,
dp_orders
AS (
    SELECT 
        --c.first_date_of_week,
        --dd.country_id,
        dd.delivery_id,
        dd.creator_id,
        dd.service_fee/100.0 AS gross_service_fee,
        dd.subtotal/100.0 AS subtotal, -- subtotal in dollars
        dd.gov/100.0 AS aov, -- average order value
        COALESCE(fda.variable_profit_ex_alloc, fda.variable_profit + fda.payment_to_customers, fda.variable_profit) AS unit_vp
        --dd.is_consumer_pickup::INT AS pickup, -- pickup flag
    FROM proddb.public.dimension_deliveries dd
    --join proddb.GLAUBERVASCONCELOS.DIMENSION_DATES c
    --on dd.created_at::date = c.calendar_date::date
    LEFT JOIN proddb.public.fact_delivery_allocation fda ON dd.delivery_id = fda.delivery_id
    LEFT JOIN proddb.public.fact_delivery_distances fdd ON dd.delivery_id = fdd.delivery_id
    LEFT JOIN proddb.public.fact_core_delivery_metrics fcdm ON fcdm.delivery_id = dd.delivery_id
    LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv 
        ON dd.store_id = nv.store_id AND nv.is_filtered_mp_vertical = 1
    LEFT JOIN proddb.static.df_sf_promo_discount_delivery_level dfp -- fyi only populated from 7/1/2023 onwards. Wiki here: https://doordash.atlassian.net/wiki/spaces/DATA/pages/4476961078/DF+SF+Promo+Discount+Static+Table
        ON dd.delivery_id = dfp.delivery_id
    LEFT JOIN public.dimension_store_ext x ON dd.store_id = x.store_id
    WHERE dd.is_filtered_core = TRUE
        AND dd.is_consumer_pickup = FALSE -- optional to exclude pickup orders
        AND dd.fulfillment_type NOT IN ('dine_in', 'shipping', 'merchant_fleet', 'virtual') -- excluding dine-in, shipping, merchant fleet, and virtual orders (giftcards)
        AND dd.is_from_store_to_us = FALSE -- excluding store-to-us orders
        -- AND dd.is_bundle_order = FALSE -- excluding bundle orders -- an optional column to filter out DoubleDash
        AND nv.business_line IS NULL -- excluding non-restaurant orders -- an optional column to exclude NV
        AND dd.country_id = 1 -- US only 
        AND dd.created_at BETWEEN '2026-03-21' AND '2026-03-27' -- date range
        AND dd.is_subscribed_consumer = TRUE
group by all
)

select 
count(distinct delivery_id) orders,
sum(gross_service_fee)*1.0000/count(distinct delivery_id) avg_gross_sf,
sum(subtotal)*1.0000/count(distinct delivery_id) avg_subtotal,
sum(aov)*1.0000/count(distinct delivery_id) avg_gov,
sum(unit_vp)*1.0000/count(distinct delivery_id) avg_UE
from dp_orders a
join new_dp b on b.consumer_id = a.creator_id

ORDERS	AVG_GROSS_SF	AVG_SUBTOTAL	AVG_GOV	AVG_UE
4893883	1.717414296582	25.744461214132	34.532057247793	2.935501573639


Limiting to active subscribers who are in their trial and PM 1-3 (3/21 snapshot) and looking at their DP Rx Delivery orders in the following week:
4.8M orders (20% of all DP) with Avg. Gross SF to be $1.7. If apply to 4.7% then one arm is 230K orders. 
- $1 depth per order on 20% DP --> 20c 
- 50c depth per order on 20% DP --> 10c

