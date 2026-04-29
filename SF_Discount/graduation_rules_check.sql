-- DP Trial or DP PM 1-4 (any signup period)
/*
create or replace table proddb.katez.new_dp as
(SELECT
  dsa.consumer_id, dsa.dte
FROM
  edw.consumer.fact_consumer_subscription__daily AS dsa
  INNER JOIN edw.consumer.dimension_consumer_subscription_plan AS sp ON dsa.consumer_subscription_plan_id = sp.consumer_subscription_plan_id
WHERE
  dsa.dte between '2026-03-21' and '2026-03-27'
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
      AND dsa.monthly_period IN (0,1,2,3)
    )
  )
group by all)
*/

create or replace table proddb.katez.new_dp
as
(
WITH params AS (
  SELECT
    '2026-03-21' AS as_of_date,
    DATEADD(year, -3, CURRENT_DATE) AS lookback_start_date
),
daily AS (
  SELECT
    d.consumer_id,
    d.user_id,
    d.subscription_id,
    d.dte,
    d.is_in_paid_balance,
    d.is_partner_plan,
    d.is_in_trial_period,
    d.dynamic_subscription_status
  FROM edw.consumer.fact_consumer_subscription__daily d
  JOIN params p
    ON d.dte BETWEEN p.lookback_start_date AND p.as_of_date
),
/* Cumulative day counts across all historical stints */
features AS (
  SELECT
    consumer_id,
    COUNT(DISTINCT IFF(
      is_in_paid_balance = TRUE
      OR dynamic_subscription_status ILIKE 'active_paid%',
      dte, NULL
    )) AS paid_days,
    COUNT(DISTINCT IFF(
      is_partner_plan = TRUE
      AND (dynamic_subscription_status ILIKE 'active%'
           OR dynamic_subscription_status ILIKE '%free_subscription%'),
      dte, NULL
    )) AS partner_days,
    COUNT(DISTINCT IFF(
      is_in_trial_period = TRUE
      OR dynamic_subscription_status ILIKE 'trial%',
      dte, NULL
    )) AS trial_days
  FROM daily
  GROUP BY 1
),
/* Current plan state: what is the consumer on TODAY? */
current_state AS (
  SELECT
    consumer_id, 
    MAX(IFF(
      is_in_paid_balance = TRUE
      OR dynamic_subscription_status ILIKE 'active_paid%',
      TRUE, FALSE
    )) AS is_dashpass_paid,
    MAX(IFF(
      is_partner_plan = TRUE
      AND (dynamic_subscription_status ILIKE 'active%'
           OR dynamic_subscription_status ILIKE '%free_subscription%'),
      TRUE, FALSE
    )) AS is_dashpass_partner,
    MAX(IFF(
      is_in_trial_period = TRUE
      OR dynamic_subscription_status ILIKE 'trial%',
      TRUE, FALSE
    )) AS is_trial_active
  FROM daily
  WHERE dte = '2026-03-21'
  GROUP BY all
)
SELECT
  f.consumer_id,
  /* Day counts */
  f.paid_days,
  f.partner_days,
  f.trial_days,
  /* Current plan state */
  COALESCE(cs.is_dashpass_paid, FALSE)    AS is_dashpass_paid,
  COALESCE(cs.is_dashpass_partner, FALSE) AS is_dashpass_partner,
  COALESCE(cs.is_trial_active, FALSE)     AS is_trial_active,
  /* Component eligibility checks */
  (f.paid_days <= 120)    AS is_paid_eligible,
  (f.partner_days <= 120) AS is_partner_eligible,
  /* Two-path eligibility:
     Path 1 (paid/partner): is_paid_eligible AND is_partner_eligible
     Path 2 (classic/other): is_trial_active AND is_paid_eligible AND is_partner_eligible
  */
  CASE
    WHEN COALESCE(cs.is_dashpass_paid, FALSE)
      OR COALESCE(cs.is_dashpass_partner, FALSE)
      THEN (f.paid_days <= 120) AND (f.partner_days <= 120)
    WHEN COALESCE(cs.is_trial_active, FALSE)
      THEN (f.paid_days <= 120) AND (f.partner_days <= 120)
    ELSE FALSE  -- no active DashPass subscription
  END AS is_eligible
FROM features f
LEFT JOIN current_state cs
  ON f.consumer_id = cs.consumer_id
  where is_eligible = TRUE
  )

  
create or replace table proddb.katez.dp_orders_7d
AS (
    SELECT 
        --c.first_date_of_week,
        --dd.country_id,
        dd.created_at::date order_dt,
        dd.delivery_id,
        dd.creator_id,
        dd.store_id
        --dd.service_fee/100.0 AS gross_service_fee,
        --dd.subtotal/100.0 AS subtotal, -- subtotal in dollars
        --dd.gov/100.0 AS aov, -- average order value
        --COALESCE(fda.variable_profit_ex_alloc, fda.variable_profit + fda.payment_to_customers, fda.variable_profit) AS unit_vp
        --dd.is_consumer_pickup::INT AS pickup, -- pickup flag
    FROM proddb.public.dimension_deliveries dd
    --join proddb.GLAUBERVASCONCELOS.DIMENSION_DATES c
    --on dd.created_at::date = c.calendar_date::date
    --LEFT JOIN proddb.public.fact_delivery_allocation fda ON dd.delivery_id = fda.delivery_id
    --LEFT JOIN proddb.public.fact_delivery_distances fdd ON dd.delivery_id = fdd.delivery_id
    --LEFT JOIN proddb.public.fact_core_delivery_metrics fcdm ON fcdm.delivery_id = dd.delivery_id
    LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv 
        ON dd.store_id = nv.store_id AND nv.is_filtered_mp_vertical = 1
    --LEFT JOIN proddb.static.df_sf_promo_discount_delivery_level dfp -- fyi only populated from 7/1/2023 onwards. Wiki here: https://doordash.atlassian.net/wiki/spaces/DATA/pages/4476961078/DF+SF+Promo+Discount+Static+Table
        --ON dd.delivery_id = dfp.delivery_id
    --LEFT JOIN public.dimension_store_ext x ON dd.store_id = x.store_id
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

create or replace table proddb.katez.orders_90d
AS (
    SELECT 
        dd.delivery_id,
        dd.creator_id,
        dd.created_at order_dt,
        dd.store_id,
        dd.is_subscribed_consumer,
        dd.fee-dd.delivery_fee+dd.service_fee_no_dscnt-dd.service_fee DP_savings
    FROM proddb.public.dimension_deliveries dd
    WHERE dd.is_filtered_core = TRUE
        AND dd.created_at::date BETWEEN date('2026-03-21') - 90 AND date('2026-03-27')--'2026-03-21' AND '2026-03-27' -- date range
        --AND dd.is_subscribed_consumer = TRUE
group by all
)
/*
create or replace table proddb.katez.tags
as (
select 
a.delivery_id,a.creator_id,a.order_dt,
case when b.consumer_id is null then 0 else 1 end eligibility_flag,
count(distinct case when c.is_subscribed_consumer then c.delivery_id end) L90D_DP_OF,
count(distinct case when a.store_id = c.store_id then c.delivery_id end) L90D_mx_orders,
sum(case when c.order_dt between a.order_dt::date - 28 and a.order_dt::date -1 then c.dp_savings end) L28D_DP_savings
from proddb.katez.dp_orders_7d a
left join proddb.katez.new_dp b on b.consumer_id = a.creator_id
    and a.order_dt = b.dte
left join proddb.katez.orders_90d c on b.consumer_id = c.creator_id
    and c.order_dt between a.order_dt::date - 90 and a.order_dt::date -1
--left join orders_90d d on b.consumer_id = d.creator_id
    --and b.store_id = d.store_id 
    --and b.order_dt between a.order_dt - 90 and a.order_dt -1
group by all
)
*/

create or replace table proddb.katez.tags2
as (
select 
a.delivery_id,a.creator_id,a.order_dt,
case when b.consumer_id is null then 0 else 1 end eligibility_flag,
count(distinct case when c.is_subscribed_consumer then c.delivery_id end) L90D_DP_OF,
count(distinct case when a.store_id = c.store_id then c.delivery_id end) L90D_mx_orders,
sum(case when c.order_dt between a.order_dt::date - 28 and a.order_dt::date -1 then c.dp_savings end) L28D_DP_savings,
sum(case when c.order_dt between a.order_dt::date - 30 and a.order_dt::date -1 then c.dp_savings end) L30D_DP_savings,
sum(case when c.order_dt between a.order_dt::date - 60 and a.order_dt::date - 31 then c.dp_savings end) L30Dto60D_DP_savings,
sum(case when c.order_dt between a.order_dt::date - 90 and a.order_dt::date - 61 then c.dp_savings end) L60Dto90D_DP_savings
from proddb.katez.dp_orders_7d a
left join proddb.katez.new_dp b on b.consumer_id = a.creator_id
    --and a.order_dt = b.dte
left join proddb.katez.orders_90d c on b.consumer_id = c.creator_id
    and c.order_dt between a.order_dt::date - 90 and a.order_dt::date -1
--left join orders_90d d on b.consumer_id = d.creator_id
    --and b.store_id = d.store_id 
    --and b.order_dt between a.order_dt - 90 and a.order_dt -1
group by all
)

select 
case when L28D_DP_savings<2000 then 1 else 0 end L28D_DP_savings_under_20,
count(distinct delivery_id) eligible_orders
from proddb.katez.tags2
  where eligibility_flag = 1 and L90D_DP_OF<20 and L90D_mx_orders<5
  group by 1
  order by 1

select 
case when L30D_DP_savings>999 and L30Dto60D_DP_savings>999 and L60Dto90D_DP_savings>999 then 1 else 0 end consecutive_3m_net_saver,
count(distinct delivery_id) eligible_orders
from proddb.katez.tags2
  where eligibility_flag = 1 and L90D_DP_OF<20 and L90D_mx_orders<5
  group by 1
  order by 1

CONSECUTIVE_3M_NET_SAVER	ELIGIBLE_ORDERS
0	1910507
1	168684
/*
select 
eligibility_flag, 
case when L90D_DP_OF<20 then 1 else 0 end L90D_DP_OF_under_20,
case when L90D_mx_orders<5 then 1 else 0 end L90D_mx_orders_under_5,
case when L28D_DP_savings<2000 then 1 else 0 end L28D_DP_savings_under_20,
count(distinct delivery_id) orders,
count(distinct creator_id) users
from proddb.katez.tags
group by all
order by 1,2,3,4

select 
eligibility_flag, 
case when L90D_DP_OF<15 then 1 else 0 end L90D_DP_OF_under_15,
case when L90D_mx_orders<5 then 1 else 0 end L90D_mx_orders_under_5,
--case when L28D_DP_savings<2000 then 1 else 0 end L28D_DP_savings_under_20,
count(distinct delivery_id) orders,
count(distinct creator_id) users
from proddb.katez.tags
group by all
order by 1,2,3

*/


-- Lifetime DP Tenure

WITH params AS (                                                                                                                                                                                                                                                                          
    SELECT                                                                                                                                                                                                                                                                                  
      CURRENT_DATE AS as_of_date,                                                                                                                                                                                                                                                           
      -- Pick a lookback that truly captures "across stints".
      -- Change to a fixed early date if you need true lifetime.                                                                                                                                                                                                                            
      DATEADD(year, -3, CURRENT_DATE) AS lookback_start_date                                                                                                                                                                                                                                
  ),                                                                                                                                                                                                                                                                                        
  daily AS (                                                                                                                                                                                                                                                                                
    SELECT                                                                                                                                                                                                                                                                                  
      d.consumer_id,                                                                                                                                                                                                                                                                        
      d.user_id,                                                                                                                                                                                                                                                                            
      d.subscription_id,                                                                                                                                                                                                                                                                    
      d.dte,                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                            
      d.is_in_paid_balance,                                                                                                                                                                                                                                                                 
      d.is_partner_plan,                                                                                                                                                                                                                                                                    
      d.is_in_trial_period,                                                                                                                                                                                                                                                                 
      d.dynamic_subscription_status                                                                                                                                                                                                                                                         
    FROM edw.consumer.fact_consumer_subscription__daily d                                                                                                                                                                                                                                   
    JOIN params p                                                                                                                                                                                                                                                                           
      ON d.dte BETWEEN p.lookback_start_date AND p.as_of_date                                                                                                                                                                                                                               
  ),                                                                                                                                                                                                                                                                                        
  features AS (                                                                                                                                                                                                                                                                             
    SELECT                                                                                                                                                                                                                                                                                  
      consumer_id,                                                                                                                                                                                                                                                                          
                                                                                                                                                                                                                                                                                            
      /* cumulative PAID days across all stints */                                                                                                                                                                                                                                          
      COUNT(DISTINCT IFF(                                                                                                                                                                                                                                                                   
        is_in_paid_balance = TRUE                                                                                                                                                                                                                                                           
        OR dynamic_subscription_status ILIKE 'active_paid%',                                                                                                                                                                                                                                
        dte, NULL                                                                                                                                                                                                                                                                           
      )) AS paid_days_on_dashpass_cume,                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                            
      /* cumulative PARTNER days across all stints */                                                                                                                                                                                                                                       
      COUNT(DISTINCT IFF(                                                                                                                                                                                                                                                                   
        is_partner_plan = TRUE                                                                                                                                                                                                                                                              
        AND (dynamic_subscription_status ILIKE 'active%' OR dynamic_subscription_status ILIKE '%free_subscription%'),                                                                                                                                                                       
        dte, NULL                                                                                                                                                                                                                                                                           
      )) AS partner_days_on_dashpass_cume,                                                                                                                                                                                                                                                  
                                                                                                                                                                                                                                                                                            
      /* optional: cumulative TRIAL days (eligibility still always true per your rule) */                                                                                                                                                                                                   
      COUNT(DISTINCT IFF(                                                                                                                                                                                                                                                                   
        is_in_trial_period = TRUE                                                                                                                                                                                                                                                           
        OR dynamic_subscription_status ILIKE 'trial%',                                                                                                                                                                                                                                      
        dte, NULL                                                                                                                                                                                                                                                                           
      )) AS trial_days_on_dashpass_cume                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                            
    FROM daily                                                                                                                                                                                                                                                                              
    GROUP BY 1                                                                                                                                                                                                                                                                              
  )                                                                                                                                                                                                                                                                                         
  SELECT                                                                                                                                                                                                                                                                                    
    consumer_id,                                                                                                                                                                                                                                                                            
    paid_days_on_dashpass_cume,
    partner_days_on_dashpass_cume,
    trial_days_on_dashpass_cume,
                                                                                                                                                                                                                                                                                            
    (paid_days_on_dashpass_cume <= 120)    AS is_paid_eligible,
    (partner_days_on_dashpass_cume <= 120) AS is_partner_eligible,                                                                                                                                                                                                                          
    TRUE AS is_trial_eligible                                                                                                                                                                                                                                                             
  FROM features;




select 
eligibility_flag, 
case when L90D_DP_OF<15 then 1 else 0 end L90D_DP_OF_under_15,
case when L90D_mx_orders<5 then 1 else 0 end L90D_mx_orders_under_5,
--case when L28D_DP_savings<2000 then 1 else 0 end L28D_DP_savings_under_20,
count(distinct delivery_id) orders,
count(distinct creator_id) users
from proddb.katez.tags
group by all
order by 1,2,3
