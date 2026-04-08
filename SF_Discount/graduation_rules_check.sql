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
      AND dsa.monthly_period IN (0, 1, 2)
    )
  )
group by all)

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

select 
eligibility_flag, 
case when L90D_DP_OF<20 then 1 else 0 end L90D_DP_OF_under_20,
case when L90D_mx_orders<5 then 1 else 0 end L90D_mx_orders_under_5,
case when L28D_DP_savings<20 then 1 else 0 end L28D_DP_savings_under_20,
count(distinct delivery_id) orders,
count(distinct creator_id) users
from proddb.katez.tags
group by all
order by 1,2,3,4
