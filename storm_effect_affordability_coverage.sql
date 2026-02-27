-- User status cohort as of 2/25
with dp as
(select 
  consumer_id, dte
FROM edw.consumer.fact_consumer_subscription__daily dsa
LEFT JOIN
  proddb.static.dashpass_annual_plan_ids b 
  ON dsa.consumer_subscription_plan_id = b.consumer_subscription_plan_id
where 1=1--is_new_subscription_date = TRUE
  and COUNTRY_ID_SUBSCRIBED_FROM = 1
  and dsa.consumer_subscription_plan_id != 10002416
  --and dsa.subscription_status != 'cancelled_subscription_creation_failed'
  and ((is_in_paid_balance = TRUE and billing_period is not null)
	or is_in_trial_balance = TRUE)
  and dte = '2025-02-25'
group by all
),

Select case when dp.consumer_id is not null then 'DP'
       when ca.days_since_first_purchase < 30 then 'New Cx'
       when days_since_last_purchase > 90 then 'Churned'
       when days_since_last_purchase between 29 and 90 then 'Dormant'
       --when ca.l28_orders between 1 and 2 then 'Occasional'
       --when ca.l28_orders between 3 and 4 then 'Habituating'
       --when ca.l28_orders >= 5 then 'RDP'
       when days_since_last_purchase < 29 then 'Active'
   end as segment,
count(distinct creator_id) users
from proddb.mattheitz.mh_customer_authority ca
left join dp on dp.consumer_id=ca.creator_id
WHERE ca.dte = '2026-02-25'
group by 1
order by 2 desc

Cx Segment USERS
Churned   161,333,525
DP   19,334,484
Dormant   16,891,175
Occasional   14,921,597
RDP   7,268,444
Habituating
5,119,834
New Cx
1,987,278


-- User status cohort as of 2/25, and last order in storm submarkets
create table proddb.
as 
(
with dp as
(select 
  consumer_id, dte
FROM edw.consumer.fact_consumer_subscription__daily dsa
LEFT JOIN
  proddb.static.dashpass_annual_plan_ids b 
  ON dsa.consumer_subscription_plan_id = b.consumer_subscription_plan_id
where 1=1--is_new_subscription_date = TRUE
  and COUNTRY_ID_SUBSCRIBED_FROM = 1
  and dsa.consumer_subscription_plan_id != 10002416
  --and dsa.subscription_status != 'cancelled_subscription_creation_failed'
  and ((is_in_paid_balance = TRUE and billing_period is not null)
	or is_in_trial_balance = TRUE)
  and dte = '2025-02-25'
group by all
),

Select case when dp.consumer_id is not null then 'DP'
       when ca.days_since_first_purchase < 30 then 'New Cx'
       when days_since_last_purchase > 90 then 'Churned'
       when days_since_last_purchase between 29 and 90 then 'Dormant'
       --when ca.l28_orders between 1 and 2 then 'Occasional'
       --when ca.l28_orders between 3 and 4 then 'Habituating'
       --when ca.l28_orders >= 5 then 'RDP'
       when days_since_last_purchase < 29 then 'Active'
   end as segment,
creator_id,
from proddb.mattheitz.mh_customer_authority ca
left join dp on dp.consumer_id=ca.creator_id
join public.dimension_deliveries dd  -- last order in storm submarkets
  on ca.creator_id = dd.creator_id
  and ca.prior_delivery_id = dd.delivery_id
  and dd.country_id = 1
  and dd.submarket_id in ()  --need to update
WHERE ca.dte = '2026-02-25'
group by all
)


Select segment,
  count(distinct a.creator_id) users
from user_cohort a
left join public.dimension_deliveries dd
 on a.creator_id = dd.creator_id
where dd.created_at BETWEEN '2025-12-18' AND '2026-02-25'--'2026-01-22' AND '2026-01-21','2026-01-22' AND '2026-02-25'
and dd.country_id = 1  
and is_filtered_core = true  
--and is_subscribed_consumer = false  
--and is_consumer_pickup = FALSE
