create or replace table proddb.katez.dp_paid_balance_0126 as
(select 
  consumer_id, SUBSCRIPTION_ID, 
  START_TIME
FROM edw.consumer.fact_consumer_subscription__daily dsa
LEFT JOIN
  proddb.static.dashpass_annual_plan_ids b 
  ON dsa.consumer_subscription_plan_id = b.consumer_subscription_plan_id
where is_new_subscription_date = TRUE
  and COUNTRY_ID_SUBSCRIBED_FROM = 1
  and dsa.consumer_subscription_plan_id != 10002416
  and dsa.subscription_status != 'cancelled_subscription_creation_failed'
  and is_in_paid_balance = TRUE
  and billing_period is not null
  and dte >= '2022-09-02'
group by all
)
