create or replace table proddb.katez.dp_signups_0126 as
(select 
  consumer_id, SUBSCRIPTION_ID, 
  START_TIME,
  CASE WHEN is_in_intraday_trial_balance = TRUE AND is_new_subscription_date = TRUE THEN 1 ELSE 0 END AS dashpass_trial_signup,
  CASE WHEN is_in_intraday_pay_balance = TRUE AND is_new_paying_subscription_date = TRUE AND is_direct_to_pay_date = TRUE AND billing_period IS NOT NULL THEN 1 ELSE 0 END AS dashpass_dtp_signup,
    -- The total number of signups is calculated by summing the above derived fields
  dashpass_trial_signup + dashpass_dtp_signup AS dashpass_signup
FROM edw.consumer.fact_consumer_subscription__daily dsa
LEFT JOIN
  proddb.static.dashpass_annual_plan_ids b 
  ON dsa.consumer_subscription_plan_id = b.consumer_subscription_plan_id
where is_new_subscription_date = TRUE
  and COUNTRY_ID_SUBSCRIBED_FROM = 1
  and dsa.consumer_subscription_plan_id != 10002416
  and dsa.subscription_status != 'cancelled_subscription_creation_failed'
  and dte >= '2022-09-02'
group by all
)

select tag_renamed,
case when DATEADD(day,-30,first_eligible_dt) <= START_TIME AND START_TIME < first_eligible_dt then -1
  when first_eligible_dt <= START_TIME AND START_TIME < DATEADD(day,30,first_eligible_dt) then 1
  when DATEADD(day,30,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,60,first_eligible_dt) then 2
  when DATEADD(day,60,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,90,first_eligible_dt) then 3
  when DATEADD(day,90,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,120,first_eligible_dt) then 4
  when DATEADD(day,120,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,150,first_eligible_dt) then 5
  when DATEADD(day,150,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,180,first_eligible_dt) then 6
  when DATEADD(day,180,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,210,first_eligible_dt) then 7
  when DATEADD(day,210,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,240,first_eligible_dt) then 8
  when DATEADD(day,240,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,270,first_eligible_dt) then 9
  when DATEADD(day,270,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,300,first_eligible_dt) then 10
  when DATEADD(day,300,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,330,first_eligible_dt) then 11
  when DATEADD(day,330,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,360,first_eligible_dt) then 12
  end as eligible_month_n,
count(distinct case when dashpass_signup = 1 then subscription_id end) as users
from proddb.katez.wbd_first_eligible_cohort a
left join proddb.katez.dp_signups_0126 b
 on a.consumer_id=b.consumer_id
 and start_time::date between DATEADD(day,-30,first_eligible_dt) and DATEADD(day,360,first_eligible_dt)
where first_eligible_dt <= DATEADD(day,-360,date'2026-02-02')
group by 1,2
order by 1,2
