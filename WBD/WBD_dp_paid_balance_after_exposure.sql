/*, dp_balance as(
select
date_trunc('month', first_exposed) as "First Exposed Week",
ceil((datediff('day', first_exposed, dte)+1) / 7) as "Week Period",
count(distinct case when tag_renamed = 'Control' and is_in_paid_balance = true and billing_period is not null then CONSUMER_ID end) as "Paid Subscriber Balance: Control",
count(distinct case when tag_renamed = 'Treatment' and is_in_paid_balance = true and billing_period is not null then CONSUMER_ID end) as "Paid Subscriber Balance: Treatment",
count(distinct case when tag_renamed = 'Control' and (is_in_trial_balance = true or (is_in_paid_balance = true and billing_period is not null)) then CONSUMER_ID end) as "DP Subscriber Balance: Control",
count(distinct case when tag_renamed = 'Treatment' and (is_in_trial_balance = true or (is_in_paid_balance = true and billing_period is not null)) then CONSUMER_ID end) as "DP Subscriber Balance: Treatment"
from
  EDW.consumer.fact_consumer_subscription__daily
join be on CONSUMER_ID = try_cast(be.user_id as integer) and dte > first_exposed
where
  country_id_subscribed_from = 1
  and dte >= '2022-09-01'
  and consumer_subscription_plan_id != 10002416
group by 1, 2
)
*/

create or replace table proddb.katez.dp_paid_balance_0126 as
(select 
  consumer_id, SUBSCRIPTION_ID, dte
FROM edw.consumer.fact_consumer_subscription__daily dsa
LEFT JOIN
  proddb.static.dashpass_annual_plan_ids b 
  ON dsa.consumer_subscription_plan_id = b.consumer_subscription_plan_id
where 1=1--is_new_subscription_date = TRUE
  and COUNTRY_ID_SUBSCRIBED_FROM = 1
  and dsa.consumer_subscription_plan_id != 10002416
  --and dsa.subscription_status != 'cancelled_subscription_creation_failed'
  and is_in_paid_balance = TRUE
  and billing_period is not null
  and dte >= '2022-09-02'
group by all
)

select tag_renamed,
case when DATEADD(day,-30,first_eligible_dt) <= dte AND dte < first_eligible_dt then -1
  when first_eligible_dt <= dte AND dte < DATEADD(day,30,first_eligible_dt) then 1
  when DATEADD(day,30,first_eligible_dt) <= dte AND dte < DATEADD(day,60,first_eligible_dt) then 2
  when DATEADD(day,60,first_eligible_dt) <= dte AND dte < DATEADD(day,90,first_eligible_dt) then 3
  when DATEADD(day,90,first_eligible_dt) <= dte AND dte < DATEADD(day,120,first_eligible_dt) then 4
  when DATEADD(day,120,first_eligible_dt) <= dte AND dte < DATEADD(day,150,first_eligible_dt) then 5
  when DATEADD(day,150,first_eligible_dt) <= dte AND dte < DATEADD(day,180,first_eligible_dt) then 6
  when DATEADD(day,180,first_eligible_dt) <= dte AND dte < DATEADD(day,210,first_eligible_dt) then 7
  when DATEADD(day,210,first_eligible_dt) <= dte AND dte < DATEADD(day,240,first_eligible_dt) then 8
  when DATEADD(day,240,first_eligible_dt) <= dte AND dte < DATEADD(day,270,first_eligible_dt) then 9
  when DATEADD(day,270,first_eligible_dt) <= dte AND dte < DATEADD(day,300,first_eligible_dt) then 10
  when DATEADD(day,300,first_eligible_dt) <= dte AND dte < DATEADD(day,330,first_eligible_dt) then 11
  when DATEADD(day,330,first_eligible_dt) <= dte AND dte < DATEADD(day,360,first_eligible_dt) then 12
  when DATEADD(day,360,first_eligible_dt) <= dte AND dte < DATEADD(day,390,first_eligible_dt) then 13
  when DATEADD(day,390,first_eligible_dt) <= dte AND dte < DATEADD(day,420,first_eligible_dt) then 14
  when DATEADD(day,420,first_eligible_dt) <= dte AND dte < DATEADD(day,450,first_eligible_dt) then 15
  when DATEADD(day,450,first_eligible_dt) <= dte AND dte < DATEADD(day,480,first_eligible_dt) then 16
  when DATEADD(day,480,first_eligible_dt) <= dte AND dte < DATEADD(day,510,first_eligible_dt) then 17
  when DATEADD(day,510,first_eligible_dt) <= dte AND dte < DATEADD(day,540,first_eligible_dt) then 18
  when DATEADD(day,540,first_eligible_dt) <= dte AND dte < DATEADD(day,570,first_eligible_dt) then 19
  when DATEADD(day,570,first_eligible_dt) <= dte AND dte < DATEADD(day,600,first_eligible_dt) then 20
  when DATEADD(day,600,first_eligible_dt) <= dte AND dte < DATEADD(day,630,first_eligible_dt) then 21
  when DATEADD(day,630,first_eligible_dt) <= dte AND dte < DATEADD(day,660,first_eligible_dt) then 22
  when DATEADD(day,660,first_eligible_dt) <= dte AND dte < DATEADD(day,690,first_eligible_dt) then 23
   when DATEADD(day,690,first_eligible_dt) <= dte AND dte < DATEADD(day,720,first_eligible_dt) then 24
   when DATEADD(day,720,first_eligible_dt) <= dte AND dte < DATEADD(day,750,first_eligible_dt) then 25
   when DATEADD(day,750,first_eligible_dt) <= dte AND dte < DATEADD(day,780,first_eligible_dt) then 26
   when DATEADD(day,780,first_eligible_dt) <= dte AND dte < DATEADD(day,810,first_eligible_dt) then 27
    when DATEADD(day,810,first_eligible_dt) <= dte AND dte < DATEADD(day,840,first_eligible_dt) then 28
    when DATEADD(day,840,first_eligible_dt) <= dte AND dte < DATEADD(day,870,first_eligible_dt) then 29
    when DATEADD(day,870,first_eligible_dt) <= dte AND dte < DATEADD(day,900,first_eligible_dt) then 30
    when DATEADD(day,900,first_eligible_dt) <= dte AND dte < DATEADD(day,930,first_eligible_dt) then 31
    when DATEADD(day,930,first_eligible_dt) <= dte AND dte < DATEADD(day,960,first_eligible_dt) then 32
    when DATEADD(day,960,first_eligible_dt) <= dte AND dte < DATEADD(day,990,first_eligible_dt) then 33
    when DATEADD(day,990,first_eligible_dt) <= dte AND dte < DATEADD(day,1020,first_eligible_dt) then 34
    when DATEADD(day,1020,first_eligible_dt) <= dte AND dte < DATEADD(day,1050,first_eligible_dt) then 35
    when DATEADD(day,1050,first_eligible_dt) <= dte AND dte < DATEADD(day,1080,first_eligible_dt) then 36
  end as eligible_month_n,
count(distinct b.consumer_id) as dp_paid_balance
from proddb.katez.wbd_first_eligible_cohort a
left join proddb.katez.dp_paid_balance_0126 b
 on a.consumer_id=b.consumer_id
 and dte::date between DATEADD(day,-30,first_eligible_dt) and DATEADD(day,360,first_eligible_dt)
where first_eligible_dt <= DATEADD(day,-360,date'2026-02-02')
group by 1,2
order by 1,2

select tag_renamed, count(distinct consumer_id) eligible_customers
from proddb.katez.wbd_first_eligible_cohort
where first_eligible_dt <= DATEADD(day,-360,date'2026-02-02')
group by 1
order by 1

TAG_RENAMED	ELIGIBLE_CUSTOMERS
Control	5793306
Treatment	110097571
