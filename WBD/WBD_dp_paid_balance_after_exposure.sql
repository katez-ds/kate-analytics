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
  when DATEADD(day,360,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,390,first_eligible_dt) then 13
  when DATEADD(day,390,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,420,first_eligible_dt) then 14
  when DATEADD(day,420,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,450,first_eligible_dt) then 15
  when DATEADD(day,450,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,480,first_eligible_dt) then 16
  when DATEADD(day,480,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,510,first_eligible_dt) then 17
  when DATEADD(day,510,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,540,first_eligible_dt) then 18
  when DATEADD(day,540,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,570,first_eligible_dt) then 19
  when DATEADD(day,570,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,600,first_eligible_dt) then 20
  when DATEADD(day,600,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,630,first_eligible_dt) then 21
  when DATEADD(day,630,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,660,first_eligible_dt) then 22
  when DATEADD(day,660,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,690,first_eligible_dt) then 23
   when DATEADD(day,690,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,720,first_eligible_dt) then 24
   when DATEADD(day,720,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,750,first_eligible_dt) then 25
   when DATEADD(day,750,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,780,first_eligible_dt) then 26
   when DATEADD(day,780,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,810,first_eligible_dt) then 27
    when DATEADD(day,810,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,840,first_eligible_dt) then 28
    when DATEADD(day,840,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,870,first_eligible_dt) then 29
    when DATEADD(day,870,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,900,first_eligible_dt) then 30
    when DATEADD(day,900,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,930,first_eligible_dt) then 31
    when DATEADD(day,930,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,960,first_eligible_dt) then 32
    when DATEADD(day,960,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,990,first_eligible_dt) then 33
    when DATEADD(day,990,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,1020,first_eligible_dt) then 34
    when DATEADD(day,1020,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,1050,first_eligible_dt) then 35
    when DATEADD(day,1050,first_eligible_dt) <= START_TIME AND START_TIME < DATEADD(day,1080,first_eligible_dt) then 36
  end as eligible_month_n,
count(distinct b.consumer_id) as dp_paid_balance
from proddb.katez.wbd_first_eligible_cohort a
left join proddb.katez.dp_paid_balance_0126 b
 on a.consumer_id=b.consumer_id
 and start_time::date between DATEADD(day,-30,first_eligible_dt) and DATEADD(day,360,first_eligible_dt)
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
