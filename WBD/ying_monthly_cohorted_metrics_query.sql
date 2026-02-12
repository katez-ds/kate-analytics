create or replace table yingxie.wbd_monthly_cohort_view_v5 as
with be as (
select
*
from yingxie.wbd_ocx_be
)
, dt as (
select
    distinct date_stamp
from proddb.static.dm_date
where date_stamp between '2022-09-01' and current_date-1
)
, cx_cnt as (
select
date_trunc('month', first_exposed) as "First Exposed Month",
ceil((datediff('day', first_exposed, date_stamp)+1) / 7) as "Week Period",
count(distinct case when tag_renamed = 'Control' then user_id end) as cx_cnt_control,
count(distinct case when tag_renamed = 'Treatment' then user_id end) as cx_cnt_treatment
from be
join dt on first_exposed::date < date_stamp
group by 1, 2
)
, graduation_prep as (
select
date_trunc('month', first_exposed) as "First Exposed Month",
ceil((datediff('day', first_exposed, date)+1) / 7) as "Week Period",
count(distinct case when tag_renamed = 'Control' then ocx.consumer_id end) as non_graduation_c,
count(distinct case when tag_renamed = 'Treatment' then ocx.consumer_id end) as non_graduation_t
from be
join yingxie.wbd_ocx_v2 as ocx on try_cast(be.user_id as integer) = consumer_id and date > first_exposed
where date <= current_date - 1
group by 1, 2
)
, dp as (
  select 
  CONSUMER_ID, 
  SUBSCRIPTION_ID, 
  CONSUMER_SUBSCRIPTION_PLAN_ID, 
  START_TIME
  from EDW.consumer.fact_consumer_subscription__daily
  where IS_NEW_SUBSCRIPTION_DATE 
  and COUNTRY_ID_SUBSCRIBED_FROM = 1
  and consumer_subscription_plan_id != 10002416
  and dte between '2022-09-01' and current_date - 1
)
, dp_adoption as( 
  select 
    date_trunc('month', first_exposed) as "First Exposed Week",
    ceil((datediff('day', first_exposed, date_stamp)+1) / 7) as "Week Period",
    count(distinct case when tag_renamed = 'Control' then ds.consumer_id end) as num_dp_signup_c,
    count(distinct case when tag_renamed = 'Treatment' then ds.consumer_id end) as num_dp_signup_t,
    count(distinct case when tag_renamed = 'Control' and ceil((datediff('day', first_exposed, convert_timezone('America/Los_Angeles', START_TIME))+1) / 7) = "Week Period" then ds.consumer_id end) as num_dp_signup_weekly_c,
    count(distinct case when tag_renamed = 'Treatment' and ceil((datediff('day', first_exposed, convert_timezone('America/Los_Angeles', START_TIME))+1) / 7) = "Week Period" then ds.consumer_id end) as num_dp_signup_weekly_t
  from be 
  join dt on date_stamp >= first_exposed::date
  join dp ds 
      on try_cast(user_id as integer) = ds.CONSUMER_ID
      and convert_timezone('America/Los_Angeles', START_TIME) >= first_exposed
      and convert_timezone('America/Los_Angeles', START_TIME)::date <= date_stamp
  group by 1, 2
)
, dp_balance as(
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
select
  a."First Exposed Month"
, a."Week Period"
, "Cx: Treatment"
, "Cx: Control"
, "AOV: Control"
, "Unit VP: Control"
, "DF: Control"
, "AOV Delta"
, "Unit VP Delta"
, "DF Delta"
, "Volume: Control"
, "Volume: Treatment"
, "Vol Gain"
, "Cumulative Vol Gain"
, "VP Impact"
, "Cumulative VP Impact"
, "GMV Impact"
, "Cumulative GMV Impact"
, "GMV Lift"
, "Cumulative GMV Lift"
, "Weekly CPIO"
, "Cumulative CPIO"
, "Order Rate Lift"
, "Cumulative Order Rate Lift"
, "Weekly Sensitivity"
, "Net Revenue Impact"
, "Cumulative Net Revenue Impact"
, "Avg Net Revenue Control"
, "Avg Net Revenue Delta"
, cx_cnt_control,
cx_cnt_treatment,
lead(cx_cnt_treatment) over (partition by a."First Exposed Month" order by a."Week Period") as next_period_cx,
case when next_period_cx = "Cx: Treatment" then graduation_prep.non_graduation_t else graduation_prep_v2.non_graduation_t end as "No Graduation Treatment", 
case when next_period_cx = "Cx: Treatment" then graduation_prep.non_graduation_c else graduation_prep_v2.non_graduation_c end as "No Graduation Control", 
1- "No Graduation Control" / cx_cnt_control as graduation_rate_c,
1- "No Graduation Treatment" / cx_cnt_treatment as graduation_rate_t,
num_dp_signup_c,
num_dp_signup_t,
num_dp_signup_c / cx_cnt_control as dp_signup_rate_c,
num_dp_signup_t / cx_cnt_treatment as dp_signup_rate_t,
percent_new_cx,
avg_tenure,
num_dp_signup_weekly_c / cx_cnt_control as dp_signup_rate_weekly_c,
num_dp_signup_weekly_t / cx_cnt_treatment as dp_signup_rate_weekly_t,
"Paid Subscriber Balance: Control",
"Paid Subscriber Balance: Treatment",
"DP Subscriber Balance: Control",
"DP Subscriber Balance: Treatment"
from yingxie.wbd_monthly_cohort_view a
join cx_cnt on cx_cnt."First Exposed Month" = a."First Exposed Month"
join dp_adoption on a."First Exposed Month" = dp_adoption."First Exposed Month"
left join dp_balance on a."First Exposed Month" = dp_balance."First Exposed Month" and dp_adoption."Week Period" = dp_balance."Week Period"
left join graduation_prep on a."First Exposed Month" = graduation_prep."First Exposed Month" and graduation_prep."Week Period" = dp_adoption."Week Period" + 1
left join graduation_prep as graduation_prep_v2 on a."First Exposed Month" = graduation_prep_v2."First Exposed Month" and graduation_prep_v2."Week Period" = dp_adoption."Week Period"
order by 1, 2
;
