-- NFSv2 Experiment

set exp_start_version = 26;
set exp_end_version = 35;
set start_date = '2025-03-25';
set end_date = '2025-05-07';
set exp_name = 'new_fee_structure_v1';
select 
   case 
     when tag = 'control' then 'Control' 
     else 'Treatment'
     end 
    as tag_renamed
  , bucket_key as user_id 
  , min(exposure_time) as first_exposed
from PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE ee
join edw.consumer.fact_consumer_subscription__daily dp on try_cast(ee.bucket_key as integer) = dp.consumer_id and dte::date = exposure_time::date - 1 and (is_in_trial_balance = true or (is_in_paid_balance = true and billing_period is not null)) -- DP Only
where experiment_name = $exp_name
  and experiment_version between $exp_start_version and $exp_end_version
  and exposure_time between $start_date and $end_date
  and (tag in ('control') or tag in (
        'treatment1a', 'treatment1b', 'treatment2a', 'treatment2b'
        , 'treatment3a', 'treatment3b', 'treatment4a', 'treatment4b'
      ))
 and bucket_key not in ('1505155093') --Exclude Dashmart re-stocking cx_id
 and SEGMENT in ('Users')
group by all
 ;

----------------------------
set exp_start_version = 26;
set exp_end_version = 72;
set start_date = '2021-02-01';
set end_date = '2022-06-16';
set exp_name = 'core_pricing_exp';
 
with first_order_date as (
    select 
        dd.creator_id,
        min(created_at::date) AS first_order_date
    from edw.finance.dimension_deliveries dd
    where
        is_filtered_core = true
    group by 1
)
, exp as (
select 
   case 
     when tag = 'us_control' then 'Control' 
     when tag = 'us_treatment5' then 'Treatment'
     end 
    as tag_renamed
  , bucket_key as user_id 
  , min(exposure_time) as first_exposed
from PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE ee
where experiment_name = $exp_name
  and experiment_version between $exp_start_version and $exp_end_version
  and exposure_time between $start_date and $end_date
  and convert_timezone('UTC','America/Los_Angeles',exposure_time) between '2021-02-01 20:00:00' and '2022-12-10 00:00:00'
    and ee.tag not in ('undefined') 
    and ee.tag in ('us_control','us_treatment5')
    and bucket_key != 'unset_bucket_value'
group by all
 )
 , exp_first_order_date as (
 select 
 exp.*,
 case when datediff('day', first_order_date, first_exposed) <= 30 then 1 else 0 end as is_new_cx
 from exp 
 join first_order_date on exp.user_id = first_order_date.creator_id
 )
, be as (
select 
    be.*
  , count(distinct delivery_id) as l365d_of
  , count(distinct case when created_at::date between first_exposed::date - 84 and first_exposed::date - 1 then delivery_id end) as l84d_of
  , count(distinct case when created_at::date between first_exposed::date - 28 and first_exposed::date - 1 then delivery_id end) as l28d_of
  , count(distinct case when is_subscribed_consumer = true then delivery_id end) as l365d_dp_of
from exp_first_order_date be
left join public.dimension_deliveries dd on be.user_id = dd.creator_id and created_at::date between first_exposed::date - 365 and first_exposed::date - 1 and is_filtered_core = true
group by all
)
, service_fee_promo_discounts as(    
  SELECT order_cart_id, sum(amount/100) as sf_discount_amount
  FROM public.maindblocal_order_cart_discount_component
  WHERE monetary_field = 'service_fee'
    and created_at::date between $start_date::date - 7 and $end_date::date + 7
    and ("GROUP" != 'subscription')
  group by 1
)
, core_dd as(
select
  is_consumer_pickup::int as pickup
, is_subscribed_consumer::int as dashpass
, IS_SUBSCRIPTION_DISCOUNT_APPLIED::int as dashpass_eligible
, coalesce(sfd.sf_discount_amount, 0) as sf_discount_amount_use
, greatest(dd.service_fee/100.0 - sf_discount_amount_use, 0) as actual_sf_paid_by_cx
, dd.*
, coalesce(fda.variable_profit_ex_alloc, fda.variable_profit + fda.payment_to_customers) as ue
from public.dimension_deliveries dd
left join fact_delivery_allocation fda on dd.delivery_id = fda.delivery_id
left join service_fee_promo_discounts sfd on dd.order_cart_id = sfd.order_cart_id
where dd.is_filtered_core = True 
  and created_at between $start_date::date - 7 and $end_date
  and dd.country_id = 1
)
, dp_signup as (
  select 
  e.*,
  SUBSCRIPTION_ID, 
  START_TIME,
  CASE WHEN is_in_intraday_trial_balance = true and is_new_subscription_date = true THEN 1 ELSE 0 END AS dashpass_trial_signup,
  CASE WHEN is_in_intraday_pay_balance = true
       and is_new_paying_subscription_date = true
       and is_direct_to_pay_date = true
       and billing_period is not null 
  THEN 1 ELSE 0 END AS dashpass_dtp_signup,
  dashpass_trial_signup + dashpass_dtp_signup AS dashpass_signup
FROM
    be e 
left join  edw.consumer.fact_consumer_subscription__daily dsa
  on e.user_id=dsa.consumer_id
  and dateadd(second, 600,coalesce(dsa.elected_time, dsa.start_time)) between e.first_exposed and current_date
LEFT JOIN
  proddb.static.dashpass_annual_plan_ids b ON dsa.consumer_subscription_plan_id = b.consumer_subscription_plan_id
where is_new_subscription_date = TRUE
  and COUNTRY_ID_SUBSCRIBED_FROM = 1
  and dsa.consumer_subscription_plan_id != 10002416
  and dsa.subscription_status != 'cancelled_subscription_creation_failed'
  and dte between $start_date::date - 7 and $end_date
)
   
, dp_adoption as(
  select 
     user_id as consumer_id
  from dp_signup
  where dashpass_signup >= 1
  group by 1
)
, comb as(
select 
  a.tag_renamed
, a.user_id as consumer_id
, a.first_exposed
, dpa.consumer_id as dp_sign_up
, case
    when is_new_cx = 1 then '0. New Cx'
    when l365d_of = 0 then '1. O Order'
    when l365d_of <= 5 then '2. 1-5 Orders'
    when l365d_of <= 10 then '3. 6-10 Orders'
    when l365d_of <= 20 then '4. 11-20 Orders'
    when l365d_of <= 30 then '5. 20-30 Orders'
    when l365d_of > 30 then '6. >= 30 Orders'
  end as cohort
, c.*
from be a
left join core_dd c on c.creator_id = a.user_id AND c.CREATED_AT >=  a.first_exposed
left join dp_adoption dpa on a.user_id = dpa.consumer_id 
)
select *
from (
  select 
  cohort,
  count(distinct case when tag_renamed = 'Control' then consumer_id end) as total_cx_c,
  count(distinct case when tag_renamed = 'Treatment' then consumer_id end) as total_cx_t,
  count(distinct case when tag_renamed = 'Control' then DELIVERY_ID end)  as "Volume Control",
  count(distinct case when tag_renamed = 'Treatment' then DELIVERY_ID end)  as "Volume Treatment",
  "Volume Control" / total_cx_c as "Order Rate Control",
  "Volume Treatment" / total_cx_t as "Order Rate Treatment",
  "Order Rate Treatment" / "Order Rate Control" - 1 as "OR Lift",
  "Volume Treatment" - "Volume Control" * total_cx_t / total_cx_c as "Volume Impact", 
  sum(case when tag_renamed = 'Treatment' then UE end) - sum(case when tag_renamed = 'Control' then UE end) * total_cx_t / total_cx_c as "VP Impact", 
  - "VP Impact" / "Volume Impact" as "GPLO/CPIO",
  sum(case when tag_renamed = 'Treatment' then gov/100.0 end) - sum(case when tag_renamed = 'Control' then gov/100.0 end) * total_cx_t / total_cx_c as "GOV Impact",
  "GOV Impact" / (sum(case when tag_renamed = 'Control' then gov/100.0 end) * total_cx_t / total_cx_c) as "GOV Lift",
  avg(case when tag_renamed = 'Control' then UE end) as "UE Control",
  avg(case when tag_renamed = 'Treatment' then UE end) as "UE Treatment",
  avg(case when tag_renamed = 'Control' then subtotal/100.0 end) as "Subtotal Control",
  avg(case when tag_renamed = 'Treatment' then subtotal/100.0 end) as "Subtotal Treatment",
  avg(case when tag_renamed = 'Control' then delivery_fee/100.0 end) as "Gross DF Control",
  avg(case when tag_renamed = 'Treatment' then delivery_fee/100.0 end) as "Gross DF Treatment",
  avg(case when tag_renamed = 'Control' then actual_sf_paid_by_cx end) as "Net SF Control",
  avg(case when tag_renamed = 'Treatment' then actual_sf_paid_by_cx end) as "Net SF Treatment",
  "Net SF Treatment" - "Net SF Control" as "Net SF Delta",
  count(distinct case when tag_renamed = 'Control' then dp_sign_up end) / total_cx_c as "DP Signup Rate Control",
  count(distinct case when tag_renamed = 'Treatment' then dp_sign_up end) / total_cx_t as "DP Signup Rate Treatment",
  "DP Signup Rate Treatment" / "DP Signup Rate Control" - 1 as "DP Signup Lift",
  "DP Signup Lift" / "Net SF Delta" as "DP Signup Sensitivity",
  - "OR Lift" / "Net SF Delta" as "Price Sensitivity"
  from comb
  group by 1
  
  union all 
  
  select 
  'Overall',
  count(distinct case when tag_renamed = 'Control' then consumer_id end) as total_cx_c,
  count(distinct case when tag_renamed = 'Treatment' then consumer_id end) as total_cx_t,
  count(distinct case when tag_renamed = 'Control' then DELIVERY_ID end)  as "Volume Control",
  count(distinct case when tag_renamed = 'Treatment' then DELIVERY_ID end)  as "Volume Treatment",
  "Volume Control" / total_cx_c as "Order Rate Control",
  "Volume Treatment" / total_cx_t as "Order Rate Treatment",
  "Order Rate Treatment" / "Order Rate Control" - 1 as "OR Lift",
  "Volume Treatment" - "Volume Control" * total_cx_t / total_cx_c as "Volume Impact", 
  sum(case when tag_renamed = 'Treatment' then UE end) - sum(case when tag_renamed = 'Control' then UE end) * total_cx_t / total_cx_c as "VP Impact", 
  - "VP Impact" / "Volume Impact" as "GPLO/CPIO",
  sum(case when tag_renamed = 'Treatment' then gov/100.0 end) - sum(case when tag_renamed = 'Control' then gov/100.0 end) * total_cx_t / total_cx_c as "GOV Impact",
  "GOV Impact" / (sum(case when tag_renamed = 'Control' then gov/100.0 end) * total_cx_t / total_cx_c) as "GOV Lift",
  avg(case when tag_renamed = 'Control' then UE end) as "UE Control",
  avg(case when tag_renamed = 'Treatment' then UE end) as "UE Treatment",
  avg(case when tag_renamed = 'Control' then subtotal/100.0 end) as "Subtotal Control",
  avg(case when tag_renamed = 'Treatment' then subtotal/100.0 end) as "Subtotal Treatment",
  avg(case when tag_renamed = 'Control' then service_fee/100.0 end) as "Gross SF Control",
  avg(case when tag_renamed = 'Treatment' then service_fee/100.0 end) as "Gross SF Treatment",
  avg(case when tag_renamed = 'Control' then ACTUAL_SF_PAID_BY_CX end) as "Net SF Control",
  avg(case when tag_renamed = 'Treatment' then ACTUAL_SF_PAID_BY_CX end) as "Net SF Treatment",
  "Net SF Treatment" - "Net SF Control" as "Net SF Delta",
  count(distinct case when tag_renamed = 'Control' then dp_sign_up end) / total_cx_c as "DP Signup Rate Control",
  count(distinct case when tag_renamed = 'Treatment' then dp_sign_up end) / total_cx_t as "DP Signup Rate Treatment",
  "DP Signup Rate Treatment" / "DP Signup Rate Control" - 1 as "DP Signup Lift",
  "DP Signup Lift" / "Net SF Delta" as "DP Signup Sensitivity",
  - "OR Lift" / "Net SF Delta" as "Price Sensitivity"
  from comb
)
order by cohort
;

-- p365d DP OF

select 
    be.*
  , count(distinct case when is_subscribed_consumer = true then delivery_id end) as l365d_dp_of
from be
left join public.dimension_deliveries dd on be.user_id = dd.creator_id and created_at::date between first_exposed::date - 365 and first_exposed::date - 1 and is_filtered_core = true
group by all

--LT DP Orders
, comb as(
select 
  a.tag_renamed
, a.user_id as consumer_id
, a.first_exposed
, dpa.consumer_id as dp_sign_up
, case
    when lifetime_dp_orders <= 10 then '1. <=10 Orders'
    when lifetime_dp_orders <= 30 then '2. 11-30 Orders'
    when lifetime_dp_orders <= 60 then '3. 31-60 Orders'
    when lifetime_dp_orders <= 90 then '4. 61-90 Orders'
    when lifetime_dp_orders <= 120 then '5. 91-120 Orders'
    when lifetime_dp_orders > 120 then '6. > 120 Orders'
  end as cohort
, c.*
from be a
left join core_dd c on c.creator_id = a.user_id AND c.CREATED_AT >=  a.first_exposed
left join dp_adoption dpa on a.user_id = dpa.consumer_id 
)
select 
  cohort,
  count(distinct case when tag_renamed = 'Control' then consumer_id end) as total_cx_c,
  count(distinct case when tag_renamed = 'Treatment' then consumer_id end) as total_cx_t,
  count(distinct case when tag_renamed = 'Control' then DELIVERY_ID end)  as "Volume Control",
  count(distinct case when tag_renamed = 'Treatment' then DELIVERY_ID end)  as "Volume Treatment",
  "Volume Control" / total_cx_c as "Order Rate Control",
  "Volume Treatment" / total_cx_t as "Order Rate Treatment",
  "Order Rate Treatment" / "Order Rate Control" - 1 as "OR Lift",
  "Volume Treatment" - "Volume Control" * total_cx_t / total_cx_c as "Volume Impact", 
  sum(case when tag_renamed = 'Treatment' then UE end) - sum(case when tag_renamed = 'Control' then UE end) * total_cx_t / total_cx_c as "VP Impact", 
  - "VP Impact" / "Volume Impact" as "GPLO/CPIO",
  sum(case when tag_renamed = 'Treatment' then gov/100.0 end) - sum(case when tag_renamed = 'Control' then gov/100.0 end) * total_cx_t / total_cx_c as "GOV Impact",
  "GOV Impact" / (sum(case when tag_renamed = 'Control' then gov/100.0 end) * total_cx_t / total_cx_c) as "GOV Lift",
  avg(case when tag_renamed = 'Control' then UE end) as "UE Control",
  avg(case when tag_renamed = 'Treatment' then UE end) as "UE Treatment",
  avg(case when tag_renamed = 'Control' then subtotal/100.0 end) as "Subtotal Control",
  avg(case when tag_renamed = 'Treatment' then subtotal/100.0 end) as "Subtotal Treatment",
  avg(case when tag_renamed = 'Control' then delivery_fee/100.0 end) as "Gross DF Control",
  avg(case when tag_renamed = 'Treatment' then delivery_fee/100.0 end) as "Gross DF Treatment",
  avg(case when tag_renamed = 'Control' then actual_sf_paid_by_cx end) as "Net SF Control",
  avg(case when tag_renamed = 'Treatment' then actual_sf_paid_by_cx end) as "Net SF Treatment",
  "Net SF Treatment" - "Net SF Control" as "Net SF Delta",
  count(distinct case when tag_renamed = 'Control' then dp_sign_up end) / total_cx_c as "DP Signup Rate Control",
  count(distinct case when tag_renamed = 'Treatment' then dp_sign_up end) / total_cx_t as "DP Signup Rate Treatment",
  "DP Signup Rate Treatment" / "DP Signup Rate Control" - 1 as "DP Signup Lift",
  "DP Signup Lift" / "Net SF Delta" as "DP Signup Sensitivity",
  - "OR Lift" / "Net SF Delta" as "Price Sensitivity"
from comb
group by 1
order by cohort
;

/*
among LT DP orders < 60 
what is the price sens by P365D OF? by LT tenure? by P365D tensure
*/
   
set exp_start_version = 26;
set exp_end_version = 35;
set start_date = '2025-03-25';
set end_date = '2025-05-07';
set exp_name = 'new_fee_structure_v1';

with first_order_date as (
    select 
        dd.creator_id,
        min(created_at::date) AS first_order_date
    from edw.finance.dimension_deliveries dd
    where
        is_filtered_core = true
    group by 1
)
, exp as (
select 
   case 
     when tag = 'control' then 'Control' 
     else 'Treatment'
     end 
    as tag_renamed
  , bucket_key as user_id 
  , min(exposure_time) as first_exposed
from PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE ee
join edw.consumer.fact_consumer_subscription__daily dp on try_cast(ee.bucket_key as integer) = dp.consumer_id and dte::date = exposure_time::date - 1 and (is_in_trial_balance = true or (is_in_paid_balance = true and billing_period is not null)) -- DP Only
where experiment_name = $exp_name
  and experiment_version between $exp_start_version and $exp_end_version
  and exposure_time between $start_date and $end_date
  and (tag in ('control') or tag in (
        'treatment1a', 'treatment1b', 'treatment2a', 'treatment2b'
        , 'treatment3a', 'treatment3b', 'treatment4a', 'treatment4b'
      ))
 and bucket_key not in ('1505155093') --Exclude Dashmart re-stocking cx_id
 and SEGMENT in ('Users')
group by all
 )
 , exp_first_order_date as (
 select 
 exp.*,
 case when datediff('day', first_order_date, first_exposed) <= 30 then 1 else 0 end as is_new_cx
 from exp 
 join first_order_date on exp.user_id = first_order_date.creator_id
 )
, be as (
select 
    be.*
 ,sum(case when is_subscribed_consumer = true and created_at::date between first_exposed::date - 28 and first_exposed::date - 1 then fee-delivery_fee+service_fee_no_dscnt-service_fee end) DP_savings_28D
 ,sum(case when is_subscribed_consumer = true and created_at::date between first_exposed::date - 365 and first_exposed::date - 1 then fee-delivery_fee+service_fee_no_dscnt-service_fee end) DP_savings_365D
  , count(distinct case when is_subscribed_consumer = true then delivery_id end) as lifetime_dp_orders
  , count(distinct case when created_at::date between first_exposed::date - 365 and first_exposed::date - 1 then delivery_id end) as l365d_of
  , count(distinct case when created_at::date between first_exposed::date - 84 and first_exposed::date - 1 then delivery_id end) as l84d_of
  , count(distinct case when created_at::date between first_exposed::date - 28 and first_exposed::date - 1 then delivery_id end) as l28d_of
  , count(distinct case when is_subscribed_consumer = true and created_at::date between first_exposed::date - 365 and first_exposed::date - 1 then delivery_id end) as l365d_dp_of
  , count(distinct case when is_subscribed_consumer = true and created_at::date between first_exposed::date - 84 and first_exposed::date - 1 then delivery_id end) as l84d_dp_of
   from exp_first_order_date be
left join public.dimension_deliveries dd on be.user_id = dd.creator_id and created_at::date <= first_exposed::date - 1 and is_filtered_core = true
group by all
)
, service_fee_promo_discounts as(    
  SELECT order_cart_id, sum(amount/100) as sf_discount_amount
  FROM public.maindblocal_order_cart_discount_component
  WHERE monetary_field = 'service_fee'
    and created_at::date between $start_date::date - 7 and $end_date::date + 7
    and ("GROUP" != 'subscription')
  group by 1
)
, core_dd as(
select
  is_consumer_pickup::int as pickup
, is_subscribed_consumer::int as dashpass
, IS_SUBSCRIPTION_DISCOUNT_APPLIED::int as dashpass_eligible
, coalesce(sfd.sf_discount_amount, 0) as sf_discount_amount_use
, greatest(dd.service_fee/100.0 - sf_discount_amount_use, 0) as actual_sf_paid_by_cx
, dd.*
, coalesce(fda.variable_profit_ex_alloc, fda.variable_profit + fda.payment_to_customers) as ue
from public.dimension_deliveries dd
left join fact_delivery_allocation fda on dd.delivery_id = fda.delivery_id
left join service_fee_promo_discounts sfd on dd.order_cart_id = sfd.order_cart_id
where dd.is_filtered_core = True 
  and created_at between $start_date::date - 7 and $end_date
  and dd.country_id = 1
)
, dp_signup as (
  select 
  e.*,
  SUBSCRIPTION_ID, 
  START_TIME,
  CASE WHEN is_in_intraday_trial_balance = true and is_new_subscription_date = true THEN 1 ELSE 0 END AS dashpass_trial_signup,
  CASE WHEN is_in_intraday_pay_balance = true
       and is_new_paying_subscription_date = true
       and is_direct_to_pay_date = true
       and billing_period is not null 
  THEN 1 ELSE 0 END AS dashpass_dtp_signup,
  dashpass_trial_signup + dashpass_dtp_signup AS dashpass_signup
FROM
    be e 
left join  edw.consumer.fact_consumer_subscription__daily dsa
  on e.user_id=dsa.consumer_id
  and dateadd(second, 600,coalesce(dsa.elected_time, dsa.start_time)) between e.first_exposed and current_date
LEFT JOIN
  proddb.static.dashpass_annual_plan_ids b ON dsa.consumer_subscription_plan_id = b.consumer_subscription_plan_id
where is_new_subscription_date = TRUE
  and COUNTRY_ID_SUBSCRIBED_FROM = 1
  and dsa.consumer_subscription_plan_id != 10002416
  and dsa.subscription_status != 'cancelled_subscription_creation_failed'
  and dte between $start_date::date - 7 and $end_date
)
   
, dp_adoption as(
  select 
     user_id as consumer_id
  from dp_signup
  where dashpass_signup >= 1
  group by 1
)
, comb as (
select 
  a.tag_renamed
, a.user_id as consumer_id
, a.first_exposed
, dpa.consumer_id as dp_sign_up
, case
    --when is_new_cx = 1 then '0. New Cx'
    when l365d_of = 0 then '1. O Order'
    when l365d_of <= 5 then '2. 1-5 Orders'
    when l365d_of <= 10 then '3. 6-10 Orders'
    when l365d_of <= 20 then '4. 11-20 Orders'
    when l365d_of <= 30 then '5. 20-30 Orders'
    when l365d_of <= 60 then '6. 30-60 Orders'
    when l365d_of > 60 then '7. >= 60 Orders'
  end as l365d_of_cohort
, case
    --when is_new_cx = 1 then '0. New Cx'
    when l365d_dp_of = 0 then '1. O Order'
    when l365d_dp_of <= 5 then '2. 1-5 Orders'
    when l365d_dp_of <= 10 then '3. 6-10 Orders'
    when l365d_dp_of <= 20 then '4. 11-20 Orders'
    when l365d_dp_of <= 30 then '5. 20-30 Orders'
    when l365d_dp_of > 30 then '6. >= 30 Orders'
  end as l365d_dp_of_cohort
, c.*
from be a
left join core_dd c on c.creator_id = a.user_id AND c.CREATED_AT >=  a.first_exposed
left join dp_adoption dpa on a.user_id = dpa.consumer_id 
where lifetime_dp_orders < 60 --LT DP orders < 60
)


select *
from (
  select 
 l365d_of_cohort,
  count(distinct case when tag_renamed = 'Control' then consumer_id end) as total_cx_c,
  count(distinct case when tag_renamed = 'Treatment' then consumer_id end) as total_cx_t,
  count(distinct case when tag_renamed = 'Control' then DELIVERY_ID end)  as "Volume Control",
  count(distinct case when tag_renamed = 'Treatment' then DELIVERY_ID end)  as "Volume Treatment",
  "Volume Control" / total_cx_c as "Order Rate Control",
  "Volume Treatment" / total_cx_t as "Order Rate Treatment",
  "Order Rate Treatment" / "Order Rate Control" - 1 as "OR Lift",
  "Volume Treatment" - "Volume Control" * total_cx_t / total_cx_c as "Volume Impact", 
  sum(case when tag_renamed = 'Treatment' then UE end) - sum(case when tag_renamed = 'Control' then UE end) * total_cx_t / total_cx_c as "VP Impact", 
  - "VP Impact" / "Volume Impact" as "GPLO/CPIO",
  sum(case when tag_renamed = 'Treatment' then gov/100.0 end) - sum(case when tag_renamed = 'Control' then gov/100.0 end) * total_cx_t / total_cx_c as "GOV Impact",
  "GOV Impact" / (sum(case when tag_renamed = 'Control' then gov/100.0 end) * total_cx_t / total_cx_c) as "GOV Lift",
  avg(case when tag_renamed = 'Control' then UE end) as "UE Control",
  avg(case when tag_renamed = 'Treatment' then UE end) as "UE Treatment",
  avg(case when tag_renamed = 'Control' then subtotal/100.0 end) as "Subtotal Control",
  avg(case when tag_renamed = 'Treatment' then subtotal/100.0 end) as "Subtotal Treatment",
  avg(case when tag_renamed = 'Control' then delivery_fee/100.0 end) as "Gross DF Control",
  avg(case when tag_renamed = 'Treatment' then delivery_fee/100.0 end) as "Gross DF Treatment",
  avg(case when tag_renamed = 'Control' then actual_sf_paid_by_cx end) as "Net SF Control",
  avg(case when tag_renamed = 'Treatment' then actual_sf_paid_by_cx end) as "Net SF Treatment",
  "Net SF Treatment" - "Net SF Control" as "Net SF Delta",
  count(distinct case when tag_renamed = 'Control' then dp_sign_up end) / total_cx_c as "DP Signup Rate Control",
  count(distinct case when tag_renamed = 'Treatment' then dp_sign_up end) / total_cx_t as "DP Signup Rate Treatment",
  "DP Signup Rate Treatment" / "DP Signup Rate Control" - 1 as "DP Signup Lift",
  "DP Signup Lift" / "Net SF Delta" as "DP Signup Sensitivity",
  - "OR Lift" / "Net SF Delta" as "Price Sensitivity"
  from comb
  group by 1
  
  union all 
  
  select 
  'Overall',
  count(distinct case when tag_renamed = 'Control' then consumer_id end) as total_cx_c,
  count(distinct case when tag_renamed = 'Treatment' then consumer_id end) as total_cx_t,
  count(distinct case when tag_renamed = 'Control' then DELIVERY_ID end)  as "Volume Control",
  count(distinct case when tag_renamed = 'Treatment' then DELIVERY_ID end)  as "Volume Treatment",
  "Volume Control" / total_cx_c as "Order Rate Control",
  "Volume Treatment" / total_cx_t as "Order Rate Treatment",
  "Order Rate Treatment" / "Order Rate Control" - 1 as "OR Lift",
  "Volume Treatment" - "Volume Control" * total_cx_t / total_cx_c as "Volume Impact", 
  sum(case when tag_renamed = 'Treatment' then UE end) - sum(case when tag_renamed = 'Control' then UE end) * total_cx_t / total_cx_c as "VP Impact", 
  - "VP Impact" / "Volume Impact" as "GPLO/CPIO",
  sum(case when tag_renamed = 'Treatment' then gov/100.0 end) - sum(case when tag_renamed = 'Control' then gov/100.0 end) * total_cx_t / total_cx_c as "GOV Impact",
  "GOV Impact" / (sum(case when tag_renamed = 'Control' then gov/100.0 end) * total_cx_t / total_cx_c) as "GOV Lift",
  avg(case when tag_renamed = 'Control' then UE end) as "UE Control",
  avg(case when tag_renamed = 'Treatment' then UE end) as "UE Treatment",
  avg(case when tag_renamed = 'Control' then subtotal/100.0 end) as "Subtotal Control",
  avg(case when tag_renamed = 'Treatment' then subtotal/100.0 end) as "Subtotal Treatment",
  avg(case when tag_renamed = 'Control' then service_fee/100.0 end) as "Gross SF Control",
  avg(case when tag_renamed = 'Treatment' then service_fee/100.0 end) as "Gross SF Treatment",
  avg(case when tag_renamed = 'Control' then ACTUAL_SF_PAID_BY_CX end) as "Net SF Control",
  avg(case when tag_renamed = 'Treatment' then ACTUAL_SF_PAID_BY_CX end) as "Net SF Treatment",
  "Net SF Treatment" - "Net SF Control" as "Net SF Delta",
  count(distinct case when tag_renamed = 'Control' then dp_sign_up end) / total_cx_c as "DP Signup Rate Control",
  count(distinct case when tag_renamed = 'Treatment' then dp_sign_up end) / total_cx_t as "DP Signup Rate Treatment",
  "DP Signup Rate Treatment" / "DP Signup Rate Control" - 1 as "DP Signup Lift",
  "DP Signup Lift" / "Net SF Delta" as "DP Signup Sensitivity",
  - "OR Lift" / "Net SF Delta" as "Price Sensitivity"
  from comb
)
order by l365d_of_cohort
;
