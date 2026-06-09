
-- DV A Control vs Treatment (limit to in old DV treatment)

-- OF
with
  be as (
    select
      a.*
    from
      static.us_universal_dv_a_be a
      join proddb.static.wbd_experiment_exposure b --old DV in treatment
            on a.user_id = b.user_id and b.tag_renamed = 'Treatment'
      join proddb.public.FACT_DYNAMIC_AUDIENCE_WBD_ORDER_FREQUENCY_L365D c
        on injected_date = a.first_exposed::date and c.consumer_id = a.user_id and injected_date>='2026-03-12'
  group by all
  ),
  core_dd as (
    select
      *
    from
      static.us_universal_dv_core_dd
  ),
dp_signup as (
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
  and dte >=first_exposed::date - 7
),
dp_adoption as(
  select 
     user_id as consumer_id, start_time
  from dp_signup
  where dashpass_signup >= 1
  group by all
),
  comb as (
    select
      a.tag_renamed,
      a.user_id as consumer_id,
      a.first_exposed,
      --cc.total_cx,
      c.*,
      L360_orders l365d_of,
      L84_orders l84d_of,
      l28_orders l28d_of,
      dpa.consumer_id as dp_sign_up
    from
      be a
      left join core_dd c on c.creator_id = a.user_id
      AND c.CREATED_AT >= a.first_exposed
      --left join cx_cnt cc on cc.tag_renamed = a.tag_renamed  
      left join proddb.mattheitz.mh_customer_authority ca
        on a.user_id=ca.creator_id and a.first_exposed::date = ca.dte::date
      left join dp_adoption dpa on a.user_id = dpa.consumer_id and dpa.start_time::date>=a.first_exposed::date
      group by all
  ),
  pen as (
    select
      tag_renamed as "Bucket",
    case when l365d_of = 0 then '1. 0 Order'
    when l365d_of <= 5 then '2. 0-5 Orders'
    when l365d_of <= 10 then '3. 5-10 Orders'
    when l365d_of <= 20 then '4. 10-20 Orders'
    when l365d_of <= 30 then '5. 20-30 Orders'
    when l365d_of <= 60 then '6. 30-60 Orders'
    when l365d_of > 60 then '7. > 60 Orders'
    end as segment,
  /*
  case when l28d_of = 0 then '1. 0 Order'
    when l28d_of <= 2 then '2. 0-2 Orders'
    when l28d_of <= 4 then '3. 2-4 Orders'
    when l28d_of <= 8 then '4. 4-8 Orders'
    when l28d_of <= 12 then '5. 8-12 Orders'
    when l28d_of <= 20 then '6. 12-16 Orders'
    when l28d_of > 16 then '7. > 16 Orders'
    end as segment,
  */
    count(distinct consumer_id) as "# Cx",
      count(
        distinct case
          when is_filtered_core = 1 then delivery_id
        end
      ) as "Volume",
      "# Cx" / sum("# Cx") over () as "Bucketing %",
      "Volume" / nullif("# Cx", 0) as "Order Rate",
      avg(
        case
          when is_filtered_core = true then delivery_fee / 100.0
        end
      ) as "Gross Delivery Fee",
      avg(
        case
          when is_filtered_core = true then actual_df_paid_by_cx
        end
      ) as "Net Delivery Fee",
      avg(
        case
          when is_filtered_core = true then actual_sf_paid_by_cx
        end
      ) as "Service Fee",
      avg(
        case
          when is_filtered_core = true then subtotal / 100.0
        end
      ) as "Subtotal",
      sum(gov) as GOV,
      sum(gov) / count(
        distinct case
          when fda_is_filtered = 1 then fda_delivery_id
        end
      ) as aov,
      sum(gov) / "# Cx" as gov_adj,
      sum(ue) as vp,
      sum(ue) / count(
        distinct case
          when fda_is_filtered = 1 then fda_delivery_id
        end
      ) as "Unit VP",
      count(distinct dp_sign_up) as "DP Signups"
    from
      comb
    group by 1,2
  )
select
    segment,
  "Bucket",
  "# Cx",
  "Volume",
  "Order Rate",
  "Order Rate" / max(
    case
      when "Bucket" = 'Treatment' then "Order Rate"
    end
  ) over (partition by segment) - 1 as "Order Rate Lift",
    "Volume" - max(
    case
      when "Bucket" = 'Treatment' then "Volume"
    end
  ) over (partition by segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by segment) as "Volume Delta",
  /*
  "Gross Delivery Fee",
  "Gross Delivery Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Gross Delivery Fee"
    end
  ) over (partition by segment) as "Gross Delivery Fee Delta",
    */
  "Net Delivery Fee",
  "Net Delivery Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Net Delivery Fee"
    end
  ) over (partition by segment) as "Net Delivery Fee Delta",
  /*
  "Service Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Service Fee"
    end
  ) over (partition by segment) as "Service Fee Delta",
  */
  "Unit VP",
  "Unit VP" - max(
    case
      when "Bucket" = 'Treatment' then "Unit VP"
    end
  ) over (partition by segment) as "Unit VP Delta",
    VP,
     vp - max(
    case
      when "Bucket" = 'Treatment' then vp
    end
  ) over (partition by segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by segment) as "VP Delta",
    "VP Delta" / (
    max(
      case
        when "Bucket" = 'Treatment' then vp
      end
    ) over (partition by segment) * "# Cx" / max(
      case
        when "Bucket" = 'Treatment' then "# Cx"
      end
    ) over (partition by segment)
  ) as "VP Lift",
  "AOV",
  "AOV" - max(
    case
      when "Bucket" = 'Treatment' then "AOV"
    end
  ) over (partition by segment) as "AOV Delta",
  /*
  "Subtotal",
  "Subtotal" - max(
    case
      when "Bucket" = 'Treatment' then "Subtotal"
    end
  ) over (partition by segment) as "Subtotal Delta",
  */
  gov_adj / max(
    case
      when "Bucket" = 'Treatment' then gov_adj
    end
  ) over (partition by segment) - 1 as "GOV Lift",
  /*
  "Order Rate" - max(
    case
      when "Bucket" = 'Treatment' then "Order Rate"
    end
  ) over (partition by segment) as "Order Rate Delta",
  "Subtotal" / aov as "Subtotal/AOV",
  */
  gov - max(
    case
      when "Bucket" = 'Treatment' then gov
    end
  ) over (partition by segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by segment) as "GOV Delta",
  - "GOV Delta" / nullif("VP Delta", 0) as "GOV:VP Ratio",
  - "VP Delta" / nullif("Volume Delta", 0) as "(-CPID)/(+GPLD) In Campaign",
   "DP Signups" / nullif("# Cx", 0) as "DP Signup Rate",
    "DP Signup Rate" / max(
    case
      when "Bucket" = 'Treatment' then "DP Signup Rate"
    end
  ) over (partition by segment) - 1 as "DP Signup Rate Lift",
"DP Signups" - max(
    case
      when "Bucket" = 'Treatment' then "DP Signups"
    end
  ) over (partition by segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by segment) as "DP Signup Gain",
   - "GOV Delta" / nullif("DP Signup Gain", 0) as "GOV:DP Ratio",
  - "Order Rate Lift" / "Net Delivery Fee Delta" as "Price Sensitivity"
from pen 
order by
  2,1

-- L28D vs L365D OF Diff
with
  be as (
    select
      a.*
    from
      static.us_universal_dv_a_be a
      join proddb.static.wbd_experiment_exposure b --old DV in treatment
            on a.user_id = b.user_id and b.tag_renamed = 'Treatment'
      join proddb.public.FACT_DYNAMIC_AUDIENCE_WBD_ORDER_FREQUENCY_L365D c
        on injected_date = a.first_exposed::date and c.consumer_id = a.user_id and injected_date>='2026-03-12'
  group by all
  ),
  core_dd as (
    select
      *
    from
      static.us_universal_dv_core_dd
  ),
dp_signup as (
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
  and dte >=first_exposed::date - 7
),
dp_adoption as(
  select 
     user_id as consumer_id, start_time
  from dp_signup
  where dashpass_signup >= 1
  group by all
),
  comb as (
    select
      a.tag_renamed,
      a.user_id as consumer_id,
      a.first_exposed,
      --cc.total_cx,
      c.*,
      L360_orders/360*7 l365d_of,
      L84_orders/12 l84d_of,
      l28_orders/4 l28d_of,
      (l28d_of - l365d_of)*1.0000 of_diff,
      dpa.consumer_id as dp_sign_up
    from
      be a
      left join core_dd c on c.creator_id = a.user_id
      AND c.CREATED_AT >= a.first_exposed
      --left join cx_cnt cc on cc.tag_renamed = a.tag_renamed  
      left join proddb.mattheitz.mh_customer_authority ca
        on a.user_id=ca.creator_id and a.first_exposed::date = ca.dte::date
      left join dp_adoption dpa on a.user_id = dpa.consumer_id and dpa.start_time::date>=a.first_exposed::date
      group by all
  ),
  pen as (
    select
      tag_renamed as "Bucket",
    case when of_diff = 0 then '0'
    when of_diff between -1 and 0 then '-1~0'
    when of_diff between -2 and -1 then '-2~-1'
    when of_diff between -3 and -2 then '-3~-2'
    when of_diff <-3 then '-3+'
    when of_diff between 0 and 1 then '0~1'
    when of_diff between 1 and 2 then '1~2'
    when of_diff between 2 and 3 then '2~3'
    when of_diff > 3 then '3+'
    end as segment,
    count(distinct consumer_id) as "# Cx",
      count(
        distinct case
          when is_filtered_core = 1 then delivery_id
        end
      ) as "Volume",
      "# Cx" / sum("# Cx") over () as "Bucketing %",
      "Volume" / nullif("# Cx", 0) as "Order Rate",
      avg(
        case
          when is_filtered_core = true then delivery_fee / 100.0
        end
      ) as "Gross Delivery Fee",
      avg(
        case
          when is_filtered_core = true then actual_df_paid_by_cx
        end
      ) as "Net Delivery Fee",
      avg(
        case
          when is_filtered_core = true then actual_sf_paid_by_cx
        end
      ) as "Service Fee",
      avg(
        case
          when is_filtered_core = true then subtotal / 100.0
        end
      ) as "Subtotal",
      sum(gov) as GOV,
      sum(gov) / count(
        distinct case
          when fda_is_filtered = 1 then fda_delivery_id
        end
      ) as aov,
      sum(gov) / "# Cx" as gov_adj,
      sum(ue) as vp,
      sum(ue) / count(
        distinct case
          when fda_is_filtered = 1 then fda_delivery_id
        end
      ) as "Unit VP",
      count(distinct dp_sign_up) as "DP Signups"
    from
      comb
    group by 1,2
  )
select
    segment,
  "Bucket",
  "# Cx",
  "Volume",
  "Order Rate",
  "Order Rate" / max(
    case
      when "Bucket" = 'Treatment' then "Order Rate"
    end
  ) over (partition by segment) - 1 as "Order Rate Lift",
    "Volume" - max(
    case
      when "Bucket" = 'Treatment' then "Volume"
    end
  ) over (partition by segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by segment) as "Volume Delta",
  /*
  "Gross Delivery Fee",
  "Gross Delivery Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Gross Delivery Fee"
    end
  ) over (partition by segment) as "Gross Delivery Fee Delta",
  */
  "Net Delivery Fee",
  "Net Delivery Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Net Delivery Fee"
    end
  ) over (partition by segment) as "Net Delivery Fee Delta",
  /*
  "Service Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Service Fee"
    end
  ) over (partition by segment) as "Service Fee Delta",
  */
  "Unit VP",
  "Unit VP" - max(
    case
      when "Bucket" = 'Treatment' then "Unit VP"
    end
  ) over (partition by segment) as "Unit VP Delta",
    VP,
     vp - max(
    case
      when "Bucket" = 'Treatment' then vp
    end
  ) over (partition by segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by segment) as "VP Delta",
    "VP Delta" / (
    max(
      case
        when "Bucket" = 'Treatment' then vp
      end
    ) over (partition by segment) * "# Cx" / max(
      case
        when "Bucket" = 'Treatment' then "# Cx"
      end
    ) over (partition by segment)
  ) as "VP Lift",
  "AOV",
  "AOV" - max(
    case
      when "Bucket" = 'Treatment' then "AOV"
    end
  ) over (partition by segment) as "AOV Delta",
  /*
  "Subtotal",
  "Subtotal" - max(
    case
      when "Bucket" = 'Treatment' then "Subtotal"
    end
  ) over (partition by segment) as "Subtotal Delta",
  */
  gov_adj / max(
    case
      when "Bucket" = 'Treatment' then gov_adj
    end
  ) over (partition by segment) - 1 as "GOV Lift",
  /*
  "Order Rate" - max(
    case
      when "Bucket" = 'Treatment' then "Order Rate"
    end
  ) over (partition by segment) as "Order Rate Delta",
  "Subtotal" / aov as "Subtotal/AOV",
  */
  gov - max(
    case
      when "Bucket" = 'Treatment' then gov
    end
  ) over (partition by segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by segment) as "GOV Delta",
  - "GOV Delta" / nullif("VP Delta", 0) as "GOV:VP Ratio",
  - "VP Delta" / nullif("Volume Delta", 0) as "(-CPID)/(+GPLD) In Campaign",
   "DP Signups" / nullif("# Cx", 0) as "DP Signup Rate",
    "DP Signup Rate" / max(
    case
      when "Bucket" = 'Treatment' then "DP Signup Rate"
    end
  ) over (partition by segment) - 1 as "DP Signup Rate Lift",
"DP Signups" - max(
    case
      when "Bucket" = 'Treatment' then "DP Signups"
    end
  ) over (partition by segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by segment) as "DP Signup Gain",
    - "GOV Delta" / nullif("DP Signup Gain", 0) as "GOV:DP Ratio",
  - "Order Rate Lift" / "Net Delivery Fee Delta" as "Price Sensitivity"
from pen 
order by
  2,1


-- Exposed Tenure

with
  be as (
    select
      a.*,b.first_exposed old_first_exposed
    from
      static.us_universal_dv_a_be a
      join proddb.static.wbd_experiment_exposure b --old DV in treatment
            on a.user_id = b.user_id and b.tag_renamed = 'Treatment'
      join proddb.public.FACT_DYNAMIC_AUDIENCE_WBD_ORDER_FREQUENCY_L365D c
        on c.consumer_id = a.user_id and injected_date='2026-03-12'
  group by all
  ),
  core_dd as (
    select
      *
    from
      static.us_universal_dv_core_dd
  ),
dp_signup as (
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
  and dte >=first_exposed::date - 7
),
dp_adoption as(
  select 
     user_id as consumer_id, start_time
  from dp_signup
  where dashpass_signup >= 1
  group by all
),
  comb as (
    select
      a.tag_renamed,
      a.user_id as consumer_id,
      a.first_exposed,
      --cc.total_cx,
      c.*,
      datediff(day,old_first_exposed,first_exposed) exposed_tenure,
      dpa.consumer_id as dp_sign_up
    from
      be a
      left join core_dd c on c.creator_id = a.user_id
      AND c.CREATED_AT >= a.first_exposed
      --left join cx_cnt cc on cc.tag_renamed = a.tag_renamed  
      left join dp_adoption dpa on a.user_id = dpa.consumer_id and dpa.start_time::date>=a.first_exposed::date
      group by all
  ),
  pen as (
    select
      tag_renamed as "Bucket",
    case when exposed_tenure <=90 then '1. <=3 Months'
    when exposed_tenure <= 365 then '2. 3-12 Months'
    when exposed_tenure <= 730 then '3. 1-2 Years'
    when exposed_tenure <= 1095 then '4. 2-3 Years'
    when exposed_tenure <= 1460 then '5. 3-4 Years'
    when exposed_tenure > 1460 then '7. >4 years'
    end as segment,
  /*
  case when l28d_of = 0 then '1. 0 Order'
    when l28d_of <= 2 then '2. 0-2 Orders'
    when l28d_of <= 4 then '3. 2-4 Orders'
    when l28d_of <= 8 then '4. 4-8 Orders'
    when l28d_of <= 12 then '5. 8-12 Orders'
    when l28d_of <= 20 then '6. 12-16 Orders'
    when l28d_of > 16 then '7. > 16 Orders'
    end as segment,
  */
    count(distinct consumer_id) as "# Cx",
      count(
        distinct case
          when is_filtered_core = 1 then delivery_id
        end
      ) as "Volume",
      "# Cx" / sum("# Cx") over () as "Bucketing %",
      "Volume" / nullif("# Cx", 0) as "Order Rate",
      avg(
        case
          when is_filtered_core = true then delivery_fee / 100.0
        end
      ) as "Gross Delivery Fee",
      avg(
        case
          when is_filtered_core = true then actual_df_paid_by_cx
        end
      ) as "Net Delivery Fee",
      avg(
        case
          when is_filtered_core = true then actual_sf_paid_by_cx
        end
      ) as "Service Fee",
      avg(
        case
          when is_filtered_core = true then subtotal / 100.0
        end
      ) as "Subtotal",
      sum(gov) as GOV,
      sum(gov) / count(
        distinct case
          when fda_is_filtered = 1 then fda_delivery_id
        end
      ) as aov,
      sum(gov) / "# Cx" as gov_adj,
      sum(ue) as vp,
      sum(ue) / count(
        distinct case
          when fda_is_filtered = 1 then fda_delivery_id
        end
      ) as "Unit VP",
      count(distinct dp_sign_up) as "DP Signups"
    from
      comb
    group by 1,2
  )
select
    segment,
  "Bucket",
  "# Cx",
  "Volume",
  "Order Rate",
  "Order Rate" / max(
    case
      when "Bucket" = 'Treatment' then "Order Rate"
    end
  ) over (partition by segment) - 1 as "Order Rate Lift",
    "Volume" - max(
    case
      when "Bucket" = 'Treatment' then "Volume"
    end
  ) over (partition by segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by segment) as "Volume Delta",
  /*
  "Gross Delivery Fee",
  "Gross Delivery Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Gross Delivery Fee"
    end
  ) over (partition by segment) as "Gross Delivery Fee Delta",
  */
  "Net Delivery Fee",
  "Net Delivery Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Net Delivery Fee"
    end
  ) over (partition by segment) as "Net Delivery Fee Delta",
  /*
  "Service Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Service Fee"
    end
  ) over (partition by segment) as "Service Fee Delta",
  */
  "Unit VP",
  "Unit VP" - max(
    case
      when "Bucket" = 'Treatment' then "Unit VP"
    end
  ) over (partition by segment) as "Unit VP Delta",
    VP,
     vp - max(
    case
      when "Bucket" = 'Treatment' then vp
    end
  ) over (partition by segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by segment) as "VP Delta",
    "VP Delta" / (
    max(
      case
        when "Bucket" = 'Treatment' then vp
      end
    ) over (partition by segment) * "# Cx" / max(
      case
        when "Bucket" = 'Treatment' then "# Cx"
      end
    ) over (partition by segment)
  ) as "VP Lift",
  "AOV",
  "AOV" - max(
    case
      when "Bucket" = 'Treatment' then "AOV"
    end
  ) over (partition by segment) as "AOV Delta",
  /*
  "Subtotal",
  "Subtotal" - max(
    case
      when "Bucket" = 'Treatment' then "Subtotal"
    end
  ) over (partition by segment) as "Subtotal Delta",
  */
  gov_adj / max(
    case
      when "Bucket" = 'Treatment' then gov_adj
    end
  ) over (partition by segment) - 1 as "GOV Lift",
  /*
  "Order Rate" - max(
    case
      when "Bucket" = 'Treatment' then "Order Rate"
    end
  ) over (partition by segment) as "Order Rate Delta",
  "Subtotal" / aov as "Subtotal/AOV",
  */
  gov - max(
    case
      when "Bucket" = 'Treatment' then gov
    end
  ) over (partition by segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by segment) as "GOV Delta",
  - "GOV Delta" / nullif("VP Delta", 0) as "GOV:VP Ratio",
  - "VP Delta" / nullif("Volume Delta", 0) as "(-CPID)/(+GPLD) In Campaign",
   "DP Signups" / nullif("# Cx", 0) as "DP Signup Rate",
    "DP Signup Rate" / max(
    case
      when "Bucket" = 'Treatment' then "DP Signup Rate"
    end
  ) over (partition by segment) - 1 as "DP Signup Rate Lift",
"DP Signups" - max(
    case
      when "Bucket" = 'Treatment' then "DP Signups"
    end
  ) over (partition by segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by segment) as "DP Signup Gain",
   - "GOV Delta" / nullif("DP Signup Gain", 0) as "GOV:DP Ratio",
  - "Order Rate Lift" / "Net Delivery Fee Delta" as "Price Sensitivity"
from pen 
order by
  2,1
  
--  where of_diff > 0 and of_diff <=2, check L365D and L28D OF
with
  be as (
    select
      a.*
    from
      static.us_universal_dv_a_be a
      join proddb.static.wbd_experiment_exposure b --old DV in treatment
            on a.user_id = b.user_id and b.tag_renamed = 'Treatment'
      join proddb.public.FACT_DYNAMIC_AUDIENCE_WBD_ORDER_FREQUENCY_L365D c
        on injected_date = a.first_exposed::date and c.consumer_id = a.user_id and injected_date>='2026-03-12'
  group by all
  ),
  core_dd as (
    select
      *
    from
      static.us_universal_dv_core_dd
  ),
dp_signup as (
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
  and dte >=first_exposed::date - 7
),
dp_adoption as(
  select 
     user_id as consumer_id, start_time
  from dp_signup
  where dashpass_signup >= 1
  group by all
),
  comb as (
    select
      a.tag_renamed,
      a.user_id as consumer_id,
      a.first_exposed,
      --cc.total_cx,
      c.*,
      L360_orders/360*7 l365d_of,
      L84_orders/12 l84d_of,
      l28_orders/4 l28d_of,
      (l28d_of - l365d_of)*1.0000 of_diff,
      dpa.consumer_id as dp_sign_up
    from
      be a
      left join core_dd c on c.creator_id = a.user_id
      AND c.CREATED_AT >= a.first_exposed
      --left join cx_cnt cc on cc.tag_renamed = a.tag_renamed  
      left join proddb.mattheitz.mh_customer_authority ca
        on a.user_id=ca.creator_id and a.first_exposed::date = ca.dte::date
      left join dp_adoption dpa on a.user_id = dpa.consumer_id and dpa.start_time::date>=a.first_exposed::date
      group by all
  ),
  pen as (
    select
      tag_renamed as "Bucket",
    case when l365d_of = 0 then '1. 0 Order'
    when l365d_of <= 5 then '2. 0-5 Orders'
    when l365d_of <= 10 then '3. 5-10 Orders'
    when l365d_of <= 20 then '4. 10-20 Orders'
    when l365d_of <= 30 then '5. 20-30 Orders'
    when l365d_of <= 60 then '6. 30-60 Orders'
    when l365d_of > 60 then '7. > 60 Orders'
    end as l365d_segment,

  case when l28d_of = 0 then '1. 0 Order'
    when l28d_of <= 2 then '2. 0-2 Orders'
    when l28d_of <= 4 then '3. 2-4 Orders'
    when l28d_of <= 8 then '4. 4-8 Orders'
    when l28d_of <= 12 then '5. 8-12 Orders'
    when l28d_of <= 20 then '6. 12-16 Orders'
    when l28d_of > 16 then '7. > 16 Orders'
    end as l28d_segment,

    count(distinct consumer_id) as "# Cx",
      count(
        distinct case
          when is_filtered_core = 1 then delivery_id
        end
      ) as "Volume",
      "# Cx" / sum("# Cx") over () as "Bucketing %",
      "Volume" / nullif("# Cx", 0) as "Order Rate",
      avg(
        case
          when is_filtered_core = true then delivery_fee / 100.0
        end
      ) as "Gross Delivery Fee",
      avg(
        case
          when is_filtered_core = true then actual_df_paid_by_cx
        end
      ) as "Net Delivery Fee",
      avg(
        case
          when is_filtered_core = true then actual_sf_paid_by_cx
        end
      ) as "Service Fee",
      avg(
        case
          when is_filtered_core = true then subtotal / 100.0
        end
      ) as "Subtotal",
      sum(gov) as GOV,
      sum(gov) / count(
        distinct case
          when fda_is_filtered = 1 then fda_delivery_id
        end
      ) as aov,
      sum(gov) / "# Cx" as gov_adj,
      sum(ue) as vp,
      sum(ue) / count(
        distinct case
          when fda_is_filtered = 1 then fda_delivery_id
        end
      ) as "Unit VP",
      count(distinct dp_sign_up) as "DP Signups"
    from
      comb
    where of_diff > 0 and of_diff <=2
    group by all
  )
select
    l365d_segment,l28d_segment,
  "Bucket",
  "# Cx",
  "Volume",
  "Order Rate",
  "Order Rate" / max(
    case
      when "Bucket" = 'Treatment' then "Order Rate"
    end
  ) over (partition by l365d_segment,l28d_segment) - 1 as "Order Rate Lift",
    "Volume" - max(
    case
      when "Bucket" = 'Treatment' then "Volume"
    end
  ) over (partition by l365d_segment,l28d_segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by l365d_segment,l28d_segment) as "Volume Delta",
  /*
  "Gross Delivery Fee",
  "Gross Delivery Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Gross Delivery Fee"
    end
  ) over (partition by segment) as "Gross Delivery Fee Delta",
  "Net Delivery Fee",
  "Net Delivery Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Net Delivery Fee"
    end
  ) over (partition by segment) as "Net Delivery Fee Delta",
  "Service Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Service Fee"
    end
  ) over (partition by segment) as "Service Fee Delta",
  */
  "Unit VP",
  "Unit VP" - max(
    case
      when "Bucket" = 'Treatment' then "Unit VP"
    end
  ) over (partition by l365d_segment,l28d_segment) as "Unit VP Delta",
    VP,
     vp - max(
    case
      when "Bucket" = 'Treatment' then vp
    end
  ) over (partition by l365d_segment,l28d_segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by l365d_segment,l28d_segment) as "VP Delta",
    "VP Delta" / (
    max(
      case
        when "Bucket" = 'Treatment' then vp
      end
    ) over (partition by l365d_segment,l28d_segment) * "# Cx" / max(
      case
        when "Bucket" = 'Treatment' then "# Cx"
      end
    ) over (partition by l365d_segment,l28d_segment)
  ) as "VP Lift",
  "AOV",
  "AOV" - max(
    case
      when "Bucket" = 'Treatment' then "AOV"
    end
  ) over (partition by l365d_segment,l28d_segment) as "AOV Delta",
  /*
  "Subtotal",
  "Subtotal" - max(
    case
      when "Bucket" = 'Treatment' then "Subtotal"
    end
  ) over (partition by segment) as "Subtotal Delta",
  */
  gov_adj / max(
    case
      when "Bucket" = 'Treatment' then gov_adj
    end
  ) over (partition by l365d_segment,l28d_segment) - 1 as "GOV Lift",
  /*
  "Order Rate" - max(
    case
      when "Bucket" = 'Treatment' then "Order Rate"
    end
  ) over (partition by segment) as "Order Rate Delta",
  "Subtotal" / aov as "Subtotal/AOV",
  */
  gov - max(
    case
      when "Bucket" = 'Treatment' then gov
    end
  ) over (partition by l365d_segment,l28d_segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by l365d_segment,l28d_segment) as "GOV Delta",
  - "GOV Delta" / nullif("VP Delta", 0) as "GOV:VP Ratio",
  - "VP Delta" / nullif("Volume Delta", 0) as "(-CPID)/(+GPLD) In Campaign",
   "DP Signups" / nullif("# Cx", 0) as "DP Signup Rate",
    "DP Signup Rate" / max(
    case
      when "Bucket" = 'Treatment' then "DP Signup Rate"
    end
  ) over (partition by l365d_segment,l28d_segment) - 1 as "DP Signup Rate Lift",
"DP Signups" - max(
    case
      when "Bucket" = 'Treatment' then "DP Signups"
    end
  ) over (partition by l365d_segment,l28d_segment) * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over (partition by l365d_segment,l28d_segment) as "DP Signup Gain",
    - "GOV Delta" / nullif("DP Signup Gain", 0) as "GOV:DP Ratio"
from pen 
order by
  3,1,2

-- Recent OF increased, by Exposed Tenure
with
  be as (
    select
      a.*
    from
      static.us_universal_dv_a_be a
      join proddb.static.wbd_experiment_exposure b --old DV in treatment
            on a.user_id = b.user_id and b.tag_renamed = 'Treatment'
      join proddb.public.FACT_DYNAMIC_AUDIENCE_WBD_ORDER_FREQUENCY_L365D c
        on c.consumer_id = a.user_id and injected_date='2026-03-12'
  group by all
  ),
  core_dd as (
    select
      *
    from
      static.us_universal_dv_core_dd
  ),
dp_signup as (
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
  and dte >=first_exposed::date - 7
),
dp_adoption as(
  select 
     user_id as consumer_id, start_time
  from dp_signup
  where dashpass_signup >= 1
  group by all
),
  comb as (
    select
      a.tag_renamed,
      a.user_id as consumer_id,
      a.first_exposed,
      --cc.total_cx,
      c.*,
      dpa.consumer_id as dp_sign_up
    from
      be a
      left join core_dd c on c.creator_id = a.user_id
      AND c.CREATED_AT >= a.first_exposed
      --left join cx_cnt cc on cc.tag_renamed = a.tag_renamed  
      left join proddb.mattheitz.mh_customer_authority ca
        on a.user_id=ca.creator_id and a.first_exposed::date = ca.dte::date
      left join dp_adoption dpa on a.user_id = dpa.consumer_id and dpa.start_time::date>=a.first_exposed::date
      group by all
  ),
  pen as (
    select
      tag_renamed as "Bucket",

    count(distinct consumer_id) as "# Cx",
      count(
        distinct case
          when is_filtered_core = 1 then delivery_id
        end
      ) as "Volume",
      "# Cx" / sum("# Cx") over () as "Bucketing %",
      "Volume" / nullif("# Cx", 0) as "Order Rate",
      avg(
        case
          when is_filtered_core = true then delivery_fee / 100.0
        end
      ) as "Gross Delivery Fee",
      avg(
        case
          when is_filtered_core = true then actual_df_paid_by_cx
        end
      ) as "Net Delivery Fee",
      avg(
        case
          when is_filtered_core = true then actual_sf_paid_by_cx
        end
      ) as "Service Fee",
      avg(
        case
          when is_filtered_core = true then subtotal / 100.0
        end
      ) as "Subtotal",
      sum(gov) as GOV,
      sum(gov) / count(
        distinct case
          when fda_is_filtered = 1 then fda_delivery_id
        end
      ) as aov,
      sum(gov) / "# Cx" as gov_adj,
      sum(ue) as vp,
      sum(ue) / count(
        distinct case
          when fda_is_filtered = 1 then fda_delivery_id
        end
      ) as "Unit VP",
      count(distinct dp_sign_up) as "DP Signups"
    from
      comb

    group by all
  )
select
    
  "Bucket",
  "# Cx",
  "Volume",
  "Order Rate",
  "Order Rate" / max(
    case
      when "Bucket" = 'Treatment' then "Order Rate"
    end
  ) over () - 1 as "Order Rate Lift",
    "Volume" - max(
    case
      when "Bucket" = 'Treatment' then "Volume"
    end
  ) over () * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over () as "Volume Delta",
  /*
  "Gross Delivery Fee",
  "Gross Delivery Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Gross Delivery Fee"
    end
  ) over (partition by segment) as "Gross Delivery Fee Delta",
  */
  "Net Delivery Fee",
  "Net Delivery Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Net Delivery Fee"
    end
  ) over () as "Net Delivery Fee Delta",
  /*
  "Service Fee" - max(
    case
      when "Bucket" = 'Treatment' then "Service Fee"
    end
  ) over (partition by segment) as "Service Fee Delta",
  */
  "Unit VP",
  "Unit VP" - max(
    case
      when "Bucket" = 'Treatment' then "Unit VP"
    end
  ) over () as "Unit VP Delta",
    VP,
     vp - max(
    case
      when "Bucket" = 'Treatment' then vp
    end
  ) over () * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over () as "VP Delta",
    "VP Delta" / (
    max(
      case
        when "Bucket" = 'Treatment' then vp
      end
    ) over () * "# Cx" / max(
      case
        when "Bucket" = 'Treatment' then "# Cx"
      end
    ) over ()
  ) as "VP Lift",
  "AOV",
  "AOV" - max(
    case
      when "Bucket" = 'Treatment' then "AOV"
    end
  ) over () as "AOV Delta",
  /*
  "Subtotal",
  "Subtotal" - max(
    case
      when "Bucket" = 'Treatment' then "Subtotal"
    end
  ) over (partition by segment) as "Subtotal Delta",
  */
  gov_adj / max(
    case
      when "Bucket" = 'Treatment' then gov_adj
    end
  ) over () - 1 as "GOV Lift",
  /*
  "Order Rate" - max(
    case
      when "Bucket" = 'Treatment' then "Order Rate"
    end
  ) over (partition by segment) as "Order Rate Delta",
  "Subtotal" / aov as "Subtotal/AOV",
  */
  gov - max(
    case
      when "Bucket" = 'Treatment' then gov
    end
  ) over () * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over () as "GOV Delta",
  - "GOV Delta" / nullif("VP Delta", 0) as "GOV:VP Ratio",
  - "VP Delta" / nullif("Volume Delta", 0) as "(-CPID)/(+GPLD) In Campaign",
   "DP Signups" / nullif("# Cx", 0) as "DP Signup Rate",
    "DP Signup Rate" / max(
    case
      when "Bucket" = 'Treatment' then "DP Signup Rate"
    end
  ) over () - 1 as "DP Signup Rate Lift",
"DP Signups" - max(
    case
      when "Bucket" = 'Treatment' then "DP Signups"
    end
  ) over () * "# Cx" / max(
    case
      when "Bucket" = 'Treatment' then "# Cx"
    end
  ) over () as "DP Signup Gain",
    - "GOV Delta" / nullif("DP Signup Gain", 0) as "GOV:DP Ratio",
    - "Order Rate Lift" / nullif("Net Delivery Fee Delta",0) as "Price Sensitivity"
from pen 


-- Order Rate Time Series

-- Query A: Weekly OR Lift — Overall, fixed cohort first_exposed BETWEEN 2026-03-12 and 2026-04-11
with
  be as (
    select a.*
    from static.us_universal_dv_a_be a
    join proddb.static.wbd_experiment_exposure b
         on a.user_id = b.user_id and b.tag_renamed = 'Treatment'
    join proddb.public.FACT_DYNAMIC_AUDIENCE_WBD_ORDER_FREQUENCY_L365D c
         on c.consumer_id = a.user_id
        and c.injected_date = a.first_exposed::date
    where a.first_exposed::date between '2026-03-12' and '2026-04-11'
    group by all
  ),
  core_dd as (
    select * from static.us_universal_dv_core_dd
  ),
  weeks as (
    select column1 as week_idx
    from (values (0),(1),(2),(3),(4),(5),(6),(7)) v
  ),
  cx_weeks as (
    select
      a.tag_renamed,
      a.user_id as consumer_id,
      w.week_idx,
      dateadd(day, 7 *  w.week_idx,      a.first_exposed::date) as week_start,
      dateadd(day, 7 * (w.week_idx + 1), a.first_exposed::date) as week_end_exclusive
    from be a
    cross join weeks w
  ),
  per_week as (
    select
      cw.tag_renamed as bucket,
      cw.week_idx,
      count(distinct cw.consumer_id) as cx,
      count(distinct case when c.is_filtered_core = 1 then c.delivery_id end) as volume,
      volume / nullif(cx, 0.0) as order_rate
    from cx_weeks cw
    left join core_dd c
      on c.creator_id  = cw.consumer_id
     and c.created_at >= cw.week_start
     and c.created_at <  cw.week_end_exclusive
    group by 1, 2
  )
select
  week_idx,
      max(case when bucket = 'Control'   then order_rate end)
    / max(case when bucket = 'Treatment' then order_rate end) - 1 as "Order Rate Lift"
from per_week
group by week_idx
order by week_idx

-- Query B (pivoted): Weekly OR Lift — by Exposed Tenure as columns
-- Fixed cohort: first_exposed BETWEEN 2026-03-12 and 2026-04-11
with
  be as (
    select a.*, b.first_exposed as old_first_exposed
    from static.us_universal_dv_a_be a
    join proddb.static.wbd_experiment_exposure b
         on a.user_id = b.user_id and b.tag_renamed = 'Treatment'
    join proddb.public.FACT_DYNAMIC_AUDIENCE_WBD_ORDER_FREQUENCY_L365D c
         on c.consumer_id = a.user_id
        and c.injected_date = a.first_exposed::date
    where a.first_exposed::date between '2026-03-12' and '2026-04-11'
    group by all
  ),
  core_dd as (
    select * from static.us_universal_dv_core_dd
  ),
  weeks as (
    select column1 as week_idx
    from (values (0),(1),(2),(3),(4),(5),(6),(7)) v
  ),
  cx_weeks as (
    select
      a.tag_renamed,
      a.user_id as consumer_id,
      case
        when datediff(day, a.old_first_exposed, a.first_exposed) <=   90 then '1. <=3 Months'
        when datediff(day, a.old_first_exposed, a.first_exposed) <=  365 then '2. 3-12 Months'
        when datediff(day, a.old_first_exposed, a.first_exposed) <=  730 then '3. 1-2 Years'
        when datediff(day, a.old_first_exposed, a.first_exposed) <= 1095 then '4. 2-3 Years'
        when datediff(day, a.old_first_exposed, a.first_exposed) <= 1460 then '5. 3-4 Years'
        else                                                                  '6. >4 Years'
      end as tenure_segment,
      w.week_idx,
      dateadd(day, 7 *  w.week_idx,      a.first_exposed::date) as week_start,
      dateadd(day, 7 * (w.week_idx + 1), a.first_exposed::date) as week_end_exclusive
    from be a
    cross join weeks w
  ),
  per_week as (
    select
      cw.tag_renamed as bucket,
      cw.tenure_segment,
      cw.week_idx,
      count(distinct cw.consumer_id) as cx,
      count(distinct case when c.is_filtered_core = 1 then c.delivery_id end) as volume,
      volume / nullif(cx, 0.0) as order_rate
    from cx_weeks cw
    left join core_dd c
      on c.creator_id  = cw.consumer_id
     and c.created_at >= cw.week_start
     and c.created_at <  cw.week_end_exclusive
    group by 1, 2, 3
  ),
  lift_per_segment as (
    select
      tenure_segment,
      week_idx,
          max(case when bucket = 'Control'   then order_rate end)
        / max(case when bucket = 'Treatment' then order_rate end) - 1 as or_lift
    from per_week
    group by tenure_segment, week_idx
  )
select
  week_idx,
  max(case when tenure_segment = '1. <=3 Months'  then or_lift end) as "<=3 Months",
  max(case when tenure_segment = '2. 3-12 Months' then or_lift end) as "3-12 Months",
  max(case when tenure_segment = '3. 1-2 Years'   then or_lift end) as "1-2 Years",
  max(case when tenure_segment = '4. 2-3 Years'   then or_lift end) as "2-3 Years",
  max(case when tenure_segment = '5. 3-4 Years'   then or_lift end) as "3-4 Years",
  max(case when tenure_segment = '6. >4 Years'    then or_lift end) as ">4 Years"
from lift_per_segment
group by week_idx
order by week_idx
