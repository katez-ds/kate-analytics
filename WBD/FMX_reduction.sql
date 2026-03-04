create or replace table proddb.katez.fmx_exposures
as
(select distinct
  e.bucket_key as consumer_id,
  e.exposure_time,
  tag,
  result,
  segment,
  dateadd(day, 30, e.exposure_time) as fmx_end_time
from PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE e
where experiment_name = 'fmx_core_challenges_test_q225'
  and exposure_time::date >= '2025-12-01')


create or replace table proddb.katez.fmx_orders
as
  (select
    f.consumer_id,
    f.exposure_time,
    d.delivery_id,
    d.created_at,
    row_number() over (partition by f.consumer_id order by d.created_at) as order_number,
    d.delivery_fee / 100.0 as base_delivery_fee,
    coalesce(p.wbd_fee_promo_discount+p.cs_fee_promo_discount, 0) as wbd_xs_discount,
    greatest(base_delivery_fee - wbd_xs_discount, 0) as actual_df_paid
  from proddb.public.dimension_deliveries d
  inner join proddb.katez.fmx_exposures f
    on d.creator_id = f.consumer_id
    and d.created_at between f.exposure_time and f.fmx_end_time
    and d.is_filtered_core = true 
  left join proddb.static.df_sf_promo_discount_delivery_level p
    on d.delivery_id = p.delivery_id
  where d.created_at::date >= '2025-12-01'
  )
;

select
  count(*) as total_fmx_orders,
  avg(iff(wbd_xs_discount > 0, 1, 0)) as pct_with_wbd_xs_discount,
  avg(iff(wbd_xs_discount = 0 and actual_df_paid = 0, 1, 0)) as pct_no_discount_and_no_df,
  
  avg(wbd_xs_discount) as avg_wbd_xs_discount_overall,
  avg(iff(wbd_xs_discount > 0, wbd_xs_discount, null)) as avg_wbd_xs_discount_when_discounted,
  avg(iff(wbd_xs_discount > 0, actual_df_paid, null)) as avg_delivery_fee_when_discounted,

  avg(actual_df_paid) as avg_df,
  avg(iff(wbd_xs_discount = 0, actual_df_paid, null)) as avg_df_when_no_discount,
  avg(iff(wbd_xs_discount = 0, iff(actual_df_paid = 0, 1, 0), null)) as pct_no_df_when_no_discount,
from fmx_orders
;

select
  iff(order_number >= 10, 10, order_number) as order_number,
  count(*) as total_fmx_orders,
  avg(iff(wbd_xs_discount > 0, 1, 0)) as pct_with_wbd_xs_discount,
  avg(iff(wbd_xs_discount = 0 and actual_df_paid = 0, 1, 0)) as pct_no_discount_and_no_df,

  avg(iff(wbd_xs_discount > 0, wbd_xs_discount, null)) as avg_wbd_xs_discount_when_discounted,
  avg(iff(wbd_xs_discount > 0, actual_df_paid, null)) as avg_delivery_fee_when_discounted
from fmx_orders
group by 1
order by 1
;

select
  datediff(day, exposure_time, created_at) as days_since_exposure,
  count(*) as total_fmx_orders,
  avg(iff(wbd_xs_discount > 0, 1, 0)) as pct_with_wbd_xs_discount,
  avg(iff(wbd_xs_discount = 0 and actual_df_paid = 0, 1, 0)) as pct_no_discount_and_no_df,

  avg(wbd_xs_discount) as avg_wbd_xs_discount_overall,
  avg(iff(wbd_xs_discount > 0, wbd_xs_discount, null)) as avg_wbd_xs_discount_when_discounted,
  avg(iff(wbd_xs_discount > 0, actual_df_paid, null)) as avg_delivery_fee_when_discounted,

  avg(actual_df_paid) as avg_df,
  avg(iff(wbd_xs_discount = 0, actual_df_paid, null)) as avg_df_when_no_discount,
  avg(iff(wbd_xs_discount = 0, iff(actual_df_paid = 0, 1, 0), null)) as pct_no_df_when_no_discount,

from fmx_orders
group by 1
order by 1
;

-- For FMX orders with WBD discount, what's the distribution of WBD+XS discounts

select
  datediff(day, exposure_time, created_at) as days_since_exposure,
  count(*) as total_fmx_orders,
  avg(iff(wbd_xs_discount > 0, 1, 0)) as pct_with_wbd_xs_discount,
  avg(iff(wbd_xs_discount = 0 and actual_df_paid = 0, 1, 0)) as pct_no_discount_and_no_df,

  avg(wbd_xs_discount) as avg_wbd_xs_discount_overall,
  avg(iff(wbd_xs_discount > 0, wbd_xs_discount, null)) as avg_wbd_xs_discount_when_discounted,
  avg(iff(wbd_xs_discount > 0, actual_df_paid, null)) as avg_delivery_fee_when_discounted,

  avg(actual_df_paid) as avg_df,
  avg(iff(wbd_xs_discount = 0, actual_df_paid, null)) as avg_df_when_no_discount,
  avg(iff(wbd_xs_discount = 0, iff(actual_df_paid = 0, 1, 0), null)) as pct_no_df_when_no_discount,

from fmx_orders
where wbd_xs_discount>0
group by 1
order by 1
