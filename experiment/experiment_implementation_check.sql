/*
DV A - Holdout layer - discount_engine_global_holdout_us, control/WBD+XS 5/95
DV B - Fee discounts layer - discount_engine_fee_discounts_us, WBD+XS/WBD+XS+PAD 5/95
DV C - Deals sandbox layer - discount_engine_deals_v1_us
*/

with universal_be as (
select
case when experiment_name = 'discount_engine_global_holdout_us' then 'DV A'
  when experiment_name = 'discount_engine_fee_discounts_us' then 'DV B'
  else 'DV C' end layer,
  experiment_name,
  EXPERIMENT_VERSION version,
    case when tag like 'treatment_wbd_only%' or tag like 'control%' then 'Control' else 'Treatment' end as tag_renamed
  , try_cast(bucket_key as integer) as user_id
  , min(cast(EXPOSURE_TIME as date)) as first_exposed
from PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
where experiment_name in ('discount_engine_global_holdout_us','discount_engine_fee_discounts_us','discount_engine_deals_v1_us')
      and experiment_version >= 2
      and exposure_time >= '2026-02-19'
      and segment = 'Users'
group by all
)
,
discount_orders as (
select 
a.delivery_id,
creator_id user_id,
cast(a.created_at as date) order_dt,
sum(wbd_fee_promo_discount+cs_fee_promo_discount+pad_fee_promo_discount) affordability_program_discount,
sum(wbd_fee_promo_discount) wbd_discount,
sum(cs_fee_promo_discount) xs_discount,
sum(pad_fee_promo_discount) pad_discount
from proddb.static.df_sf_promo_discount_delivery_level a
left join proddb.public.dimension_deliveries b
  on a.delivery_id = b.delivery_id
where cast(a.created_at as date) >= '2026-02-19'
  and cast(b.created_at as date) >= '2026-02-19'
  and (wbd_fee_promo_discount>0 or cs_fee_promo_discount>0 or pad_fee_promo_discount>0)
group by 1,2,3
)

select layer, experiment_name, version,tag_renamed,
count(distinct a.user_id) exposed_users,
count(distinct b.user_id) ordered_with_discount,
count(distinct b.user_id)*1.0000/count(distinct a.user_id) share_of_users_with_discount_order,
count(distinct b.delivery_id) discount_orders,
--sum(affordability_program_discount) affordability_program_discount,
--sum(wbd_discount) wbd_discount,
--sum(xs_discount) xs_discount,
--sum(pad_discount) pad_discount,
sum(affordability_program_discount)*1.0000/count(distinct b.delivery_id) avg_affordability_discount,
sum(wbd_discount)*1.0000/count(distinct case when wbd_discount>0 then b.delivery_id end) avg_wbd_discount,
sum(xs_discount)*1.0000/count(distinct case when xs_discount>0 then b.delivery_id end) avg_xs_discount,
sum(pad_discount)*1.0000/count(distinct case when pad_discount>0 then b.delivery_id end) avg_pad_discount
from universal_be a
left join discount_orders b
  on a.user_id = b.user_id and b.order_dt>= a.first_exposed
group by all
order by all
