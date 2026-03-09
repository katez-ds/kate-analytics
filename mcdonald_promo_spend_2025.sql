-- McDonald Biz ID 5579
-- how much DD spends on McD's promo campaigns -- whether co-funded alongside the Mx or solely DD funded (i.e., Cx facing promos that get redeemed on McD's)

with  discount_orders as (
    select
      a.delivery_id,
      creator_id user_id,
      cast(a.created_at as date) order_dt,
      sum(
        wbd_fee_promo_discount + cs_fee_promo_discount + pad_fee_promo_discount
      ) affordability_program_discount,
      --sum(wbd_fee_promo_discount + cs_fee_promo_discount) wbd_xs_discount,
      --sum(pad_fee_promo_discount) pad_discount
    from proddb.public.dimension_deliveries b 
      left join proddb.static.df_sf_promo_discount_delivery_level a
      on a.delivery_id = b.delivery_id
    where
      1 = 1
      and order_dt between '2025-01-01' and '2025-12-31'
      and b.created_at:: date between '2025-01-01' and '2025-12-31'
      and country_id = 1
    group by
      1,
      2,
      3
  )
select --experiment_name, version,tag,
  order_dt,
  count(distinct b.delivery_id) discount_orders,
  sum(wbd_xs_discount) wbd_xs_discount,
  sum(pad_discount) pad_discount,
  sum(wbd_xs_discount + pad_discount) total_discount,
  sum(wbd_xs_discount + pad_discount) * 1.0000 / count(distinct b.delivery_id) avg_promo_spend,
  sum(wbd_xs_discount) * 1.0000 / count(
    distinct case
      when wbd_xs_discount > 0 then b.delivery_id
    end
  ) avg_wbd_xs_promo_spend,
  sum(pad_discount) * 1.0000 / count(
    distinct case
      when pad_discount > 0 then b.delivery_id
    end
  ) avg_pad_promo_spend,
  count(
    distinct case
      when wbd_xs_discount > 0 then b.delivery_id
    end
  ) wbd_xs_orders,
  count(
    distinct case
      when pad_discount > 0 then b.delivery_id
    end
  ) pad_orders
from
  discount_orders b
  left join universal_be a on a.user_id = b.user_id
  and b.order_dt >= a.first_exposed
group by all
order by
  all
