with  discount_orders as (
    select
      a.delivery_id,
      creator_id user_id,
      cast(a.created_at as date) order_dt,
      sum(
        wbd_fee_promo_discount + cs_fee_promo_discount + pad_fee_promo_discount
      ) affordability_program_discount,
      sum(wbd_fee_promo_discount) wbd_discount,
      sum(cs_fee_promo_discount) xs_discount,
      sum(pad_fee_promo_discount) pad_discount
    from
      proddb.static.df_sf_promo_discount_delivery_level a
      left join proddb.public.dimension_deliveries b on a.delivery_id = b.delivery_id
    where
      1 = 1
      and cast(a.created_at as date) between '2025-02-12' and '2025-02-25'
      and cast(b.created_at as date) between '2025-02-12' and '2025-02-25'
      and (
        wbd_fee_promo_discount > 0
        or cs_fee_promo_discount > 0
        or pad_fee_promo_discount > 0
      )
      and country_id = 1
    group by
      1,
      2,
      3
  )
select --experiment_name, version,tag,
  order_dt,
  count(distinct b.delivery_id) discount_orders,
  sum(wbd_discount) wbd_discount,
  sum(xs_discount) xs_discount,
  sum(pad_discount) pad_discount,
  sum(wbd_discount + xs_discount + pad_discount) total_discount,
  sum(wbd_discount + xs_discount + pad_discount) * 1.0000 / count(distinct b.delivery_id) avg_promo_spend,
  sum(wbd_discount) * 1.0000 / count(
    distinct case
      when wbd_discount > 0 then b.delivery_id
    end
  ) avg_wbd_promo_spend,
  sum(xs_discount) * 1.0000 / count(
    distinct case
      when xs_discount > 0 then b.delivery_id
    end
  ) avg_xs_promo_spend,
  sum(pad_discount) * 1.0000 / count(
    distinct case
      when pad_discount > 0 then b.delivery_id
    end
  ) avg_pad_promo_spend,
  count(
    distinct case
      when wbd_discount > 0 then b.delivery_id
    end
  ) wbd_orders,
  count(
    distinct case
      when xs_discount > 0 then b.delivery_id
    end
  ) xs_orders,
  count(
    distinct case
      when pad_discount > 0 then b.delivery_id
    end
  ) pad_orders
from
  discount_orders b
group by all
order by
  all
