-- Distribution of WBD DF promo discount dollars in June 2026, US only
select
  bucket_order,
  discount_bucket,
  count(distinct delivery_id) as discounted_deliveries,
  round(sum(wbd_df_promo_discount), 2) as total_wbd_df_discount_usd,
  round(avg(wbd_df_promo_discount), 2) as avg_wbd_df_discount_usd,
  round(
    count(distinct delivery_id) / nullif(sum(count(distinct delivery_id)) over (), 0),
    4
  ) as pct_of_discounted_deliveries
from
  (
    select
      dfp.delivery_id,
      dfp.wbd_df_promo_discount,
      case
        when dfp.wbd_df_promo_discount < 1 then 1
        when dfp.wbd_df_promo_discount < 2 then 2
        when dfp.wbd_df_promo_discount < 3 then 3
        when dfp.wbd_df_promo_discount < 4 then 4
        when dfp.wbd_df_promo_discount < 5 then 5
        when dfp.wbd_df_promo_discount < 10 then 6
        else 7
      end as bucket_order,
      case
        when dfp.wbd_df_promo_discount < 1 then '$0.00-$0.99'
        when dfp.wbd_df_promo_discount < 2 then '$1.00-$1.99'
        when dfp.wbd_df_promo_discount < 3 then '$2.00-$2.99'
        when dfp.wbd_df_promo_discount < 4 then '$3.00-$3.99'
        when dfp.wbd_df_promo_discount < 5 then '$4.00-$4.99'
        when dfp.wbd_df_promo_discount < 10 then '$5.00-$9.99'
        else '$10.00+'
      end as discount_bucket
    from
      proddb.static.df_sf_promo_discount_delivery_level dfp
      inner join proddb.public.dimension_deliveries dd on dfp.delivery_id = dd.delivery_id
    where
      dfp.created_at::date between '2026-06-01' and '2026-06-30'
      and dfp.wbd_df_promo_discount > 0
      and dd.country_id = 1
  ) x
group by
  1,
  2
order by
  1;
BUCKET_ORDER	DISCOUNT_BUCKET	DISCOUNTED_DELIVERIES	TOTAL_WBD_DF_DISCOUNT_USD	AVG_WBD_DF_DISCOUNT_USD	PCT_OF_DISCOUNTED_DELIVERIES
1	$0.00-$0.99	5120019	3081453.73	0.6	0.3123
2	$1.00-$1.99	4677416	6940245.83	1.48	0.2853
3	$2.00-$2.99	3813014	9011681.44	2.36	0.2326
4	$3.00-$3.99	2163525	7381583.68	3.41	0.132
5	$4.00-$4.99	507648	2137017.43	4.21	0.031
6	$5.00-$9.99	113938	569690	5	0.0069


-- Compare gross vs subscription-adjusted delivery fee for US classic Rx orders in June 2026
select
  avg(delivery_fee) / 100.0 as avg_gross_delivery_fee_usd,
  avg(dd.delivery_fee) / 100.0 as avg_subscription_adjusted_delivery_fee_usd
from
  edw.finance.dimension_deliveries dd
where
  dd.active_date between '2026-06-01' and '2026-06-30'
  and dd.country_id = 1
  and dd.is_filtered_core = true
  and dd.is_rx = true
  and dd.is_subscribed_consumer = false;

$2.47
