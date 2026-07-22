CREATE OR REPLACE TABLE yingxie.exposed_cx_crm_late_bloomers AS
SELECT e.campaign_analyzer_id
     , CASE
           WHEN e.campaign_country = 'AU'
               THEN 'AUS'
           WHEN e.campaign_country = 'CA'
               THEN 'CAN'
           ELSE e.campaign_country END AS campaign_country_final
     , CASE
           WHEN e.campaign_vertical = 'Convenience 3P'
               THEN 'convenience'
           ELSE campaign_vertical END AS campaign_vertical_final
     , CONCAT(campaign_country_final, '-', campaign_vertical_final, '-', business_campaign_name) AS campaign_key
     , e.campaign_name
     , e.consumer_bucket
     , e.consumer_id
     , is_control_flg
     , MIN(e.start_time_derived::DATE) AS start_time_derived
     , MIN(e.cohort_week_date::DATE) AS cohort_week_date_start
     , MIN(e.end_time_derived::DATE) AS end_time_derived
FROM edw.consumer.campaign_analyzer_exposures e
WHERE campaign_name = 'ep_consumer_dormant_late_bloomers_us_v1_t1'
  AND campaign_country_final = 'US'
GROUP BY ALL
;

with be as (
select 
case when wbd.tag_renamed = 'Control' then 'Control'
     when wbd.tag_renamed = 'Treatment' then 'Treatment'
end as tag_renamed
, first_exposed
, start_time_derived
, end_time_derived
, user_id
from yingxie.exposed_cx_crm_late_bloomers crm
join static.wbd_experiment_exposure wbd on crm.consumer_id = wbd.user_id
where start_time_derived between '2024-12-15' and '2024-12-15'::date + 90
and is_control_flg = 0
)
, service_fee_promo_discounts as(    
  select 
    order_cart_id,
    sum(amount/100) as sf_discount_amount
  from public.maindblocal_order_cart_discount_component
  where monetary_field = 'service_fee'
    and created_at::date >= '2024-08-19'::date - 7
    and ("GROUP" != 'subscription')
  group by 1
)
, delivery_fee_promo_discounts as(    
  select 
    order_cart_id,
    sum(amount/100) as df_discount_amount
  from public.maindblocal_order_cart_discount_component
  where monetary_field = 'delivery_fee'
    and created_at::date >= '2024-08-19'::date - 7
    and ("GROUP" != 'subscription')
  group by 1
)
, wbd as (
  select 
    order_cart_id,
    sum(amount) / 100.0 AS total_promo_amount
  from proddb.public.maindblocal_order_cart_discount_component ocdc
  where monetary_field = 'delivery_fee'
  and ocdc.created_at >= DATEADD(day, -7, '2024-11-08') 
  and ocdc."GROUP" = 'welcome_back_discount'
  and lower(status) ='applied'
  group by 1
)
, core_dd as (
    select
    is_subscribed_consumer::int as dashpass
  , is_consumer_pickup::int as pickup
  , convert_timezone('UTC','America/Los_Angeles',dd.created_at) as created_at_pst
  , coalesce(dfd.df_discount_amount, 0) as df_discount_amount_use
  , greatest(dd.delivery_fee/100.0 - df_discount_amount_use, 0) as actual_df_paid_by_cx
  , coalesce(sfd.sf_discount_amount, 0) as sf_discount_amount_use
  , greatest(dd.service_fee/100.0 - sf_discount_amount_use, 0) as actual_sf_paid_by_cx
  , dd.DELIVERY_FEE 
  , dd.GOV
  , dd.SUBTOTAL
  , dd.service_fee 
  , dd.small_order_fee 
  , dd.LEGISLATIVE_FEE 
  , dd.expand_range_fee
  , case when nv.store_id is not null then 1 else 0 end as nv
  , nv.store_id as nv_store_id
  , dd.ORDER_CART_ID
  , dd.DELIVERY_ID 
  , dd.CREATOR_ID 
  , dd.STORE_ID
  , dd.CREATED_AT
  , fda.Variable_Profit_Ex_Alloc as ue
  , fdd.straightline_r2c_distance/1609.34 as r2c
  , dd.fulfillment_type
  , dd.IS_BUNDLE_ORDER
  , dd.SUBSCRIPTION_ALLOC
  , dd.tip
  , dd.delivery_rating
  , dd.NET_REVENUE
  , dd.SUBTOTAL_ADJUSTED
  , dd.is_group_order
  , dd.BUSINESS_LINE
  , dd.SUBMARKET_ID
  , dd.SUBMARKET_NAME
  , dd.IS_SUBSCRIPTION_DISCOUNT_APPLIED::int as dashpass_eligible
  , case when nv.store_id is null then coalesce(wbd.total_promo_amount, 0) else 0 end as wbd_promo
  from edw.finance.dimension_deliveries dd
  left join fact_delivery_allocation fda on dd.delivery_id = fda.delivery_id
  left join edw.cng.dimension_new_vertical_store_tags as nv on dd.store_id = nv.store_id and nv.is_filtered_mp_vertical = 1
  left join delivery_fee_promo_discounts dfd on dd.order_cart_id = dfd.order_cart_id
  left join service_fee_promo_discounts sfd on dd.order_cart_id = sfd.order_cart_id
  left join public.fact_delivery_distances fdd on fdd.delivery_id = dd.delivery_id
  left join wbd on dd.order_cart_id = wbd.order_cart_id
where dd.is_filtered_core = True 
 and created_at_pst < current_date
 and dd.created_at >= '2024-12-15'
 and dd.country_id = 1
)
, cx_cnt as (
select 
  be.tag_renamed, 
  count(distinct be.user_id) as total_cx
from be
group by 1
)
, comb as(
select 
  a.tag_renamed
, a.user_id as consumer_id
, cc.total_cx
, c.*
from be a 
left join core_dd c on c.creator_id = a.user_id AND c.created_at between start_time_derived and end_time_derived
left join cx_cnt cc on cc.tag_renamed = a.tag_renamed
)
  
, pen as(
  select 
    tag_renamed as "Bucket"
  , total_cx as "# Cx"
  , count(distinct delivery_id) as "Volume"
  , "# Cx" / sum("# Cx") over () as "Bucketing %"
  , "Volume"/nullif("# Cx",0) as "Order Rate"
  , count(distinct case when delivery_id is not null then consumer_id end) as "Converted Cx"
  , avg(actual_df_paid_by_cx) as "Actual Delivery Fee Paid by Cx"
  , avg(delivery_fee/100.0) as "Pre Promo DF"
  , avg(actual_sf_paid_by_cx) as "Service Fee"
  , avg(subtotal/100.0) as "Subtotal"
  , avg(gov/100.0) as aov
  , sum(gov/100.0) as GOV
  , sum(gov/100.0) / "# Cx" as gov_adj
  , avg(ue) as "Unit VP"
  , sum(ue) as vp
  , "Order Rate" * "Unit VP" as "Net VP Cx Level"
  , "Unit VP" * "Volume" / "# Cx" as vp_adj
  , sum(wbd_promo) as "WBD Spending"
  from comb 
  group by 1, 2
  order by 1
)
select 
  "Bucket"
, "# Cx"
, "Volume"
, "Order Rate" / max(case when "Bucket" = 'Control' then "Order Rate" end) over () - 1 as "Order Rate Lift" 
, "WBD Spending"
, gov - max(case when "Bucket" = 'Control' then gov end) over () * "# Cx" /max(case when "Bucket" = 'Control' then "# Cx" end) over () as "GOV Delta" 
, "Volume" - max(case when "Bucket" = 'Control' then "Volume" end) over () * "# Cx"/ max(case when "Bucket" = 'Control' then "# Cx" end) over () as "Volume Delta" 
, vp - max(case when "Bucket" = 'Control' then vp end) over () * "# Cx"/ max(case when "Bucket" = 'Control' then "# Cx" end) over () as "VP Delta" 
, "Net VP Cx Level" - max(case when "Bucket" = 'Control' then "Net VP Cx Level" end) over () as "Net VP Delta"
, "Order Rate" - max(case when "Bucket" = 'Control' then "Order Rate" end) over () as "Order Rate Delta" 
, -"Net VP Delta" / nullif("Order Rate Delta", 0) as "(-CPID)/(+GPLD) In Campaign"
, "Unit VP"
, "Actual Delivery Fee Paid by Cx"
from pen
order by 1
;
