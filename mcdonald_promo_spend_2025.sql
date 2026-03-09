-- McDonald Biz ID 5579
-- how much DD spends on McD's promo campaigns -- whether co-funded alongside the Mx or solely DD funded (i.e., Cx facing promos that get redeemed on McD's)

sum(wbd_fee_promo_discount+cs_fee_promo_discount+pad_fee_promo_discount) affordability_program_discount,
sum(mx_funded_cx_discount) mx_funded_discount,
sum(crm_discount) crm_discount,

    
    select
    sum(wbd_fee_promo_discount+cs_fee_promo_discount+pad_fee_promo_discount) affordability_program_discount,
    sum(mx_funded_cx_discount) mx_funded_discount,
    sum(crm_discount) crm_discount
    from proddb.public.dimension_deliveries b 
      left join proddb.static.df_sf_promo_discount_delivery_level a
      on a.delivery_id = b.delivery_id
    where
      1 = 1
      and order_dt between '2025-01-01' and '2025-12-31'
      and b.created_at:: date between '2025-01-01' and '2025-12-31'
      and country_id = 1
      and business_id = 5579
