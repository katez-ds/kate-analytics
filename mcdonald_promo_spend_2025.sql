-- McDonald Biz ID 5579
-- how much DD spends on McD's promo campaigns -- whether co-funded alongside the Mx or solely DD funded (i.e., Cx facing promos that get redeemed on McD's)

    
    select
    sum(wbd_fee_promo_discount+cs_fee_promo_discount+pad_fee_promo_discount) affordability_program_discount,
    sum(mx_funded_cx_discount) mx_funded_discount,
    sum(crm_discount) crm_discount
    from proddb.public.dimension_deliveries b 
    left join proddb.static.df_sf_promo_discount_delivery_level a
      on a.delivery_id = b.delivery_id
    left join 
        (select delivery_id,
        case when campaign_id is not null then coalesce(FDA_OTHER_PROMOTIONS_BASE + FDA_PROMOTION_CATCH_ALL + FDA_CONSUMER_RETENTION - FDA_BUNDLES_PRICING_DISCOUNT, 0) else 0 end AS crm_discount
        from proddb.public.fact_order_discounts_and_promotions_extended
        where crm_discount >0
        and year(active_date) = 2025
        group by 1,2) c
        on b.delivery_id = c.delivery_id
    where
      1 = 1
      and b.created_at:: date between '2025-01-01' and '2025-12-31'
      and b.created_at:: date between '2025-01-01' and '2025-12-31'
      and country_id = 1
      and business_id = 5579

AFFORDABILITY_PROGRAM_DISCOUNT	MX_FUNDED_DISCOUNT	CRM_DISCOUNT
66014232.640000	286931195.430000	35207169.713421703436


-- Diane's query for CRM
SELECT sum(coalesce(CASE WHEN sub_transaction_funding_entity_type IN ('SUB_TRANSACTION_FUNDED_ENTITY_TYPE_DOORDASH') THEN discount_subsidy_local END, 0)) / 100 AS doordash_funded_consumer_discount_local

FROM proddb.public.dimension_deliveries AS b
LEFT JOIN edw.ads.fact_promo_order_redemption AS fpor
    ON b.delivery_id = fpor.delivery_id
WHERE
    TRUE
    AND b.created_at::date BETWEEN '2025-01-01' AND '2025-12-31'
    AND b.created_at::date BETWEEN '2025-01-01' AND '2025-12-31'
    AND b.country_id = 1
    AND b.business_id = 5579
