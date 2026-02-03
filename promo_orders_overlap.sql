select 
case when wbd_fee_promo_discount > 0 then 1
else 0 end wbd_ind,
case when cs_fee_promo_discount > 0 then 1
else 0 end xs_ind,
case when pad_fee_promo_discount > 0 then 1
else 0 end pad_ind,
case when mx_funded_cx_discount > 0 then 1
else 0 end mx_ind,
case when b.delivery_id is not null then 1
else 0 end crm_ind,
count(distinct a.delivery_id) orders,
sum(wbd_fee_promo_discount+cs_fee_promo_discount+pad_fee_promo_discount) affordability_program_discount,
sum(mx_funded_cx_discount) mx_funded_discount,
sum(crm_discount) crm_discount,
sum(total_fee_promo_discount) total_discount
from proddb.static.df_sf_promo_discount_delivery_level a
left join 
    (select delivery_id,
    case when campaign_id is not null then coalesce(FDA_OTHER_PROMOTIONS_BASE + FDA_PROMOTION_CATCH_ALL + FDA_CONSUMER_RETENTION - FDA_BUNDLES_PRICING_DISCOUNT, 0) else 0 end AS crm_discount
    from proddb.public.fact_order_discounts_and_promotions_extended
    where crm_discount >0
    and year(active_date) = 2025
    group by 1,2) b
    on a.delivery_id = b.delivery_id
where year(created_at) = 2025
group by 1,2,3,4,5

WBD_IND	XS_IND	PAD_IND	MX_IND	CRM_IND	ORDERS	AFFORDABILITY_PROGRAM_DISCOUNT	MX_FUNDED_DISCOUNT	CRM_DISCOUNT	TOTAL_DISCOUNT
1	0	0	1	0	22733169	46946339.210000	137043471.790000		48209984.320000
0	1	0	1	1	513742	691255.960000	2765163.540000	1097765.746843753740	702631.600000
1	0	0	0	1	9186906	20400844.410000	0.000000	64763922.338667961564	21929067.430000
0	0	1	1	0	4097	11035.150000	25268.350000		11078.660000
1	1	0	1	1	4006	11549.330000	22092.380000	8709.470446288040	11638.190000
0	0	0	1	0	271554574	0.000000	1765048478.870000		48362134.950000
0	1	0	0	1	1168449	1692529.440000	0.000000	7334287.048594647580	1703423.950000
0	1	0	0	0	46870801	57989992.440000	0.000000		58366398.290000
1	0	1	1	1	4075103	13113784.030000	24038160.290000	5251945.372719932559	13205162.510000
0	0	0	1	1	48572704	0.000000	281585100.910000	127045817.840445826748	5270971.350000
0	0	0	0	0	2044388575	0.000000	-1.200000		544286241.590000
0	0	1	1	1	4351047	9834991.170000	25427714.220000	10359049.829269765991	9961905.480000
1	0	1	0	1	920523	3015822.970000	0.000000	6419420.654405513809	3098990.950000
1	1	0	0	1	13333	44002.080000	0.000000	82681.430446457253	44078.700000
1	0	0	1	1	2786450	6231914.550000	14891947.520000	6951600.538672489694	6939344.830000
1	0	1	0	0	19112158	60728652.460000	0.000000		61015018.820000
1	0	1	1	0	2908	9669.230000	18168.870000		9734.180000
1	0	0	0	0	128564772	266806028.920000	0.000000		272863769.000000
0	0	0	0	1	88171860	0.000000	0.000000	638066381.514244836475	67380256.420000
1	1	0	1	0	64316	185315.500000	390601.530000		186367.140000
0	0	1	0	0	23848944	53580226.710000	0.000000		53926514.950000
0	1	0	1	0	7703008	10164014.000000	46397159.930000		10289747.450000
0	0	1	0	1	1068098	2105583.800000	0.000000	8014751.263151119324	2192651.150000
1	1	0	0	0	339900	948254.140000	0.000000		950916.340000

-- 2025 Classical/Restaurant Orders with discount
with non_dp_delivery as
(select dd.delivery_id,nv.business_line
FROM proddb.public.dimension_deliveries dd
LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv 
        ON dd.store_id = nv.store_id AND nv.is_filtered_mp_vertical = 1
WHERE dd.is_filtered_core = TRUE
        -- AND dd.is_consumer_pickup = FALSE -- optional to exclude pickup orders
        AND dd.fulfillment_type NOT IN ('dine_in', 'shipping', 'merchant_fleet', 'virtual') -- excluding dine-in, shipping, merchant fleet, and virtual orders (giftcards)
        AND dd.is_from_store_to_us = FALSE -- excluding store-to-us orders
        AND dd.is_bundle_order = FALSE -- excluding bundle orders -- an optional column to filter out DoubleDash
        --AND nv.business_line IS NULL -- excluding non-restaurant orders -- an optional column to exclude NV
        --AND dd.country_id = 1 -- US only 
        AND year(dd.created_at) =2025
        AND is_subscribed_consumer = FALSE
group by 1,2)

-- Classic Order
select 
count(distinct o.delivery_id) orders,
count(distinct case when wbd_fee_promo_discount+cs_fee_promo_discount+pad_fee_promo_discount+mx_funded_cx_discount+total_fee_promo_discount>0 then o.delivery_id end) discount_orders
--delivery_id, pad_df_promo_discount, mx_funded_cx_discount
from proddb.static.df_sf_promo_discount_delivery_level o
join non_dp_delivery d
on o.delivery_id = d.delivery_id
and year(created_at) = 2025
and business_line IS NULL --restaurant only


-- Classic Order
ORDERS	DISCOUNT_ORDERS
773637344	370271048  48%

-- Classic Restaurant Order

ORDERS	DISCOUNT_ORDERS
677835267	339389384 50%
