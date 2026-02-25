with intermittent_mau as
(select creator_id, active_period_cnt from proddb.public.hv_mau
where as_of_date = date'2026-02-12'
and active_period_cnt<=5
group by all)

, pad_rank as (
select consumer_id, ELIGIBILITY_STATUS, rank() over (partition by consumer_id order by LAST_UPDATED_AT desc) as rnk
from EDW.PAD.affordability_incentive_iq_pad_eligibility_union 
where LAST_UPDATED_AT::date <= date'2026-02-12'
)


, pad_eligible as (
select consumer_id
from pad_rank
where rnk = 1
and ELIGIBILITY_STATUS = 'ELIGIBLE'
group by 1
)

-- Note: PAD eligibility does not exclude DP users
,affordability_eligiliy as ( 
-- WBD Cx eligible on a given date
select  
'wbd' as program, wbd.consumer_id
from proddb.public.FACT_DYNAMIC_AUDIENCE_WBD_ORDER_FREQUENCY_L365D wbd
left join proddb.public.cx_sensitivity_v2 psm on wbd.consumer_id = psm.consumer_id and prediction_datetime_est = injected_date
where injected_date = date'2026-02-12'
and not (cohort = 'p84d_active_very_insensitive' and L365D_ORDER_COUNT >= 13)
and not (cohort = 'p84d_active_insensitive' and L365D_ORDER_COUNT >= 20)
group by all
-- XS Cx eligible on a given date
union all
select 'xs' as program, consumer_id
from edw.pad.cross_shopper_daily_snapshot_cross_shopper_customer_v3_daily_snapshot
where import_date = date'2026-02-12'
group by all
-- PAD Cx eligible on a given date
union all
select 'PAD' as program, consumer_id
from pad_eligible
group by all
)

select count(distinct a.creator_id) intermittent_mau,
count(distinct b.consumer_id) covered_by_affordability
from intermittent_mau a
left join affordability_eligiliy b
on a.creator_id = b.consumer_id

INTERMITTENT_MAU	COVERED_BY_AFFORDABILITY
55761781	45618235  82%


-- By Price Sensitivity
select cohort, count(distinct a.creator_id) intermittent_mau,
count(distinct b.consumer_id) covered_by_affordability
from intermittent_mau a
left join affordability_eligiliy b
on a.creator_id = b.consumer_id
left join proddb.public.cx_sensitivity_v2 psm on a.creator_id = psm.consumer_id and prediction_datetime_est = date'2026-02-12'
group by 1
order by 1

COHORT	INTERMITTENT_MAU	COVERED_BY_AFFORDABILITY
dp_active_p84d_active_insensitive	1500381	447103
dp_active_p84d_active_middle	4441881	1955292
dp_active_p84d_active_sensitive	5870159	2621864
dp_active_p84d_churned	1549347	844465
p84d_active_insensitive	4178878	3389403
p84d_active_middle	5973837	5583021
p84d_active_sensitive	6377079	6170783
p84d_active_very_insensitive	1414062	566172
p84d_active_very_sensitive	12121908	11796097
p84d_churned	12247272	12198845
null	86977	45190

create or replace table proddb.katez.consumer_orders
as
(select dd.delivery_id, dd.creator_id consumer_id,created_at::date order_dt
FROM proddb.public.dimension_deliveries dd
LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv 
        ON dd.store_id = nv.store_id AND nv.is_filtered_mp_vertical = 1
WHERE dd.is_filtered_core = TRUE
        -- AND dd.is_consumer_pickup = FALSE -- optional to exclude pickup orders
        AND dd.fulfillment_type NOT IN ('dine_in', 'shipping', 'merchant_fleet', 'virtual') -- excluding dine-in, shipping, merchant fleet, and virtual orders (giftcards)
        AND dd.is_from_store_to_us = FALSE -- excluding store-to-us orders
        AND dd.is_bundle_order = FALSE -- excluding bundle orders -- an optional column to filter out DoubleDash
        AND nv.business_line IS NULL -- restaurant orders 
        --AND dd.country_id = 1 -- US only 
        AND dd.created_at::date >= date'2022-08-01'
        AND is_subscribed_consumer = FALSE
group by 1,2,3
)
        
select min(L28D_weekly_orders), max(L28D_weekly_orders),
min(L180D_weekly_orders), max(L180D_weekly_orders),
min(ratio),max(ratio),
min(diff), max(diff)
from proddb.katez.intermittent_of_021226

MIN(L28D_WEEKLY_ORDERS)	MAX(L28D_WEEKLY_ORDERS)	MIN(L180D_WEEKLY_ORDERS)	MAX(L180D_WEEKLY_ORDERS)	MIN(RATIO)	MAX(RATIO)	MIN(DIFF)	MAX(DIFF)
0.000000	56.250000	0.000000	33.288892	0.000000000000	6.428635714929	-31.150000	46.061108

-- By Order Frequency Delta (orders_per_week_last_28d - orders_per_week_last_180d）
select 
case when diff = 0 then '0'
when diff > 0 and diff <5 then cast(ceil(diff) as varchar)
when diff < 0 and diff >-5 then cast(floor(diff) as varchar)
when diff>=5 then '>=5'
when diff<=-5 then '<=-5'
end delta,
sum(intermittent_mau), sum(covered_by_affordability) 
from proddb.katez.intermittent_of_021226
group by 1
order by 1

DELTA	SUM(INTERMITTENT_MAU)	SUM(COVERED_BY_AFFORDABILITY)
-1	28391716	27501035
-2	343152	236236
-3	61590	34538
-4	18128	8471
-5	7433	2764
0	377	87
1	12666139	11488720
2	647970	384106
3	114674	36440
4	32682	7057
5	12107	1919
<=-5	6719	1754
>=5	10349	1194
