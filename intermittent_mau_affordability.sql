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
	86977	45190
