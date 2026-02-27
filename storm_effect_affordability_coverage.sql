-- User status cohort as of 2/25
with dp as
(select 
  consumer_id, dte
FROM edw.consumer.fact_consumer_subscription__daily dsa
LEFT JOIN
  proddb.static.dashpass_annual_plan_ids b 
  ON dsa.consumer_subscription_plan_id = b.consumer_subscription_plan_id
where 1=1--is_new_subscription_date = TRUE
  and COUNTRY_ID_SUBSCRIBED_FROM = 1
  and dsa.consumer_subscription_plan_id != 10002416
  --and dsa.subscription_status != 'cancelled_subscription_creation_failed'
  and ((is_in_paid_balance = TRUE and billing_period is not null)
	or is_in_trial_balance = TRUE)
  and dte = '2025-02-25'
group by all
),

Select case when dp.consumer_id is not null then 'DP'
       when ca.days_since_first_purchase < 30 then 'New Cx'
       when days_since_last_purchase > 90 then 'Churned'
       when days_since_last_purchase between 29 and 90 then 'Dormant'
       --when ca.l28_orders between 1 and 2 then 'Occasional'
       --when ca.l28_orders between 3 and 4 then 'Habituating'
       --when ca.l28_orders >= 5 then 'RDP'
       when days_since_last_purchase < 29 then 'Active'
   end as segment,
count(distinct creator_id) users
from proddb.mattheitz.mh_customer_authority ca
left join dp on dp.consumer_id=ca.creator_id
WHERE ca.dte = '2026-02-25'
group by 1
order by 2 desc

Cx Segment USERS
Churned   161,333,525
DP   19,334,484
Dormant   16,891,175
Occasional   14,921,597
RDP   7,268,444
Habituating
5,119,834
New Cx
1,987,278


-- User status cohort as of 2/25, and last order in storm submarkets
create or replace table proddb.katez.storm_cohort
as 
(
with dp as
(select 
  consumer_id, dte
FROM edw.consumer.fact_consumer_subscription__daily dsa
LEFT JOIN
  proddb.static.dashpass_annual_plan_ids b 
  ON dsa.consumer_subscription_plan_id = b.consumer_subscription_plan_id
where 1=1--is_new_subscription_date = TRUE
  and COUNTRY_ID_SUBSCRIBED_FROM = 1
  and dsa.consumer_subscription_plan_id != 10002416
  --and dsa.subscription_status != 'cancelled_subscription_creation_failed'
  and ((is_in_paid_balance = TRUE and billing_period is not null)
	or is_in_trial_balance = TRUE)
  and dte = '2025-02-25'
group by all
)

Select case when dp.consumer_id is not null then 'DP'
       when ca.days_since_first_purchase < 30 then 'New Cx'
       when days_since_last_purchase > 90 then 'Churned'
       when days_since_last_purchase between 29 and 90 then 'Dormant'
       --when ca.l28_orders between 1 and 2 then 'Occasional'
       --when ca.l28_orders between 3 and 4 then 'Habituating'
       --when ca.l28_orders >= 5 then 'RDP'
       when days_since_last_purchase < 29 then 'Active'
   end as segment,
ca.creator_id
from proddb.mattheitz.mh_customer_authority ca
left join dp on dp.consumer_id=ca.creator_id
join proddb.public.dimension_deliveries dd  -- last order in storm submarkets
  on ca.creator_id = dd.creator_id
  and ca.prior_delivery_id = dd.delivery_id
  and dd.country_id = 1
  and dd.submarket_id in (583,237,13,57,237,13,302,237,1146,57,299,576,13,30,81,1146,237,337,55,200,68,4,1898,1208,1071,1214,1060,884,800,894,892,827,629,1059,1193,86,85,864,1196,1194,1082,497,1137,879,810,829,581,1058,1081,1164,582,763,896,900,1130,2052,234,579,1687,84,577,1820,1073,77,796,811,1139,1262,9,1173,1165,906,802,1166,1207,78,1856,236,865,767,819,80,1233,1150,1160,914,831,100,344,342,34,1392,1169,1072,1212,1688,1148,58,1289,1074,1201,806,1232,1234,1391,1219,2048,1252,1244,870,807,1161,1134,1591,1158,1145,893,1129,809,897,815,868,913,567,595,876,801,571,336,332,74,1238,1213,808,79,59,1063,1241,1144,1155,662,1067,1171,860,877,871,38,31,578,601,600,98,97,1084,1200,1080,907,871,1146,303,7,70,63,57,76,75,17,7347,304,72,8,1590,1235,1210,663,1355,1616,1202,1521,1125,1136,880,1162,761,899,1163,830,866,576,898,1124,873,803,765,888,795,883,569,568,575,564,64,65,565,563,602,71,299,96,81,134,30,20,54,5,1627,1861,1541,9167,305,1247,1249,1608,2085,2116,904,1025,1198,1203,585,875,580,911,874,862,903,1061,959,905,1217,926,302,307,583,301,62,237,73,99,25,23,29,570,36,594,574,1064,308,1079,1209,1390,574,17,8,1152,1229,1220,29,99,36,305,334,587,590,592,570,596,597,801,586,902,1135,909,891,905,1077,1217,1230,1167,1215,9167,1392,1609,1078,1159,865,1161,1229,1990,802,332,1261,889,1170,1149,1490,1795,1769,1760,1757,1784,5369,5380,5374,5373,5393,5566,9415,9492,9527,9536,9553,9582,1785,5400,9537,9562,304,8,72,1248,1148,3789,1078,1125,237,819,57,30,576,795,890,1616,1590,801,830,761,1247,875,81,1166,299,663,302,1146,866,1235,565,1025,576,1235,1025,1868,1146,1125,795,1166,586,1868,5,662,583,96,1170,17
,73,1520,6111,7544,1251,9032,8,2225,883,7347,8510,8950,65,6003,8952,70,1390,1908,6108,8506,8623,320072,17,1203,6175,75,888,63,2085,5737,6002,6004,8953,8454,64,304,62,1521,6001,1610,5140,96,5758,765,899,1124,5,303,574,898,2224,5862,1202,134,1615,6548,8951,602,575,71,72,583,5170
)  --need to update
WHERE ca.dte = '2026-02-25'
group by all
)

-- (Benchmark) User status cohort as of 2/25, and last order NOT in storm submarkets
create or replace table proddb.katez.storm_cohort
as 
(
with dp as
(select 
  consumer_id, dte
FROM edw.consumer.fact_consumer_subscription__daily dsa
LEFT JOIN
  proddb.static.dashpass_annual_plan_ids b 
  ON dsa.consumer_subscription_plan_id = b.consumer_subscription_plan_id
where 1=1--is_new_subscription_date = TRUE
  and COUNTRY_ID_SUBSCRIBED_FROM = 1
  and dsa.consumer_subscription_plan_id != 10002416
  --and dsa.subscription_status != 'cancelled_subscription_creation_failed'
  and ((is_in_paid_balance = TRUE and billing_period is not null)
	or is_in_trial_balance = TRUE)
  and dte = '2025-02-25'
group by all
)

Select case when dp.consumer_id is not null then 'DP'
       when ca.days_since_first_purchase < 30 then 'New Cx'
       when days_since_last_purchase > 90 then 'Churned'
       when days_since_last_purchase between 29 and 90 then 'Dormant'
       --when ca.l28_orders between 1 and 2 then 'Occasional'
       --when ca.l28_orders between 3 and 4 then 'Habituating'
       --when ca.l28_orders >= 5 then 'RDP'
       when days_since_last_purchase < 29 then 'Active'
   end as segment,
ca.creator_id
from proddb.mattheitz.mh_customer_authority ca
left join dp on dp.consumer_id=ca.creator_id
join proddb.public.dimension_deliveries dd  -- last order in storm submarkets
  on ca.creator_id = dd.creator_id
  and ca.prior_delivery_id = dd.delivery_id
  and dd.country_id = 1
  and dd.submarket_id not in (583,237,13,57,237,13,302,237,1146,57,299,576,13,30,81,1146,237,337,55,200,68,4,1898,1208,1071,1214,1060,884,800,894,892,827,629,1059,1193,86,85,864,1196,1194,1082,497,1137,879,810,829,581,1058,1081,1164,582,763,896,900,1130,2052,234,579,1687,84,577,1820,1073,77,796,811,1139,1262,9,1173,1165,906,802,1166,1207,78,1856,236,865,767,819,80,1233,1150,1160,914,831,100,344,342,34,1392,1169,1072,1212,1688,1148,58,1289,1074,1201,806,1232,1234,1391,1219,2048,1252,1244,870,807,1161,1134,1591,1158,1145,893,1129,809,897,815,868,913,567,595,876,801,571,336,332,74,1238,1213,808,79,59,1063,1241,1144,1155,662,1067,1171,860,877,871,38,31,578,601,600,98,97,1084,1200,1080,907,871,1146,303,7,70,63,57,76,75,17,7347,304,72,8,1590,1235,1210,663,1355,1616,1202,1521,1125,1136,880,1162,761,899,1163,830,866,576,898,1124,873,803,765,888,795,883,569,568,575,564,64,65,565,563,602,71,299,96,81,134,30,20,54,5,1627,1861,1541,9167,305,1247,1249,1608,2085,2116,904,1025,1198,1203,585,875,580,911,874,862,903,1061,959,905,1217,926,302,307,583,301,62,237,73,99,25,23,29,570,36,594,574,1064,308,1079,1209,1390,574,17,8,1152,1229,1220,29,99,36,305,334,587,590,592,570,596,597,801,586,902,1135,909,891,905,1077,1217,1230,1167,1215,9167,1392,1609,1078,1159,865,1161,1229,1990,802,332,1261,889,1170,1149,1490,1795,1769,1760,1757,1784,5369,5380,5374,5373,5393,5566,9415,9492,9527,9536,9553,9582,1785,5400,9537,9562,304,8,72,1248,1148,3789,1078,1125,237,819,57,30,576,795,890,1616,1590,801,830,761,1247,875,81,1166,299,663,302,1146,866,1235,565,1025,576,1235,1025,1868,1146,1125,795,1166,586,1868,5,662,583,96,1170,17
,73,1520,6111,7544,1251,9032,8,2225,883,7347,8510,8950,65,6003,8952,70,1390,1908,6108,8506,8623,320072,17,1203,6175,75,888,63,2085,5737,6002,6004,8953,8454,64,304,62,1521,6001,1610,5140,96,5758,765,899,1124,5,303,574,898,2224,5862,1202,134,1615,6548,8951,602,575,71,72,583,5170
)  --need to update
WHERE ca.dte = '2026-02-25'
group by all
)

-- Affordability as of 2/25/26
create or replace table proddb.katez.affordability_eligiliy_022526
	 as ( 

with pad_rank as (
select consumer_id, ELIGIBILITY_STATUS, rank() over (partition by consumer_id order by LAST_UPDATED_AT desc) as rnk
from EDW.PAD.affordability_incentive_iq_pad_eligibility_union 
where LAST_UPDATED_AT::date <= date'2026-02-25'
)
, pad_eligible as (
select consumer_id
from pad_rank
where rnk = 1
and ELIGIBILITY_STATUS = 'ELIGIBLE'
group by 1
)
-- WBD Cx eligible on a given date
select  
'wbd' as program, wbd.consumer_id
from proddb.public.FACT_DYNAMIC_AUDIENCE_WBD_ORDER_FREQUENCY_L365D wbd
left join proddb.public.cx_sensitivity_v2 psm on wbd.consumer_id = psm.consumer_id and prediction_datetime_est = injected_date
where injected_date = date'2026-02-25'
and prediction_datetime_est = date'2026-02-25'
and not (cohort = 'p84d_active_very_insensitive' and L365D_ORDER_COUNT >= 13)
and not (cohort = 'p84d_active_insensitive' and L365D_ORDER_COUNT >= 20)
group by all
-- XS Cx eligible on a given date
union all
select 'xs' as program, consumer_id
from edw.pad.cross_shopper_daily_snapshot_cross_shopper_customer_v3_daily_snapshot
where import_date = date'2026-02-25'
group by all
-- PAD Cx eligible on a given date
union all
select 'PAD' as program, consumer_id
from pad_eligible
group by all
)

-- TAM: coverage by segment
select segment, 
count(distinct creator_id) users,
count(distinct consumer_id)*1.0000/count(distinct creator_id) coverage
from proddb.katez.storm_cohort a
left join proddb.katez.affordability_eligiliy_022526 b
on a.creator_id = b.consumer_id
group by 1
order by 2 desc

SEGMENT	USERS	COVERAGE
Churned	35317025	0.396740
Active	5888281	0.642427
DP	4660219	0.383731
Dormant	3627933	0.906790
New Cx	427730	0.544816


-- Storm Cohort: Order frequency pre post by Segment	
Select segment,
count(distinct a.creator_id) users,
count(distinct case when dd.created_at between '2025-12-18' AND '2026-01-21' then delivery_id end)*1.0000/count(distinct a.creator_id) pre_of,
count(distinct case when dd.created_at between '2026-01-22' AND '2026-02-25' then delivery_id end)*1.0000/count(distinct a.creator_id) storm_of
from proddb.katez.storm_cohort a
--join visitors v on a.creator_id = v.user_id --only Cx visited during storm 35D
left join proddb.public.dimension_deliveries dd --check order frequency
 on a.creator_id = dd.creator_id and dd.created_at BETWEEN '2025-12-18' AND '2026-02-25'--'2025-12-18' AND '2026-01-21','2026-01-22' AND '2026-02-25'
and dd.country_id = 1  
and is_filtered_core = true  
--and is_subscribed_consumer = false  
--and is_consumer_pickup = FALSE
group by 1
order by 2 desc

-- Non Storm Cohort: Order frequency pre post by Segment

-- Storm 35D visited users
with visitors as
(select user_id from proddb.public.fact_unique_visitors_full_utc
where 1=1
AND event_date between '2026-01-22' and '2026-02-25'
AND experience = 'doordash')
