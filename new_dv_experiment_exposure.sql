/*
DV A - Holdout layer - discount_engine_global_holdout_us, control/WBD+XS 5/95
DV B - Fee discounts layer - discount_engine_fee_discounts_us, WBD+XS/WBD+XS+PAD 5/95
DV C - Deals sandbox layer - discount_engine_deals_v1_us,
*/

with universal_be as (
select
    case when tag like 'treatment_wbd_only%' then 'Control' else 'Treatment' end as tag_renamed
  , try_cast(bucket_key as integer) as user_id
  , min(EXPOSURE_TIME) as first_exposed
from PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
where experiment_name = 'discount_engine_fee_discounts_us'  -- DV B
      and experiment_version >= 2
      and exposure_time >= '2026-02-19'
      and segment = 'Users'
group by all
)
, pad_be as (
select
    case when tag = 'control' then 'Control'
    else 'Treatment'
    end as tag_renamed,
    bucket_key as user_id,
    min(exposure_time) as first_exposed
from
    EDW.PAD.pad_gtm_v3_c14428ea0fbd4b26a206e533ce1b2a48_deduplicate_exposure_table be1
group by all
)
, be as (
select 
universal_be.user_id,
universal_be.first_exposed,
case 
    when universal_be.tag_renamed = 'Control' and pad_be.tag_renamed = 'Control' then 'Control'
    when universal_be.tag_renamed = 'Control' and pad_be.tag_renamed = 'Treatment' then 'Old Treatment New Control'
    when universal_be.tag_renamed = 'Treatment' and pad_be.tag_renamed = 'Control' then 'Old Control New Treatment'
    when universal_be.tag_renamed = 'Treatment' and pad_be.tag_renamed = 'Treatment' then 'Old Treatment New Treatment'
end as tag_renamed
from universal_be
left join pad_be using (user_id)
)

select
case when experiment_name = 'discount_engine_global_holdout_us' then 'DV A'
  when experiment_name = 'discount_engine_fee_discounts_us' then 'DV B'
  when experiment_name = 'discount_engine_deals_v1_us' then 'DV C'
  else 'Others' end layer, tag, count(distinct try_cast(bucket_key as integer)) users
from PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE
where experiment_name --= 'discount_engine_deals_v1_us'
in ('discount_engine_global_holdout_us','discount_engine_fee_discounts_us','discount_engine_deals_v1_us')
      and experiment_version >= 2
      and exposure_time >= '2026-02-19'
      and segment = 'Users'
group by 1,2
order by 1,2

LAYER	TAG	USERS
DV A	control_1	767336
DV A	control_2	770751
DV A	control_3	768571
DV A	control_4	770599
DV A	control_5	769563
DV A	treatment	82800079
DV B	treatment	4184820
DV B	treatment_wbd_only	220733
DV C	treatment	3549832
