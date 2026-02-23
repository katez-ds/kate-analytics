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
