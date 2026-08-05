with be as (
  SELECT
    bucket_key as user_id,
    experiment_name,
    tag tag,
    MIN(experiment_version) as experiment_version,
    MIN(exposure_time) as exposure_time
from proddb.public.fact_dedup_experiment_exposure ee
join proddb.public.fact_unique_visitors_full_UTC b
    on try_to_number(ee.bucket_key) = b.user_id
    and b.event_date = exposure_time::DATE
    and country_name = 'United States'
where experiment_name = 'discount_engine_us_wbd_v2'
      and experiment_version >=0
      and exposure_time::date >= '2026-07-29'
      and segment = 'Users'
      and RESULT is not null
GROUP BY 1, 2, 3
)
select
  be.tag,
  count(distinct d.consumer_id) as distinct_cx,
  count(distinct case when d.is_df_discount_e end) as df_discount_eligible_cx,
  count(distinct case when d.is_zero_df_eligible then d.consumer_id end) as zero_df_eligible_cx,
  count(distinct case when d.is_sf_discount_e end) as sf_discount_eligible_cx,
  count(distinct case when d.is_df_discount_eligible then d.store_id end) as df_eligible_merchants,
  div0(count(distinct case when d.is_df_discoid end),
       count(distinct case when d.is_df_discount_eligible then d.consumer_id end)) as df_eligible_merchants_per_user
from be
join proddb.ml.discount_engine_log_v1 d
  on be.user_id = d.consumer_id
where d.model_id = 'discount_engine_m0_us_fee_and_deal_layer_v1_1'
  and d.country_id = 1
  and event_date::date >= '2026-07-29'
group by 1
order by distinct_cx desc;
