—- total unique US-eligible Cx on a given day:
select count(distinct consumer_id) as total_eligible_cx
from proddb.ml.discount_engine_log_v1
where event_date = '2026-06-01'
  and country_id = 1
  and (is_df_discount_eligible or is_zero_df_eligible or is_sf_discount_eligible)

—- breakdown by MODEL_ID and eligibility flag:
select
  model_id,
  count(distinct consumer_id) as distinct_cx,
  count(distinct case when is_df_discount_eligible then consumer_id end) as df_discount_eligible_cx,
  count(distinct case when is_zero_df_eligible then consumer_id end) as zero_df_eligible_cx,
  count(distinct case when is_sf_discount_eligible then consumer_id end) as sf_discount_eligible_cx
from proddb.ml.discount_engine_log_v1
where event_date = '2026-06-01'
  and country_id = 1
group by 1
order by distinct_cx desc
