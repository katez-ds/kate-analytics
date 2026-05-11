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
where experiment_name = 'new_fee_structure_v5_wave2'
      and experiment_version >= 1
      and exposure_time >= '2026-05-08'
      and segment = 'Users'
      and RESULT is not null
GROUP BY 1, 2, 3
)

SELECT
  user_id as bucket_key,
  experiment_name,
  tag,
  tag as result,
  experiment_version,
  exposure_time
FROM
  be
