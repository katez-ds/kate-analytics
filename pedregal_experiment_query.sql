with m as (
  select analysis_id, metric_id, metric_name, result_type, result_id
  from raw_taulu.platform.analysis_metric_results_details
  where analysis_id = '5ca81a5c-fe9a-4ecf-a503-bebe0a8b8b73'
  qualify row_number() over (
    partition by analysis_id, metric_id, result_type
    order by updated_at desc
  ) = 1
) 
, base_curie_output as (
select
    m.metric_name
  , v.value::string                                             as variant_name
  , r.metric_results_is_control[v.index]::boolean               as is_control
  , r.metric_results_stats_value[v.index]::float                as metric_value
  , r.metric_results_stats_avg_treatment_effect[v.index]::float as metric_impact_absolute
  , r.metric_results_stats_rel_treatment_effect[v.index]::float as metric_impact_relative
  , r.metric_results_stats_p_value[v.index]::float              as p_value
  , r.metric_results_stats_sample_size[v.index]::number         as EXPOSURES
  , r.updated_at as analyzed_at
from m
join raw_taulu.platform.analysis_results r
  on r.id = m.result_id
, lateral flatten(input => r.metric_results_variant) v
order by m.metric_name, variant_name
)
, base_with_control_value as (
  select
      b.*
    , c.metric_value as control_value
    , c.exposures    as control_num_cx
    , b.exposures    as treatment_num_cx
  from base_curie_output b
  left join base_curie_output c
    on c.metric_name = b.metric_name and c.variant_name = 'control'
  where b.variant_name != 'control'
 )
, pivoted as (
  select
      b.variant_name as bucket
    , avg(case when metric_name='order_rate_per_entity'     then metric_impact_relative end) as or_lift
    , avg(case when metric_name='variable_profit_per_order' then metric_impact_absolute end) as unit_vp_delta
    , avg(case when metric_name='order_rate_per_entity'     then control_num_cx end)         as cx_control
    , avg(case when metric_name='order_rate_per_entity'     then treatment_num_cx end)       as cx_treatment
    , avg(case when metric_name='order_rate_per_entity'     then control_value end)          as or_control
    , avg(case when metric_name='variable_profit_per_order' then control_value end)          as unit_vp_control
  from base_with_control_value b
  group by 1
)










