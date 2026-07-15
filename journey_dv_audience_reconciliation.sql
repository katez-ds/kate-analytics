/*
Journey targeted audience is much smaller than what shows up in DV / Curie.

Why: proddb.public.fact_dedup_experiment_exposure is keyed by experiment_name
alone, and accepts writes from ANY journey whose DV_NODE shares that
attribute_definition_name. Cloned workflows (LIVE<->TESTING twins,
URBAN<->RURAL partitions, shadow tests) inherit the same node_uuid /
experiment name, so several journeys -- not just yours -- can be feeding the
same Curie experiment. That's usually why Curie looks bigger than the cohort
you actually targeted.

Source: doordash/cursor-analytics journey-service-investigation skill,
Recipe 8 (Experiment-split reconciliation), "Pitfall (multi-journey writers /
clone collision)".

Fill in: <journey_uuid>, <dv_node_uuid>, <experiment_name>, <start>, <end>.
*/

-- ============================================================
-- 1. Filter Curie/DV to only the Cx your journey actually tagged
--    (inner-join journey events -> exposure table, scoped to journey_uuid)
-- ============================================================
with j as (
    select distinct cast(entity_id as varchar) as consumer_id,
                    node_evaluate_result as branch
    from edw.growth.dl_fact_journey_events
    where journey_uuid = '<journey_uuid>'
        and split_part(node_uuid, '@', 1) = '<dv_node_uuid>'
        and node_execution_status = 'PASS'
        and is_shadow_test = 0
        and event_date between '<start>' and '<end>'
),
x as (
    select distinct bucket_key::varchar as consumer_id, tag
    from proddb.public.fact_dedup_experiment_exposure
    where experiment_name = '<experiment_name>'
        and exposure_time::date between '<start>' and '<end>'
)
select j.consumer_id, j.branch, x.tag
from j
inner join x on j.consumer_id = x.consumer_id;
-- ^ this is "Curie filtered to Journey-tagged users"

-- ============================================================
-- 2. Full reconciliation bucket counts (journey-only / exposure-only /
--    matched / mismatched-branch)
-- ============================================================
with j as (
    select distinct cast(entity_id as varchar) as consumer_id,
                    node_evaluate_result as branch
    from edw.growth.dl_fact_journey_events
    where journey_uuid = '<journey_uuid>'
        and split_part(node_uuid, '@', 1) = '<dv_node_uuid>'
        and node_execution_status = 'PASS'
        and is_shadow_test = 0
        and event_date between '<start>' and '<end>'
),
x as (
    select distinct bucket_key::varchar as consumer_id, tag
    from proddb.public.fact_dedup_experiment_exposure
    where experiment_name = '<experiment_name>'
        and exposure_time::date between '<start>' and '<end>'
)
select case when j.consumer_id is null then 'exposure only'
            when x.consumer_id is null then 'journey only'
            when j.branch = x.tag      then 'matched'
            else 'mismatched branch' end as bucket,
       count(*) as cx
from j full outer join x on j.consumer_id = x.consumer_id
group by 1;

-- Notes:
--   - Use `tag`, not `result`, for the variant on fact_dedup_experiment_exposure
--     (result is the legacy column).
--   - node_evaluate_result may be JSON-stringified (e.g. "npws_30d_t2" with
--     literal quotes) -- strip before comparing: replace(node_evaluate_result, '"', '')
--   - Doesn't work for TESTING-stage journeys: TESTING DV nodes route 100% to
--     the default/holdout variant and never write to fact_dedup_experiment_exposure.

-- ============================================================
-- 3. Diagnostic: inventory every journey writing to this experiment
--    (run this BEFORE assuming an upstream gate is dropping Cx)
-- ============================================================
with all_dv_nodes as (
    select journey_uuid, node_uuid, attribute_definition_name
    from edw.growth.dimension_journey_node
    where attribute_definition_name = '<experiment_name>'
      and node_type = 'DV_NODE'
    qualify row_number() over (partition by journey_uuid, node_uuid
        order by snapshot_date desc, updated_at_utc desc) = 1
),
journey_meta as (
    select journey_uuid, journey_name, lifecycle_stage
    from edw.growth.dimension_journey
    qualify row_number() over (partition by journey_uuid order by snapshot_date desc) = 1
)
select j.journey_uuid, j.journey_name, j.lifecycle_stage,
       count(distinct e.entity_id) as distinct_cx_pass,
       max(e.event_time) as last_pass_event,
       any_value(e.is_shadow_test) as sample_is_shadow_test
from edw.growth.dl_fact_journey_events e
join all_dv_nodes a on e.journey_uuid = a.journey_uuid
   and split_part(e.node_uuid, '@', 1) = a.node_uuid
join journey_meta j on j.journey_uuid = a.journey_uuid
where e.node_execution_status = 'PASS'
  and e.event_date between '<start>' and '<end>'
group by all;
-- If multiple journey_uuids show meaningful distinct_cx_pass, other workflows
-- are co-writing to the same Curie experiment -- that's the source of the gap.
