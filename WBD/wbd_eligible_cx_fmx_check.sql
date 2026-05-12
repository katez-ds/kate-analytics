-- Aggregate distinct-consumer counts for April 2026 WBD-eligible users
-- filtered to new consumers (first order in last 31 days), with SUMA and FMX flags

create or replace table proddb.katez.wbd_fmx_flags as
(SELECT
    case when creator_id is null then 0 else 1 end is_new_cx_flag,
    case when suma.linked_consumer_id is null then 0 else 1 end is_suma_flag,
    case when fmx.consumer_id is null then 0 else 1 end is_fmx_etl_eligible_flag,
    case when suma.linked_consumer_id is null 
         and creator_id is not null then 1 else 0 end AS new_consumers_but_not_suma_flag,
    COUNT(DISTINCT base.consumer_id) AS wbd_eligible_users
FROM (
    SELECT DISTINCT
        wbd.injected_date AS dte,
        wbd.consumer_id,
        creator_id
    FROM proddb.public.fact_dynamic_audience_wbd_order_frequency_l365d AS wbd
    LEFT JOIN proddb.mattheitz.mh_customer_authority AS mca
        ON wbd.injected_date = mca.dte
       AND wbd.consumer_id = mca.creator_id
    WHERE wbd.injected_date BETWEEN '2026-04-01'::DATE AND '2026-04-30'::DATE
      AND mca.dte BETWEEN '2026-04-01'::DATE AND '2026-04-30'::DATE
      AND mca.activated_at BETWEEN DATEADD('day', -30, mca.dte) AND mca.dte
) AS base
LEFT JOIN (
    SELECT DISTINCT
        snapshot_date,
        linked_consumer_id,
        consumer_id
    FROM edw.consumer.suma_consumer_links_direct
    where direct_link_types::string like '%phone%'
    and snapshot_date BETWEEN '2026-04-01'::DATE AND '2026-04-30'::DATE
group by all
) AS suma
    ON base.dte = suma.snapshot_date
   AND base.consumer_id = suma.consumer_id
LEFT JOIN (
    SELECT DISTINCT
        TRY_TO_NUMBER(consumer_id) AS consumer_id,
        first_order_date
    FROM proddb.public.fmx_audience_wbd_eligible
group by all
) AS fmx
    ON base.consumer_id = fmx.consumer_id
   AND fmx.first_order_date BETWEEN DATEADD('day', -30, base.dte) AND base.dte
group by all
)
