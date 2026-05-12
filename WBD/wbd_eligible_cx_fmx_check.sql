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
       AND mca.activated_at BETWEEN DATEADD('day', -30, mca.dte) AND mca.dte
       AND mca.dte = '2026-05-01'::DATE--BETWEEN '2026-04-01'::DATE AND '2026-04-30'::DATE
    WHERE wbd.injected_date = '2026-05-01'--BETWEEN '2026-04-01'::DATE AND '2026-04-30'::DATE
) AS base
LEFT JOIN (
    SELECT DISTINCT
        snapshot_date,
        linked_consumer_id,
        consumer_id
    FROM edw.consumer.suma_consumer_links_direct
    where direct_link_types::string like '%phone%'
    and snapshot_date = '2026-05-01'--BETWEEN '2026-04-01'::DATE AND '2026-04-30'::DATE
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


select is_fmx_etl_eligible_flag,new_consumers_but_not_suma_flag,
sum(wbd_eligible_users) wbd_users
from proddb.katez.wbd_fmx_flags
where new_consumers_but_not_suma_flag = 1 or is_fmx_etl_eligible_flag = 1
group by 1,2


IS_FMX_ETL_ELIGIBLE_FLAG	NEW_CONSUMERS_BUT_NOT_SUMA_FLAG	WBD_USERS
0	1	396299
1	1	446756
1	0	96

/*
From Tony Caletti
For fraud, we leverage this filter is_fraud = false in this table x360.prod.consumer to exclude any fraud Cx. The fraud team has more granular data sources for specific use cases, you can reach out to @Yi for more info
For SUMA, the two main tables are identity_insights_prod.public.user_clusters and edw.consumer.suma_consumer_links_direct

The first table tracks all links (strong/weak and direct/indirect) while the second table only tracks direct links (if Cx A is linked to B, and B is linked to C, A to C is an indirect link). The benefit of the second table is that it has info about the method for linking two accounts (ie. phone number vs address etc...) which can be helpful for targeting certain kinds of SUMA or follow up analysis
*/
For the first table, here is a below query
with audience_clusters as (
    select distinct
        m.user_id,
        uc.cluster_id
    from my_audience m
    left join identity_insights_prod.public.user_clusters uc
        on m.user_id = uc.user_id
        and uc.cluster_type = 'CX_SUMA'
        and coalesce(uc.__is_deleted, false) = false
)

select
    ac.user_id,
    count(distinct cm.user_id) as suma_cluster_user_cnt
from audience_clusters ac
left join identity_insights_prod.public.user_clusters cm
    on ac.cluster_id = cm.cluster_id
    and cm.cluster_type = 'CX_SUMA'
    and ac.user_id != cm.user_id
    and coalesce(cm.__is_deleted, false) = false
;

--Here is one for the second table
select
    m.consumer_id,
    count(distinct scld.linked_consumer_id) as directly_linked_consumer_cnt,
    count(distinct scld.linked_user) as directly_linked_user_cnt,
    array_agg(distinct scld.direct_link_types) within group (order by scld.direct_link_types::varchar) as direct_link_types_seen
from <your_audience_table> m
left join edw.consumer.suma_consumer_links_direct scld
    on m.consumer_id = scld.consumer_id
group by all
;

