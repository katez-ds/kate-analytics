
-- MP
With all_users as(
    SELECT user_id as user_cons_id
    FROM proddb.public.fact_unique_visitors_full_UTC uv
    left join geo_intelligence.public.maindb_submarket ms on ms.id = uv.dd_submarket_id
    left join geo_intelligence.public.maindb_market mm on mm.id = ms.market_id
    WHERE event_date between current_date - 28 and current_date  --use 4 week date range later
        and country_id = 1
        and DD_submarket_id = 38
    group by 1
    )
    
, core_dd as(
    select dd.creator_id, dd.delivery_id, dd.created_at, dd.active_date
    from public.dimension_deliveries dd
    where 
        dd.is_filtered_core = true and 
        dd.created_at::date >= current_date - 28 --use 4 week date range later
        and country_id = 1
        and submarket_id = 38
    )

, pen as(
    select 
        user_cons_id
        , count(distinct delivery_id) as num_delivs
        , case when num_delivs > 0 then 1 else 0 end as conversion
    from all_users au 
    left join core_dd dd on au.user_cons_id = dd.creator_id
    group by 1 
    )
    
select count(distinct user_cons_id) as num_users
    , sum(conversion) as purchasers
    , avg(conversion) as conversion
    , avg(num_delivs) as order_rate
    , stddev(num_delivs) as std_dev_order_rate
    , sum(num_delivs) as order_volume
from pen

-- Classic

With all_users as(
    SELECT user_id as user_cons_id
    FROM proddb.public.fact_unique_visitors_full_UTC uv
    left join geo_intelligence.public.maindb_submarket ms on ms.id = uv.dd_submarket_id
    left join geo_intelligence.public.maindb_market mm on mm.id = ms.market_id
    WHERE event_date between current_date - 28 and current_date  --use 4 week date range later
        and country_id = 1
        and DD_submarket_id = 38
        and IS_DASHPASS = 0 -- classic
    group by 1
    )
  
, core_dd as(
    select dd.creator_id, dd.delivery_id, dd.created_at, dd.active_date
    from public.dimension_deliveries dd
    where 
        dd.is_filtered_core = true and 
        dd.created_at::date >= current_date - 28 --use 4 week date range later
        and country_id = 1
        and submarket_id = 38
        and IS_SUBSCRIBED_CONSUMER = FALSE -- classic orders only
    )

, pen as(
    select 
        user_cons_id
        , count(distinct delivery_id) as num_delivs
        , case when num_delivs > 0 then 1 else 0 end as conversion
    from all_users au 
    left join core_dd dd on au.user_cons_id = dd.creator_id
    group by 1 
    )
    
select count(distinct user_cons_id) as num_users
    , sum(conversion) as purchasers
    , avg(conversion) as conversion
    , avg(num_delivs) as order_rate
    , stddev(num_delivs) as std_dev_order_rate
    , sum(num_delivs) as order_volume
from pen

