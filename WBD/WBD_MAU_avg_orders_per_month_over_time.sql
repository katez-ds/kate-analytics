create or replace table proddb.katez.consumer_orders
as
(select dd.delivery_id, dd.creator_id consumer_id,created_at::date order_dt
FROM proddb.public.dimension_deliveries dd
LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv 
        ON dd.store_id = nv.store_id AND nv.is_filtered_mp_vertical = 1
WHERE dd.is_filtered_core = TRUE
        -- AND dd.is_consumer_pickup = FALSE -- optional to exclude pickup orders
        AND dd.fulfillment_type NOT IN ('dine_in', 'shipping', 'merchant_fleet', 'virtual') -- excluding dine-in, shipping, merchant fleet, and virtual orders (giftcards)
        AND dd.is_from_store_to_us = FALSE -- excluding store-to-us orders
        AND dd.is_bundle_order = FALSE -- excluding bundle orders -- an optional column to filter out DoubleDash
        AND nv.business_line IS NULL -- restaurant orders 
        AND dd.country_id = 1 -- US only 
        AND dd.created_at::date >= date'2022-08-01'
        AND is_subscribed_consumer = FALSE
group by 1,2,3
)

select tag_renamed, count(distinct consumer_id) eligible_customers
from proddb.katez.wbd_first_eligible_cohort
where first_eligible_dt <= DATEADD(day,-180,date'2026-02-02')
group by 1
order by 1

TAG_RENAMED	ELIGIBLE_CUSTOMERS
Control	6290772
Treatment	119563951

-- Control vs Treatment: orders by months from first eligible
select tag_renamed,
case when DATEADD(day,-30,first_eligible_dt) <= order_dt AND order_dt < first_eligible_dt then -1
  when first_eligible_dt <= order_dt AND order_dt < DATEADD(day,30,first_eligible_dt) then 1
  when DATEADD(day,30,first_eligible_dt) <= order_dt AND order_dt < DATEADD(day,60,first_eligible_dt) then 2
  when DATEADD(day,60,first_eligible_dt) <= order_dt AND order_dt < DATEADD(day,90,first_eligible_dt) then 3
  when DATEADD(day,90,first_eligible_dt) <= order_dt AND order_dt < DATEADD(day,120,first_eligible_dt) then 4
  when DATEADD(day,120,first_eligible_dt) <= order_dt AND order_dt < DATEADD(day,150,first_eligible_dt) then 5
  when DATEADD(day,150,first_eligible_dt) <= order_dt AND order_dt < DATEADD(day,180,first_eligible_dt) then 6
  end as eligible_month_n,
count(distinct b.delivery_id) as orders
from proddb.katez.wbd_first_eligible_cohort a
left join proddb.katez.consumer_orders b
on a.consumer_id = b.consumer_id
and order_dt between DATEADD(day,-30,first_eligible_dt) and DATEADD(day,180,first_eligible_dt)
where first_eligible_dt <= DATEADD(day,-180,date'2026-02-02')
group by 1,2
order by 1,2


-- Control vs Treatment: MAU by months from first eligible

select tag_renamed,
case when DATEADD(day,-30,first_eligible_dt) <= order_dt AND order_dt < first_eligible_dt then -1
  when first_eligible_dt <= order_dt AND order_dt < DATEADD(day,30,first_eligible_dt) then 1
  when DATEADD(day,30,first_eligible_dt) <= order_dt AND order_dt < DATEADD(day,60,first_eligible_dt) then 2
  when DATEADD(day,60,first_eligible_dt) <= order_dt AND order_dt < DATEADD(day,90,first_eligible_dt) then 3
  when DATEADD(day,90,first_eligible_dt) <= order_dt AND order_dt < DATEADD(day,120,first_eligible_dt) then 4
  when DATEADD(day,120,first_eligible_dt) <= order_dt AND order_dt < DATEADD(day,150,first_eligible_dt) then 5
  when DATEADD(day,150,first_eligible_dt) <= order_dt AND order_dt < DATEADD(day,180,first_eligible_dt) then 6
  end as eligible_month_n,
count(distinct b.consumer_id) as users
from proddb.katez.wbd_first_eligible_cohort a
left join proddb.katez.consumer_orders b
on a.consumer_id = b.consumer_id
and order_dt between DATEADD(day,-30,first_eligible_dt) and DATEADD(day,180,first_eligible_dt)
where first_eligible_dt <= DATEADD(day,-180,date'2026-02-02')
group by 1,2
order by 1,2
