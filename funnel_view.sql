with
  funnel_view as (
    SELECT
      experiment_name,
      experiment_version,
      try_cast (bucket_key as integer) as bucket_key,
      segment,
      case
        when coalesce(tag, result) in {{control_1-1}} then 'Control'
        else coalesce(tag, result)
      end as experiment_group,
      event_date,
      exposure_time,
      unique_core_visitor,
      unique_store_content_page_visitor,
      unique_store_page_visitor,
      unique_order_cart_page_visitor,
      unique_checkout_page_visitor,
      unique_purchaser
    FROM
      PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE exp
      JOIN proddb.public.fact_unique_visitors_full_utc uv ON try_cast (exp.bucket_key as integer) = uv.user_id
      AND exp.exposure_time:: date = uv.event_date
      AND uv.experience = 'doordash'
    WHERE
      experiment_name = {{experiment_name-1}}
      AND experiment_version BETWEEN {{experiment_version_start-1}} AND {{experiment_version_end-1}}
      AND (
        lower(segment) not in ('dogfooding', 'development', 'employees')
        OR segment is null
      )
      AND exposure_time:: date BETWEEN {{start_time-1}} AND {{end_time-1}}
      AND result is not null
      AND (
        tag in {{control_1-1}}
        or tag in {{treatment_1-1}}
      )
      and SEGMENT in {{segment-1}}
      and try_cast (BUCKET_KEY as integer) != 1505155093 -- 1505155093 is the high volume account (100s of orders per day across 100+ sites) that DashMart uses to MFF (micro fulfill) items for our warehouse operations.
  )
SELECT
  *,
  daily_cx_traffic_store_page / daily_cx_traffic_homepage as store_page_visit_rate,
  daily_cx_traffic_order_cart_page / daily_cx_traffic_homepage as order_cart_page_visit_rate,
  daily_cx_traffic_checkout_page / daily_cx_traffic_homepage as checkout_page_visit_rate,
  daily_cx_traffic_purchased / daily_cx_traffic_homepage as purchased_rate,
  daily_cx_traffic_order_cart_page / daily_cx_traffic_store_page as store_page_order_cart_page_converted_rate,
  daily_cx_traffic_checkout_page / daily_cx_traffic_order_cart_page as order_cart_page_checkout_converted_rate,
  daily_cx_traffic_purchased / daily_cx_traffic_checkout_page as checkout_purchased_converted_rate
FROM
  (
    SELECT
      experiment_group,
      COUNT(
        DISTINCT CASE
          WHEN unique_core_visitor = 1 THEN bucket_key | | event_date
        END
      ) AS daily_cx_traffic,
      COUNT(
        DISTINCT CASE
          WHEN unique_store_content_page_visitor = 1 THEN bucket_key | | event_date
        END
      ) AS daily_cx_traffic_homepage,
      COUNT(
        DISTINCT CASE
          WHEN unique_store_page_visitor = 1 THEN bucket_key | | event_date
        END
      ) AS daily_cx_traffic_store_page,
      COUNT(
        DISTINCT CASE
          WHEN unique_order_cart_page_visitor = 1 THEN bucket_key | | event_date
        END
      ) AS daily_cx_traffic_order_cart_page,
      COUNT(
        DISTINCT CASE
          WHEN unique_checkout_page_visitor = 1 THEN bucket_key | | event_date
        END
      ) AS daily_cx_traffic_checkout_page,
      COUNT(
        DISTINCT CASE
          WHEN unique_purchaser = 1 THEN bucket_key | | event_date
        END
      ) AS daily_cx_traffic_purchased
    FROM
      funnel_view
    GROUP BY all
  )
