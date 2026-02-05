select dd.delivery_id, dd.creator_id consumer_id,created_at::date order_dt
FROM proddb.public.dimension_deliveries dd
LEFT JOIN edw.cng.dimension_new_vertical_store_tags nv 
        ON dd.store_id = nv.store_id AND nv.is_filtered_mp_vertical = 1
WHERE dd.is_filtered_core = TRUE
        -- AND dd.is_consumer_pickup = FALSE -- optional to exclude pickup orders
        AND dd.fulfillment_type NOT IN ('dine_in', 'shipping', 'merchant_fleet', 'virtual') -- excluding dine-in, shipping, merchant fleet, and virtual orders (giftcards)
        AND dd.is_from_store_to_us = FALSE -- excluding store-to-us orders
        AND dd.is_bundle_order = FALSE -- excluding bundle orders -- an optional column to filter out DoubleDash
        AND nv.business_line IS NULL -- restaurant orders 
        --AND dd.country_id = 1 -- US only 
        AND dd.created_at::date >= date'2022-08-01'
        AND is_subscribed_consumer = FALSE
group by 1,2,3
