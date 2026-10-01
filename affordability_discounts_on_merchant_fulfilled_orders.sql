WITH disc AS (
    SELECT
        delivery_id,
        created_at,
        (wbd_fee_promo_discount + wbd_df_promo_discount + wbd_sf_promo_discount) AS wbd_discount,
        (pad_fee_promo_discount + pad_df_promo_discount + pad_sf_promo_discount) AS pad_discount
    FROM proddb.static.df_sf_promo_discount_delivery_level
    WHERE CAST(created_at AS DATE) = '2026-09-30'
      AND (
            (wbd_fee_promo_discount + wbd_df_promo_discount + wbd_sf_promo_discount) > 0
         OR (pad_fee_promo_discount + pad_df_promo_discount + pad_sf_promo_discount) > 0
      )
)
SELECT
    dd.fulfillment_type,
    COUNT(*) AS discount_orders,
    SUM(disc.wbd_discount) AS total_wbd_discount,
    SUM(disc.pad_discount) AS total_pad_discount,
    SUM(disc.wbd_discount + disc.pad_discount) AS total_wbd_pad_discount
FROM disc
JOIN proddb.public.dimension_deliveries dd
    ON disc.delivery_id = dd.delivery_id
WHERE dd.is_filtered_core = TRUE
  AND dd.is_subscribed_consumer = FALSE
  AND dd.country_id = 1
  AND NOT EXISTS (
        SELECT 1 FROM edw.cng.dimension_new_vertical_store_tags nv
        WHERE nv.store_id = dd.store_id AND nv.is_filtered_mp_vertical = 1
      )
GROUP BY 1
ORDER BY discount_orders DESC;


Strict Classic Rx (US, restaurant-only, core, non-DashPass) — this is what you asked for:

┌────────────────────┬─────────────────┬──────────┬───────────────┬─────────────┬───────────────┐
│  Fulfillment Type  │ Discount Orders │    %     │      WBD      │     PAD     │ Total WBD+PAD │
├────────────────────┼─────────────────┼──────────┼───────────────┼─────────────┼───────────────┤
│ dasher             │ 481,802         │ 99.9722% │ $1,503,409.40 │ $274,424.42 │ $1,777,833.82 │
├────────────────────┼─────────────────┼──────────┼───────────────┼─────────────┼───────────────┤
│ sidewalk_robot     │ 92              │ 0.0191%  │ $246.38       │ $29.50      │ $275.88       │
├────────────────────┼─────────────────┼──────────┼───────────────┼─────────────┼───────────────┤
│ merchant_fleet     │ 40              │ 0.0083%  │ $101.76       │ $0.00       │ $101.76       │
├────────────────────┼─────────────────┼──────────┼───────────────┼─────────────┼───────────────┤
│ autonomous_vehicle │ 2               │ 0.0004%  │ $5.00         │ $0.00       │ $5.00         │
└────────────────────┴─────────────────┴──────────┴───────────────┴─────────────┴───────────────┘
