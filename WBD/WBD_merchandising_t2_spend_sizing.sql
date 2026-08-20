-- Ballpark spend uplift from WBD merchandising T2. Assuming we do actually see the 30bp OR uplift, what would that mean for spend increase in T2.

SELECT
    FLOOR(DATEDIFF('day', '2026-07-01', CAST(dd.created_at AS DATE)) / 7) + 1 AS week_num,
    MIN(CAST(dd.created_at AS DATE)) AS week_start,
    MAX(CAST(dd.created_at AS DATE)) AS week_end,
    SUM(df.wbd_df_promo_discount) AS wbd_df_spend
FROM proddb.static.df_sf_promo_discount_delivery_level df
JOIN proddb.public.dimension_deliveries dd
  ON df.delivery_id = dd.delivery_id
WHERE dd.country_id = 1
  AND CAST(dd.created_at AS DATE) BETWEEN '2026-07-01' AND '2026-07-28'
GROUP BY 1
ORDER BY 1


 Average weekly: $6,605,224.15

# Classic Cx
Calc
1. Incremental orders/user = 3.77 × 0.30% = 0.01131
2. Incremental orders (12wk, target arm) = 18,229,515 × 0.01131 ≈ 206,176 orders
3. Incremental WBD spend (12wk) = 206,176 × $0.5399 ≈ $111,306 over the full 12-week run (~$9,275/week)

For context, that's about 0.14% of the ~$6.6M/week baseline WBD run rate.

Assumptions worth double-checking:
- Treating "1/3 of US exposed users" as literally the Classic Cx per-arm population from your existing power analysis (18.23M) — tell me if you meant a different/larger population.
- "30bps" = relative lift, not an absolute +0.003 orders/user (that alternate reading would give a ~13x smaller number, since 3.77 × 0.0030 relative-equivalent would only be 0.0030 vs 0.01131 absolute).
- $/order is blended across ALL Classic orders (not just WBD-discounted ones), per your "overall average" choice — this already reflects that only a fraction of classic orders receive WBD.



# All Cx
Calc
1. Incremental orders/user = 7.32 × 0.30% = 0.02196
2. Incremental orders (12wk) = 27,323,810 × 0.02196 ≈ 600,031 orders
3. Incremental WBD spend (12wk) = 600,031 × $0.1321 ≈ $79,270 over 12 weeks (~$6,606/week)

That's lower than the Classic-only estimate ($111,306), even with a bigger population and higher order-rate baseline — because blending in DashPass orders (which get $0 WBD) drags the average $/order down from $0.54 to $0.13 by more than the extra population/order-rate makes up for. Since WBD spend only ever lands on Classic (non-DashPass) orders, the Classic-scoped version is the more accurate one if you expect the incremental orders to behave like the general population's order mix — the All-Cx version implicitly assumes some of the incremental orders are DashPass orders that can never receive WBD.

Given that mechanical fact, which population basis matches how you actually want to frame this for the experiment write-up — All-Cx (simpler, but structurally understates true WBD-relevant impact) or Classic-only (mechanically correct for WBD but a narrower framing)?
