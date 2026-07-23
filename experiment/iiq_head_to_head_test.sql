## total unique US-eligible Cx on a given day:
select count(distinct consumer_id) as total_eligible_cx
from proddb.ml.discount_engine_log_v1
where event_date = '2026-06-01'
  and country_id = 1
  and (is_df_discount_eligible or is_zero_df_eligible or is_sf_discount_eligible)

## breakdown by MODEL_ID and eligibility flag:
select
  model_id,
  count(distinct consumer_id) as distinct_cx,
  count(distinct case when is_df_discount_eligible then consumer_id end) as df_discount_eligible_cx,
  count(distinct case when is_zero_df_eligible then consumer_id end) as zero_df_eligible_cx,
  count(distinct case when is_sf_discount_eligible then consumer_id end) as sf_discount_eligible_cx
from proddb.ml.discount_engine_log_v1
where event_date = '2026-06-01'
  and country_id = 1
group by 1
order by distinct_cx desc

-- ORIGAMI IIQ head-to-head readout - sheet "IIQ H2H ORIGAMI Test Sizing" rows 5-8
-- (Inactive / resurrection_rev_v2 family): DEWO, non-monetary (organiccore_nonmon),
-- CEWO, Low frequency resurrection.
--
-- Adapted from experiment/old_iiq_head_to_head_test.sql per our discussion:
--   1) CRM population source: edw.consumer.campaign_analyzer_exposures does NOT have
--      these DVs (checked 2026-07-22, zero rows) -> sourced from
--      proddb.public.fact_dedup_experiment_exposure.experiment_name instead.
--      Population = each DV's own non-control tag(s) (tag NOT ILIKE '%control%'),
--      i.e. "got into the journey, excluding that DV's own holdout."
--   2) Incentive-arm tag source swapped: static.wbd_experiment_exposure ->
--      proddb.static.us_universal_dv_a_be (same shape: tag_renamed/user_id/first_exposed).
--   3) Launch date + 90D window (confirmed via fact_dedup_experiment_exposure on
--      2026-07-22): earliest first_exposed across all 4 DVs' treatment tags = 2026-04-10
--      (control tags lagged to 2026-04-14 -- same rollout, staged bucketing).
--      Analysis window = 2026-04-10 through 2026-04-10 + 90 = 2026-07-09, applied as a
--      SINGLE shared window per DV (not per-user) -- these 4 DVs bucket in a tight
--      ~4-day spread, so a shared campaign-level window (matching the original
--      late-bloomers single-launch-date pattern) is a clean fit. This also naturally
--      excludes the `lowfrequencyresurrection` DV's later `treatment_siw` sub-arm
--      (first exposed 2026-07-10, one day past the window) without extra logic.
--   4) All aggregation/window steps partition by dv_name in addition to Bucket,
--      since we want a Control/Treatment comparison PER DV, not pooled across all 4.
--   5) Multi-arm DV tags collapse to one population bucket: `tag NOT ILIKE '%control%'`
--      treats ALL non-control tags (treatment, treatment2, treatment_siw, T1/T2/T3, etc.)
--      as a single "in the journey" population -- per instruction, group multiple
--      treatment arms into 1 rather than keeping them separate.
--   6) Table renamed dv_name (was campaign_name) and created under katez schema
--      (was yingxie) per instruction.
--   7) "DV Spending" = affordability spend during the analysis window (not a "Q1 Spend"
--      style budget figure), sourced from proddb.static.df_sf_promo_discount_delivery_level
--      per reference (promo_orders_overlap.sql): TOTAL_FEE_PROMO_DISCOUNT, which covers
--      all 3 affordability programs (WBD+CS+PAD), joined on delivery_id.
--
-- NOT included yet: the 2 "Active" DVs (Dormant Prevention, Post resurrection -- sheet
-- rows 2-3) and Dealseeker (row 4, multi-arm treatment2/3) -- rows 2-3 are still rolling
-- enrollment and not yet 90 days mature as of today; Dealseeker just needs the same
-- multi-arm collapse rule above applied once we add it.

CREATE OR REPLACE TABLE katez.exposed_cx_crm_origami_cohorts AS
select distinct
  e.experiment_name as dv_name,
  try_cast(e.bucket_key as integer) as consumer_id,
  dl.launch_date as start_time_derived,
  dl.launch_date + 90 as end_time_derived
from proddb.public.fact_dedup_experiment_exposure e
join (
  select * from (values
    ('ep_consumer_usmp_resurrection_rev_v2_dewo',                date '2026-04-10'),
    ('ep_consumer_usmp_resurrection_rev_v2_dealseekers',  date '2026-04-10'),
    --('ep_consumer_usmp_resurrection_rev_v2_organiccore_nonmon',  date '2026-04-10'),
    ('ep_consumer_usmp_resurrection_rev_v2_cewo',                 date '2026-04-10'),
    ('ep_consumer_usmp_resurrection_rev_v2_lowfrequencyresurrection', date '2026-04-10')
  ) as t(dv_name, launch_date)
) dl on dl.dv_name = e.experiment_name
where e.segment = 'Users'
  and e.tag not ilike '%control%'
  and try_cast(e.bucket_key as integer) is not null
  and e.exposure_time::date between dl.launch_date and dl.launch_date + 90
;

with be as (
select
  crm.dv_name
, case when dv.tag_renamed = 'Control' then 'Control'
       when dv.tag_renamed = 'Treatment' then 'Treatment'
  end as tag_renamed
, dv.first_exposed
, crm.start_time_derived
, crm.end_time_derived
, dv.user_id
from katez.exposed_cx_crm_origami_cohorts crm
join proddb.static.us_universal_dv_a_be dv on crm.consumer_id = dv.user_id
)
, service_fee_promo_discounts as(
  select
    order_cart_id,
    sum(amount/100) as sf_discount_amount
  from public.maindblocal_order_cart_discount_component
  where monetary_field = 'service_fee'
    and created_at::date >= date '2026-04-10' - 7
    and ("GROUP" != 'subscription')
  group by 1
)
, delivery_fee_promo_discounts as(
  select
    order_cart_id,
    sum(amount/100) as df_discount_amount
  from public.maindblocal_order_cart_discount_component
  where monetary_field = 'delivery_fee'
    and created_at::date >= date '2026-04-10' - 7
    and ("GROUP" != 'subscription')
  group by 1
)
, core_dd as (
    select
    is_subscribed_consumer::int as dashpass
  , is_consumer_pickup::int as pickup
  , convert_timezone('UTC','America/Los_Angeles',dd.created_at) as created_at_pst
  , coalesce(dfd.df_discount_amount, 0) as df_discount_amount_use
  , greatest(dd.delivery_fee/100.0 - df_discount_amount_use, 0) as actual_df_paid_by_cx
  , coalesce(sfd.sf_discount_amount, 0) as sf_discount_amount_use
  , greatest(dd.service_fee/100.0 - sf_discount_amount_use, 0) as actual_sf_paid_by_cx
  , dd.DELIVERY_FEE
  , dd.GOV
  , dd.SUBTOTAL
  , dd.service_fee
  , dd.small_order_fee
  , dd.LEGISLATIVE_FEE
  , dd.expand_range_fee
  , case when nv.store_id is not null then 1 else 0 end as nv
  , nv.store_id as nv_store_id
  , dd.ORDER_CART_ID
  , dd.DELIVERY_ID
  , dd.CREATOR_ID
  , dd.STORE_ID
  , dd.CREATED_AT
  , fda.Variable_Profit_Ex_Alloc as ue
  , fdd.straightline_r2c_distance/1609.34 as r2c
  , dd.fulfillment_type
  , dd.IS_BUNDLE_ORDER
  , dd.SUBSCRIPTION_ALLOC
  , dd.tip
  , dd.delivery_rating
  , dd.NET_REVENUE
  , dd.SUBTOTAL_ADJUSTED
  , dd.is_group_order
  , dd.BUSINESS_LINE
  , dd.SUBMARKET_ID
  , dd.SUBMARKET_NAME
  , dd.IS_SUBSCRIPTION_DISCOUNT_APPLIED::int as dashpass_eligible
  , case when nv.store_id is null then coalesce(affd.total_fee_promo_discount, 0) else 0 end as dv_promo_amount
  from edw.finance.dimension_deliveries dd
  left join fact_delivery_allocation fda on dd.delivery_id = fda.delivery_id
  left join edw.cng.dimension_new_vertical_store_tags as nv on dd.store_id = nv.store_id and nv.is_filtered_mp_vertical = 1
  left join delivery_fee_promo_discounts dfd on dd.order_cart_id = dfd.order_cart_id
  left join service_fee_promo_discounts sfd on dd.order_cart_id = sfd.order_cart_id
  left join public.fact_delivery_distances fdd on fdd.delivery_id = dd.delivery_id
  left join proddb.static.df_sf_promo_discount_delivery_level affd on dd.delivery_id = affd.delivery_id
where dd.is_filtered_core = True
 and dd.created_at >= date '2026-04-10'
 and dd.created_at <= date '2026-04-10' + 90
 and dd.country_id = 1
)
, cx_cnt as (
select
  be.dv_name,
  be.tag_renamed,
  count(distinct be.user_id) as total_cx
from be
group by 1, 2
)
, comb as(
select
  a.dv_name
, a.tag_renamed
, a.user_id as consumer_id
, cc.total_cx
, c.*
from be a
left join core_dd c on c.creator_id = a.user_id AND c.created_at between a.start_time_derived and a.end_time_derived
left join cx_cnt cc on cc.dv_name = a.dv_name and cc.tag_renamed = a.tag_renamed
)

, pen as(
  select
    dv_name
  , tag_renamed as "Bucket"
  , total_cx as "# Cx"
  , count(distinct delivery_id) as "Volume"
  , "# Cx" / sum("# Cx") over (partition by dv_name) as "Bucketing %"
  , "Volume"/nullif("# Cx",0) as "Order Rate"
  , count(distinct case when delivery_id is not null then consumer_id end) as "Converted Cx"
  , avg(actual_df_paid_by_cx) as "Actual Delivery Fee Paid by Cx"
  , avg(delivery_fee/100.0) as "Pre Promo DF"
  , avg(actual_sf_paid_by_cx) as "Service Fee"
  , avg(subtotal/100.0) as "Subtotal"
  , avg(gov/100.0) as aov
  , sum(gov/100.0) as GOV
  , sum(gov/100.0) / "# Cx" as gov_adj
  , avg(ue) as "Unit VP"
  , sum(ue) as vp
  , "Order Rate" * "Unit VP" as "Net VP Cx Level"
  , "Unit VP" * "Volume" / "# Cx" as vp_adj
  , sum(dv_promo_amount) as "DV Spending"
  from comb
  group by 1, 2, 3
  order by 1, 2
)
select
  dv_name
, "Bucket"
, "# Cx"
, "Volume"
, "Order Rate" / max(case when "Bucket" = 'Control' then "Order Rate" end) over (partition by dv_name) - 1 as "Order Rate Lift"
, "DV Spending"
, gov - max(case when "Bucket" = 'Control' then gov end) over (partition by dv_name) * "# Cx" /max(case when "Bucket" = 'Control' then "# Cx" end) over (partition by dv_name) as "GOV Delta"
, "Volume" - max(case when "Bucket" = 'Control' then "Volume" end) over (partition by dv_name) * "# Cx"/ max(case when "Bucket" = 'Control' then "# Cx" end) over (partition by dv_name) as "Volume Delta"
, vp - max(case when "Bucket" = 'Control' then vp end) over (partition by dv_name) * "# Cx"/ max(case when "Bucket" = 'Control' then "# Cx" end) over (partition by dv_name) as "VP Delta"
, "Net VP Cx Level" - max(case when "Bucket" = 'Control' then "Net VP Cx Level" end) over (partition by dv_name) as "Net VP Delta"
, "Order Rate" - max(case when "Bucket" = 'Control' then "Order Rate" end) over (partition by dv_name) as "Order Rate Delta"
, -"Net VP Delta" / nullif("Order Rate Delta", 0) as "(-CPID)/(+GPLD) In Campaign"
, "Unit VP"
, "Actual Delivery Fee Paid by Cx"
from pen
order by 1, 2
;
