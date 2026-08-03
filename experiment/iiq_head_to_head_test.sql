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

-- ORIGAMI IIQ head-to-head readout - sheet "IIQ H2H ORIGAMI Test Sizing"
-- (Inactive / resurrection_rev_v2 family): DEWO, CEWO, Low frequency resurrection,
-- Dealseeker. organiccore_nonmon (non-monetary) commented out below -- no matching
-- row in the sheet currently (removed since sheet restructure).
--
-- Adapted from experiment/old_iiq_head_to_head_test.sql per our discussion:
--   1) CRM population source: edw.consumer.campaign_analyzer_exposures does NOT have
--      these DVs (checked 2026-07-22, zero rows) -> sourced from
--      proddb.public.fact_dedup_experiment_exposure.experiment_name instead.
--      Population = each DV's own non-control tag(s) (tag NOT ILIKE '%control%'),
--      i.e. "got into the journey, excluding that DV's own holdout."
--   2) Incentive-arm tag source swapped: static.wbd_experiment_exposure ->
--      proddb.static.us_universal_dv_a_be (same shape: tag_renamed/user_id/first_exposed).
--   3) Launch date + window_days: `dl.launch_date`/`dl.window_days` in the VALUES list
--      define (a) the ENROLLMENT filter -- which users qualify, i.e. those whose own
--      exposure_time falls in [launch_date, launch_date + window_days] -- and (b) the
--      max window length used per DV. They do NOT directly become start_time_derived/
--      end_time_derived for every user (see fix in item 8 below).
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
-- Dealseeker added (multi-arm: control, treatment, treatment2, treatment3 -- same
-- launch date 2026-04-10, collapsed via the tag NOT ILIKE '%control%' rule above,
-- same as lowfrequencyresurrection's treatment_siw).
--
-- ep_consumer_new_dormant_prevention_us_v1 added (2026-07-23). This DV has 3 tags
-- (control, treatmentA, treatmentC) -- per instruction, ONLY treatmentC counts as
-- Treatment; treatmentA is excluded entirely (not collapsed in), since treatmentA
-- stopped getting new exposures 2026-03-04 (retired arm) while control/treatmentC ran
-- through 2026-05-20. Handled via an explicit per-DV tag filter below rather than the
-- generic "NOT ILIKE '%control%'" collapse rule. Also uses segment = 'All Users' (not
-- 'Users' like the other DVs -- confirmed 'Users' matches ZERO rows for this DV).
-- Launch date UPDATED 2026-07-23: 2026-01-28 -> 2026-03-12 (60D window, was 90D).
-- 2026-03-12 is exactly when proddb.static.us_universal_dv_a_be's own data starts, so
-- this resolves the earlier coverage-gap issue (the original 1/28 launch predated the
-- universal DV table's data by ~6 weeks, silently dropping early enrollees via the
-- inner join and producing a suspicious negative-CPID read).
--
-- Discount CTEs / core_dd date bounds are DYNAMIC (min/max pulled from the cohorts
-- table itself) instead of a single hardcoded literal, since DVs span different
-- launch dates and window lengths.
--
-- ep_consumer_active_post_resurrection_us MERGED IN (2026-07-23): 30D window instead
-- of 90D (too recent for a mature 90D read). Window length is now per-DV via a
-- `window_days` column in the VALUES list (was a hardcoded "+90" everywhere) -- every
-- downstream date bound (discount CTEs, crm_discounts, core_dd) already derives from
-- MIN(start_time_derived)/MAX(end_time_derived) across the whole cohorts table, so
-- adding a shorter-window DV just narrows that DV's own end_time_derived without
-- affecting the others (the 90D DVs still set the MAX bound).
--   8) CRITICAL FIX (2026-07-24): start_time_derived/end_time_derived were WRONG --
--      they were set to the SAME shared launch_date/launch_date+window_days for
--      EVERY user in a DV, instead of each user's OWN exposure date. In
--      old_iiq_head_to_head_test.sql, start_time_derived/end_time_derived come
--      directly from edw.consumer.campaign_analyzer_exposures as real PER-USER
--      fields; the "BETWEEN launch AND launch+90" clause there is only an
--      enrollment filter, not the definition of the window itself. Our shared-window
--      version caused the `comb` join (`c.created_at between a.start_time_derived and
--      a.end_time_derived`) to attribute orders to users from BEFORE they were even
--      exposed (e.g. a user first bucketed on day 89 still had start_time_derived =
--      day 0), massively over-counting Volume/GOV/Spend. Fixed: start_time_derived =
--      MIN(exposure_time::date) per user (their own first exposure), end_time_derived
--      = that + window_days -- genuinely per-user now, enrollment filter unchanged.
--
--   9) REWRITE (2026-07-24): now pulls population + real per-user dates DIRECTLY from
--      edw.consumer.campaign_analyzer_exposures, mirroring old_iiq_head_to_head_test.sql's
--      CREATE TABLE exactly -- no more fact_dedup_experiment_exposure join for this step.
--      This table WAS confirmed to have these DVs (item 1 above was wrong) -- it's just
--      keyed on a different `campaign_name` value than the DV/experiment name, found via
--      population-overlap analysis (2026-07-24), not name-guessing:
--        dewo -> 'dewo', cewo -> 'cewo', low-freq -> 'low_frequency_resurrection',
--        dealseeker -> 'deal_seekers_resurrection',
--        dormant prevention -> 'ep_consumer_new_dormant_prevention_us_q325_tC_split_4',
--        post resurrection -> 'ep_consumer_active_post_resurrection_us_t1'/'_t2'/'_t3'
--      is_control_flg = 0 replaces all the tag/segment special-casing (treatmentC-only,
--      segment='All Users', etc.) -- this table's own control flag is a clean binary,
--      no visibility into fact_dedup's sub-arm tags needed.
--      launch_date/window_days are KEPT (per instruction) as (a) the ENROLLMENT filter
--      -- only include users whose REAL start_time_derived falls in [launch_date,
--      launch_date + window_days] -- and (b) an UPPER BOUND on end_time_derived, not a
--      replacement for it: end_time_derived = LEAST(real end_time_derived from this
--      table, start_time_derived + window_days) -- i.e. if the real measurement period
--      is shorter than your window_days, use the real (shorter) one; if it would run
--      longer, cap it at window_days. start_time_derived is always each user's own real
--      date (never a shared literal) -- this is what avoids re-introducing the item-8
--      shared-window bug.
--
-- NOT included yet: Dormant Prevention... actually now included (see above); FMX not
-- yet added (no launch date confirmed for fmx_core_challenges_test_q225).

--  10) MULTI-WINDOW (2026-07-25): switched from "first window only" (GROUP BY +
--      MIN()) to "every distinct, non-overlapping eligibility window is its own
--      observation" (per instruction -- "option 2"). CEWO (~40% of pop) and
--      Dealseeker (~19%) have consumers with genuine multiple re-entry windows within
--      the analysis period (confirmed via gap analysis 2026-07-24); the other 4 DVs
--      have exactly one window per consumer, so this is a no-op for them. `SELECT
--      DISTINCT` (not GROUP BY) preserves each distinct (start, end) pair per
--      consumer instead of collapsing to MIN(). Everything downstream (comb/cx_cnt/
--      pen) already generalizes correctly to multiple rows per consumer with no
--      further changes: cx_cnt's COUNT(DISTINCT user_id) dedupes population count
--      regardless of row count, and comb's per-row date-range join can't double-count
--      a single order across two windows since they're non-overlapping by construction.
CREATE OR REPLACE TABLE katez.exposed_cx_crm_origami_cohorts AS
select
  dv_name,
  consumer_id,
  start_time_derived,
  -- window_days is an UPPER BOUND, not a replacement: use the real end_time_derived
  -- if it's shorter, otherwise cap at start_time_derived + window_days.
  least(real_end_time_derived, start_time_derived + window_days) as end_time_derived
from (
  select distinct
    dl.dv_name,
    e.consumer_id,
    e.start_time_derived::date as start_time_derived,
    e.end_time_derived::date as real_end_time_derived,
    dl.window_days
  from edw.consumer.campaign_analyzer_exposures e
  join (
    select * from (values
      ('ep_consumer_usmp_resurrection_rev_v2_dewo',                'dewo',                                                  date '2026-04-10', 90),
      ('ep_consumer_usmp_resurrection_rev_v2_cewo',                 'cewo',                                                  date '2026-04-10', 90),
      ('ep_consumer_usmp_resurrection_rev_v2_lowfrequencyresurrection', 'low_frequency_resurrection',                        date '2026-04-10', 90),
      ('ep_consumer_usmp_resurrection_rev_v2_dealseekers',          'deal_seekers_resurrection',                             date '2026-04-10', 90),
      ('ep_consumer_new_dormant_prevention_us_v1',                  'ep_consumer_new_dormant_prevention_us_q325_tC_split_4', date '2026-03-12', 60),
      ('ep_consumer_active_post_resurrection_us',                   'ep_consumer_active_post_resurrection_us_t1',           date '2026-05-30', 30),
      ('ep_consumer_active_post_resurrection_us',                   'ep_consumer_active_post_resurrection_us_t2',           date '2026-05-30', 30),
      ('ep_consumer_active_post_resurrection_us',                   'ep_consumer_active_post_resurrection_us_t3',           date '2026-05-30', 30)
    ) as t(dv_name, analyzer_campaign_name, launch_date, window_days)
  ) dl on dl.analyzer_campaign_name = e.campaign_name
  where e.is_control_flg = 0
    and e.campaign_country = 'US'
    and e.start_time_derived::date between dl.launch_date and dl.launch_date + dl.window_days
)
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
    and created_at::date >= (select min(start_time_derived) from katez.exposed_cx_crm_origami_cohorts) - 7
    and ("GROUP" != 'subscription')
  group by 1
)
, delivery_fee_promo_discounts as(
  select
    order_cart_id,
    sum(amount/100) as df_discount_amount
  from public.maindblocal_order_cart_discount_component
  where monetary_field = 'delivery_fee'
    and created_at::date >= (select min(start_time_derived) from katez.exposed_cx_crm_origami_cohorts) - 7
    and ("GROUP" != 'subscription')
  group by 1
)
, crm_discounts as (
  -- Per mcdonald_promo_spend_2025.sql pattern: CRM discount $ is NOT in
  -- df_sf_promo_discount_delivery_level -- it's computed from
  -- fact_order_discounts_and_promotions_extended's FDA_* fields when campaign_id
  -- is set. Verified all referenced columns exist (checked 2026-07-23).
  -- campaign_business_campaign carried through so downstream can filter to each
  -- DV's OWN campaign specifically (checked 2026-07-24: matched against real
  -- campaign names in this table -- see dv_campaign_map below).
  select
    delivery_id,
    campaign_business_campaign,
    case when campaign_id is not null
      then coalesce(FDA_OTHER_PROMOTIONS_BASE + FDA_PROMOTION_CATCH_ALL + FDA_CONSUMER_RETENTION - FDA_BUNDLES_PRICING_DISCOUNT, 0)
      else 0 end as crm_discount_amount
  from proddb.public.fact_order_discounts_and_promotions_extended
  where active_date between (select min(start_time_derived) from katez.exposed_cx_crm_origami_cohorts)
                         and (select max(end_time_derived) from katez.exposed_cx_crm_origami_cohorts)
  group by 1, 2, 3
)
, dv_campaign_map as (
  -- Maps each DV to its own CRM campaign name in fact_order_discounts_and_promotions_extended.
  -- DEWO/CEWO/Low-Freq/Dormant Prevention are exact matches (confirmed 2026-07-24);
  -- Dealseeker/Post resurrection are best-guess (sheet gives no/different old-program
  -- name for these two) -- flag if wrong.
  select * from (values
    ('ep_consumer_usmp_resurrection_rev_v2_dewo',                'DEWO Automation'),
    ('ep_consumer_usmp_resurrection_rev_v2_cewo',                 'Resurrection Automation - US'),
    ('ep_consumer_usmp_resurrection_rev_v2_lowfrequencyresurrection', 'Low Frequency Resurrection'),
    ('ep_consumer_new_dormant_prevention_us_v1',                  'Dormant Prevention'),
    ('ep_consumer_usmp_resurrection_rev_v2_dealseekers',          'Deal Seeker Resurrection'),
    ('ep_consumer_active_post_resurrection_us',                   'Post Resurrection Challenges')
  ) as t(dv_name, campaign_business_campaign)
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
  , coalesce(crmd.crm_discount_amount, 0) as crm_discount_amount
  , crmd.campaign_business_campaign
  from edw.finance.dimension_deliveries dd
  left join fact_delivery_allocation fda on dd.delivery_id = fda.delivery_id
  left join edw.cng.dimension_new_vertical_store_tags as nv on dd.store_id = nv.store_id and nv.is_filtered_mp_vertical = 1
  left join delivery_fee_promo_discounts dfd on dd.order_cart_id = dfd.order_cart_id
  left join service_fee_promo_discounts sfd on dd.order_cart_id = sfd.order_cart_id
  left join public.fact_delivery_distances fdd on fdd.delivery_id = dd.delivery_id
  left join proddb.static.df_sf_promo_discount_delivery_level affd on dd.delivery_id = affd.delivery_id
  left join crm_discounts crmd on dd.delivery_id = crmd.delivery_id
where dd.is_filtered_core = True
 and dd.created_at >= (select min(start_time_derived) from katez.exposed_cx_crm_origami_cohorts)
 and dd.created_at <= (select max(end_time_derived) from katez.exposed_cx_crm_origami_cohorts)
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
--  11) FIRST 28D MAU (2026-08-03): whether a user placed >=1 order in the first 28 days
--      after their own eligibility start (start_time_derived) -- [start, start+28].
--      Every DV's window_days (30/60/90D) is >=28, so this always stays inside the
--      existing in-campaign window/core_dd bound -- no separate date extension needed.
, first28_activity as (
  select distinct
    a.dv_name,
    a.tag_renamed,
    a.user_id
  from be a
  join core_dd c on c.creator_id = a.user_id
    and c.created_at between a.start_time_derived and a.start_time_derived + 28
)
, first28_cnt as (
  select
    dv_name,
    tag_renamed,
    count(distinct user_id) as first28_active_cx
  from first28_activity
  group by 1, 2
)
, comb as(
select
  a.dv_name
, a.tag_renamed
, a.user_id as consumer_id
, cc.total_cx
, fc.first28_active_cx
, c.*
, case when c.campaign_business_campaign = dcm.campaign_business_campaign then c.crm_discount_amount else 0 end as own_campaign_crm_discount_amount
from be a
left join core_dd c on c.creator_id = a.user_id AND c.created_at between a.start_time_derived and a.end_time_derived
left join cx_cnt cc on cc.dv_name = a.dv_name and cc.tag_renamed = a.tag_renamed
left join first28_cnt fc on fc.dv_name = a.dv_name and fc.tag_renamed = a.tag_renamed
left join dv_campaign_map dcm on dcm.dv_name = a.dv_name
)

, pen as(
  select
    dv_name
  , tag_renamed as "Bucket"
  , total_cx as "# Cx"
  , max(first28_active_cx) / nullif(total_cx, 0) as "First 28D MAU"
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
  , sum(crm_discount_amount) as "CRM Spend (Any Campaign)"
  , sum(own_campaign_crm_discount_amount) as "CRM Spend (Own Campaign)"
  , count(distinct case when dv_promo_amount > 0 then delivery_id end) as "Discount Orders"
  , "Discount Orders" / nullif("Volume", 0) as "Order Discount Overlap %"
  , count(distinct case when dv_promo_amount > 0 and crm_discount_amount > 0 then delivery_id end) as "CRM+Affordability Overlap Orders"
  , sum(case when crm_discount_amount > 0 then dv_promo_amount else 0 end) as "Affordability Spend on CRM-Overlap Orders"
  from comb
  group by 1, 2, 3
  order by 1, 2
)
select
  dv_name
, "Bucket"
, "# Cx"
, "Volume"
, "First 28D MAU"
, "First 28D MAU" - max(case when "Bucket" = 'Control' then "First 28D MAU" end) over (partition by dv_name) as "First 28D MAU Delta"
, "Order Rate" / max(case when "Bucket" = 'Control' then "Order Rate" end) over (partition by dv_name) - 1 as "Order Rate Lift"
, "DV Spending"
, "CRM Spend (Any Campaign)"
, "CRM Spend (Own Campaign)"
, gov - max(case when "Bucket" = 'Control' then gov end) over (partition by dv_name) * "# Cx" /max(case when "Bucket" = 'Control' then "# Cx" end) over (partition by dv_name) as "GOV Delta"
, "Volume" - max(case when "Bucket" = 'Control' then "Volume" end) over (partition by dv_name) * "# Cx"/ max(case when "Bucket" = 'Control' then "# Cx" end) over (partition by dv_name) as "Volume Delta"
, vp - max(case when "Bucket" = 'Control' then vp end) over (partition by dv_name) * "# Cx"/ max(case when "Bucket" = 'Control' then "# Cx" end) over (partition by dv_name) as "VP Delta"
, "Net VP Cx Level" - max(case when "Bucket" = 'Control' then "Net VP Cx Level" end) over (partition by dv_name) as "Net VP Delta"
, "Order Rate" - max(case when "Bucket" = 'Control' then "Order Rate" end) over (partition by dv_name) as "Order Rate Delta"
, -"Net VP Delta" / nullif("Order Rate Delta", 0) as "(-CPID)/(+GPLD) In Campaign"
, "Unit VP"
, "Actual Delivery Fee Paid by Cx"
, "Discount Orders"
, "Order Discount Overlap %"
, "CRM+Affordability Overlap Orders"
, "Affordability Spend on CRM-Overlap Orders"
from pen
order by 1, 2
;

