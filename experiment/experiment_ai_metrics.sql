# Base metric classification — copy and customize for your team.
#
# core_primary_metrics: always reported in every callout
# metric_classification.primary: business-critical metrics, report if reaches significance
# metric_classification.guardrail: safety/quality metrics, do not report
# Everything not listed is classified as "secondary", do not report

/*
Output Format Requirement:
1. Experiment name linked to Curie
2. Objective (Can leverage objective/context in Design Doc, e.g. https://docs.google.com/document/d/1FkW4EchBybjGIj7bbBqhksbjAwWH6kxIbA_2oMY35C0/edit?tab=t.0) 
skip status (only rport on In experiment), rollout, team notes
4. Multi Arm: We can keep this, but wondering if we can provide some context around what each arms is by reading the Design doc. i.e. Pulling bucket name and arm description.
3. Core Primary Metrics: can use the current format. 
  If easy to update:
  a. Show the user friendly alias (e.g. Order Rate instead of order_rate_per_entity)
  b. When a metric reaches significance, bold the % number
4. Other Metrics: focus on the other primary metircs below, but only show when reaches stats significant.
  a. For the DF/SF, show the absolute impact (delta) instead of relative lift %
  */
  
core_primary_metrics:
  - order_rate_per_entity -> Order Rate
  - gov_per_cx_curie -> GOV
  - consumers_mau -> MAU
  - variable_profit_per_order -> VP

metric_classification:
  primary:
    - dashpass_signup -> DP Signup
    - dashpass_paid_balance -> DP Paid Balance
    - cxp_net_delivery_fee_per_order -> Net DF Delta
    - cxp_net_service_fee_per_order -> Net SF Delta

  
# do not report
  guardrail: 
    - ads_promotion_promotion_cx_discount
    - ads_revenue
    - consumer_mto
    - core_quality_aotw
    - core_quality_asap
    - core_quality_botw
    - core_quality_cancellation
    - core_quality_late20
    - core_quality_otw
    - cx_app_quality_action_load_latency_android
    - cx_app_quality_action_load_latency_ios
    - cx_app_quality_action_load_latency_web
    - cx_app_quality_crash_android
    - cx_app_quality_crash_ios
    - cx_app_quality_crash_web
    - cx_app_quality_hitch_android
    - cx_app_quality_hitch_ios
    - cx_app_quality_inp_web
    - cx_app_quality_page_action_error_android
    - cx_app_quality_page_action_error_ios
    - cx_app_quality_page_action_error_web
    - cx_app_quality_page_load_error_android
    - cx_app_quality_page_load_error_ios
    - cx_app_quality_page_load_error_web
    - cx_app_quality_page_load_latency_android
    - cx_app_quality_page_load_latency_ios
    - cx_app_quality_page_load_latency_web
    - cx_app_quality_single_metric_ios
    - cx_app_quality_tbt_web
    - ox_subtotal_combined
