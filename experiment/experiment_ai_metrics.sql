# Base metric classification — copy and customize for your team.
#
# core_primary_metrics: always reported in every callout
# metric_classification.primary: business-critical metrics
# metric_classification.guardrail: safety/quality metrics
# Everything not listed is classified as "secondary".

core_primary_metrics:
  - consumers_mau
  - order_rate_per_entity
  - order_rate_per_entity_7d

metric_classification:
  primary:
    - cng_order_rate_nc
    - consumer_order_frequency_l_28_d
    - consumers_mau
    - dashpass_signup
    - dsmp_gov
    - dsmp_order_frequency_7d
    - dsmp_order_rate
    - dsmp_order_rate_14d
    - dsmp_order_rate_7d
    - gov_per_order_curie
    - nv_mau
    - order_frequency_per_entity_7d
    - order_rate_per_entity
    - order_rate_per_entity_7d
    - variable_profit_per_order
    - webx_conversion_rate
    - webx_order_rate

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
