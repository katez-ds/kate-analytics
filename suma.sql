

SUMA Consumers:
f.cx_fraud_flag = 1
AND (
    COALESCE(f.suma_link_to_fraud, 0) = 1
    OR NOT (
        COALESCE(f.cnr_abuse, 0) = 1 OR COALESCE(f.chargeback_fraud, 0) = 1
        OR COALESCE(f.promo_abuse, 0) = 1 OR COALESCE(f.block_list, 0) = 1
    )
)

Non-SUMA Fraud Consumers (the exact complement):
f.cx_fraud_flag = 1
AND COALESCE(f.suma_link_to_fraud, 0) != 1
AND (
    COALESCE(f.cnr_abuse, 0) = 1 OR COALESCE(f.chargeback_fraud, 0) = 1
    OR COALESCE(f.promo_abuse, 0) = 1 OR COALESCE(f.block_list, 0) = 1
)

Plain-English logic:
- cx_fraud_flag = 1 is the fraud gate — both rows require it.
- If suma_link_to_fraud = 1 → SUMA.
- Else, if any of cnr_abuse / chargeback_fraud / promo_abuse / block_list = 1 → Non-SUMA Fraud (one of the other fraud types).
- Else (fraud-flagged but none of the specific sub-flags set) → defaults to SUMA (the catch-all quirk kept intentionally, per Kate 2026-08-21, to match the CA reference query stg_wbd_fraud_ca for direct comparability).

Point-in-time join condition on both: f.etl_create_date_utc::date <= order_date AND f.upstream_updated_date_utc::date > order_date — since the table is a slowly-changing-dimension history, not a per-day snapshot.
