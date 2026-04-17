WITH consumer_universe AS (
        SELECT DISTINCT consumer_id
        FROM edw.consumer.fact_consumer_subscription__daily
        WHERE dte BETWEEN DATEADD(day, -{buffer_window}, '{active_date}') AND '{active_date}'
    ),
    lifetime_counts AS (
        SELECT
            d.consumer_id,
            COUNT(DISTINCT IFF(
                d.is_in_paid_balance = TRUE
                OR d.dynamic_subscription_status ILIKE 'active_paid%',
                d.dte, NULL
            )) AS paid_days_lifetime,
            COUNT(DISTINCT IFF(
                d.is_partner_plan = TRUE
                AND (d.dynamic_subscription_status ILIKE 'active%'
                     OR d.dynamic_subscription_status ILIKE '%free_subscription%'),
                d.dte, NULL
            )) AS partner_days_lifetime,
            COUNT(DISTINCT IFF(
                d.is_in_trial_period = TRUE
                OR d.dynamic_subscription_status ILIKE 'trial%',
                d.dte, NULL
            )) AS trial_days_lifetime
        FROM edw.consumer.fact_consumer_subscription__daily d
        WHERE d.dte BETWEEN '{LOOKBACK_START_DATE}' AND '{active_date}'
        GROUP BY 1
    ),
    rolling_counts AS (
        SELECT
            d.consumer_id,
            COUNT(DISTINCT IFF(
                d.is_in_paid_balance = TRUE
                OR d.dynamic_subscription_status ILIKE 'active_paid%',
                d.dte, NULL
            )) AS p365d_paid_days,
            COUNT(DISTINCT IFF(
                d.is_partner_plan = TRUE
                AND (d.dynamic_subscription_status ILIKE 'active%'
                     OR d.dynamic_subscription_status ILIKE '%free_subscription%'),
                d.dte, NULL
            )) AS p365d_partner_days,
            COUNT(DISTINCT IFF(
                d.is_in_trial_period = TRUE
                OR d.dynamic_subscription_status ILIKE 'trial%',
                d.dte, NULL
            )) AS p365d_trial_days
        FROM edw.consumer.fact_consumer_subscription__daily d
        WHERE d.dte BETWEEN DATEADD(day, -{ROLLING_WINDOW_DAYS}, '{active_date}') AND '{active_date}'
        GROUP BY 1
    )
    SELECT
        cu.consumer_id,
        COALESCE(lc.paid_days_lifetime, 0) AS paid_days_lifetime,
        COALESCE(lc.partner_days_lifetime, 0) AS partner_days_lifetime,
        COALESCE(lc.trial_days_lifetime, 0) AS trial_days_lifetime,
        COALESCE(rc.p365d_paid_days, 0) AS p365d_paid_days,
        COALESCE(rc.p365d_partner_days, 0) AS p365d_partner_days,
        COALESCE(rc.p365d_trial_days, 0) AS p365d_trial_days
    FROM consumer_universe cu
    LEFT JOIN lifetime_counts lc ON cu.consumer_id = lc.consumer_id
    LEFT JOIN rolling_counts rc ON cu.consumer_id = rc.consumer_id
