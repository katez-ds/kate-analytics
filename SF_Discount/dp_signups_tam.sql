-- Average daily first-ever DashPass signups of any kind in the US over the last 30 complete days
SELECT
  AVG(daily_first_ever_signups) AS avg_daily_first_ever_dashpass_signups_us_last_30d
FROM
  (
    SELECT
      active_date,
      COUNT(DISTINCT consumer_id) AS daily_first_ever_signups
    FROM
      (
        SELECT
          consumer_id,
          active_date,
          country,
          subscription_event_time,
          subscription_id
        FROM
          edw.growth.fact_consumer_dashpass_signups
        QUALIFY
          ROW_NUMBER() OVER (
            PARTITION BY
              consumer_id
            ORDER BY
              subscription_event_time,
              subscription_id
          ) = 1
      ) first_signup
    WHERE
      active_date >= DATEADD(day, -30, CURRENT_DATE())
      AND active_date < CURRENT_DATE()
      AND country = 'United States'
    GROUP BY
      active_date
  ) d;

52236 * 4.7% = 2455

-- Current US DashPass subscribers who are either in trial
-- or within their first 3 paid months
SELECT
  COUNT(DISTINCT dsa.consumer_id) AS dashpass_subscribers_in_trial_or_first_3_paid_months_us
FROM
  edw.consumer.fact_consumer_subscription__daily dsa
  INNER JOIN edw.consumer.dimension_consumer_subscription_plan sp ON dsa.consumer_subscription_plan_id = sp.consumer_subscription_plan_id
WHERE
  dsa.dte = (
    SELECT
      MAX(dte)
    FROM
      edw.consumer.fact_consumer_subscription__daily
    WHERE
      dte <= CURRENT_DATE()
  )
  AND dsa.country_id_subscribed_from = 1
  AND sp.plan_type = 'DASHPASS'
  AND COALESCE(dsa.subscription_status, '') <> 'cancelled_subscription_creation_failed'
  AND dsa.consumer_subscription_plan_id <> 10002416
  AND (
    (
      dsa.dynamic_subscription_status = 'active_trial'
      AND dsa.is_in_trial_balance = TRUE
    )
    OR (
      dsa.dynamic_subscription_status = 'active_paid'
      AND dsa.is_in_paid_balance = TRUE
      AND dsa.billing_period IS NOT NULL
      AND dsa.first_subscription_successful_charge_date IS NOT NULL
      AND dsa.dte < DATEADD(
        month,
        3,
        dsa.first_subscription_successful_charge_date
      )
    )
  );
