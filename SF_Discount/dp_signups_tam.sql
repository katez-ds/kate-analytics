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
