WITH
unnested_activity AS (
    SELECT
        user_id,
        (UNNEST(device_activity_datelist)::device_activity_datelist_type).browser_type,
        (UNNEST(device_activity_datelist)::device_activity_datelist_type).date_list,
        device_activity_datelist
    FROM
        user_devices_cumulated udc
    WHERE
        udc.current_event_date = '2023-01-31'
),
-- ------------------------------
generated_days AS (
    SELECT
        generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS valid_date
),
-- ------------------------------
active_days AS (
    SELECT
        ua.user_id,
        ua.browser_type,
        ua.date_list @> ARRAY[DATE(gd.valid_date)] AS is_active,
        EXTRACT(DAY FROM DATE('2023-01-31') - gd.valid_date) AS days_since,
        ua.device_activity_datelist
    FROM
        unnested_activity ua
        CROSS JOIN generated_days gd
)
-- ------------------------------
SELECT
    user_id,
    browser_type,
    SUM(
        CASE 
            WHEN is_active THEN POW(2, 32 - days_since)  
            ELSE 0 
        END
    )::BIGINT::BIT(32) AS datelist_int,
    DATE('2023-01-31') AS current_event_date
FROM
    active_days
GROUP BY
    user_id,
    browser_type
ORDER BY
    user_id,
    browser_type;
