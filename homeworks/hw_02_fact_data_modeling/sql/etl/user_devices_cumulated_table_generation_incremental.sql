INSERT INTO user_devices_cumulated
    WITH
    variables AS (
        SELECT
            DATE('2023-01-31') AS current_event_date
    ),
    -- ------------------------------
    last_date_records AS (
        SELECT
            user_id,
            device_activity_datelist,
            current_event_date
        FROM
            user_devices_cumulated
        WHERE
            current_event_date = (SELECT current_event_date - INTERVAL '1 day' FROM variables)
    ),
    -- ------------------------------
    current_date_records AS (
        SELECT
            e.user_id,
            ARRAY_AGG(DISTINCT
                ROW(
                    d.browser_type,
                    ARRAY[DATE(e.event_time)]
                )::device_activity_datelist_type
            ) AS device_activity_datelist
        FROM
            devices_deduplicated d
            JOIN events_deduplicated e ON e.device_id = d.device_id
        WHERE
            DATE(e.event_time) = (SELECT current_event_date FROM variables)
            AND e.user_id IS NOT NULL
        GROUP BY
            e.user_id
    )
    -- ------------------------------
    SELECT
        COALESCE(cdr.user_id, ldr.user_id) AS user_id,
        CASE 
            WHEN ldr.device_activity_datelist IS NULL 
                THEN cdr.device_activity_datelist
            WHEN cdr.device_activity_datelist IS NULL
                THEN ldr.device_activity_datelist
            WHEN cdr.device_activity_datelist IS NOT NULL 
                THEN 
                    (
                        SELECT
                            ARRAY_AGG(
                                ROW(
                                    COALESCE(cad.browser_type, lad.browser_type),
                                    COALESCE(lad.date_list, ARRAY[]::DATE[]) || COALESCE(cad.date_list, ARRAY[]::DATE[])                                 
                                )::device_activity_datelist_type
                            )
                        FROM
                            UNNEST(ldr.device_activity_datelist) lad
                            FULL JOIN UNNEST(cdr.device_activity_datelist) cad ON cad.browser_type = lad.browser_type
                    ) 
        END AS device_activity_datelist,
        (SELECT current_event_date FROM variables) AS current_event_date
    FROM
        last_date_records ldr
        FULL JOIN current_date_records cdr ON cdr.user_id = ldr.user_id
-- ------------------------------
ON CONFLICT (user_id, current_event_date)
DO UPDATE SET
    device_activity_datelist = EXCLUDED.device_activity_datelist;
