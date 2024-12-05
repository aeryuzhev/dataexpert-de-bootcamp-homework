DO
$$
DECLARE
    start_date DATE := '2023-01-01'; 
    end_date DATE := '2023-01-31'; 
BEGIN
    WHILE start_date <= end_date LOOP
        INSERT INTO hosts_cumulated
            WITH
            last_date_records AS (
                SELECT
                    host,
                    host_activity_datelist,
                    current_host_date
                FROM
                    hosts_cumulated
                WHERE
                    current_host_date = start_date - INTERVAL '1 day'
            ),
            -- ------------------------------
            current_date_records AS (
                SELECT
                    host,
                    ARRAY_AGG(DISTINCT DATE(event_time)) AS host_activity_datelist
                FROM
                    events_deduplicated
                WHERE
                    DATE(event_time) = start_date
                    AND host IS NOT NULL
                GROUP BY
                    host
            )
            -- ------------------------------
            SELECT
                COALESCE(cdr.host, ldr.host) AS host,
                (
                    COALESCE(ldr.host_activity_datelist, ARRAY[]::DATE[]) || 
                    COALESCE(cdr.host_activity_datelist, ARRAY[]::DATE[])
                ) AS host_activity_datelist,
                start_date AS current_host_date
            FROM
                last_date_records ldr
                FULL JOIN current_date_records cdr ON cdr.host = ldr.host
        -- ------------------------------
        ON CONFLICT (host, current_host_date)
        DO UPDATE SET
            host_activity_datelist = EXCLUDED.host_activity_datelist;
        -- ------------------------------
        start_date := start_date + INTERVAL '1 day';
    END LOOP;
END;
$$
