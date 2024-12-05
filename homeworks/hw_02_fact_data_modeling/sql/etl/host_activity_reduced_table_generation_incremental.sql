INSERT INTO host_activity_reduced
    WITH
    variables AS (
        SELECT
            DATE('2023-01-31') AS current_event_date
    ),
    -- ------------------------------
    transformed_variables AS (
        SELECT
            EXTRACT('days' FROM current_event_date)::INTEGER AS current_month_day_number,
            DATE(date_trunc('month', current_event_date)) AS month_first_date
        FROM
            variables
    ),
    -- ------------------------------
    last_date_records AS (
        SELECT
            host,
            hit_array,
            unique_visitors    
        FROM
            host_activity_reduced
        WHERE
            month_start = (SELECT month_first_date FROM transformed_variables)
    ),
    -- ------------------------------
    current_date_records AS (
        SELECT
            host,
            COUNT(1) AS num_site_hits,
            COUNT(DISTINCT user_id) AS num_unique_visitors
        FROM
            events_deduplicated
        WHERE
            DATE(event_time) = (SELECT current_event_date FROM variables)
            AND host IS NOT NULL
        GROUP BY
            host
    )
    -- ------------------------------
    SELECT
        COALESCE(cdr.host, ldr.host) AS host,
        (SELECT month_first_date FROM transformed_variables) AS month_start,
        COALESCE(
            ldr.hit_array, array_fill(0, (SELECT ARRAY[(current_month_day_number - 1)] FROM transformed_variables))
        ) || ARRAY[COALESCE(cdr.num_site_hits, 0)] AS hit_array,
        COALESCE(
            ldr.hit_array, array_fill(0, (SELECT ARRAY[(current_month_day_number - 1)] FROM transformed_variables))
        ) || ARRAY[COALESCE(cdr.num_unique_visitors, 0)] AS unique_visitors        
    FROM
        current_date_records cdr
        FULL JOIN last_date_records ldr ON ldr.host = cdr.host
-- ------------------------------
ON CONFLICT (host, month_start)
DO UPDATE SET
    hit_array = EXCLUDED.hit_array,
    unique_visitors = EXCLUDED.unique_visitors;
