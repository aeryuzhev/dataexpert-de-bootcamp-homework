DROP TABLE IF EXISTS events_deduplicated;
-- --------------------------------------------------------------------------------------
CREATE TABLE events_deduplicated AS (
    SELECT
        url,
        referrer,
        user_id,
        device_id,
        host,
        event_time  
    FROM
        events
    GROUP BY
        url,
        referrer,
        user_id,
        device_id,
        host,
        event_time    
);
