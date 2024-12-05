DROP TABLE IF EXISTS user_devices_cumulated;
-- --------------------------------------------------------------------------------------
CREATE TABLE user_devices_cumulated (
    user_id                   NUMERIC,
    device_activity_datelist  device_activity_datelist_type[],
    current_event_date        DATE,
    -- ------------------------------
    PRIMARY KEY (user_id, current_event_date)
);
