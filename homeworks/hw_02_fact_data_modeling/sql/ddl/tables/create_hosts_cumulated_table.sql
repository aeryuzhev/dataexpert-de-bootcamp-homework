DROP TABLE IF EXISTS hosts_cumulated;
-- --------------------------------------------------------------------------------------
CREATE TABLE hosts_cumulated (
    host                    TEXT,
    host_activity_datelist  DATE[],
    current_host_date       DATE,
    -- ------------------------------
    PRIMARY KEY (host, current_host_date)
);
