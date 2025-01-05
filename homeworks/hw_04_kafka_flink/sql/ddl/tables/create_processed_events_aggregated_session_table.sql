DROP TABLE IF EXISTS processed_events_aggregated_session;
-- --------------------------------------------------------------------------------------
CREATE TABLE processed_events_aggregated_session (
    ip VARCHAR,
    host VARCHAR,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    num_hits BIGINT
);
