DROP TABLE IF EXISTS actors_history_scd;
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS actors_history_scd (
    actor_id       TEXT,
    actor          TEXT,
    quality_class  quality_class_type,
    is_active      BOOLEAN,
    current_year   INTEGER,
    start_year     INTEGER,
    end_year       INTEGER,
    -- ------------------------------
    PRIMARY KEY (actor_id, start_year, current_year)
);

