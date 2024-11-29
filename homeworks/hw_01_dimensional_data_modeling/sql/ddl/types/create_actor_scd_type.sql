CREATE TYPE actor_scd_type AS (
    quality_class  quality_class_type,
    is_active      BOOLEAN,
    start_year     INTEGER,
    end_year       INTEGER
);
