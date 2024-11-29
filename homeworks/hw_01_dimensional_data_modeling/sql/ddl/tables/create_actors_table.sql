DROP TABLE IF EXISTS actors;
-- -------------------------------------------------------------------------------------
CREATE TABLE actors (
    actor_id       TEXT,
    actor          TEXT,
    films          film_details_type[],
    quality_class  quality_class_type,
    current_year   INTEGER,
    is_active      BOOLEAN,
    -- ------------------------------
    PRIMARY KEY (actor_id, current_year)
);
