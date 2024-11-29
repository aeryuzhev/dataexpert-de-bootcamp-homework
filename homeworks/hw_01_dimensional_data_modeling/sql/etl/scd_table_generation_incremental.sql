INSERT INTO actors_history_scd
    WITH
    variables AS (
        SELECT
            1979 AS last_year,
            1980 AS current_year
    ),
    -- ------------------------------
    last_year_scd_records AS (
        SELECT
            actor_id,
            actor,
            quality_class,
            is_active,
            start_year,
            end_year
        FROM 
            actors_history_scd
        WHERE
            current_year = (SELECT last_year FROM variables)
            AND end_year = (SELECT last_year FROM variables)
    ),
    -- ------------------------------
    historical_scd_records AS (
        SELECT
            actor_id,
            actor,
            quality_class,
            is_active,
            start_year,
            end_year
        FROM
            actors_history_scd
        WHERE
            current_year = (SELECT last_year FROM variables)
            AND end_year < (SELECT last_year FROM variables)
    ),
    -- ------------------------------
    current_year_records AS (
        SELECT
            actor_id,
            actor,
            quality_class,
            is_active,
            current_year
        FROM
            actors
        WHERE
            current_year = (SELECT current_year FROM variables)
    ),
    -- ------------------------------
    unchanged_records AS (
        SELECT
            cy.actor_id,
            cy.actor,
            cy.quality_class,
            cy.is_active,
            ly.start_year,
            cy.current_year AS end_year
        FROM
            current_year_records cy
            JOIN last_year_scd_records ly ON ly.actor_id = cy.actor_id
        WHERE
            cy.quality_class = ly.quality_class
            AND cy.is_active = ly.is_active
    ),
    -- ------------------------------
    new_records AS (
        SELECT
            cy.actor_id,
            cy.actor,
            cy.quality_class,
            cy.is_active,
            cy.current_year AS start_year,
            cy.current_year AS end_year
        FROM
            current_year_records cy
            LEFT JOIN last_year_scd_records ly ON ly.actor_id = cy.actor_id
        WHERE
            ly.actor_id IS NULL
    ),
    -- ------------------------------
    changed_records AS (
        SELECT
            cy.actor_id,
            cy.actor,
            UNNEST(ARRAY[
                ROW(
                    ly.quality_class,
                    ly.is_active,
                    ly.start_year,
                    ly.end_year                
                )::actor_scd_type,
                ROW(
                    cy.quality_class,
                    cy.is_active,
                    cy.current_year,
                    cy.current_year                
                )::actor_scd_type
            ]) AS records
        FROM
            current_year_records cy
            LEFT JOIN last_year_scd_records ly ON ly.actor_id = cy.actor_id
        WHERE
            (cy.quality_class <> ly.quality_class
            OR cy.is_active <> ly.is_active)
    ),
    unnested_changed_records AS (
        SELECT
            actor_id,
            actor,
            (records::actor_scd_type).quality_class,
            (records::actor_scd_type).is_active,
            (records::actor_scd_type).start_year,
            (records::actor_scd_type).end_year
        FROM
            changed_records
    )
    -- ------------------------------
    SELECT
        q.actor_id,
        q.actor,
        q.quality_class,
        q.is_active,
        (SELECT current_year FROM variables) AS current_year,
        q.start_year,
        q.end_year
    FROM (
        SELECT * FROM historical_scd_records
        UNION ALL
        SELECT * FROM unchanged_records 
        UNION ALL
        SELECT * FROM new_records
        UNION ALL
        SELECT * FROM unnested_changed_records
    ) q
-- ------------------------------
ON CONFLICT (actor_id, start_year, current_year)
DO UPDATE SET
    actor = EXCLUDED.actor,
    quality_class = EXCLUDED.quality_class,
    is_active = EXCLUDED.is_active,
    end_year = EXCLUDED.end_year;
