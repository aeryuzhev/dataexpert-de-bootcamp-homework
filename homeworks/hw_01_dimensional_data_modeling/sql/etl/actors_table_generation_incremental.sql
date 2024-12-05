INSERT INTO actors
    WITH
    variables AS (
        SELECT
            1970 AS current_year
    ),
    -- ------------------------------
    last_year AS (
        SELECT
            actor_id,	
            actor,
            films,
            quality_class,
            current_year,
            is_active 		
        FROM
            actors
        WHERE
            current_year = (SELECT current_year - 1 FROM variables)
    ),
    -- ------------------------------
    current_year AS (
        SELECT 
            actorid AS actor_id,
            actor AS actor,
            year AS current_year,
            TRUE AS is_active,
            ARRAY_AGG(
                ROW(
                    film,
                    votes,
                    rating,
                    filmid
                )::film_details_type
             ) AS film_details,
            (CASE 
                WHEN AVG(rating) > 8 THEN 'star'
                WHEN AVG(rating) > 7 THEN 'good'
                WHEN AVG(rating) > 6 THEN 'average'
                ELSE 'bad'
            END)::quality_class_type AS quality_class
        FROM 
            actor_films af
        WHERE 
            year = (SELECT current_year FROM variables)
        GROUP BY
            actor_id,
            actor,
            current_year,
            is_active
    )
    -- ------------------------------
    SELECT
        COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
        COALESCE(ly.actor, cy.actor) AS actor,
        (
            COALESCE(ly.films, array[]::film_details_type[]) || 
            COALESCE(cy.film_details, ARRAY[]::film_details_type[])
        ) AS films,
        COALESCE(cy.quality_class, ly.quality_class) AS quality_class,
        (SELECT current_year FROM variables) AS current_year,
        COALESCE(cy.is_active, FALSE) AS is_active
    FROM
        last_year ly
        FULL JOIN current_year cy ON cy.actor_id = ly.actor_id
-- ------------------------------
ON CONFLICT(actor_id, current_year)
DO UPDATE SET
    actor = EXCLUDED.actor,
    films = EXCLUDED.films,
    quality_class = EXCLUDED.quality_class,
    is_active = EXCLUDED.is_active;
